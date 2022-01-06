#include "global.h"
#include "row.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "all_indexes.h"
#include "tpcc_helper.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"

RC tpcc_wl::init() {
    workload::init();
    string path = "./benchmarks/";
#if TPCC_SMALL
    path += "TPCC_short_schema.txt";
#else
    path += "TPCC_full_schema.txt";
#endif
    cout<<"reading schema file: "<<path<<std::endl;
    init_schema(path.c_str());
    cout<<"TPCC schema initialized"<<std::endl;
    next_tid = 0;
    init_table();
    next_tid = 0;
    return RCOK;
}

void tpcc_wl::setbench_deinit() {
    workload::setbench_deinit();
    for (auto name_tableptr_pair : tables) {
        auto tableptr = name_tableptr_pair.second;
        tableptr->setbench_deinit();
        free(tableptr);
    }
    if (tpcc_buffer) {
        for (int i=0;i<g_thread_cnt;++i) {
            if (tpcc_buffer[i]) {
                free(tpcc_buffer[i]);
                tpcc_buffer[i] = NULL;
            }
        }
        delete[] tpcc_buffer;
        tpcc_buffer = NULL;
    }
}

RC tpcc_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_warehouse = tables["WAREHOUSE"];
    t_district = tables["DISTRICT"];
    t_customer = tables["CUSTOMER"];
    t_history = tables["HISTORY"];
    t_neworder = tables["NEW-ORDER"];
    t_order = tables["ORDER"];
    t_orderline = tables["ORDER-LINE"];
    t_item = tables["ITEM"];
    t_stock = tables["STOCK"];

    i_neworder = indexes["NEWORDER_IDX"];
    i_order = indexes["ORDER_IDX"];
    i_orderline = indexes["ORDERLINE_IDX"];
    i_orderline_wd = indexes["ORDERLINE_WD_IDX"];
    i_item = indexes["ITEM_IDX"];
    i_warehouse = indexes["WAREHOUSE_IDX"];
    i_district = indexes["DISTRICT_IDX"];
    i_customer_id = indexes["CUSTOMER_ID_IDX"];
    i_customer_last = indexes["CUSTOMER_LAST_IDX"];
    i_stock = indexes["STOCK_IDX"];
    return RCOK;
}

RC tpcc_wl::init_table() {
    num_wh = g_num_wh;

    /******** fill in data ************/
    // data filling process:
    //- item
    //- wh
    //	- stock
    // 	- dist
    //  	- cust
    //	  	- hist
    //		- order 
    //		- new order
    //		- order line
    /**********************************/
//    RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
    tpcc_buffer = new drand48_data * [g_num_wh];
    pthread_t * p_thds = new pthread_t[g_num_wh /*- 1*/];
    for (uint32_t i = 0; i<g_num_wh /*- 1*/; i++)
        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    /*threadInitWarehouse(this);*/
    for (uint32_t i = 0; i<g_num_wh /*- 1*/; i++)
        pthread_join(p_thds[i], NULL);
//    RLU_FINISH();
    delete[] p_thds;

    printf("TPCC Data Initialization Complete!\n");
    return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
    txn_manager = (tpcc_txn_man *) _mm_malloc(sizeof (tpcc_txn_man), ALIGNMENT);
    new(txn_manager) tpcc_txn_man();
    txn_manager->init(h_thd, this, h_thd->get_thd_id());
    return RCOK;
}

// TODO ITEM table is assumed to be in partition 0

void tpcc_wl::init_tab_item() {
#ifdef VERBOSE_1
    cout<<"init_tab_item "<<std::endl;
#endif 
    uint64_t perm[g_max_items];
    init_permutation(perm, g_max_items, 1); //wid = 1 tid =0 
#ifdef SKIP_PERMUTATIONS
    for (unsigned i = 0; i<g_max_items; ++i) perm[i] = i+1;
#endif
    for (UInt32 i = 0; i<g_max_items; i++) {
        UInt32 key = (UInt32) perm[i];
        row_t * row;
        uint64_t row_id;
        t_item->get_new_row(row, 0, row_id);
        row->set_primary_key(key);
        row->set_value(I_ID, key);
        row->set_value(I_IM_ID, URand(1L, 10000L, 0));
        char name[25]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(14, 24, name, 0);
        row->set_value(I_NAME, name);
        row->set_value(I_PRICE, URand(1, 100, 0));
        char data[51]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(26, 50, data, 0);
        // TODO in TPCC, "original" should start at a random position
        if (RAND(10, 0)==0)
            strcpy(data, "original");
        row->set_value(I_DATA, data);

        index_insert(i_item, itemKey(key), row, 0);
    }
}

void tpcc_wl::init_tab_wh(uint32_t wid) {
#ifdef VERBOSE_1
    cout<<"init_tab_wh("<<wid<<")"<<std::endl;
#endif 
    assert(wid>=1&&wid<=g_num_wh);
    row_t * row;
    uint64_t row_id;
    t_warehouse->get_new_row(row, 0, row_id);
    row->set_primary_key(wid);

    row->set_value(W_ID, wid);
    char name[11]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
    MakeAlphaString(6, 10, name, wid-1);
    row->set_value(W_NAME, name);
    char street[21]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
    row->set_value(W_CITY, street);
    char state[3]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
    MakeAlphaString(2, 2, state, wid-1); /* State */
    row->set_value(W_STATE, state);
    char zip[10]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
    MakeNumberString(9, 9, zip, wid-1); /* Zip */
    row->set_value(W_ZIP, zip);
    double tax = (double) URand(0L, 200L, wid-1)/1000.0;
    double w_ytd = 300000.00;
    row->set_value(W_TAX, tax);
    row->set_value(W_YTD, w_ytd);

    index_insert(i_warehouse, wid, row, wh_to_part(wid));
    return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
#ifdef VERBOSE_1
    cout<<"init_tab_dist("<<wid<<")"<<std::endl;
#endif
    uint64_t perm_did[DIST_PER_WARE];
    init_permutation(perm_did, DIST_PER_WARE, wid);
#ifdef SKIP_PERMUTATIONS
    for (unsigned i = 0; i<DIST_PER_WARE; ++i) perm_did[i] = i+1;
#endif
    for (int i = 0; i<DIST_PER_WARE; i++) {
        uint64_t did = perm_did[i];
        row_t * row;
        uint64_t row_id;
        t_district->get_new_row(row, 0, row_id);
        row->set_primary_key(did);

        row->set_value(D_ID, did);
        row->set_value(D_W_ID, wid);
        char name[11]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(6, 10, name, wid-1);
        row->set_value(D_NAME, name);
        char street[21]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(D_CITY, street);
        char state[3]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(D_STATE, state);
        char zip[9];
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(D_ZIP, zip);
        double tax = (double) URand(0L, 200L, wid-1)/1000.0;
        double w_ytd = 30000.00;
        row->set_value(D_TAX, tax);
        row->set_value(D_YTD, w_ytd);
        row->set_value(D_NEXT_O_ID, 3001);
        index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
    }
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
#ifdef VERBOSE_1
    cout<<"init_tab_stock("<<wid<<")"<<std::endl;
#endif
    uint64_t perm_sid[g_max_items];
    init_permutation(perm_sid, g_max_items, wid);
#ifdef SKIP_PERMUTATIONS
    for (unsigned i = 0; i<g_max_items; ++i) perm_sid[i] = i+1;
#endif
    for (UInt32 i = 0; i<g_max_items; i++) {
        UInt32 sid = (UInt32) perm_sid[i];
        row_t * row;
        uint64_t row_id;
        t_stock->get_new_row(row, 0, row_id);
        row->set_primary_key(sid);
        row->set_value(S_I_ID, sid);
        row->set_value(S_W_ID, wid);
        row->set_value(S_QUANTITY, URand(10, 100, wid-1));
        row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
        char s_dist[25]; // this was somehow the correct size in the original DBx implementation... but basically no other string was!?
        char row_name[10] = "S_DIST_";
        for (int j = 1; j<=10; j++) {
            if (j<10) {
                row_name[7] = '0';
                row_name[8] = j+'0';
            } else {
                row_name[7] = '1';
                row_name[8] = '0';
            }
            row_name[9] = '\0';
            MakeAlphaString(24, 24, s_dist, wid-1);
            row->set_value(row_name, s_dist);
        }
        row->set_value(S_YTD, 0);
        row->set_value(S_ORDER_CNT, 0);
        char s_data[51]; // original TPCC code messed up this index size (50 instead of 51), causing nasty string overflows with a roughly 1 in 26 probability!!!
        int len = MakeAlphaString(26, 50, s_data, wid-1);
        if (rand()%100<10) {
            int idx = URand(0, len-8, wid-1);
            strcpy(&s_data[idx], "original");
        }
        row->set_value(S_DATA, s_data);
#endif
        index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
    }
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
#ifdef VERBOSE_1
    cout<<"init_tab_cust("<<did<<", "<<wid<<")"<<std::endl;
#endif
    assert(g_cust_per_dist>=1000);
    uint64_t perm_cid[g_cust_per_dist];
    init_permutation(perm_cid, g_cust_per_dist, wid);
#ifdef SKIP_PERMUTATIONS
    for (unsigned i = 0; i<g_cust_per_dist; ++i) perm_cid[i] = i+1;
#endif
    for (UInt32 i = 0; i<g_cust_per_dist; i++) {
        UInt32 cid = (UInt32) perm_cid[i];
        row_t * row;
        uint64_t row_id;
        t_customer->get_new_row(row, 0, row_id);
        row->set_primary_key(cid);

        row->set_value(C_ID, cid);
        row->set_value(C_D_ID, did);
        row->set_value(C_W_ID, wid);
        char c_last[LASTNAME_LEN];
        if (cid<=1000)
            Lastname(cid-1, c_last);
        else
            Lastname(NURand(255, 0, 999, wid-1), c_last);
        row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
        char tmp[3] = "OE";
        row->set_value(C_MIDDLE, tmp);
        char c_first[FIRSTNAME_LEN+1]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(FIRSTNAME_MINLEN, sizeof (c_first)-1, c_first, wid-1); // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        row->set_value(C_FIRST, c_first);
        char street[21]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
        row->set_value(C_CITY, street);
        char state[3]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(2, 2, state, wid-1); /* State */
        row->set_value(C_STATE, state);
        char zip[10]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeNumberString(9, 9, zip, wid-1); /* Zip */
        row->set_value(C_ZIP, zip);
        char phone[17]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeNumberString(16, 16, phone, wid-1); /* Zip */ // ???? this is an original dbx comment....
        row->set_value(C_PHONE, phone);
        row->set_value(C_SINCE, 0);
        row->set_value(C_CREDIT_LIM, 50000);
        row->set_value(C_DELIVERY_CNT, 0);
        char c_data[501]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
        MakeAlphaString(300, 500, c_data, wid-1);
        row->set_value(C_DATA, c_data);
#endif
        if (RAND(10, wid-1)==0) {
            char tmp[] = "GC";
            row->set_value(C_CREDIT, tmp);
        } else {
            char tmp[] = "BC";
            row->set_value(C_CREDIT, tmp);
        }
        row->set_value(C_DISCOUNT, (double) RAND(5000, wid-1)/10000);
        row->set_value(C_BALANCE, -10.0);
        row->set_value(C_YTD_PAYMENT, 10.0);
        row->set_value(C_PAYMENT_CNT, 1);
        uint64_t key;
#ifdef USE_RANGE_QUERIES
        key = custNPKey_ordered_by_cid(c_last, cid, did, wid);
#else
        key = custNPKey(c_last, did, wid);
#endif
        index_insert(i_customer_last, key, row, wh_to_part(wid));
        key = custKey(cid, did, wid);
        index_insert(i_customer_id, key, row, wh_to_part(wid));
    }
}

void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
#ifdef VERBOSE_1
    cout<<"init_tab_hist("<<c_id<<", "<<d_id<<", "<<w_id<<")"<<std::endl;
#endif
    row_t * row;
    uint64_t row_id;
    t_history->get_new_row(row, 0, row_id);
    row->set_primary_key(0);
    row->set_value(H_C_ID, c_id);
    row->set_value(H_C_D_ID, d_id);
    row->set_value(H_D_ID, d_id);
    row->set_value(H_C_W_ID, w_id);
    row->set_value(H_W_ID, w_id);
    row->set_value(H_DATE, 0);
    row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
    char h_data[25]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
    MakeAlphaString(12, 24, h_data, w_id-1);
    row->set_value(H_DATA, h_data);
#endif
    // just free the row right away, because it isn't even used in the original dbx implementation...
    row->setbench_deinit();
    free(row);
}

void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
#ifdef VERBOSE_1
    cout<<"init_tab_order("<<did<<", "<<wid<<")"<<std::endl;
#endif
    uint64_t perm_cid[g_cust_per_dist];
    init_permutation(perm_cid, g_cust_per_dist, wid); /* initialize permutation of customer numbers */
    uint64_t perm_oid[g_cust_per_dist];
    init_permutation(perm_oid, g_cust_per_dist, wid);
#ifdef SKIP_PERMUTATIONS
    for (unsigned i = 0; i<g_cust_per_dist; ++i) perm_oid[i] = i+1;
#endif
    for (UInt32 i = 0; i<g_cust_per_dist; i++) {
        UInt32 oid = (UInt32) perm_oid[i];
        row_t * row;
        uint64_t row_id;
        t_order->get_new_row(row, 0, row_id);
        row->set_primary_key(oid);
        uint64_t o_ol_cnt = 1;
        uint64_t cid = perm_cid[i]; //get_permutation();
        row->set_value(O_ID, oid);
        row->set_value(O_C_ID, cid);
        row->set_value(O_D_ID, did);
        row->set_value(O_W_ID, wid);
        uint64_t o_entry = 2013;
        row->set_value(O_ENTRY_D, o_entry);
        if (oid<2101)
            row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
        else
            row->set_value(O_CARRIER_ID, 0);
        o_ol_cnt = URand(5, 15, wid-1);
        row->set_value(O_OL_CNT, o_ol_cnt);
        row->set_value(O_ALL_LOCAL, 1);
        index_insert(i_order, orderPrimaryKey(wid, did, oid), row, wh_to_part(wid));

        // ORDER-LINE	
#if !TPCC_SMALL
        for (uint32_t ol = 1; ol<=o_ol_cnt; ol++) {
            t_orderline->get_new_row(row, 0, row_id);
            row->set_value(OL_O_ID, oid);
            row->set_value(OL_D_ID, did);
            row->set_value(OL_W_ID, wid);
            row->set_value(OL_NUMBER, ol);
            row->set_value(OL_I_ID, URand(1, 100000, wid-1));
            row->set_value(OL_SUPPLY_W_ID, wid);
            if (oid<2101) {
                row->set_value(OL_DELIVERY_D, o_entry);
                row->set_value(OL_AMOUNT, 0);
            } else {
                row->set_value(OL_DELIVERY_D, 0);
                row->set_value(OL_AMOUNT, (double) URand(1, 999999, wid-1)/100);
            }
            row->set_value(OL_QUANTITY, 5);
            char ol_dist_info[25]; // FIXED: this was too small in the original DBx1000 implementation, causing nasty overflows!
            MakeAlphaString(24, 24, ol_dist_info, wid-1);
            row->set_value(OL_DIST_INFO, ol_dist_info);
            index_insert(i_orderline, orderlineKey(wid, did, oid), row, wh_to_part(wid));
            index_insert(i_orderline_wd, orderline_wdKey(wid, did), row, wh_to_part(wid));
        }
#endif
        // NEW ORDER
        if (oid>2100) {
            t_neworder->get_new_row(row, 0, row_id);
            row->set_value(NO_O_ID, oid);
            row->set_value(NO_D_ID, did);
            row->set_value(NO_W_ID, wid);
            index_insert(i_neworder, neworderKey(wid, did, oid), row, wh_to_part(wid));
        }
    }
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

//  The modern version of the Fisher–Yates shuffle, designed for computer use, was introduced by Richard Durstenfeld in 1964[2] 
//  and popularized by Donald E. Knuth in The Art of Computer Programming as "Algorithm P".[3] 
//  -- To shuffle an array a of n elements (indices 0..n-1):
//  for i from 0 to n−2 do
//      j ← random integer such that i ≤ j < n
//         exchange a[i] and a[j]

void
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t size, uint64_t wid) {
    uint32_t i;
    // Init with consecutive values
    for (i = 0; i<size; i++)
        perm_c_id[i] = i+1;

    // shuffle
    for (i = 0; i<size-1; i++) {
        uint64_t j = URand(i, size-1, wid-1); //maya: I thing this should be i and not i+1
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
}

/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
    tpcc_wl * wl = (tpcc_wl *) This;

    int __tid = ATOM_FETCH_ADD(wl->next_tid, 1);
    tid = __tid;
#ifdef VERBOSE_1
    cout<<"TPCC_WL INIT: Assigned thread ID="<<__tid<<std::endl;
#endif

    thread_pinning::bindThread(__tid);
//    urcu::registerThread(__tid);
//    rlu_self = &rlu_tdata[__tid];
//    RLU_THREAD_INIT(rlu_self);
    
    uint32_t wid = __tid+1;
//    cout<<"TPCC_WL DEBUG: tpcc_buffer="<<(uintptr_t) tpcc_buffer<<" __tid="<<__tid<<endl;
    tpcc_buffer[__tid] = (drand48_data *) _mm_malloc(sizeof (drand48_data), ALIGNMENT);
    assert((uint64_t) __tid<g_num_wh);
    srand48_r(wid, tpcc_buffer[__tid]);

    wl->initThread(__tid);

    if (__tid==0)
        wl->init_tab_item();
    wl->init_tab_wh(wid);
    wl->init_tab_dist(wid);
    wl->init_tab_stock(wid);
    for (uint64_t did = 1; did<=DIST_PER_WARE; did++) {
        wl->init_tab_cust(did, wid);
        wl->init_tab_order(did, wid);
        for (uint64_t cid = 1; cid<=g_cust_per_dist; cid++)
            wl->init_tab_hist(cid, did, wid);
    }

    wl->deinitThread(__tid);

//    RLU_THREAD_FINISH(rlu_self);
//    urcu::unregisterThread();
    return NULL;
}
