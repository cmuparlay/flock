#include <sched.h>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "all_indexes.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

int ycsb_wl::next_tid;

RC ycsb_wl::init() {
    workload::init();
    next_tid = 0;
    char * cpath = getenv("GRAPHITE_HOME");
    string path;
    if (cpath==NULL)
        path = "./benchmarks/YCSB_schema.txt";
    else {
        path = string(cpath);
        path += "/tests/apps/dbms/YCSB_schema.txt";
    }
    init_schema(path);

    init_table_parallel();
    //	init_table();
    return RCOK;
}

void ycsb_wl::setbench_deinit() {
    workload::setbench_deinit();
    for (auto name_tableptr_pair : tables) {
        auto tableptr = name_tableptr_pair.second;
        tableptr->setbench_deinit();
        free(tableptr);
    }
    if (perm) {
        free(perm);
        perm = NULL;
    }
}

RC ycsb_wl::init_schema(std::string schema_file) {
    workload::init_schema(schema_file);
    the_table = tables["MAIN_TABLE"];
    the_index = indexes["MAIN_INDEX"];
    return RCOK;
}

int
ycsb_wl::key_to_part(uint64_t key) {
    uint64_t rows_per_part = g_synth_table_size/g_part_cnt;
    return key/rows_per_part;
}

RC ycsb_wl::init_table() {
    RC rc;
    uint64_t total_row = 0;
    while (true) {
        for (UInt32 part_id = 0; part_id<g_part_cnt; part_id++) {
            if (total_row>g_synth_table_size)
                goto ins_done;
            row_t * new_row = NULL;
            uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id);
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
            assert(rc==RCOK);
            uint64_t primary_key = total_row;
            new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
            Catalog * schema = the_table->get_schema();
            for (UInt32 fid = 0; fid<schema->get_field_cnt(); fid++) {
                int field_size = schema->get_field_size(fid);
                char value[field_size];
                for (int i = 0; i<field_size; i++)
                    value[i] = (char) rand()%(1<<8);
                new_row->set_value(fid, value);
            }
            itemid_t * m_item =
                    (itemid_t *) mem_allocator.alloc(sizeof (itemid_t), part_id);
            assert(m_item!=NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc==RCOK);
            total_row++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

static void init_permutation(uint64_t * array, uint64_t size) {
    uint32_t i;

    myrand rdm;
    rdm.init(get_sys_clock());

    // Init with consecutive values
    for (i = 0; i<size; i++)
        array[i] = i+1; //dirty hack to make sure key != 0

#ifdef SKIP_PERMUTATIONS
    return;
#endif 

    // shuffle
    for (i = 0; i<size-1; i++) {
        uint64_t j = i+(rdm.next()%(size-i));
        uint64_t tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
    }
}

// init table in parallel

void ycsb_wl::init_table_parallel() {

    perm = (uint64_t*) malloc(sizeof (uint64_t)*g_synth_table_size);
    init_permutation(perm, g_synth_table_size);

    enable_thread_mem_pool = true;
    pthread_t p_thds[g_init_parallelism /*- 1*/];
//    RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
    for (UInt32 i = 0; i<g_init_parallelism /*- 1*/; i++)
        pthread_create(&p_thds[i], NULL, threadInitTable, this);
    /*threadInitTable(this);*/

    for (uint32_t i = 0; i<g_init_parallelism /*- 1*/; i++) {
        int rc = pthread_join(p_thds[i], NULL);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
    }
//    RLU_FINISH();
    enable_thread_mem_pool = false;
    mem_allocator.unregister();
}

void * ycsb_wl::init_table_slice() {
    UInt32 __tid = ATOM_FETCH_ADD(next_tid, 1);
//    urcu::registerThread(__tid);
//    rlu_self = &rlu_tdata[__tid];
//    RLU_THREAD_INIT(rlu_self);
    thread_pinning::bindThread(__tid);
    //	// set cpu affinity
    //	set_affinity(__tid);

    tid = __tid;
//    cout<<"YCSB_WL INIT: Assigned thread ID="<<tid<<std::endl;
    this->initThread(tid);

    mem_allocator.register_thread(__tid);
    RC rc;
    if (g_synth_table_size%g_init_parallelism) {
        cout<<"g_synth_table_size="<<g_synth_table_size<<" g_init_parallelism="<<g_init_parallelism<<endl;
    }
    assert(g_synth_table_size%g_init_parallelism==0);
    assert(__tid<g_init_parallelism);
    while ((UInt32) ATOM_FETCH_ADD(next_tid, 0)<g_init_parallelism) {
    }
    assert((UInt32) ATOM_FETCH_ADD(next_tid, 0)==g_init_parallelism);
    uint64_t slice_size = g_synth_table_size/g_init_parallelism;

    for (uint64_t i = slice_size*__tid;
         i<slice_size*(__tid+1);
         i++
         ) {
        uint64_t key = perm[i];
        row_t * new_row = NULL;
        uint64_t row_id;
        int part_id = key_to_part(key);
        rc = the_table->get_new_row(new_row, part_id, row_id);
        assert(rc==RCOK);
        uint64_t primary_key = key;
        new_row->set_primary_key(primary_key);
        new_row->set_value(0, &primary_key);
        Catalog * schema = the_table->get_schema();

        for (UInt32 fid = 0; fid<schema->get_field_cnt(); fid++) {
            char value[6] = "hello";
            new_row->set_value(fid, value);
        }

        itemid_t * m_item =
                (itemid_t *) mem_allocator.alloc(sizeof (itemid_t), part_id);
        assert(m_item!=NULL);
        m_item->type = DT_row;
        m_item->location = new_row;
        m_item->valid = true;
        uint64_t idx_key = primary_key;

        rc = the_index->index_insert(idx_key, m_item, part_id);
        assert(rc==RCOK);
    }

    this->deinitThread(tid);
//    RLU_THREAD_FINISH(rlu_self);
//    urcu::unregisterThread();
    return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
    txn_manager = (ycsb_txn_man *)
            _mm_malloc(sizeof (ycsb_txn_man), ALIGNMENT);
    new(txn_manager) ycsb_txn_man();
    txn_manager->init(h_thd, this, h_thd->get_thd_id());
    return RCOK;
}


