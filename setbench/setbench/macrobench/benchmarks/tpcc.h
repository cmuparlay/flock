#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include "txn.h"

class table_t;
class Index;
class tpcc_query;

class tpcc_wl : public workload {
public:
    RC init();
    void setbench_deinit();
    RC init_table();
    RC init_schema(const char * schema_file);
    RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
    table_t * t_warehouse;
    table_t * t_district;
    table_t * t_customer;
    table_t * t_history; // UNUSED AS OF DBx1000 PUBLICATION TIME (!!)
    table_t * t_neworder; // missing index (NO_W_ID, NO_D_ID, NO_O_ID) [NOT PRESENT IN ORIGINAL DBx]
    table_t * t_order;
    table_t * t_orderline;
    table_t * t_item;
    table_t * t_stock;

    Index * i_neworder;
    Index * i_item;
    Index * i_warehouse;
    Index * i_district;
    Index * i_customer_id;
    Index * i_customer_last;                            // SHARES ROWS WITH i_customer_id !!
    Index * i_stock;
    Index * i_order; // key = (w_id, d_id, o_id)
    Index * i_orderline; // key = (w_id, d_id, o_id)
    Index * i_orderline_wd; // key = (w_id, d_id).      // SHARES ROWS WITH i_orderline !!

    bool ** delivering;
    uint32_t next_tid;
private:
    uint64_t num_wh;
    void init_tab_item();
    void init_tab_wh(uint32_t wid);
    void init_tab_dist(uint64_t w_id);
    void init_tab_stock(uint64_t w_id);
    void init_tab_cust(uint64_t d_id, uint64_t w_id);
    void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
    void init_tab_order(uint64_t d_id, uint64_t w_id);

    void init_permutation(uint64_t * perm_c_id, uint64_t size, uint64_t wid);

    static void * threadInitItem(void * This);
    static void * threadInitWh(void * This);
    static void * threadInitDist(void * This);
    static void * threadInitStock(void * This);
    static void * threadInitCust(void * This);
    static void * threadInitHist(void * This);
    static void * threadInitOrder(void * This);

    static void * threadInitWarehouse(void * This);
};

class tpcc_txn_man : public txn_man {
public:
    void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
    RC run_txn(base_query * query);
private:
    tpcc_wl * _wl;
    RC run_payment(tpcc_query * m_query);
    RC run_new_order(tpcc_query * m_query);
    RC run_order_status(tpcc_query * query);
    RC run_delivery(tpcc_query * query);
    RC run_stock_level(tpcc_query * query);
};

#endif
