#include "global.h"
#include "row.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "all_indexes.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
    RC rc;
    ycsb_query * m_query = (ycsb_query *) query;
    ycsb_wl * wl = (ycsb_wl *) h_wl;
    itemid_t * m_item = NULL;
    row_cnt = 0;

    for (uint32_t rid = 0; rid<m_query->request_cnt; rid++) {
        ycsb_request * req = &m_query->requests[rid];
        uint64_t key = req->key + 1; //dirty hack to make sure key != 0	
        int part_id = wl->key_to_part(key);
        bool finish_req = false;
        UInt32 iteration = 0;
        while (!finish_req) {
            if (iteration==0) {
                m_item = index_read(_wl->the_index, key, part_id);
//                if (m_item==NULL) {
//                    cout<<"item is null, key is "<<key<<std::endl;
//                }
//                assert(m_item!=NULL);
                if (m_item==NULL) {
                    rc = Abort;
                    goto final;
                }
            }
//#ifdef IDX_BTREE
//            else {
//                _wl->the_index->index_next(get_thd_id(), m_item);
//                if (m_item==NULL)
//                    break;
//            }
//#endif
            row_t * row = ((row_t *) m_item->location);
            row_t * row_local;
            access_t type = req->rtype;

            row_local = get_row(row, type);
            if (row_local==NULL) {
                rc = Abort;
                goto final;
            }

            // Computation //
            // Only do computation when there are more than 1 requests.
            if (m_query->request_cnt>1) {
                if (req->rtype==RD||req->rtype==SCAN) {
                    //                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;
                    char * data = row_local->get_data();
                    __attribute__ ((unused)) uint64_t fval = *(uint64_t *) (&data[fid*10]);
                    //                  }
                } else {
                    assert(req->rtype==WR);
                    //					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                    int fid = 0;
                    char * data = row->get_data();
                    *(uint64_t *) (&data[fid*10]) = 0;
                    //					}
                }
            }


            iteration++;
            if (req->rtype==RD||req->rtype==WR||iteration==req->scan_len)
                finish_req = true;
        }
    }
    rc = RCOK;
    final :
    rc = finish(rc);
    return rc;
}

