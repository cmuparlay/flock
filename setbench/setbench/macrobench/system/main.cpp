#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

using namespace std;

//#include "urcu_impl.h"

void * f_warmup(void *);
void * f_real(void *);

thread_t ** m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);

        thread_pinning::configurePolicy(g_thread_cnt, g_thr_pinning_policy);
	
//        urcu::init(g_thread_cnt);
//        rlu_tdata = new rlu_thread_data_t[MAX_THREADS_POW2];
        
//        tree_malloc::init();
	papi_init_program(g_thread_cnt);
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), ALIGNMENT);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; 
                        printf("running YCSB workload\n");
                        break;
		case TPCC :
			m_wl = new tpcc_wl; 
#ifdef READ_ONLY
                        printf("running READ ONLY TPCC workload\n");
#else
                        printf("running TPCC workload\n");
#endif
                        break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			printf("running TEST workload\n");
                        break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload initialized!\n");
	switch (CC_ALG) {
            case NO_WAIT: 
                printf("using NO_WAIT concurrency control\n");    
                break;
            case WAIT_DIE: 
                printf("using WAIT_DIE concurrency control\n");    
                break;
            case DL_DETECT: 
                printf("using DL_DETECT concurrency control\n");    
                break;
            case TIMESTAMP: 
                printf("using TIMESTAMP concurrency control\n");    
                break; 
            case MVCC: 
                printf("using MVCC concurrency control\n");    
                break; 
            case HSTORE: 
                printf("using HSTORE concurrency control\n");    
                break; 
            case OCC: 
                printf("using OCC concurrency control\n");    
                break; 
            case TICTOC: 
                printf("using TICTOC concurrency control\n");    
                break; 
            case SILO: 
                printf("using SILO concurrency control\n");    
                break; 
            case VLL: 
                printf("using VLL concurrency control\n");    
                break;
            case HEKATON: 
                printf("using HEKATON concurrency control\n");    
                break;    
        }
        
        switch (ISOLATION_LEVEL) {
            case SERIALIZABLE: 
                printf("using SERIALIZABLE isolation level\n");    
                break;
            case SNAPSHOT: 
                printf("using SNAPSHOT isolation level\n");    
                break;
            case REPEATABLE_READ: 
                printf("using REPEATABLE_READ isolation level\n");    
                break;
        }

	uint64_t thd_cnt = g_thread_cnt;
        printf("running %d threads\n",g_thread_cnt);
        
	pthread_t p_thds[thd_cnt /*- 1*/];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++) {
            m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), ALIGNMENT);

            stats.init(i); //////////////////////////////////////////////////////
        }
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), ALIGNMENT);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif
        
	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
//                RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
		for (uint32_t i = 0; i < thd_cnt /*- 1*/; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f_warmup, (void *)vid);
		}
		/*f_warmup((void *)(thd_cnt - 1));*/
		for (uint32_t i = 0; i < thd_cnt /*- 1*/; i++)
			pthread_join(p_thds[i], NULL);
//                RLU_FINISH();
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
//        RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt /*- 1*/; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f_real, (void *)vid);
	}
	/*f_real((void *)(thd_cnt - 1));*/
	for (uint32_t i = 0; i < thd_cnt /*- 1*/; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();
//        RLU_FINISH();
	
#ifdef  VERBOSE_1
        for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
            printf("Index: %s\n", it->first.c_str());
            it->second->printLockCounts();
        }
#endif  
        for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
            printf("Index: %s\n", it->first.c_str());
            it->second->print_stats();
        }
        
	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print(m_wl);
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
        
        /*********************************************************************
         * CLEANUP DATA TO ENSURE WE HAVEN'T MISSED ANY LEAKS
         * This was notably missing in DBx1000...
         ********************************************************************/
        
#if !defined NO_CLEANUP_AFTER_WORKLOAD
        
	for (uint32_t i = 0; i < thd_cnt; i++) {
            stats.setbench_deinit(i); //////////////////////////////////////////////////////
        }
        
        // free indexes
        for (map<string,Index*>::iterator it = m_wl->indexes.begin(); it!=m_wl->indexes.end(); it++) {
            printf("\n\ndeleting index: %s\n", it->first.c_str());
            it->second->~Index();
            free(it->second);
        }
        
        if (glob_manager) {
            glob_manager->setbench_deinit();
            free(glob_manager);
            glob_manager = NULL;
        }
        
        if (m_thds) {
            for (uint32_t i = 0; i < thd_cnt; i++) {
                m_thds[i]->setbench_deinit();
                free(m_thds[i]);
            }
            delete[] m_thds;
        }
        
        if (WORKLOAD != TEST) {
            if (query_queue) {
                query_queue->setbench_deinit();
                free(query_queue);
                query_queue = NULL;
            }
        }
        
        if (m_wl) {
            m_wl->setbench_deinit();
            delete m_wl;
            m_wl = NULL;
        }
        
        thread_pinning::setbench_deinit(g_thread_cnt);
        
#endif
        
	return 0;
}

void * f_warmup(void * id) {
	uint64_t __tid = (uint64_t)id;
//        urcu::registerThread(__tid);
        thread_pinning::bindThread(__tid);
        tid = __tid;
#ifdef VERBOSE_1
        cout<<"WARMUP: Assigned thread ID="<<tid<<std::endl;
#endif
//        rlu_self = &rlu_tdata[__tid];
//        RLU_THREAD_INIT(rlu_self);
        m_thds[__tid]->_wl->initThread(tid);
	m_thds[__tid]->run();
        m_thds[__tid]->_wl->deinitThread(tid);
//        RLU_THREAD_FINISH(rlu_self);
//        urcu::unregisterThread();
	return NULL;
}

void * f_real(void * id) {
	uint64_t __tid = (uint64_t)id;
        tid = __tid;
//        urcu::registerThread(__tid);
        thread_pinning::bindThread(__tid);
        papi_create_eventset(__tid);
#ifdef VERBOSE_1
        cout<<"REAL: Assigned thread ID="<<tid<<std::endl;
#endif
//        rlu_self = &rlu_tdata[__tid];
//        RLU_THREAD_INIT(rlu_self);
        m_thds[__tid]->_wl->initThread(tid);
	m_thds[__tid]->run();
        m_thds[__tid]->_wl->deinitThread(tid);
//        RLU_THREAD_FINISH(rlu_self);
//        urcu::unregisterThread();
	return NULL;
}
