#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"

void Manager::setbench_deinit() {
//    printf("called destructor Manager::setbench_deinit()\n");
    if (timestamp) {
        free(timestamp);
        timestamp = NULL;
    }
    if (_epoch) {
        free((void *) _epoch);
        _epoch = NULL;
    }
    if (_last_epoch_update_time) {
        free((void *) _last_epoch_update_time);
        _last_epoch_update_time = NULL;
    }
    if (all_ts) {
        for (uint32_t i = 0; i < g_thread_cnt; i++)  {
            if (all_ts[i]) {
                free((void *) all_ts[i]);
                all_ts[i] = NULL;
            }
        }
        free((void *) all_ts);
        all_ts = NULL;
    }
    if (_all_txns) {
        for (int i=0;i<g_thread_cnt;++i) {
            if (_all_txns[i]) {
//                printf("  in destructor Manager::setbench_deinit() cleaned up _all_txns[%d]\n", i);
                _all_txns[i]->setbench_deinit();
                free(_all_txns[i]);
                _all_txns[i] = NULL;
            }
        }
//        printf("  in destructor Manager::setbench_deinit() cleaned up _all_txns\n");
        delete[] _all_txns;
        _all_txns = NULL;
    }
}

void Manager::init() {
	timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), ALIGNMENT);
	*timestamp = 1;
	_last_min_ts_time = 0;
	_min_ts = 0;
	_epoch = (uint64_t *) _mm_malloc(sizeof(uint64_t), ALIGNMENT);
	_last_epoch_update_time = (ts_t *) _mm_malloc(sizeof(uint64_t), ALIGNMENT);
//	_epoch = 0;
//	_last_epoch_update_time = 0;
	all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, ALIGNMENT);
	for (uint32_t i = 0; i < g_thread_cnt; i++) 
		all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), ALIGNMENT);

	_all_txns = new txn_man * [g_thread_cnt];
	for (UInt32 i = 0; i < g_thread_cnt; i++) {
		*all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < BUCKET_CNT; i++)
		pthread_mutex_init( &mutexes[i], NULL );
}

uint64_t 
Manager::get_ts(uint64_t thread_id) {
	if (g_ts_batch_alloc)
		assert(g_ts_alloc == TS_CAS);
	uint64_t time;
//	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex );
		time = ++(*timestamp);
		pthread_mutex_unlock( &ts_mutex );
		break;
	case TS_CAS :
		if (g_ts_batch_alloc)
			time = ATOM_FETCH_ADD((*timestamp), g_ts_batch_num);
		else 
			time = ATOM_FETCH_ADD((*timestamp), 1);
		break;
	case TS_HW :
#ifndef NOGRAPHITE
		time = CarbonGetTimestamp();
#else
		assert(false);
#endif
		break;
	case TS_CLOCK :
		time = get_sys_clock() * g_thread_cnt + thread_id;
		break;
	default :
		assert(false);
	}
//	INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
	return time;
}

ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	uint64_t last_time = _last_min_ts_time; 
	if (tid == 0 && now - last_time > MIN_TS_INTVL)
	{ 
		ts_t min = UINT64_MAX;
    	for (UInt32 i = 0; i < g_thread_cnt; i++) 
	    	if (*all_ts[i] < min)
    	    	min = *all_ts[i];
		if (min > _min_ts)
			_min_ts = min;
	}
	return _min_ts;
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
	assert( ts >= *all_ts[thd_id] || 
		*all_ts[thd_id] == UINT64_MAX);
	*all_ts[thd_id] = ts;
}

void Manager::set_txn_man(txn_man * txn) {
	int thd_id = txn->get_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}
 
void Manager::lock_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_lock( &mutexes[bid] );	
}

void Manager::release_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_unlock( &mutexes[bid] );
}
	
void
Manager::update_epoch()
{
	ts_t time = get_sys_clock();
	if (time - *_last_epoch_update_time > LOG_BATCH_TIME * 1000 * 1000) {
		*_epoch = *_epoch + 1;
		*_last_epoch_update_time = time;
	}
}
