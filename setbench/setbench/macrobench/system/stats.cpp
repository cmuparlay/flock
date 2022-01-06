#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"
#include "wl.h"

#define BILLION 1000000000UL

void Stats_thd::init(uint64_t thd_id) {
	clear();
	all_debug1 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, ALIGNMENT);
	all_debug2 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, ALIGNMENT);
}

void Stats_thd::setbench_deinit() {
    if (all_debug1) {
        free(all_debug1);
        all_debug1 = NULL;
    }
    if (all_debug2) {
        free(all_debug2);
        all_debug2 = NULL;
    }
}

void Stats_thd::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	run_time = 0;
        for (int i=0;i<MAX_NUM_INDEXES;++i) {
            stats_indexes[i].clear();
        }
	time_man = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	time_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_wait = 0;
	time_ts_alloc = 0;
	latency = 0;
	time_query = 0;
}

void Stats_tmp_index::clear() {
    timeContains = 0;
    timeInsert = 0;
    timeRangeQuery = 0;
    numContains = 0;
    numInsert = 0;
    numRangeQuery = 0;
}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
	time_man = 0;
	time_index = 0;
	time_wait = 0;
        for (int i=0;i<MAX_NUM_INDEXES;++i) {
            stats_indexes[i].clear();
        }
}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
	_stats = (Stats_thd**) 
			_mm_malloc(sizeof(Stats_thd*) * g_thread_cnt, ALIGNMENT);
	tmp_stats = (Stats_tmp**) 
			_mm_malloc(sizeof(Stats_tmp*) * g_thread_cnt, ALIGNMENT);
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
}

void Stats::init(uint64_t thread_id) {
    if (!STATS_ENABLE) return;
    _stats[thread_id] = (Stats_thd *) 
            _mm_malloc(sizeof(Stats_thd), ALIGNMENT);
    tmp_stats[thread_id] = (Stats_tmp *)
            _mm_malloc(sizeof(Stats_tmp), ALIGNMENT);

    _stats[thread_id]->init(thread_id);
    tmp_stats[thread_id]->init();
}

void Stats::setbench_deinit(uint64_t thread_id) {
    if (!STATS_ENABLE) return;
    if (_stats[thread_id]) {
        _stats[thread_id]->setbench_deinit();
        free(_stats[thread_id]);
        _stats[thread_id] = NULL;
    }
    if (tmp_stats[thread_id]) {
        free(tmp_stats[thread_id]);
        tmp_stats[thread_id] = NULL;
    }
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

void Stats::add_debug(uint64_t thd_id, uint64_t value, uint32_t select) {
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		if (select == 1)
			_stats[thd_id]->all_debug1[tnum] = value;
		else if (select == 2)
			_stats[thd_id]->all_debug2[tnum] = value;
	}
}

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
		_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
		_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
#               define COMMIT_ACCUMULATE(name) for (int i=0;i<MAX_NUM_INDEXES;++i) _stats[thd_id]->stats_indexes[i].name += tmp_stats[thd_id]->stats_indexes[i].name
                COMMIT_ACCUMULATE(timeContains);
                COMMIT_ACCUMULATE(timeInsert);
                COMMIT_ACCUMULATE(timeRangeQuery);
                COMMIT_ACCUMULATE(numContains);
                COMMIT_ACCUMULATE(numInsert);
                COMMIT_ACCUMULATE(numRangeQuery);
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print(workload * wl) {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	double total_run_time = 0;
	double total_time_man = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_time_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_wait = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_time_query = 0;
	for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_run_time += _stats[tid]->run_time;
		total_time_man += _stats[tid]->time_man;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_time_index += _stats[tid]->time_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_wait += _stats[tid]->time_wait;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_time_query += _stats[tid]->time_query;
		
		printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);
	}
	FILE * outf;
//	if (output_file != NULL) {
//		outf = fopen(output_file, "w");
//		fprintf(outf, "[summary] txn_cnt=%ld, abort_cnt=%ld"
//			", run_time=%f, time_wait=%f, time_ts_alloc=%f"
//			", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
//			", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
//			", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f"
//                        ", nthreads=%d, throughput=%f, optimal_throughput=%f\n",
//			total_txn_cnt, 
//			total_abort_cnt,
//			total_run_time / BILLION,
//			total_time_wait / BILLION,
//			total_time_ts_alloc / BILLION,
//			(total_time_man - total_time_wait) / BILLION,
//			total_time_index / BILLION,
//			total_time_abort / BILLION,
//			total_time_cleanup / BILLION,
//			total_latency / BILLION / total_txn_cnt,
//			deadlock,
//			cycle_detect,
//			dl_detect_time / BILLION,
//			dl_wait_time / BILLION,
//			total_time_query / BILLION,
//			total_debug1, // / BILLION,
//			total_debug2, // / BILLION,
//			total_debug3, // / BILLION,
//			total_debug4, // / BILLION,
//			total_debug5 / BILLION,
//                        g_thread_cnt,
//                        total_txn_cnt/(total_run_time / BILLION)*g_thread_cnt,
//                        total_txn_cnt/((total_run_time - total_time_index)/ BILLION)*g_thread_cnt
//		);
//		fclose(outf);
//	}
        
#       define LOAD_STAT(index, tid, name) auto name = _stats[(tid)]->stats_indexes[index->index_id].name
#       define ACCUM_STAT(index, tid, name) name += _stats[(tid)]->stats_indexes[index->index_id].name
        
        /**
         * Compute per-thread per-index stats
         */
        for (auto it = wl->indexes.begin(); it != wl->indexes.end(); it++) {
            Index * index = it->second;
            for (int tid=0;tid<g_thread_cnt;++tid) {
                LOAD_STAT(index, tid, numContains);
                LOAD_STAT(index, tid, timeContains);
                LOAD_STAT(index, tid, numInsert);
                LOAD_STAT(index, tid, timeInsert);
                LOAD_STAT(index, tid, numRangeQuery);
                LOAD_STAT(index, tid, timeRangeQuery);
                timeContains /= BILLION;
                timeInsert /= BILLION;
                timeRangeQuery /= BILLION;
                uint64_t ixTotalOps = numContains + numInsert + numRangeQuery;
                double ixTotalTime = timeContains + timeInsert + timeRangeQuery;
                double ixThroughput = ixTotalOps / (ixTotalTime / g_thread_cnt);
                printf("Per-thread per-index stats: index=%s, thread=%d"
                        ", numContains=%ld, timeContains=%f"
                        ", numInsert=%ld, timeInsert=%f"
                        ", numRangeQuery=%ld, timeRangeQuery=%f"
                        ", totalOperations=%ld, totalTime=%f, throughput=%f\n"
                        , index->index_name.c_str()
                        , tid
                        , numContains
                        , timeContains
                        , numInsert
                        , timeInsert
                        , numRangeQuery
                        , timeRangeQuery
                        , ixTotalOps
                        , ixTotalTime
                        , ixThroughput
                );
            }
        }
        
        /**
         * Compute per-index stats
         */
        for (auto it = wl->indexes.begin(); it != wl->indexes.end(); it++) {
            Index * index = it->second;
            
            uint64_t numContains = 0;
            double timeContains = 0;
            uint64_t numInsert = 0;
            double timeInsert = 0;
            uint64_t numRangeQuery = 0;
            double timeRangeQuery = 0;
            for (int tid=0;tid<g_thread_cnt;++tid) {
                ACCUM_STAT(index, tid, numContains);
                ACCUM_STAT(index, tid, timeContains);
                ACCUM_STAT(index, tid, numInsert);
                ACCUM_STAT(index, tid, timeInsert);
                ACCUM_STAT(index, tid, numRangeQuery);
                ACCUM_STAT(index, tid, timeRangeQuery);
            }
            timeContains /= BILLION;
            timeInsert /= BILLION;
            timeRangeQuery /= BILLION;

            uint64_t ixTotalOps = numContains + numInsert + numRangeQuery;
            double ixTotalTime = timeContains + timeInsert + timeRangeQuery;
            double ixThroughput = ixTotalOps / (ixTotalTime / g_thread_cnt);
            printf("Per-index stats: index=%s"
                    ", numContains=%ld, timeContains=%f"
                    ", numInsert=%ld, timeInsert=%f"
                    ", numRangeQuery=%ld, timeRangeQuery=%f"
                    ", totalOps=%ld, totalTime=%f, throughput=%f\n"
                    , index->index_name.c_str()
                    , numContains
                    , timeContains
                    , numInsert
                    , timeInsert
                    , numRangeQuery
                    , timeRangeQuery
                    , ixTotalOps
                    , ixTotalTime
                    , ixThroughput
            );
        }
        
        /**
         * Compute aggregate index stats
         */
        uint64_t numContains = 0;
        double timeContains = 0;
        uint64_t numInsert = 0;
        double timeInsert = 0;
        uint64_t numRangeQuery = 0;
        double timeRangeQuery = 0;
        for (auto it = wl->indexes.begin(); it != wl->indexes.end(); it++) {
            Index * index = it->second;
            for (int tid=0;tid<g_thread_cnt;++tid) {
                ACCUM_STAT(index, tid, numContains);
                ACCUM_STAT(index, tid, timeContains);
                ACCUM_STAT(index, tid, numInsert);
                ACCUM_STAT(index, tid, timeInsert);
                ACCUM_STAT(index, tid, numRangeQuery);
                ACCUM_STAT(index, tid, timeRangeQuery);
            }
        }
        timeContains /= BILLION;
        timeInsert /= BILLION;
        timeRangeQuery /= BILLION;
        uint64_t ixTotalOps = numContains + numInsert + numRangeQuery;
        double ixTotalTime = timeContains + timeInsert + timeRangeQuery;
        double ixThroughput = ixTotalOps / (ixTotalTime / g_thread_cnt);
        printf("Aggregate index stats: "
                "numContains=%ld, timeContains=%f, numInsert=%ld, timeInsert=%f"
                ", numRangeQuery=%ld, timeRangeQuery=%f"
                ", totalOps=%ld, totalTime=%f, throughput=%f\n"
                , numContains
                , timeContains
                , numInsert
                , timeInsert
                , numRangeQuery
                , timeRangeQuery
                , ixTotalOps
                , ixTotalTime
                , ixThroughput
        );
        
        /**
         * Print summary
         */
	printf("[summary] txn_cnt=%ld, abort_cnt=%ld"
		", run_time=%f, time_wait=%f, time_ts_alloc=%f"
		", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
		", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
		", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f"
                ", ixNumContains=%ld, ixTimeContains=%f, ixNumInsert=%ld, ixTimeInsert=%f"
                ", ixNumRangeQuery=%ld, ixTimeRangeQuery=%f"
                ", ixTotalOps=%ld, ixTotalTime=%f, ixThroughput=%f"
                ", nthreads=%d, throughput=%f"
                ", node_size=%zd, descriptor_size=%zd"
                "\n",
		total_txn_cnt, 
		total_abort_cnt,
		total_run_time / BILLION,
		total_time_wait / BILLION,
		total_time_ts_alloc / BILLION,
		(total_time_man - total_time_wait) / BILLION,
		total_time_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_latency / BILLION / total_txn_cnt,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_debug1 / BILLION,
		total_debug2, // / BILLION,
		total_debug3, // / BILLION,
		total_debug4, // / BILLION,
		total_debug5,  // / BILLION 
                numContains,
                timeContains,
                numInsert,
                timeInsert,
                numRangeQuery,
                timeRangeQuery,
                ixTotalOps,
                ixTotalTime,
                ixThroughput,
                g_thread_cnt,
                total_txn_cnt/(total_run_time / BILLION)*g_thread_cnt,
                wl->indexes.begin()->second->getNodeSize(),
                wl->indexes.begin()->second->getDescriptorSize()
	);

	papi_print_counters( total_txn_cnt/(total_run_time / BILLION)*g_thread_cnt);

	if (g_prt_lat_distr)
		print_lat_distr();
}

void Stats::print_lat_distr() {
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
			fprintf(outf, "[all_debug1 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
			fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
}
