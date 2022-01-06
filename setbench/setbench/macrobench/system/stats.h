#pragma once 

#define MAX_NUM_INDEXES 10

class workload;

class Stats_tmp_index {
public:
    void clear();
    double timeContains;
    double timeInsert;
    double timeRangeQuery;
    uint64_t numContains;
    uint64_t numInsert;
    uint64_t numRangeQuery;
};

class Stats_thd {
public:
	void init(uint64_t thd_id);
        void setbench_deinit();
	void clear();

	char _pad2[CL_SIZE];
	uint64_t txn_cnt;
	uint64_t abort_cnt;
	double run_time;
        Stats_tmp_index stats_indexes[MAX_NUM_INDEXES];
	double time_man;        // unused
	double time_index;      // unused
	double time_wait;       // unused
	double time_abort;      // unused
	double time_cleanup;    // unused
	uint64_t time_ts_alloc; // unused
	double time_query;      // unused
	uint64_t wait_cnt;
	uint64_t debug1;
	uint64_t debug2;
	uint64_t debug3;
	uint64_t debug4;
	uint64_t debug5;
	
	uint64_t latency;       // unused
	uint64_t * all_debug1;
	uint64_t * all_debug2;
	char _pad[CL_SIZE];
};

class Stats_tmp {
public:
	void init();
	void clear();
	char _pad2[CL_SIZE];
        Stats_tmp_index stats_indexes[MAX_NUM_INDEXES];
	double time_man;    // unused
	double time_index;  // unused
	double time_wait;   // unused
	char _pad[CL_SIZE];
};

class Stats {
public:
	// PER THREAD statistics
	Stats_thd ** _stats;
	// stats are first written to tmp_stats, if the txn successfully commits, 
	// copy the values in tmp_stats to _stats
	Stats_tmp ** tmp_stats;
	
	// GLOBAL statistics
	double dl_detect_time;  // unused
	double dl_wait_time;    // unused
	uint64_t cycle_detect;
	uint64_t deadlock;	

	void init();
        void setbench_deinit(uint64_t thread_id);
	void init(uint64_t thread_id);
	void clear(uint64_t tid);
	void add_debug(uint64_t thd_id, uint64_t value, uint32_t select);
	void commit(uint64_t thd_id);
	void abort(uint64_t thd_id);
	void print(workload * wl);
	void print_lat_distr();
};
