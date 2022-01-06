/* 
 * File:   rlu.h
 * Taken from https://github.com/rlu-sync/rlu/ (a1e7e9e  on Aug 23, 2015)
 * 
 * 
 * Alexander Matveev, Nir Shavit, Pascal Felber, and Patrick Marlier. 2015. 
 * Read-log-update: a lightweight synchronization mechanism for concurrent programming. 
 * In Proceedings of the 25th Symposium on Operating Systems Principles (SOSP '15)
 */

#ifndef RLU_H
#define RLU_H

#define RLU_USED

#ifdef __cplusplus
extern "C" {
#endif

/////////////////////////////////////////////////////////////////////////////////////////
// INCLUDES
/////////////////////////////////////////////////////////////////////////////////////////
#ifndef KERNEL
# include <stdint.h>
# include <limits.h>
#else /* KERNEL */
typedef long intptr_t; // Hack for kernel that does not have intptr_t
#endif /* KERNEL */

/////////////////////////////////////////////////////////////////////////////////////////
// DEFINES - CONFIGURATION
/////////////////////////////////////////////////////////////////////////////////////////
#define RLU_TYPE_FINE_GRAINED (1)
#define RLU_TYPE_COARSE_GRAINED (2)

#define RLU_MAX_THREADS (256)

#define RLU_MAX_WRITE_SETS (200) // Minimum value is 2
#define RLU_MAX_FREE_NODES (100000)

#define RLU_MAX_WRITE_SET_BUFFER_SIZE (100000)

#define RLU_MAX_NESTED_WRITER_LOCKS (20)
#define RLU_MAX_WRITER_LOCKS (20000)

#define RLU_GENERAL_WRITER_LOCK (RLU_MAX_WRITER_LOCKS-1)

//#define RLU_ENABLE_TRACE_1
//#define RLU_ENABLE_TRACE_2
//#define RLU_ENABLE_TRACE_3

/////////////////////////////////////////////////////////////////////////////////////////
// DEFINES - INTERNAL
/////////////////////////////////////////////////////////////////////////////////////////

#define rlu_likely(x) __builtin_expect ((x), 1)

#define RLU_DEFAULT_PADDING (16)

#define RLU_OBJ_HEADER_SIZE (sizeof(rlu_obj_header_t))
#define RLU_MOVE_PTR_BACK(p_obj, offset) ((intptr_t *)(((volatile unsigned char *)p_obj) - offset))
#define RLU_OBJ_TO_H(p_obj) ((volatile rlu_obj_header_t *)RLU_MOVE_PTR_BACK(p_obj, RLU_OBJ_HEADER_SIZE))

#define RLU_GET_COPY(p_obj) (RLU_OBJ_TO_H(p_obj)->p_obj_copy)
#define RLU_PTR_IS_LOCKED(p_obj_copy) (p_obj_copy != NULL)

#define RLU_IS_UNLOCKED(p_obj) (!RLU_PTR_IS_LOCKED(RLU_GET_COPY(p_obj)))

/////////////////////////////////////////////////////////////////////////////////////////
// TYPES
/////////////////////////////////////////////////////////////////////////////////////////
typedef size_t obj_size_t;

typedef struct rlu_obj_header {
	volatile intptr_t *p_obj_copy;
} rlu_obj_header_t;

typedef struct rlu_ws_obj_header {
	volatile intptr_t *p_obj_actual;
	volatile obj_size_t obj_size;
	volatile unsigned long run_counter;
	volatile long thread_id;
} rlu_ws_obj_header_t;

typedef struct writer_locks {
	long size;
	long ids[RLU_MAX_NESTED_WRITER_LOCKS];
} writer_locks_t;

typedef struct obj_list {
	volatile writer_locks_t writer_locks;
	unsigned int num_of_objs;
	volatile intptr_t *p_cur;
	volatile unsigned char buffer[RLU_MAX_WRITE_SET_BUFFER_SIZE];
} obj_list_t;

typedef struct wait_entry {
	volatile unsigned char is_wait;
	volatile unsigned long run_counter;
} wait_entry_t;

typedef struct rlu_thread_data {

	long padding_0[RLU_DEFAULT_PADDING];

	long uniq_id;

	char is_check_locks;
	char is_write_detected;
	char is_steal;
	int type;
	int max_write_sets;

	long padding_1[RLU_DEFAULT_PADDING];

	volatile unsigned long run_counter;
	volatile long local_version;
	volatile long local_commit_version;
	volatile long is_no_quiescence;
	volatile long is_sync;

	long padding_2[RLU_DEFAULT_PADDING];

	volatile long writer_version;
	volatile long last_writer_version;

	long padding_3[RLU_DEFAULT_PADDING];

	wait_entry_t q_threads[RLU_MAX_THREADS];

	long ws_head_counter;
	long ws_wb_counter;
	long ws_tail_counter;
	long ws_cur_id;
	volatile obj_list_t obj_write_set[RLU_MAX_WRITE_SETS];

	long padding_4[RLU_DEFAULT_PADDING];

	long free_nodes_size;
	intptr_t *free_nodes[RLU_MAX_FREE_NODES];

	long padding_5[RLU_DEFAULT_PADDING];

	long n_starts;
	long n_finish;
	long n_writers;
	long n_writer_writeback;
	long n_pure_readers;
	long n_aborts;
	long n_steals;
	long n_writer_sync_waits;
	long n_writeback_q_iters;
	long n_sync_requests;
	long n_sync_and_writeback;

	long padding_6[RLU_DEFAULT_PADDING];

} rlu_thread_data_t;


/////////////////////////////////////////////////////////////////////////////////////////
// EXTERNAL FUNCTIONS
/////////////////////////////////////////////////////////////////////////////////////////

void rlu_init(int type, int max_write_sets);
void rlu_finish(void);
void rlu_print_stats(void);

void rlu_thread_init(rlu_thread_data_t *self);
void rlu_thread_finish(rlu_thread_data_t *self);

intptr_t *rlu_alloc(obj_size_t obj_size);
void rlu_free(rlu_thread_data_t *self, intptr_t *p_obj);

void rlu_reader_lock(rlu_thread_data_t *self);
void rlu_reader_unlock(rlu_thread_data_t *self);

int rlu_try_lock(rlu_thread_data_t *self, intptr_t **p_p_obj, size_t obj_size);
void rlu_abort(rlu_thread_data_t *self);

int rlu_try_writer_lock(rlu_thread_data_t *self, int writer_lock_id);
void rlu_lock(rlu_thread_data_t *self, intptr_t **p_p_obj, unsigned int obj_size);

intptr_t *rlu_deref_slow_path(rlu_thread_data_t *self, intptr_t *p_obj);

int rlu_cmp_ptrs(intptr_t *p_obj_1, intptr_t *p_obj_2);
void rlu_assign_pointer(intptr_t **p_ptr, intptr_t *p_obj);

void rlu_sync_checkpoint(rlu_thread_data_t *self);

/////////////////////////////////////////////////////////////////////////////////////////
// EXTERNAL MACROS
/////////////////////////////////////////////////////////////////////////////////////////

#define RLU_INIT(type, max_write_sets) rlu_init(type, max_write_sets);
#define RLU_FINISH() rlu_finish();
#define RLU_PRINT_STATS() rlu_print_stats()

#define RLU_THREAD_INIT(self) rlu_thread_init(self)
#define RLU_THREAD_FINISH(self) rlu_thread_finish(self)

#define RLU_READER_LOCK(self) rlu_reader_lock(self)
#define RLU_READER_UNLOCK(self) rlu_reader_unlock(self)

#define RLU_ALLOC(obj_size) ((void *)rlu_alloc(obj_size))
#define RLU_FREE(self, p_obj) rlu_free(self, (intptr_t *)p_obj)

#define RLU_TRY_WRITER_LOCK(self, writer_lock_id) rlu_try_writer_lock(self, writer_lock_id)
#define RLU_LOCK(self, p_p_obj) rlu_lock(self, (intptr_t **)p_p_obj, sizeof(**p_p_obj))

#define RLU_TRY_LOCK(self, p_p_obj) rlu_try_lock(self, (intptr_t **)p_p_obj, sizeof(**p_p_obj))
#define RLU_ABORT(self) rlu_abort(self)

#define RLU_IS_SAME_PTRS(p_obj_1, p_obj_2) rlu_cmp_ptrs((intptr_t *)p_obj_1, (intptr_t *)p_obj_2)
#define RLU_ASSIGN_PTR(self, p_ptr, p_obj) rlu_assign_pointer((intptr_t **)p_ptr, (intptr_t *)p_obj)

#define RLU_DEREF(self, p_obj) RLU_DEREF_INTERNAL(self, (intptr_t *)p_obj)

#define RLU_DEREF_INTERNAL(self, p_obj) ({ \
	intptr_t *p_cur_obj; \
	if (rlu_likely((self)->is_check_locks == 0)) { \
		p_cur_obj = p_obj; \
	} else { \
		if (rlu_likely((p_obj != NULL) && RLU_IS_UNLOCKED(p_obj))) { \
			p_cur_obj = p_obj; \
		} else { \
			p_cur_obj = rlu_deref_slow_path((self), p_obj); \
		} \
	}; p_cur_obj; })


#ifdef __cplusplus
}
#endif

#endif /* RLU_H */

