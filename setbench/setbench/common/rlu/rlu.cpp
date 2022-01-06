/* 
 * File:   rlu.c
 * Taken from https://github.com/rlu-sync/rlu/ (ae47f5b  on Aug 31, 2015)
 * 
 * 
 * Alexander Matveev, Nir Shavit, Pascal Felber, and Patrick Marlier. 2015. 
 * Read-log-update: a lightweight synchronization mechanism for concurrent programming. 
 * In Proceedings of the 25th Symposium on Operating Systems Principles (SOSP '15)
 */

/////////////////////////////////////////////////////////////////////////////////////////
// INCLUDES
/////////////////////////////////////////////////////////////////////////////////////////

#ifndef KERNEL
# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# define likely(x) __builtin_expect ((x), 1)
# define unlikely(x) __builtin_expect ((x), 0)
//# define free(ptr) freefn(ptr)
//# define malloc(ptr) allocfn(ptr)
#else /* KERNEL */
# include <linux/printk.h>
# include <linux/string.h>
/*# include <linux/slab.h>*/
# include <linux/bug.h>
# define printf(...) pr_info(__VA_ARGS__)
/* TODO */
# define printf_err(...) pr_err(__VA_ARGS__)
# define fprintf(arg, ...) pr_err(__VA_ARGS__)
//# define free(ptr) freefn(ptr) /*kfree(ptr)*/
//# define malloc(size) allocfn(size) /*kmalloc(size, GFP_KERNEL)*/
#endif /* KERNEL */

#include "rlu.h"

/////////////////////////////////////////////////////////////////////////////////////////
// DEFINES - GENERAL
/////////////////////////////////////////////////////////////////////////////////////////

#define MAX_VERSION (LONG_MAX-1)

#define LOCK_ID(th_id) (th_id + 1)

#define WS_INDEX(ws_counter) ((ws_counter) % RLU_MAX_WRITE_SETS)

#define ALIGN_NUMBER (8)
#define ALIGN_MASK (ALIGN_NUMBER-1)
#define PERFORM_ALIGNMENT(obj_size) (obj_size + (ALIGN_NUMBER - (obj_size & ALIGN_MASK)))
#define ALIGN_OBJ_SIZE(obj_size) ((obj_size & ALIGN_MASK) ? PERFORM_ALIGNMENT(obj_size) : obj_size)

#define MOVE_PTR_FORWARD(p_obj, offset) ((intptr_t *)(((volatile unsigned char *)p_obj) + offset))
#define MOVE_PTR_BACK(p_obj, offset) ((intptr_t *)(((volatile unsigned char *)p_obj) - offset))

#define OBJ_HEADER_SIZE (sizeof(rlu_obj_header_t))
#define WS_OBJ_HEADER_SIZE (sizeof(rlu_ws_obj_header_t))

#define OBJ_TO_H(p_obj) ((volatile rlu_obj_header_t *)MOVE_PTR_BACK(p_obj, OBJ_HEADER_SIZE))
#define H_TO_OBJ(p_h_obj) ((volatile intptr_t *)MOVE_PTR_FORWARD(p_h_obj, OBJ_HEADER_SIZE))
#define OBJ_COPY_TO_WS_H(p_obj_copy) ((volatile rlu_ws_obj_header_t *)MOVE_PTR_BACK(p_obj_copy, (OBJ_HEADER_SIZE+WS_OBJ_HEADER_SIZE)))

#define PTR_ID_OBJ_COPY ((intptr_t *)0x12341234)

#define GET_COPY(p_obj) (OBJ_TO_H(p_obj)->p_obj_copy)

#define PTR_IS_LOCKED(p_obj_copy) (p_obj_copy != NULL)
#define PTR_IS_COPY(p_obj_copy) (p_obj_copy == PTR_ID_OBJ_COPY)

#define PTR_GET_WS_HEADER(p_obj_copy) (OBJ_COPY_TO_WS_H(p_obj_copy))

#define WS_GET_THREAD_ID(p_ws_obj_header) (p_ws_obj_header->thread_id)
#define WS_GET_RUN_COUNTER(p_ws_obj_header) (p_ws_obj_header->run_counter)

#define IS_UNLOCKED(p_obj) (!PTR_IS_LOCKED(GET_COPY(p_obj)))
#define IS_COPY(p_obj) (PTR_IS_COPY(GET_COPY(p_obj)))
#define GET_THREAD_ID(p_obj) (WS_GET_THREAD_ID(PTR_GET_WS_HEADER(GET_COPY(p_obj))))

#define GET_ACTUAL(p_obj_copy) (PTR_GET_WS_HEADER(p_obj_copy)->p_obj_actual)
#define FORCE_ACTUAL(p_obj) (IS_COPY(p_obj) ? GET_ACTUAL(p_obj) : p_obj)

#define TRY_CAS_PTR_OBJ_COPY(p_obj, new_ptr_obj_copy) (CAS((volatile int64_t *)&(OBJ_TO_H(p_obj)->p_obj_copy), 0, (int64_t)new_ptr_obj_copy) == 0)
#define UNLOCK(p_obj) OBJ_TO_H(p_obj)->p_obj_copy = NULL

#ifdef __x86_64
# define RLU_CACHE_LINE_SIZE (64)
#elif defined(__PPC64__)
# define RLU_CACHE_LINE_SIZE (128)
#endif

/////////////////////////////////////////////////////////////////////////////////////////
// DEFINES - ATOMICS
/////////////////////////////////////////////////////////////////////////////////////////
/* TODO for PPC */
#define CPU_RELAX() asm volatile("pause\n": : :"memory");
#define MEMBARSTLD() __sync_synchronize()
#define FETCH_AND_ADD(addr, v) __sync_fetch_and_add((addr), (v))
#define CAS(addr, expected_value, new_value) __sync_val_compare_and_swap((addr), (expected_value), (new_value))


/////////////////////////////////////////////////////////////////////////////////////////
// DEFINES - ASSERT AND DEBUG
/////////////////////////////////////////////////////////////////////////////////////////
#define RLU_TRACE_GLOBAL(fmt, ...) \
    fprintf(stderr, "%s:%d:%s(): " fmt, \
            __FILE__, __LINE__, __func__, \
            __VA_ARGS__);

#define RLU_TRACE(self, fmt, ...) \
    fprintf(stderr, "[%ld][%ld][%ld]:%s:%d:%s(): " fmt, \
            self->uniq_id, self->run_counter, self->local_version, \
			__FILE__, __LINE__, __func__, \
            __VA_ARGS__);

#define RLU_ASSERT(cond) \
    if (unlikely(!(cond))) { \
        printf ("\n-----------------------------------------------\n"); \
        printf ("\nAssertion failure: %s:%d '%s'\n", __FILE__, __LINE__, #cond); \
        abort(); \
    }

#define RLU_ASSERT_MSG(cond, self, fmt, ...) \
    if (unlikely(!(cond))) { \
        printf ("\n-----------------------------------------------\n"); \
        printf ("\nAssertion failure: %s:%d '%s'\n", __FILE__, __LINE__, #cond); \
        RLU_TRACE(self, fmt, __VA_ARGS__); \
        abort(); \
    }

#ifdef RLU_ENABLE_TRACE_1
#define TRACE_1(self, fmt, ...) RLU_TRACE(self, fmt, __VA_ARGS__)
#else
#define TRACE_1(self, fmt, ...)
#endif
#ifdef RLU_ENABLE_TRACE_2
#define TRACE_2(self, fmt, ...) RLU_TRACE(self, fmt, __VA_ARGS__)
#else
#define TRACE_2(self, fmt, ...)
#endif
#ifdef RLU_ENABLE_TRACE_3
#define TRACE_3(self, fmt, ...) RLU_TRACE(self, fmt, __VA_ARGS__)
#define TRACE_3_GLOBAL(fmt, ...) RLU_TRACE_GLOBAL(fmt, __VA_ARGS__)
#else
#define TRACE_3(self, fmt, ...)
#define TRACE_3_GLOBAL(fmt, ...)
#endif

#define Q_ITERS_LIMIT (100000000)

/////////////////////////////////////////////////////////////////////////////////////////
// TYPES
/////////////////////////////////////////////////////////////////////////////////////////
typedef struct rlu_data {
	volatile long n_starts;
	volatile long n_finish;
	volatile long n_writers;

	volatile long n_writer_writeback;
	volatile long n_writeback_q_iters;
	volatile long n_pure_readers;
	volatile long n_steals;
	volatile long n_aborts;
	volatile long n_sync_requests;
	volatile long n_sync_and_writeback;

} rlu_data_t;

/////////////////////////////////////////////////////////////////////////////////////////
// GLOBALS
/////////////////////////////////////////////////////////////////////////////////////////
static volatile rlu_data_t g_rlu_data = {0,};

static volatile int g_rlu_type = 0;
static volatile int g_rlu_max_write_sets = 0;

static volatile long g_rlu_cur_threads = 0;
static volatile rlu_thread_data_t *g_rlu_threads[RLU_MAX_THREADS*4] = {0,};

static volatile long g_rlu_writer_locks[RLU_MAX_WRITER_LOCKS] = {0,};

static volatile long g_rlu_array[RLU_CACHE_LINE_SIZE * 64] = {0,};

#define g_rlu_writer_version g_rlu_array[RLU_CACHE_LINE_SIZE * 2]
#define g_rlu_commit_version g_rlu_array[RLU_CACHE_LINE_SIZE * 4]

//static MEMMGMT_T * recmgr = NULL;

// segregated malloc
static void* dummy_thr(void *p) { return 0; }
#include <dlfcn.h>
#include <pthread.h>
static void* (*allocfn)(size_t size);
static void (*freefn)(void *ptr);

/////////////////////////////////////////////////////////
// HELPER FUNCTIONS
/////////////////////////////////////////////////////////
#ifdef KERNEL
static void abort(void)
{
	BUG();
	/* if that doesn't kill us, halt */
	panic("Oops failed to kill thread");
}
#endif /* KERNEL */

/////////////////////////////////////////////////////////////////////////////////////////
// INTERNAL FUNCTIONS
/////////////////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////////////////
// Writer locks
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_reset_writer_locks(rlu_thread_data_t *self, long ws_id) {
	self->obj_write_set[ws_id].writer_locks.size = 0;
}

static void rlu_add_writer_lock(rlu_thread_data_t *self, long writer_lock_id) {
	int i;
	long n_locks;
	
	n_locks = self->obj_write_set[self->ws_cur_id].writer_locks.size;
	for (i = 0; i < n_locks; i++) {
		RLU_ASSERT(self->obj_write_set[self->ws_cur_id].writer_locks.ids[i] != writer_lock_id);
	}
	
	self->obj_write_set[self->ws_cur_id].writer_locks.ids[n_locks] = writer_lock_id;
	self->obj_write_set[self->ws_cur_id].writer_locks.size++;
	
	RLU_ASSERT(self->obj_write_set[self->ws_cur_id].writer_locks.size < RLU_MAX_NESTED_WRITER_LOCKS);
}

static int rlu_try_acquire_writer_lock(rlu_thread_data_t *self, long writer_lock_id) {
	volatile long cur_lock;
	
	cur_lock = g_rlu_writer_locks[writer_lock_id];
	if (cur_lock == 0) {
		if (CAS(&g_rlu_writer_locks[writer_lock_id], 0, LOCK_ID(self->uniq_id)) == 0) {
			return 1;
		}
	}
	
	return 0;	
}

static void rlu_release_writer_lock(rlu_thread_data_t *self, long writer_lock_id) {
	RLU_ASSERT(g_rlu_writer_locks[writer_lock_id] == LOCK_ID(self->uniq_id));
	
	g_rlu_writer_locks[writer_lock_id] = 0;
}

static void rlu_release_writer_locks(rlu_thread_data_t *self, int ws_id) {
	int i;
	
	for (i = 0; i < self->obj_write_set[ws_id].writer_locks.size; i++) {
		rlu_release_writer_lock(self, self->obj_write_set[ws_id].writer_locks.ids[i]);
	}
}

/////////////////////////////////////////////////////////////////////////////////////////
// Write-set processing
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_reset(rlu_thread_data_t *self) {
	self->is_write_detected = 0;
	self->is_steal = 1;
	self->is_check_locks = 1;

}

static void rlu_reset_write_set(rlu_thread_data_t *self, long ws_counter) {
	long ws_id = WS_INDEX(ws_counter);

	self->obj_write_set[ws_id].num_of_objs = 0;
	self->obj_write_set[ws_id].p_cur = (intptr_t *)&(self->obj_write_set[ws_id].buffer[0]);
	
	rlu_reset_writer_locks(self, ws_id);
}

static intptr_t *rlu_add_ws_obj_header_to_write_set(rlu_thread_data_t *self, intptr_t *p_obj, obj_size_t obj_size) {
	intptr_t *p_cur;
	rlu_ws_obj_header_t *p_ws_obj_h;
	rlu_obj_header_t *p_obj_h;

	p_cur = (intptr_t *)self->obj_write_set[self->ws_cur_id].p_cur;

	p_ws_obj_h = (rlu_ws_obj_header_t *)p_cur;

	p_ws_obj_h->p_obj_actual = p_obj;
	p_ws_obj_h->obj_size = obj_size;
	p_ws_obj_h->run_counter = self->run_counter;
	p_ws_obj_h->thread_id = self->uniq_id;

	p_cur = MOVE_PTR_FORWARD(p_cur, WS_OBJ_HEADER_SIZE);

	p_obj_h = (rlu_obj_header_t *)p_cur;
	p_obj_h->p_obj_copy = PTR_ID_OBJ_COPY;

	p_cur = MOVE_PTR_FORWARD(p_cur, OBJ_HEADER_SIZE);

	self->obj_write_set[self->ws_cur_id].p_cur = p_cur;

	return p_cur;

}

static void rlu_add_obj_copy_to_write_set(rlu_thread_data_t *self, intptr_t *p_obj, obj_size_t obj_size) {
	intptr_t *p_cur;
	long cur_ws_size;

	p_cur = (intptr_t *)self->obj_write_set[self->ws_cur_id].p_cur;

	memcpy((unsigned char *)p_cur, (unsigned char *)p_obj, obj_size);

	p_cur = MOVE_PTR_FORWARD(p_cur, ALIGN_OBJ_SIZE(obj_size));

	self->obj_write_set[self->ws_cur_id].p_cur = p_cur;
	self->obj_write_set[self->ws_cur_id].num_of_objs++;

	cur_ws_size = (long)p_cur - (long)self->obj_write_set[self->ws_cur_id].buffer;
	RLU_ASSERT(cur_ws_size < RLU_MAX_WRITE_SET_BUFFER_SIZE);
}

static void rlu_writeback_write_set(rlu_thread_data_t *self, long ws_counter) {
	unsigned int i;
	long ws_id;
	obj_size_t obj_size;
	intptr_t *p_cur;
	intptr_t *p_obj_copy;
	intptr_t *p_obj_actual;
	rlu_ws_obj_header_t *p_ws_obj_h;
	rlu_obj_header_t *p_obj_h;

	ws_id = WS_INDEX(ws_counter);

	p_cur = (intptr_t *)&(self->obj_write_set[ws_id].buffer[0]);

	for (i = 0; i < self->obj_write_set[ws_id].num_of_objs; i++) {

		p_ws_obj_h = (rlu_ws_obj_header_t *)p_cur;

		p_obj_actual = (intptr_t *)p_ws_obj_h->p_obj_actual;
		obj_size = (obj_size_t)p_ws_obj_h->obj_size;

		p_cur = MOVE_PTR_FORWARD(p_cur, WS_OBJ_HEADER_SIZE);
		p_obj_h = (rlu_obj_header_t *)p_cur;

		RLU_ASSERT(p_obj_h->p_obj_copy == PTR_ID_OBJ_COPY);

		p_cur = MOVE_PTR_FORWARD(p_cur, OBJ_HEADER_SIZE);

		p_obj_copy = (intptr_t *)p_cur;

		TRACE_2(self, "[%ld] rlu_writeback_and_unlock: copy [%p] <- [%p] [%zu]\n",
			self->writer_version, p_obj_actual, p_obj_copy, obj_size);

		memcpy((unsigned char *)p_obj_actual, (unsigned char *)p_obj_copy, obj_size);

		p_cur = MOVE_PTR_FORWARD(p_cur, ALIGN_OBJ_SIZE(obj_size));

		RLU_ASSERT_MSG(GET_THREAD_ID(p_obj_actual) == self->uniq_id,
			self, "th_id = %ld my_id = %ld\n p_obj_actual = %p num_of_objs = %u\n",
			GET_THREAD_ID(p_obj_actual), self->uniq_id, p_obj_actual, self->obj_write_set[ws_id].num_of_objs);

		UNLOCK(p_obj_actual);

	}

	RLU_ASSERT(p_cur == self->obj_write_set[ws_id].p_cur);
}

static int rlu_writeback_write_sets_and_unlock(rlu_thread_data_t *self) {
	long ws_wb_num;
	long ws_counter;

	for (ws_counter = self->ws_head_counter; ws_counter < self->ws_wb_counter; ws_counter++) {
		rlu_reset_write_set(self, ws_counter);
	}

	self->ws_head_counter = self->ws_wb_counter;

	ws_wb_num = 0;
	for (ws_counter = self->ws_wb_counter; ws_counter < self->ws_tail_counter; ws_counter++) {
		rlu_writeback_write_set(self, ws_counter);
		ws_wb_num++;
	}

	self->ws_wb_counter = self->ws_tail_counter;

	return ws_wb_num;

}

static void rlu_unlock_objs(rlu_thread_data_t *self, int ws_counter) {
	unsigned int i;
	long ws_id;
	obj_size_t obj_size;
	intptr_t *p_cur;
	intptr_t *p_obj_actual;
	rlu_ws_obj_header_t *p_ws_obj_h;
	rlu_obj_header_t *p_obj_h;

	ws_id = WS_INDEX(ws_counter);

	p_cur = (intptr_t *)&(self->obj_write_set[ws_id].buffer[0]);

	for (i = 0; i < self->obj_write_set[ws_id].num_of_objs; i++) {

		p_ws_obj_h = (rlu_ws_obj_header_t *)p_cur;

		p_obj_actual = (intptr_t *)p_ws_obj_h->p_obj_actual;
		obj_size = p_ws_obj_h->obj_size;

		p_cur = MOVE_PTR_FORWARD(p_cur, WS_OBJ_HEADER_SIZE);
		p_obj_h = (rlu_obj_header_t *)p_cur;

		RLU_ASSERT(p_obj_h->p_obj_copy == PTR_ID_OBJ_COPY);

		p_cur = MOVE_PTR_FORWARD(p_cur, OBJ_HEADER_SIZE);

		RLU_ASSERT(GET_COPY(p_obj_actual) == p_cur);

		p_cur = MOVE_PTR_FORWARD(p_cur, ALIGN_OBJ_SIZE(obj_size));

		UNLOCK(p_obj_actual);
	}
}

/////////////////////////////////////////////////////////////////////////////////////////
// Thread asserts
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_assert_in_section(rlu_thread_data_t *self) {
	RLU_ASSERT(self->run_counter & 0x1);
}

/////////////////////////////////////////////////////////////////////////////////////////
// Thread register and unregister
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_register_thread(rlu_thread_data_t *self) {
	RLU_ASSERT((self->run_counter & 0x1) == 0);

	FETCH_AND_ADD(&self->run_counter, 1);

	self->local_version = g_rlu_writer_version;
	self->local_commit_version = g_rlu_commit_version;
}

static void rlu_unregister_thread(rlu_thread_data_t *self) {
	RLU_ASSERT((self->run_counter & 0x1) != 0);

	self->run_counter++;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Free buffer processing
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_process_free(rlu_thread_data_t *self) {
	int i;
	intptr_t *p_obj;

	TRACE_3(self, "start free process free_nodes_size = %ld.\n", self->free_nodes_size);

	for (i = 0; i < self->free_nodes_size; i++) {
		p_obj = self->free_nodes[i];

		RLU_ASSERT_MSG(IS_UNLOCKED(p_obj),
			self, "object is locked. p_obj = %p th_id = %ld\n",
			p_obj, GET_THREAD_ID(p_obj));

		TRACE_3(self, "freeing: p_obj = %p, p_actual = %p\n",
			p_obj, (intptr_t *)OBJ_TO_H(p_obj));

		freefn((intptr_t *)OBJ_TO_H(p_obj));
	}

	self->free_nodes_size = 0;

}

/////////////////////////////////////////////////////////////////////////////////////////
// Sync
/////////////////////////////////////////////////////////////////////////////////////////
static void rlu_init_quiescence(rlu_thread_data_t *self) {
	int th_id;

	MEMBARSTLD();

	for (th_id = 0; th_id < g_rlu_cur_threads; th_id++) {

		self->q_threads[th_id].is_wait = 0;

		if (th_id == self->uniq_id) {
			// No need to wait for myself
			continue;
		}

		if (g_rlu_threads[th_id] == NULL) {
			// No need to wait for uninitialized threads
			continue;
		}

		self->q_threads[th_id].run_counter = g_rlu_threads[th_id]->run_counter;

		if (self->q_threads[th_id].run_counter & 0x1) {
			// The other thread is running -> wait for the thread
			self->q_threads[th_id].is_wait = 1;	
		}
	}
}

static long rlu_wait_for_quiescence(rlu_thread_data_t *self, long version_limit) {
	int th_id;
	long iters;
	long cur_threads;

	iters = 0;
	cur_threads = g_rlu_cur_threads;
	for (th_id = 0; th_id < cur_threads; th_id++) {

		while (self->q_threads[th_id].is_wait) {
			iters++;

			if (self->q_threads[th_id].run_counter != g_rlu_threads[th_id]->run_counter) {
				self->q_threads[th_id].is_wait = 0;
				break;
			}

			if (version_limit) {
				if (g_rlu_threads[th_id]->local_version >= version_limit) {
					self->q_threads[th_id].is_wait = 0;
					break;
				}
			}

			if (iters > Q_ITERS_LIMIT) {
				iters = 0;
				printf("[%ld] waiting for [%d] with: local_version = %ld , run_cnt = %ld\n", self->uniq_id, th_id,
					g_rlu_threads[th_id]->local_version, g_rlu_threads[th_id]->run_counter);
			}

			CPU_RELAX();

		}
	}


	return iters;
}

static void rlu_synchronize(rlu_thread_data_t *self) {
	long q_iters;

	if (self->is_no_quiescence) {
		return;
	}

	rlu_init_quiescence(self);

	q_iters = rlu_wait_for_quiescence(self, self->writer_version);

	self->n_writeback_q_iters += q_iters;

}

static void rlu_sync_and_writeback(rlu_thread_data_t *self) {
	long ws_num;
	long ws_wb_num;

	RLU_ASSERT((self->run_counter & 0x1) == 0);

	if (self->ws_tail_counter == self->ws_head_counter) {
		return;
	}

	self->n_sync_and_writeback++;

	ws_num = self->ws_tail_counter - self->ws_wb_counter;

	self->writer_version = g_rlu_writer_version + 1;
        FETCH_AND_ADD(&g_rlu_writer_version, 1);

	TRACE_1(self, "start, ws_num = %ld writer_version = %ld\n",
		ws_num, self->writer_version);

	rlu_synchronize(self);

	ws_wb_num = rlu_writeback_write_sets_and_unlock(self);

	RLU_ASSERT_MSG(ws_num == ws_wb_num, self, "failed: %ld != %ld\n", ws_num, ws_wb_num);
        
	self->last_writer_version = self->writer_version;

	self->writer_version = MAX_VERSION;

	FETCH_AND_ADD(&g_rlu_commit_version, 1);

	if (self->is_sync) {
		self->is_sync = 0;
	}

	rlu_process_free(self);

}

static void rlu_send_sync_request(int other_th_id) {
	g_rlu_threads[other_th_id]->is_sync++;
	MEMBARSTLD();
}

static void rlu_commit_write_set(rlu_thread_data_t *self) {
	self->n_writers++;
	self->n_writer_writeback++;

	// Move to the next write-set
	self->ws_tail_counter++;
	self->ws_cur_id = WS_INDEX(self->ws_tail_counter);

	// Sync and writeback when:
	// (1) All write-sets are full
	// (2) Aggregared MAX_ACTUAL_WRITE_SETS
	if ((WS_INDEX(self->ws_tail_counter) == WS_INDEX(self->ws_head_counter)) ||
		((self->ws_tail_counter - self->ws_wb_counter) >= self->max_write_sets)) {
		rlu_sync_and_writeback(self);
	}

	RLU_ASSERT(self->ws_tail_counter > self->ws_head_counter);
	RLU_ASSERT(WS_INDEX(self->ws_tail_counter) != WS_INDEX(self->ws_head_counter));
}

/////////////////////////////////////////////////////////////////////////////////////////
// EXTERNAL FUNCTIONS
/////////////////////////////////////////////////////////////////////////////////////////
void rlu_init(int type, int max_write_sets) {

        // create segregated jemalloc
	char *lib = getenv("TREE_MALLOC");
	if (!lib) {
		printf("no TREE_MALLOC defined: using default!\n");
                allocfn = malloc;
                freefn = free;
		return;
	}
	void *h = dlopen(lib, RTLD_NOW | RTLD_LOCAL);
	if (!h) {
		fprintf(stderr, "unable to load '%s': %s\n", lib, dlerror());
		exit(1);
	}

	// If the allocator exports pthread_create(), we assume it does so to detect
	// multi-threading (through interposition on pthread_create()) and so call
	// this function (since it might not be called otherwise, if the standard
	// allocator does a similar trick).
	int (*pthread_create)(pthread_t *, const pthread_attr_t *, void *(*) (void *), void *);
	pthread_create = (__typeof(pthread_create)) dlsym(h, "pthread_create");
	if (pthread_create) {
		pthread_t thr;
		pthread_create(&thr, NULL, dummy_thr, NULL);
		pthread_join(thr, NULL);
	}
	allocfn = (__typeof(allocfn)) dlsym(h, "malloc");
	if (!allocfn) {
		fprintf(stderr, "unable to resolve malloc\n");
		exit(1);
	}
	freefn = (__typeof(freefn)) dlsym(h, "free");
	if (!freefn) {
		fprintf(stderr, "unable to resolve free\n");
		exit(1);
	}
    
        //recmgr = new MEMMGMT_T(RLU_MAX_THREADS, SIGQUIT);
	g_rlu_writer_version = 0;
	g_rlu_commit_version = 0;
        g_rlu_cur_threads = 0;
	
	if (type == RLU_TYPE_COARSE_GRAINED) {
		g_rlu_type = RLU_TYPE_COARSE_GRAINED;
		g_rlu_max_write_sets = 1;
		printf("RLU - COARSE_GRAINED initialized\n");
		
	} else if (type == RLU_TYPE_FINE_GRAINED) {
		g_rlu_type = RLU_TYPE_FINE_GRAINED;
		g_rlu_max_write_sets = max_write_sets;
		printf("RLU - FINE_GRAINED initialized [max_write_sets = %d]\n", g_rlu_max_write_sets);
		
	} else {
		RLU_TRACE_GLOBAL("unknown type [%d]", type);
		abort();
	}

	RLU_ASSERT(RLU_MAX_WRITE_SETS >= 2);	
	RLU_ASSERT(max_write_sets >= 1);
	RLU_ASSERT(max_write_sets * 2 <= RLU_MAX_WRITE_SETS);
}

void rlu_finish(void) { }

void rlu_print_stats(void) {
	printf("=================================================\n");
	printf("RLU statistics:\n");
	printf("-------------------------------------------------\n");
	printf("  t_starts = %lu\n", g_rlu_data.n_starts);
	printf("  t_finish = %lu\n", g_rlu_data.n_finish);
	printf("  t_writers = %lu\n", g_rlu_data.n_writers);
	printf("-------------------------------------------------\n");
	printf("  t_writer_writebacks = %lu\n", g_rlu_data.n_writer_writeback);
	printf("  t_writeback_q_iters = %lu\n", g_rlu_data.n_writeback_q_iters);
	if (g_rlu_data.n_writer_writeback > 0) {
		printf("  a_writeback_q_iters = %lu\n", g_rlu_data.n_writeback_q_iters / g_rlu_data.n_writer_writeback);
	} else {
		printf("  a_writeback_q_iters = 0\n");
	}
	printf("  t_pure_readers = %lu\n", g_rlu_data.n_pure_readers);
	printf("  t_steals = %lu\n", g_rlu_data.n_steals);
	printf("  t_aborts = %lu\n", g_rlu_data.n_aborts);
	printf("  t_sync_requests = %lu\n", g_rlu_data.n_sync_requests);
	printf("  t_sync_and_writeback = %lu\n", g_rlu_data.n_sync_and_writeback);

	printf("=================================================\n");
}

void rlu_thread_init(rlu_thread_data_t *self) {
	int ws_counter;
               
	memset(self, 0, sizeof(rlu_thread_data_t));
	
	self->type = g_rlu_type;
	self->max_write_sets = g_rlu_max_write_sets;

	self->uniq_id = FETCH_AND_ADD(&g_rlu_cur_threads, 1);

	self->local_version = 0;
        self->writer_version = MAX_VERSION;

	for (ws_counter = 0; ws_counter < RLU_MAX_WRITE_SETS; ws_counter++) {
		rlu_reset_write_set(self, ws_counter);
	}

	g_rlu_threads[self->uniq_id] = self;
	MEMBARSTLD();

}

void rlu_thread_finish(rlu_thread_data_t *self) {
	rlu_sync_and_writeback(self);
	rlu_sync_and_writeback(self);

	FETCH_AND_ADD(&g_rlu_data.n_starts, self->n_starts);
	FETCH_AND_ADD(&g_rlu_data.n_finish, self->n_finish);
	FETCH_AND_ADD(&g_rlu_data.n_writers, self->n_writers);
	FETCH_AND_ADD(&g_rlu_data.n_writer_writeback, self->n_writer_writeback);
	FETCH_AND_ADD(&g_rlu_data.n_writeback_q_iters, self->n_writeback_q_iters);
	FETCH_AND_ADD(&g_rlu_data.n_pure_readers, self->n_pure_readers);
	FETCH_AND_ADD(&g_rlu_data.n_steals, self->n_steals);
	FETCH_AND_ADD(&g_rlu_data.n_aborts, self->n_aborts);
	FETCH_AND_ADD(&g_rlu_data.n_sync_requests, self->n_sync_requests);
	FETCH_AND_ADD(&g_rlu_data.n_sync_and_writeback, self->n_sync_and_writeback);
}

intptr_t *rlu_alloc(obj_size_t obj_size) {
	intptr_t *ptr;
	rlu_obj_header_t *p_obj_h;

	ptr = (intptr_t *)allocfn(OBJ_HEADER_SIZE + obj_size);
	if (ptr == NULL) {
		return NULL;
	}
	p_obj_h = (rlu_obj_header_t *)ptr;
	p_obj_h->p_obj_copy = NULL;

	TRACE_3_GLOBAL("ptr=%p full_size=%zu ptr_to_obj=%p\n",
		ptr, (OBJ_HEADER_SIZE + obj_size), (intptr_t *)H_TO_OBJ(p_obj_h));

	return (intptr_t *)H_TO_OBJ(p_obj_h);
}

void rlu_free(rlu_thread_data_t *self, intptr_t *p_obj) {
	if (p_obj == NULL) {
		return;
	}

	if (self == NULL) {
		freefn((intptr_t *)OBJ_TO_H(p_obj));
		return;
	}
	
	rlu_assert_in_section(self);
	
	p_obj = (intptr_t *)FORCE_ACTUAL(p_obj);

	self->free_nodes[self->free_nodes_size] = p_obj;
	self->free_nodes_size++;

	RLU_ASSERT(self->free_nodes_size < RLU_MAX_FREE_NODES);

}

void rlu_sync_checkpoint(rlu_thread_data_t *self) {

	if (likely(!self->is_sync)) {
		return;
	}

	self->n_sync_requests++;
	rlu_sync_and_writeback(self);

}

void rlu_reader_lock(rlu_thread_data_t *self) {
	self->n_starts++;

	rlu_sync_checkpoint(self);

	rlu_reset(self);

	rlu_register_thread(self);

	self->is_steal = 1;
	if ((self->local_version - self->local_commit_version) == 0) {
		self->is_steal = 0;
	}

	self->is_check_locks = 1;
	if (((self->local_version - self->local_commit_version) == 0) && ((self->ws_tail_counter - self->ws_wb_counter) == 0)) {
		self->is_check_locks = 0;
		self->n_pure_readers++;
	}
}

int rlu_try_writer_lock(rlu_thread_data_t *self, int writer_lock_id) {
	RLU_ASSERT(self->type == RLU_TYPE_COARSE_GRAINED);
	
	if (!rlu_try_acquire_writer_lock(self, writer_lock_id)) {
		return 0;
	}
	
	rlu_add_writer_lock(self, writer_lock_id);
	
	return 1;
}

void rlu_reader_unlock(rlu_thread_data_t *self) {
	self->n_finish++;

	rlu_unregister_thread(self);

	if (self->is_write_detected) {
		self->is_write_detected = 0;
		rlu_commit_write_set(self);
		rlu_release_writer_locks(self, WS_INDEX(self->ws_tail_counter - 1));
	} else {
		rlu_release_writer_locks(self, self->ws_cur_id);
		rlu_reset_writer_locks(self, self->ws_cur_id);
	} 
	
	rlu_sync_checkpoint(self);

}

intptr_t *rlu_deref_slow_path(rlu_thread_data_t *self, intptr_t *p_obj) {
	long th_id;
	intptr_t *p_obj_copy;
	volatile rlu_ws_obj_header_t *p_ws_obj_h;

	if (p_obj == NULL) {
		return p_obj;
	}

	p_obj_copy = (intptr_t *)GET_COPY(p_obj);

	if (!PTR_IS_LOCKED(p_obj_copy)) {
		return p_obj;
	}

	if (PTR_IS_COPY(p_obj_copy)) {
		// p_obj points to a copy -> it has been already dereferenced.
		TRACE_1(self, "got pointer to a copy. p_obj = %p p_obj_copy = %p.\n", p_obj, p_obj_copy);
		return p_obj;
	}

	p_ws_obj_h = PTR_GET_WS_HEADER(p_obj_copy);

	th_id = WS_GET_THREAD_ID(p_ws_obj_h);

	if (th_id == self->uniq_id) {
		// p_obj is locked by this thread -> return the copy
		TRACE_1(self, "got pointer to a copy. p_obj = %p th_id = %ld.\n", p_obj, th_id);
		return p_obj_copy;
	}

	// p_obj is locked by another thread
	if ((self->is_steal) &&
		(g_rlu_threads[th_id]->writer_version <= self->local_version)) {
		// This thread started after the other thread updated g_writer_version.
		// and this thread observed a valid p_obj_copy (!= NULL)
		// => The other thread is going to wait for this thread to finish before reusing the write-set log
		//    (to which p_obj_copy points)
		TRACE_1(self, "take copy from other writer th_id = %ld p_obj = %p p_obj_copy = %p\n",
			th_id, p_obj, p_obj_copy);
		self->n_steals++;
		return p_obj_copy;
	}

	return p_obj;
}

int rlu_try_lock(rlu_thread_data_t *self, intptr_t **p_p_obj, size_t obj_size) {
	intptr_t *p_obj;
	intptr_t *p_obj_copy;
	volatile long th_id;
	volatile rlu_ws_obj_header_t *p_ws_obj_h;

	p_obj = *p_p_obj;

	RLU_ASSERT_MSG(p_obj != NULL, self, "[%ld] rlu_try_lock: tried to lock a NULL pointer\n", self->writer_version);

	p_obj_copy = (intptr_t *)GET_COPY(p_obj);

	if (PTR_IS_COPY(p_obj_copy)) {
		TRACE_1(self, "tried to lock a copy of an object. p_obj = %p\n", p_obj);
		TRACE_1(self, " => converting \n => copy: %p\n", p_obj);

		p_obj = (intptr_t *)GET_ACTUAL(p_obj);
		p_obj_copy = (intptr_t *)GET_COPY(p_obj);

		TRACE_1(self, " => real: %p , p_obj_copy = %p\n", p_obj, p_obj_copy);
	}

	if (PTR_IS_LOCKED(p_obj_copy)) {
		p_ws_obj_h = PTR_GET_WS_HEADER(p_obj_copy);

		th_id = WS_GET_THREAD_ID(p_ws_obj_h);

		if (th_id == self->uniq_id) {
			if (self->run_counter == WS_GET_RUN_COUNTER(p_ws_obj_h)) {
				// p_obj already locked by current execution of this thread.
				// => return copy
				TRACE_2(self, "[%ld] already locked by this thread. p_obj = %p th_id = %ld\n",
					self->local_version, p_obj, th_id);
				*p_p_obj = p_obj_copy;
				return 1;
			}

			TRACE_1(self, "[%ld] already locked by another execution of this thread -> fail and sync. p_obj = %p th_id = %ld\n",
				self->local_version, p_obj, th_id);
			// p_obj is locked by another execution of this thread.
			self->is_sync++;
			return 0;
		}

		// p_obj already locked by another thread.
		// => send sync request to the other thread
		// => in the meantime -> sync this thread
		TRACE_1(self, "[%ld] already locked by another thread -> fail and request sync. p_obj = %p th_id = %ld\n",
			self->local_version, p_obj, th_id);

		rlu_send_sync_request(th_id);
		self->is_sync++;
		return 0;
	}

	// p_obj is free

	// Indicate that write-set is updated
	if (self->is_write_detected == 0) {
		self->is_write_detected = 1;
		self->is_check_locks = 1;
	}

	// Add write-set header for the object
	p_obj_copy = rlu_add_ws_obj_header_to_write_set(self, p_obj, obj_size);

	// Try lock p_obj -> install pointer to copy
	if (!TRY_CAS_PTR_OBJ_COPY(p_obj, p_obj_copy)) {
		TRACE_1(self, "[%ld] CAS failed\n", self->local_version);
		return 0;
	}

	// Locked successfully

	// Copy object to write-set
	rlu_add_obj_copy_to_write_set(self, p_obj, obj_size);

	RLU_ASSERT_MSG(GET_COPY(p_obj) == p_obj_copy,
		self, "p_obj_copy = %p my_p_obj_copy = %p\n",
		GET_COPY(p_obj), p_obj_copy);

	TRACE_2(self, "[%ld] p_obj = %p is locked, p_obj_copy = %p , th_id = %ld\n",
		self->local_version, p_obj, GET_COPY(p_obj), GET_THREAD_ID(p_obj));

	*p_p_obj = p_obj_copy;
	return 1;

}

void rlu_lock(rlu_thread_data_t *self, intptr_t **p_p_obj, unsigned int obj_size) {
	RLU_ASSERT(self->type == RLU_TYPE_COARSE_GRAINED);
	
	RLU_ASSERT(rlu_try_lock(self, p_p_obj, obj_size) != 0);
}

void rlu_abort(rlu_thread_data_t *self) {
	self->n_aborts++;

	rlu_unregister_thread(self);

	if (self->is_write_detected) {
		self->is_write_detected = 0;
		rlu_unlock_objs(self, self->ws_tail_counter);
		
		rlu_release_writer_locks(self, self->ws_cur_id);
		rlu_reset_write_set(self, self->ws_tail_counter);
	} else {
		rlu_release_writer_locks(self, self->ws_cur_id);
		rlu_reset_writer_locks(self, self->ws_cur_id);
	}

	rlu_sync_checkpoint(self);

}

int rlu_cmp_ptrs(intptr_t *p_obj_1, intptr_t *p_obj_2) {
	if (p_obj_1 != NULL) {
		p_obj_1 = (intptr_t *)FORCE_ACTUAL(p_obj_1);
	}

	if (p_obj_2 != NULL) {
		p_obj_2 = (intptr_t *)FORCE_ACTUAL(p_obj_2);
	}

	return p_obj_1 == p_obj_2;
}

void rlu_assign_pointer(intptr_t **p_ptr, intptr_t *p_obj) {
	if (p_obj != NULL) {
		p_obj = (intptr_t *)FORCE_ACTUAL(p_obj);
	}

	*p_ptr = p_obj;
}