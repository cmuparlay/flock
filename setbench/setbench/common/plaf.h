/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 * 
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef MACHINECONSTANTS_H
#define	MACHINECONSTANTS_H

#ifndef MAX_THREADS_POW2
    #define MAX_THREADS_POW2 512 // MUST BE A POWER OF TWO, since this is used for some bitwise operations
#endif
#ifndef LOGICAL_PROCESSORS
    #define LOGICAL_PROCESSORS MAX_THREADS_POW2
#endif

#ifndef SOFTWARE_BARRIER
#   define SOFTWARE_BARRIER asm volatile("": : :"memory")
#endif

// the following definition is only used to pad data to avoid false sharing.
// although the number of words per cache line is actually 8, we inflate this
// figure to counteract the effects of prefetching multiple adjacent cache lines.
#define PREFETCH_SIZE_WORDS 16
#define PREFETCH_SIZE_BYTES 128
#define BYTES_IN_CACHE_LINE 64

#define CAT2(x, y) x##y
#define CAT(x, y) CAT2(x, y)

#define PAD64 volatile char CAT(___padding, __COUNTER__)[64]
#define PAD volatile char CAT(___padding, __COUNTER__)[128]

#define CASB __sync_bool_compare_and_swap
#define CASV __sync_val_compare_and_swap
#define FAA __sync_fetch_and_add

#endif	/* MACHINECONSTANTS_H */
