/*
 * File:   globals_extern.h
 * Author: trbot
 *
 * Created on March 9, 2015, 1:32 PM
 */

#ifndef GLOBALS_EXTERN_H
#define	GLOBALS_EXTERN_H

// enable USE_TRACE if you want low level functionality tracing using std::cout
//#define USE_TRACE

#include <string>

#include "debugprinting.h"
#include "plaf.h"
#include <atomic>

#ifndef __rtm_force_inline
#define __rtm_force_inline __attribute__((always_inline)) inline
#endif

#ifndef DEBUG
#define DEBUG if(0)
#define DEBUG1 if(0)
#define DEBUG2 if(0)
#define DEBUG3 if(0) /* rarely used */
#endif

#ifdef __unix__
#define POSIX_SYSTEM
#else
#error NOT UNIX SYSTEM
#endif

#ifndef TRACE
#ifdef USE_TRACE
extern std::atomic_bool ___trace;
#define TRACE_TOGGLE {bool ___t = ___trace; ___trace = !___t;}
#define TRACE_ON {___trace = true;}
#define TRACE DEBUG if(___trace)
extern std::atomic_bool ___validateops;
#define VALIDATEOPS_ON {___validateops = true;}
#define VALIDATEOPS DEBUG if(___validateops)
#endif
#endif

/**
 * Enable global statistics access using gstats_global.h
 */

#include "gstats_global.h"

/**
 * Setup timing code
 */

#include "server_clock.h"

/**
 * Configure record manager: reclaimer, allocator and pool
 */

#ifndef PRINTS
    #define STR(x) XSTR(x)
    #define XSTR(x) #x
    #define PRINTI(name) { std::cout<<#name<<"="<<name<<std::endl; }
    #define PRINTS(name) { std::cout<<#name<<"="<<STR(name)<<std::endl; }
#endif

#define _EVAL(a) a
#define _PASTE2(a,b) a##b
#define PASTE(a,b) _PASTE2(a,b)
#ifdef RECLAIM_TYPE
    #define RECLAIM PASTE(reclaimer_,RECLAIM_TYPE)
    // #define RECLAIM_DOT_H PASTE(RECLAIM,.h)
    #include STR(RECLAIM.h)
#else
    #define RECLAIM reclaimer_debra
    #include "reclaimer_debra.h"
#endif
#ifdef ALLOC_TYPE
    #define ALLOC PASTE(allocator_,ALLOC_TYPE)
    // #define ALLOC_DOT_H PASTE(ALLOC,.h)
    #include STR(ALLOC.h)
#else
    #define ALLOC allocator_new
    #include "allocator_new.h"
#endif
#ifdef POOL_TYPE
    #define POOL PASTE(pool_,POOL_TYPE)
    // #define POOL_DOT_H PASTE(POOL,.h)
    #include STR(POOL.h)
#else
    #define POOL pool_none
    #include "pool_none.h"
#endif

#endif	/* GLOBALS_EXTERN_H */
