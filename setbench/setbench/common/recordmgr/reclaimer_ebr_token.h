/**
 * Variant of EBR using token passing.
 *
 * Copyright (C) 2019 Trevor Brown
 *
 */

#ifndef RECLAIM_EBR_TOKEN_H
#define	RECLAIM_EBR_TOKEN_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <sstream>
#include <limits.h>
#include "blockbag.h"
#include "plaf.h"
#include "allocator_interface.h"
#include "reclaimer_interface.h"

// optional statistics tracking
#include "gstats_definitions_epochs.h"

template <typename T = void, class Pool = pool_interface<T> >
class reclaimer_ebr_token : public reclaimer_interface<T, Pool> {
protected:

#ifdef RAPID_RECLAMATION
#else
#endif

    class ThreadData {
    private:
        PAD;
    public:
        volatile int token;
        int tokenCount; // how many times this thread has had the token
        blockbag<T> * curr;
        blockbag<T> * last;
    public:
        ThreadData() {}
    };

    ThreadData threadData[MAX_THREADS_POW2];
    PAD;

public:
    template<typename _Tp1>
    struct rebind {
        typedef reclaimer_ebr_token<_Tp1, Pool> other;
    };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 {
        typedef reclaimer_ebr_token<_Tp1, _Tp2> other;
    };

    inline void getSafeBlockbags(const int tid, blockbag<T> ** bags) {
        setbench_error("unsupported operation");
    }

    long long getSizeInNodes() {
        long long sum = 0;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            sum += threadData[tid].curr->computeSize();
            sum += threadData[tid].last->computeSize();
        }
        return sum;
    }
    std::string getSizeString() {
        std::stringstream ss;
        ss<<getSizeInNodes();
        return ss.str();
    }

    std::string getDetailsString() {
        std::stringstream ss;
        long long sum[2];

        sum[0] = 0;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            sum[0] += threadData[tid].curr->computeSize();
        }
        ss<<sum[0]<<" ";

        sum[1] = 0;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            sum[1] += threadData[tid].last->computeSize();
        }
        ss<<sum[1]<<" ";

        return ss.str();
    }

    inline static bool quiescenceIsPerRecordType() { return false; }

    inline bool isQuiescent(const int tid) {
        return false;
    }

    inline static bool isProtected(const int tid, T * const obj) {
        return true;
    }
    inline static bool isQProtected(const int tid, T * const obj) {
        return false;
    }
    inline static bool protect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool memoryBarrier = true) {
        return true;
    }
    inline static void unprotect(const int tid, T * const obj) {}
    inline static bool qProtect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool memoryBarrier = true) {
        return true;
    }
    inline static void qUnprotectAll(const int tid) {}

    inline static bool shouldHelp() { return true; }

    // rotate the epoch bags and reclaim any objects retired two epochs ago.
    inline void rotateEpochBags(const int tid) {
        blockbag<T> * const freeable = threadData[tid].last;
#ifdef GSTATS_HANDLE_STATS
        GSTATS_APPEND(tid, limbo_reclamation_event_size, freeable->computeSize());
        GSTATS_ADD(tid, limbo_reclamation_event_count, 1);
#endif
        this->pool->addMoveFullBlocks(tid, freeable); // moves any full blocks (may leave a non-full block behind)
        SOFTWARE_BARRIER;

        // swap curr and last
        threadData[tid].last = threadData[tid].curr;
        threadData[tid].curr = freeable;
    }

    template <typename... Rest>
    class BagRotator {
    public:
        BagRotator() {}
        inline void rotateAllEpochBags(const int tid, void * const * const reclaimers, const int i) {
        }
    };

    template <typename First, typename... Rest>
    class BagRotator<First, Rest...> : public BagRotator<Rest...> {
    public:
        inline void rotateAllEpochBags(const int tid, void * const * const reclaimers, const int i) {
            typedef typename Pool::template rebindAlloc<First>::other classAlloc;
            typedef typename Pool::template rebind2<First, classAlloc>::other classPool;

            ((reclaimer_ebr_token<First, classPool> * const) reclaimers[i])->rotateEpochBags(tid);
            ((BagRotator<Rest...> *) this)->rotateAllEpochBags(tid, reclaimers, 1+i);
        }
    };

    // returns true if the call rotated the epoch bags for thread tid
    template <typename First, typename... Rest>
    inline bool startOp(const int tid, void * const * const reclaimers, const int numReclaimers, const bool readOnly = false) {
        SOFTWARE_BARRIER; // prevent token passing from happening before we are really quiescent

        bool result = false;
        if (threadData[tid].token) {
// #if defined GSTATS_HANDLE_STATS
//             GSTATS_APPEND(tid, token_received_time_split_ms, GSTATS_TIMER_SPLIT(tid, timersplit_token_received)/1000000);
//             GSTATS_SET_IX(tid, token_received_time_last_ms, GSTATS_TIMER_ELAPSED(tid, timer_bag_rotation_start)/1000000, 0);
// #endif

            ++threadData[tid].tokenCount;

            // pass token
            threadData[tid].token = 0;
            threadData[(tid+1) % this->NUM_PROCESSES].token = 1;
            __sync_synchronize();

// #if defined GSTATS_HANDLE_STATS
//             auto startTime = GSTATS_TIMER_ELAPSED(tid, timer_bag_rotation_start)/1000;
//             GSTATS_APPEND(tid, bag_rotation_start_time_us, startTime);
//             GSTATS_APPEND(tid, bag_rotation_reclaim_size, threadData[tid].last->computeSize());
// #endif
            // rotate bags to reclaim everything retired *before* our last increment,
            // unless tokenCount is 1, in which case there is no last increment.
            // TODO: does it matter? last bag should be empty in this case.
            //       maybe it does, because after rotating it might not be...
            //       on the other hand, those objects *are* retired before this increment...
            //       so, it seems like the if-statement isn't needed.
            //if (threadData[tid].tokenCount > 1) {
                BagRotator<First, Rest...> rotator;
                rotator.rotateAllEpochBags(tid, reclaimers, 0);
                result = true;
            //}

// #if defined GSTATS_HANDLE_STATS
//             auto endTime = GSTATS_TIMER_ELAPSED(tid, timer_bag_rotation_start)/1000;
//             GSTATS_APPEND(tid, bag_rotation_end_time_us, endTime);
//             GSTATS_APPEND(tid, bag_rotation_duration_split_ms, (endTime - startTime)/1000);
// #endif
        }

        // in common case, this is lightning fast...
        return result;
    }

    inline void endOp(const int tid) {

    }

    // for all schemes except reference counting
    inline void retire(const int tid, T* p) {
        threadData[tid].curr->add(p);
        DEBUG2 this->debug->addRetired(tid, 1);
    }

    void debugPrintStatus(const int tid) {
//        if (tid == 0) {
//            std::cout<<"this->NUM_PROCESSES="<<this->NUM_PROCESSES<<std::endl;
//        }
//        std::cout<<"token_counts_tid"<<tid<<"="<<threadData[tid].tokenCount<<std::endl;
//        std::cout<<"bag_curr_size_tid"<<tid<<"="<<threadData[tid].curr->computeSize()<<std::endl;
//        std::cout<<"bag_last_size_tid"<<tid<<"="<<threadData[tid].last->computeSize()<<std::endl;
// #if defined GSTATS_HANDLE_STATS
//         GSTATS_APPEND(tid, bag_curr_size, threadData[tid].curr->computeSize());
//         GSTATS_APPEND(tid, bag_last_size, threadData[tid].last->computeSize());
//         GSTATS_APPEND(tid, token_counts, threadData[tid].tokenCount);
// #endif
    }

    void initThread(const int tid) {
        if (threadData[tid].curr == NULL) {
            threadData[tid].curr = new blockbag<T>(tid, this->pool->blockpools[tid]);
        }
        if (threadData[tid].last == NULL) {
            threadData[tid].last = new blockbag<T>(tid, this->pool->blockpools[tid]);
        }
#ifdef GSTATS_HANDLE_STATS
        GSTATS_CLEAR_TIMERS;
#endif
    }

    void deinitThread(const int tid) {
        // WARNING: this moves objects to the pool immediately,
        // which is only safe if this thread is deinitializing specifically
        // because *ALL THREADS* have already finished accessing
        // the data structure and are now quiescent!!
        if (threadData[tid].curr) {
            this->pool->addMoveAll(tid, threadData[tid].curr);
            delete threadData[tid].curr;
            threadData[tid].curr = NULL;
        }
        if (threadData[tid].last) {
            this->pool->addMoveAll(tid, threadData[tid].last);
            delete threadData[tid].last;
            threadData[tid].last = NULL;
        }
    }

    reclaimer_ebr_token(const int numProcesses, Pool *_pool, debugInfo * const _debug, RecoveryMgr<void *> * const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE std::cout<<"constructor reclaimer_ebr_token helping="<<this->shouldHelp()<<std::endl;// scanThreshold="<<scanThreshold<<std::endl;
        for (int tid=0;tid<numProcesses;++tid) {
            threadData[tid].token       = (tid == 0 ? 1 : 0); // first thread starts with the token
            threadData[tid].tokenCount  = 0; // thread with token will update this itself
            threadData[tid].curr = NULL;
            threadData[tid].last = NULL;
        }
    }
    ~reclaimer_ebr_token() {
//        VERBOSE DEBUG std::cout<<"destructor reclaimer_ebr_token"<<std::endl;
//
////        std::cout<<"token_counts=";
////        for (int tid=0;tid<this->NUM_PROCESSES;++tid) std::cout<<threadData[tid].tokenCount<<" ";
////        std::cout<<std::endl;
////
////        std::cout<<"bag_curr_size=";
////        for (int tid=0;tid<this->NUM_PROCESSES;++tid) std::cout<<threadData[tid].curr->computeSize()<<" ";
////        std::cout<<std::endl;
////
////        std::cout<<"bag_last_size=";
////        for (int tid=0;tid<this->NUM_PROCESSES;++tid) std::cout<<threadData[tid].last->computeSize()<<" ";
////        std::cout<<std::endl;
//
//        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
//
//            // move contents of all bags into pool
//
//            if (threadData[tid].curr) {
//                this->pool->addMoveAll(tid, threadData[tid].curr);
//                delete threadData[tid].curr;
//            }
//
//            if (threadData[tid].last) {
//                this->pool->addMoveAll(tid, threadData[tid].last);
//                delete threadData[tid].last;
//            }
//        }
    }

};

#endif

