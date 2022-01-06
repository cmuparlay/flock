/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECLAIM_DEBRA_H
#define	RECLAIM_DEBRA_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <sstream>
#include <limits.h>
#include "blockbag.h"
#include "plaf.h"
#include "allocator_interface.h"
#include "reclaimer_interface.h"
#ifdef GSTATS_HANDLE_STATS
#   include "server_clock.h"
#endif

// optional statistics tracking
#include "gstats_definitions_epochs.h"

template <typename T = void, class Pool = pool_interface<T> >
class reclaimer_debra : public reclaimer_interface<T, Pool> {
protected:
#define DEBRA_DISABLE_READONLY_OPT

#define EPOCH_INCREMENT 2
#define BITS_EPOCH(ann) ((ann)&~(EPOCH_INCREMENT-1))
#define QUIESCENT(ann) ((ann)&1)
#define GET_WITH_QUIESCENT(ann) ((ann)|1)

#if !defined DEBRA_ORIGINAL_FREE || !DEBRA_ORIGINAL_FREE
    #define DEAMORTIZE_FREE_CALLS
#endif

// #define DEAMORTIZE_ADAPTIVELY

#ifdef RAPID_RECLAMATION
#define MIN_OPS_BEFORE_READ 1
//#define MIN_OPS_BEFORE_CAS_EPOCH 1
#else
#define MIN_OPS_BEFORE_READ 10
//#define MIN_OPS_BEFORE_CAS_EPOCH 100
#endif

#define NUMBER_OF_EPOCH_BAGS 3 // 9 for range query support
#define NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS 0 // 3 for range query support

    class ThreadData {
    private:
        PAD;
    public:
        std::atomic_long announcedEpoch;
        long localvar_announcedEpoch; // copy of the above, but without the volatile tag, to try to make the read in enterQstate more efficient
    private:
        PAD;
    public:
        blockbag<T> * epochbags[NUMBER_OF_EPOCH_BAGS];
        // note: oldest bag is number (index+1)%NUMBER_OF_EPOCH_BAGS
        int index; // index of currentBag in epochbags for this process
    private:
        PAD;
    public:
        blockbag<T> * currentBag;  // pointer to current epoch bag for this process
#ifdef DEAMORTIZE_FREE_CALLS
        blockbag<T> * deamortizedFreeables;
        int numFreesPerStartOp;
#endif
        int checked;               // how far we've come in checking the announced epochs of other threads
        int opsSinceRead;
        ThreadData() {}
    private:
        PAD;
    };

    PAD;
    ThreadData threadData[MAX_THREADS_POW2];
    PAD;

    // for epoch based reclamation
//    PAD; // not needed after superclass layout
    volatile long epoch;
    PAD;

public:
    template<typename _Tp1>
    struct rebind {
        typedef reclaimer_debra<_Tp1, Pool> other;
    };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 {
        typedef reclaimer_debra<_Tp1, _Tp2> other;
    };

    inline void getSafeBlockbags(const int tid, blockbag<T> ** bags) {
        if (NUMBER_OF_EPOCH_BAGS < 9 || NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS < 3) {
            setbench_error("unsupported operation with these parameters (see if-statement above this line)")
        }
        SOFTWARE_BARRIER;
        int ix = threadData[tid].index;
        bags[0] = threadData[tid].epochbags[ix];
        bags[1] = threadData[tid].epochbags[(ix+NUMBER_OF_EPOCH_BAGS-1)%NUMBER_OF_EPOCH_BAGS];
        bags[2] = threadData[tid].epochbags[(ix+NUMBER_OF_EPOCH_BAGS-2)%NUMBER_OF_EPOCH_BAGS];
        bags[3] = NULL;
        SOFTWARE_BARRIER;
    }

    long long getSizeInNodes() {
        long long sum = 0;
        //std::cout<<"NUM_PROC="<<this->NUM_PROCESSES<<std::endl;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            for (int j=0;j<NUMBER_OF_EPOCH_BAGS;++j) {
                if (threadData[tid].epochbags[j]) {
                    sum += threadData[tid].epochbags[j]->computeSize();
                }
            }
        }
        return sum;
    }
    std::string getSizeString() {
        std::stringstream ss;
        ss<<getSizeInNodes(); //<<" in epoch bags";
        return ss.str();
    }

    std::string getDetailsString() {
        std::stringstream ss;
        long long sum[NUMBER_OF_EPOCH_BAGS];
        for (int j=0;j<NUMBER_OF_EPOCH_BAGS;++j) {
            sum[j] = 0;
            for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
                if (threadData[tid].epochbags[j]) {
                    sum[j] += threadData[tid].epochbags[j]->computeSize();
                }
            }
            ss<<sum[j]<<" ";
        }
        return ss.str();
    }

    inline static bool quiescenceIsPerRecordType() { return false; }

    inline bool isQuiescent(const int tid) {
        return QUIESCENT(threadData[tid].announcedEpoch.load(std::memory_order_relaxed));
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

    // try to clean up: must only be called by a single thread as part of the test harness!
    template <typename First, typename... Rest>
    void debugGCSingleThreaded(void * const * const reclaimers, const int numReclaimers) {
        BagRotator<First, Rest...> rotator;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            printf("tid %3d reclaimer_debra::gc: bags before %6d %6d %6d numReclaimers=%d\n", tid, threadData[tid].epochbags[0]->computeSize(), threadData[tid].epochbags[1]->computeSize(), threadData[tid].epochbags[2]->computeSize(), numReclaimers);
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            printf("tid %3d reclaimer_debra::gc: bags after  %6d %6d %6d numReclaimers=%d\n", tid, threadData[tid].epochbags[0]->computeSize(), threadData[tid].epochbags[1]->computeSize(), threadData[tid].epochbags[2]->computeSize(), numReclaimers);

            /**
             * want to make sure this is actually happening,
             * and we want to prove to ourselves that the bags are empty
             * (or close to empty) when the experiment starts!!!
             */
        }
    }

    // rotate the epoch bags and reclaim any objects retired two epochs ago.
    inline void rotateEpochBags(const int tid) {
        int nextIndex = (threadData[tid].index+1) % NUMBER_OF_EPOCH_BAGS;
        blockbag<T> * const freeable = threadData[tid].epochbags[(nextIndex+NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS) % NUMBER_OF_EPOCH_BAGS];
#ifdef GSTATS_HANDLE_STATS
        GSTATS_APPEND(tid, limbo_reclamation_event_size, freeable->computeSize());
        GSTATS_ADD(tid, limbo_reclamation_event_count, 1);
        TIMELINE_START_C(tid, tid < 96);
        // DURATION_START(tid);
#endif

        int numLeftover = 0;
#ifdef DEAMORTIZE_FREE_CALLS
        auto freelist = threadData[tid].deamortizedFreeables;
        if (!freelist->isEmpty()) {
            numLeftover += (freelist->isEmpty()
                    ? 0
                    : (freelist->getSizeInBlocks()-1)*BLOCK_SIZE + freelist->getHeadSize());

            // // "CATCH-UP" bulk free
            // this->pool->addMoveFullBlocks(tid, freelist);

#   if defined DEAMORTIZE_ADAPTIVELY
            // adaptive deamortized free count
            if (numLeftover >= BLOCK_SIZE) {
                ++threadData[tid].numFreesPerStartOp;
            } else if (numLeftover == 0) {
                --threadData[tid].numFreesPerStartOp;
                if (threadData[tid].numFreesPerStartOp < 1) {
                    threadData[tid].numFreesPerStartOp = 1;
                }
            }
#   endif
        }
        // TIMELINE_BLIP_Llu(tid, "numFreesPerStartOp", threadData[tid].numFreesPerStartOp);
        freelist->appendMoveFullBlocks(freeable);
#else
        numLeftover += (freeable->getSizeInBlocks()-1)*BLOCK_SIZE + freeable->getHeadSize();
        this->pool->addMoveFullBlocks(tid, freeable); // moves any full blocks (may leave a non-full block behind)
#endif
        SOFTWARE_BARRIER;

#ifdef GSTATS_HANDLE_STATS
        // DURATION_END(tid, duration_rotateAndFree);
        // std::stringstream ss;
        // ss<<numLeftover;
        // ss<<numLeftover<<":"<<threadData[tid].numFreesPerStartOp;
        // TIMELINE_END_Ls(tid, "rotateEpochBags", ss.str().c_str()); //threadData[tid].localvar_announcedEpoch);
        if (tid < 96) {
            // TIMELINE_END_Llu(tid, "rotateEpochBags", numLeftover);
            TIMELINE_BLIP_Llu(tid, "freelistAppend", numLeftover);
        }
#endif

        threadData[tid].index = nextIndex;
        threadData[tid].currentBag = threadData[tid].epochbags[nextIndex];
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

            ((reclaimer_debra<First, classPool> * const) reclaimers[i])->rotateEpochBags(tid);
            ((BagRotator<Rest...> *) this)->rotateAllEpochBags(tid, reclaimers, 1+i);
        }
    };

    // objects reclaimed by this epoch manager.
    // returns true if the call rotated the epoch bags for thread tid
    // (and reclaimed any objects retired two epochs ago).
    // otherwise, the call returns false.
    template <typename First, typename... Rest>
    inline bool startOp(const int tid, void * const * const reclaimers, const int numReclaimers, const bool readOnly = false) {
        SOFTWARE_BARRIER; // prevent any bookkeeping from being moved after this point by the compiler.
        bool result = false;

        long readEpoch = epoch;
        const long ann = threadData[tid].localvar_announcedEpoch;
        threadData[tid].localvar_announcedEpoch = readEpoch;

        // if our announced epoch was different from the current epoch
        if (readEpoch != ann /* invariant: ann is not quiescent */) {
            // rotate the epoch bags and
            // reclaim any objects retired two epochs ago.
            threadData[tid].checked = 0;
            BagRotator<First, Rest...> rotator;
            //auto time = get_server_clock();
            //GSTATS_APPEND(tid, thread_reclamation_start, time);
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            //auto time2 = get_server_clock();
            //GSTATS_APPEND(tid, thread_reclamation_end, time);
            //this->template rotateAllEpochBags<First, Rest...>(tid, reclaimers, 0);
            result = true;
        }

#ifdef DEAMORTIZE_FREE_CALLS
        // TODO: make this work for each object type
#   if defined DEAMORTIZE_ADAPTIVELY
        for (int i=0;i<threadData[tid].numFreesPerStartOp;++i) {
            if (!threadData[tid].deamortizedFreeables->isEmpty()) {
                this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
            } else {
                break;
            }
        }
#   else
        if (!threadData[tid].deamortizedFreeables->isEmpty()) {
            this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
        }
        // if (!threadData[tid].deamortizedFreeables->isEmpty()) {
        //     this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
        // }
        // if (!threadData[tid].deamortizedFreeables->isEmpty()) {
        //     this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
        // }
#   endif
#endif

        // we should announce AFTER rotating bags if we're going to do so!!
        // (very problematic interaction with lazy dirty page purging in jemalloc triggered by bag rotation,
        //  which causes massive non-quiescent regions if non-Q announcement happens before bag rotation)
        SOFTWARE_BARRIER;
        threadData[tid].announcedEpoch.store(readEpoch, std::memory_order_relaxed); // note: this must be written, regardless of whether the announced epochs are the same, because the quiescent bit will vary
#if defined GSTATS_HANDLE_STATS
        GSTATS_SET(tid, thread_announced_epoch, readEpoch);
#endif
        SOFTWARE_BARRIER;
        // note: readEpoch, when written to announcedEpoch[tid],
        //       sets the state to non-quiescent and non-neutralized

#ifndef DEBRA_DISABLE_READONLY_OPT
        if (!readOnly) {
#endif
            // incrementally scan the announced epochs of all threads
            if (++threadData[tid].opsSinceRead == MIN_OPS_BEFORE_READ) {
                threadData[tid].opsSinceRead = 0;
                int otherTid = threadData[tid].checked;
                long otherAnnounce = threadData[otherTid].announcedEpoch.load(std::memory_order_relaxed);
                if (BITS_EPOCH(otherAnnounce) == readEpoch || QUIESCENT(otherAnnounce)) {
                    const int c = ++threadData[tid].checked;
                    if (c >= this->NUM_PROCESSES /*&& c > MIN_OPS_BEFORE_CAS_EPOCH*/) {
                        if (__sync_bool_compare_and_swap(&epoch, readEpoch, readEpoch+EPOCH_INCREMENT)) {
#if defined GSTATS_HANDLE_STATS
                            // GSTATS_SET_IX(tid, num_prop_epoch_latency, GSTATS_TIMER_SPLIT(tid, timersplit_epoch), readEpoch+EPOCH_INCREMENT);
                            TIMELINE_BLIP_Llu(tid, "advanceEpoch", readEpoch);
#endif
                        }
                    }
                }
            }
#ifndef DEBRA_DISABLE_READONLY_OPT
        }
#endif
        return result;
    }

    inline void endOp(const int tid) {
        threadData[tid].announcedEpoch.store(GET_WITH_QUIESCENT(threadData[tid].localvar_announcedEpoch), std::memory_order_relaxed);
    }

    // for all schemes except reference counting
    inline void retire(const int tid, T* p) {
        threadData[tid].currentBag->add(p);
        DEBUG2 this->debug->addRetired(tid, 1);
    }

    void debugPrintStatus(const int tid) {
        if (tid == 0) {
            std::cout<<"global_epoch_counter="<<epoch/EPOCH_INCREMENT<<std::endl;
        }
    }

    void initThread(const int tid) {
        for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
            if (threadData[tid].epochbags[i] == NULL) {
                threadData[tid].epochbags[i] = new blockbag<T>(tid, this->pool->blockpools[tid]);
            }
        }
        threadData[tid].currentBag = threadData[tid].epochbags[0];
#ifdef DEAMORTIZE_FREE_CALLS
        threadData[tid].deamortizedFreeables = new blockbag<T>(tid, this->pool->blockpools[tid]);
        threadData[tid].numFreesPerStartOp = 1;
#endif
        threadData[tid].opsSinceRead = 0;
        threadData[tid].checked = 0;
#ifdef GSTATS_HANDLE_STATS
        GSTATS_CLEAR_TIMERS;
#endif
    }

    void deinitThread(const int tid) {
        // WARNING: this moves objects to the pool immediately,
        // which is only safe if this thread is deinitializing specifically
        // because *ALL THREADS* have already finished accessing
        // the data structure and are now quiescent!!
        for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
            if (threadData[tid].epochbags[i]) {
                this->pool->addMoveAll(tid, threadData[tid].epochbags[i]);
                delete threadData[tid].epochbags[i];
                threadData[tid].epochbags[i] = NULL;
            }
        }
#ifdef DEAMORTIZE_FREE_CALLS
        this->pool->addMoveAll(tid, threadData[tid].deamortizedFreeables);
        delete threadData[tid].deamortizedFreeables;
#endif
    }

    reclaimer_debra(const int numProcesses, Pool *_pool, debugInfo * const _debug, RecoveryMgr<void *> * const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE std::cout<<"constructor reclaimer_debra helping="<<this->shouldHelp()<<std::endl;// scanThreshold="<<scanThreshold<<std::endl;
        epoch = 0;
        for (int tid=0;tid<numProcesses;++tid) {
            threadData[tid].index = 0;
            threadData[tid].localvar_announcedEpoch = GET_WITH_QUIESCENT(0);
            threadData[tid].announcedEpoch.store(GET_WITH_QUIESCENT(0), std::memory_order_relaxed);
            for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
                threadData[tid].epochbags[i] = NULL;
            }
#ifdef DEAMORTIZE_FREE_CALLS
            threadData[tid].deamortizedFreeables = NULL;
#endif
        }
    }
    ~reclaimer_debra() {
//        VERBOSE DEBUG std::cout<<"destructor reclaimer_debra"<<std::endl;
//        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
//            // move contents of all bags into pool
//            for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
//                if (threadData[tid].epochbags[i]) {
//                    this->pool->addMoveAll(tid, threadData[tid].epochbags[i]);
//                    delete threadData[tid].epochbags[i];
//                }
//            }
//        }
    }

};

#endif

