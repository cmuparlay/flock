/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECLAIM_DEBRACAP_H
#define	RECLAIM_DEBRACAP_H

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
class reclaimer_debracap : public reclaimer_interface<T, Pool> {
protected:
#define DEBRA_DISABLE_READONLY_OPT

#define EPOCH_INCREMENT 2
#define BITS_EPOCH(ann) ((ann)&~(EPOCH_INCREMENT-1))
#define QUIESCENT(ann) ((ann)&1)
#define GET_WITH_QUIESCENT(ann) ((ann)|1)

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
        int checked;               // how far we've come in checking the announced epochs of other threads
        int opsSinceRead;
        int timesBagTooLargeSinceRotation;
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
        typedef reclaimer_debracap<_Tp1, Pool> other;
    };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 {
        typedef reclaimer_debracap<_Tp1, _Tp2> other;
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
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            for (int j=0;j<NUMBER_OF_EPOCH_BAGS;++j) {
                sum += threadData[tid].epochbags[j]->computeSize();
            }
        }
        return sum;
    }
    std::string getDetailsString() { return ""; }
    std::string getSizeString() {
        std::stringstream ss;
        ss<<getSizeInNodes(); //<<" in epoch bags";
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

    // rotate the epoch bags and reclaim any objects retired two epochs ago.
    inline void rotateEpochBags(const int tid) {
        int nextIndex = (threadData[tid].index+1) % NUMBER_OF_EPOCH_BAGS;
        blockbag<T> * const freeable = threadData[tid].epochbags[(nextIndex+NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS) % NUMBER_OF_EPOCH_BAGS];
#ifdef GSTATS_HANDLE_STATS
        GSTATS_APPEND(tid, limbo_reclamation_event_size, freeable->computeSize());
        GSTATS_ADD(tid, limbo_reclamation_event_count, 1);
#endif
        this->pool->addMoveFullBlocks(tid, freeable); // moves any full blocks (may leave a non-full block behind)
        SOFTWARE_BARRIER;
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

            ((reclaimer_debracap<First, classPool> * const) reclaimers[i])->rotateEpochBags(tid);
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
        threadData[tid].announcedEpoch.store(readEpoch, std::memory_order_relaxed); // note: this must be written, regardless of whether the announced epochs are the same, because the quiescent bit will vary
        // note: readEpoch, when written to announcedEpoch[tid],
        //       sets the state to non-quiescent and non-neutralized

        // if our announced epoch was different from the current epoch
        if (readEpoch != ann /* invariant: ann is not quiescent */) {
            // rotate the epoch bags and
            // reclaim any objects retired two epochs ago.
            threadData[tid].checked = 0;
            BagRotator<First, Rest...> rotator;
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            //this->template rotateAllEpochBags<First, Rest...>(tid, reclaimers, 0);
            result = true;
        }

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
                        __sync_bool_compare_and_swap(&epoch, readEpoch, readEpoch+EPOCH_INCREMENT);
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
        if (threadData[tid].currentBag->getSizeInBlocks() >= 2) {
            // only execute the following logic once every X times (starting at 0) we see that our current bag is too large
            // (resetting the count when we rotate bags).
            // if we are being prevented from reclaiming often, then we will quickly (within X operations) scan all threads.
            // otherwise, we will avoid scanning all threads too often.
            if ((++threadData[tid].timesBagTooLargeSinceRotation) % 1000) return;

            long readEpoch = epoch;
            const long ann = threadData[tid].localvar_announcedEpoch;

            // if our announced epoch is different from the current epoch, skip the following, since we're going to rotate limbo bags on our next operation, anyway
            if (readEpoch != BITS_EPOCH(ann)) return;

            // scan all threads (skipping any threads we've already checked) to see if we can advance the epoch (and subsequently reclaim memory)
            for (; threadData[tid].checked < this->NUM_PROCESSES; ++threadData[tid].checked) {
                const int otherTid = threadData[tid].checked;
                long otherAnnounce = threadData[otherTid].announcedEpoch.load(std::memory_order_relaxed);
                if (!(BITS_EPOCH(otherAnnounce) == readEpoch || QUIESCENT(otherAnnounce))) return;
            }
            __sync_bool_compare_and_swap(&epoch, readEpoch, readEpoch+EPOCH_INCREMENT);
        }
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
        threadData[tid].opsSinceRead = 0;
        threadData[tid].checked = 0;
        threadData[tid].timesBagTooLargeSinceRotation = 0;
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
    }

    reclaimer_debracap(const int numProcesses, Pool *_pool, debugInfo * const _debug, RecoveryMgr<void *> * const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE std::cout<<"constructor reclaimer_debracap helping="<<this->shouldHelp()<<std::endl;// scanThreshold="<<scanThreshold<<std::endl;
        epoch = 0;
        for (int tid=0;tid<numProcesses;++tid) {
            threadData[tid].index = 0;
            threadData[tid].localvar_announcedEpoch = GET_WITH_QUIESCENT(0);
            threadData[tid].announcedEpoch.store(GET_WITH_QUIESCENT(0), std::memory_order_relaxed);
            for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
                threadData[tid].epochbags[i] = NULL;
            }
        }
    }
    ~reclaimer_debracap() {
//        VERBOSE DEBUG std::cout<<"destructor reclaimer_debracap"<<std::endl;
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

