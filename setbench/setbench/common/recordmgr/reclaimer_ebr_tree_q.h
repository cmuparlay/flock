/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECLAIM_TREE_EBR_Q_H
#define	RECLAIM_TREE_EBR_Q_H

#include <cassert>
#include <iostream>
#include <sstream>
#include <limits.h>
#include "blockbag.h"
#include "plaf.h"
#include "allocator_interface.h"
#include "reclaimer_interface.h"
#include "gstats_global.h"

// optional statistics tracking
#include "gstats_definitions_epochs.h"

#define EPOCH_INCREMENT ((size_t) 1 << (size_t) 32)
#define GET_SEQUENCE(ann) ((((ann)&0x00000000fffffffeLL))>>1)
#define GET_EPOCH(ann) (BITS_EPOCH(ann) >> 32LL)
#define BITS_EPOCH(ann) ((ann)&0xffffffff00000000LL)
#define QUIESCENT_MASK (0x1)
#define QUIESCENT(ann) ((ann)&QUIESCENT_MASK)
#define GET_WITH_QUIESCENT(ann) ((ann)|QUIESCENT_MASK)
#define SEQUENCE_INCREMENT (1)

#define NUMBER_OF_EPOCH_BAGS 3
#define NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS 0

template <typename T = void, class Pool = pool_interface<T> >
class reclaimer_ebr_tree_q : public reclaimer_interface<T, Pool> {
private:
    static int roundUpPow2(int x) {
        unsigned int v = (unsigned int) x;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return (int) v;
    }

    class thread_data_t {
    public:
        union {
            char bytes[192];
            struct {
                blockbag<T> * epochbags[NUMBER_OF_EPOCH_BAGS];
                blockbag<T> * currentBag;
                size_t index;
                size_t timeSinceTryAdvance;
                size_t timesBagTooLargeSinceRotation;
            };
        };
    };
    struct epoch_node_t {
        volatile size_t v;
        volatile char padding[PREFETCH_SIZE_BYTES - sizeof(v)];
    } __attribute__((aligned(PREFETCH_SIZE_BYTES)));

#   define EBRT_SIBLING(ix) ((ix)^1)
#   define EBRT_LEFT_CHILD(ix) (2*(ix))
#   define EBRT_RIGHT_CHILD(ix) (2*(ix)+1)
#   define EBRT_PARENT(ix) ((ix)/2)
#   define EBRT_LEAF(tid) ((tid)+(numThreadsPow2))
#   define EBRT_IS_LEAF(ix) ((ix)>=(numThreadsPow2))
#   define EBRT_ROOT (1)

    class epoch_tree {
    private:
        const int numThreadsPow2;
        const int numNodes;
        epoch_node_t * const nodes; // note: nodes[EBRT_ROOT] contains the real epoch number
    public:
        epoch_tree(const int numThreads)
        : numThreadsPow2(roundUpPow2(numThreads))
        , numNodes(2*numThreadsPow2)
        , nodes(new epoch_node_t[numNodes]) {
            for (int ix=0;ix<numNodes;++ix) {
                nodes[ix].v = GET_WITH_QUIESCENT(0);
            }
            nodes[EBRT_ROOT].v = EPOCH_INCREMENT;
        }
        ~epoch_tree() {
            delete[] nodes;
        }
        void announce(const int tid, const size_t val) {
            // announce new value
            auto cix = EBRT_LEAF(tid);
            nodes[cix].v = val;
            __sync_synchronize();
//            GSTATS_SET(tid, thread_announced_epoch, val);
        }
        void tryAdvance(const int tid, const bool startingOp) {
            auto cix = EBRT_LEAF(tid);
            auto val = nodes[cix].v;

            while (cix > EBRT_ROOT) {
                // get parent's value
                auto pix = EBRT_PARENT(cix);
                auto p = &nodes[pix];
                auto pval = p->v;

                SOFTWARE_BARRIER;

                // get sibling's value (AFTER PARENT)
                auto sval = nodes[EBRT_SIBLING(cix)].v;

                // get current node's value (AFTER PARENT)
                auto cval = nodes[cix].v;

                if (startingOp) {
                    while (true) {
                        // note: we know for a fact that because we are non-Q, we are propagating non-Q to the root
                        // (in the case where we are becoming Q (the !startingOp case), our propagation of Q is dependent not only on the sibling's Q-ness but the CURRENT node's continued Q-ness)

                        // CAN ONLY PROPAGATE MY VALUE BLINDLY IF MY SIBLING IS QUIESCENT (OTHERWISE, MUST PROPAGATE MIN(MY VALUE, SIBLING VALUE))
                        auto newval = QUIESCENT(sval) ? BITS_EPOCH(val) : std::min(BITS_EPOCH(val), BITS_EPOCH(sval));
                        if (pix == EBRT_ROOT) newval += EPOCH_INCREMENT;                                                // TODO: detect epoch overflow

                        if (QUIESCENT(pval)) {                                  // TODO: unlikely ?
                            if (BITS_EPOCH(pval) < newval) {                    // TODO: unlikely
                                auto retval = CASV(&p->v, pval, newval);
                                if (retval == pval) break; // continue at parent
                                pval = retval;             // retry at this node
                                SOFTWARE_BARRIER;
                                sval = nodes[EBRT_SIBLING(cix)].v; // reread sibling value AFTER parent
                            } else {
                                auto retval = CASV(&p->v, pval, pval+SEQUENCE_INCREMENT);                               // TODO: detect sequence overflow
                                if (retval == pval) break; // continue at parent
                                pval = retval;             // retry at this node
                                SOFTWARE_BARRIER;
                                sval = nodes[EBRT_SIBLING(cix)].v; // reread sibling value AFTER parent
                            }
                        } else {
                            if (BITS_EPOCH(pval) < newval) {                    // TODO: unlikely
                                auto retval = CASV(&p->v, pval, newval);
                                if (retval == pval) break; // continue at parent
                                pval = retval;             // retry at this node
                                SOFTWARE_BARRIER;
                                sval = nodes[EBRT_SIBLING(cix)].v; // reread sibling value AFTER parent
                            } else {
                                return; //break;                     // continue at parent
                            }
                        }
                    }
                } else {
                    while (true) {
                        if (QUIESCENT(pval)) {                                  // TODO: unlikely ?
                            break; // continue at parent
                        } else {
                            if (QUIESCENT(cval) && QUIESCENT(sval)) {           // TODO: unlikely ?
                                auto newval = val;
                                if (pix == EBRT_ROOT) newval += EPOCH_INCREMENT;                                        // TODO: detect epoch overflow

                                if (BITS_EPOCH(pval) < BITS_EPOCH(newval)) {    // TODO: unlikely
                                    auto retval = CASV(&p->v, pval, newval);
                                    if (retval == pval) break; // continue at parent
                                    pval = retval;             // retry at this node
                                    sval = nodes[EBRT_SIBLING(cix)].v;          // reread sibling value AFTER parent
                                    cval = nodes[cix].v;                        // reread current value AFTER parent
                                } else {
                                    auto retval = CASV(&p->v, pval, pval+SEQUENCE_INCREMENT);                           // TODO: detect sequence overflow
                                    if (retval == pval) break; // continue at parent
                                    pval = retval;             // retry at this node
                                    sval = nodes[EBRT_SIBLING(cix)].v;          // reread sibling value AFTER parent
                                    cval = nodes[cix].v;                        // reread current value AFTER parent
                                }
                            } else {
                                return; // do NOT need to continue at the parent!
                            }
                        }
                    }
                }

                cix = pix;
            }
        }
        size_t read() {
            SOFTWARE_BARRIER;
            return nodes[EBRT_ROOT].v; // root
        }
        size_t readThread(const int tid) {
            return nodes[EBRT_LEAF(tid)].v;
        }
        void debugPrint() {
            int row = 0;
            int rowsize = 1;
            int ix = EBRT_ROOT;
            for (; rowsize <= numThreadsPow2; ++row, rowsize<<=1) {
                std::cout<<"level "<<row<<":";
                for (int i=0;i<rowsize;++i) {
                    auto v = nodes[ix].v;
                    std::cout<<" "<<(BITS_EPOCH(v)>>32LL)<<","<<(GET_SEQUENCE(v))<<","<<((int) QUIESCENT(v));
                    ++ix;
                }
                std::cout<<std::endl;
            }
//            for (int ix=0;ix<numNodes;++ix) {
//                std::cout<<"nodes["<<ix<<"].v="<<nodes[ix].v<<std::endl;
//            }
        }
    };
protected:
//    PAD; // not needed after superclass layout
    thread_data_t thread_data[MAX_THREADS_POW2];
//    PAD; // not needed after padding in preceding var
    epoch_tree * epoch;
    PAD;

public:
    template<typename _Tp1>
    struct rebind { typedef reclaimer_ebr_tree_q<_Tp1, Pool> other; };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 { typedef reclaimer_ebr_tree_q<_Tp1, _Tp2> other; };

    inline void getSafeBlockbags(const int tid, blockbag<T> ** bags) {
        SOFTWARE_BARRIER;
        int ix = thread_data[tid].index;
        bags[0] = thread_data[tid].epochbags[ix];
        bags[1] = thread_data[tid].epochbags[((ix+NUMBER_OF_EPOCH_BAGS-1)%NUMBER_OF_EPOCH_BAGS)];
        bags[2] = thread_data[tid].epochbags[((ix+NUMBER_OF_EPOCH_BAGS-2)%NUMBER_OF_EPOCH_BAGS)];
        bags[3] = NULL;
        SOFTWARE_BARRIER;
    }

    size_t getSizeInNodes() {
        size_t sum = 0;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            for (int j=0;j<NUMBER_OF_EPOCH_BAGS;++j) {
                sum += thread_data[tid].epochbags[j]->computeSize();
            }
        }
        return sum;
    }
    std::string getDetailsString() { return ""; }
    std::string getSizeString() {
        std::stringstream ss;
        ss<<getSizeInNodes();
        return ss.str();
    }

    inline static bool quiescenceIsPerRecordType() { return false; }
    inline bool isQuiescent(const int tid) { return QUIESCENT(epoch->readThread(tid)); }
    inline static bool isProtected(const int tid, T * const obj) { return true; }
    inline static bool isQProtected(const int tid, T * const obj) { return false; }
    inline static bool protect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool memoryBarrier = true) { return true; }
    inline static void unprotect(const int tid, T * const obj) {}
    inline static bool qProtect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool memoryBarrier = true) { return true; }
    inline static void qUnprotectAll(const int tid) {}
    inline static bool shouldHelp() { return true; }

private:
    inline void rotateEpochBags(const int tid) {
        int nextIndex = (thread_data[tid].index+1) % NUMBER_OF_EPOCH_BAGS;
        blockbag<T> * const freeable = thread_data[tid].epochbags[((nextIndex+NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS) % NUMBER_OF_EPOCH_BAGS)];
        this->pool->addMoveFullBlocks(tid, freeable); // moves any full blocks (may leave one non-full block behind)
        SOFTWARE_BARRIER;
        thread_data[tid].index = nextIndex;
        thread_data[tid].currentBag = thread_data[tid].epochbags[nextIndex];
    }

public:
    inline void endOp(const int tid) {
//        GSTATS_TIMER_APPEND_ELAPSED(tid, timersplit_guard, num_prop_guard_split);
        auto ann = epoch->readThread(tid) + 1;
        epoch->announce(tid, ann);
        assert(QUIESCENT(ann));
        epoch->tryAdvance(tid, false);
    }

    inline bool startOp(const int tid, void * const * const reclaimers, const int numReclaimers) {
        SOFTWARE_BARRIER; // prevent any bookkeeping from being moved after this point by the compiler.
//        GSTATS_TIMER_RESET(tid, timersplit_guard);
//        GSTATS_ADD(tid, num_getguard, 1);
        bool result = false;

//        GSTATS_SET(tid, num_prop_root_read_time, get_server_clock());
        auto readEpoch = BITS_EPOCH(epoch->read());
//        GSTATS_SET(tid, num_prop_hist_readepoch, readEpoch);
//        __sync_synchronize();
        auto ann = epoch->readThread(tid);

        // if our announced epoch is different from the current epoch
        if (readEpoch != BITS_EPOCH(ann)) {
            thread_data[tid].timesBagTooLargeSinceRotation = 0;
            for (int i=0;i<numReclaimers;++i) {
                ((reclaimer_ebr_tree_q<T, Pool> * const) reclaimers[i])->rotateEpochBags(tid);
            }
            result = true;
        }

        epoch->announce(tid, readEpoch); // must always announce because of quiescence bit
        epoch->tryAdvance(tid, true);
        return result;
    }

    inline void retire(const int tid, T* p) {
        thread_data[tid].currentBag->add(p);
        DEBUG2 this->debug->addRetired(tid, 1);
//        if (thread_data[tid].currentBag->getSizeInBlocks() >= 2) {
//            // only execute the following logic once every X times (starting at 0) we see that our current bag is too large
//            // (resetting the count when we rotate bags).
//            // if we are being prevented from reclaiming often, then we will quickly (within X operations) scan all threads.
//            // otherwise, we will avoid scanning all threads too often.
//            if ((++thread_data[tid].timesBagTooLargeSinceRotation) % 1000) return;
//
//            auto readEpoch = epoch;
//            const auto ann = announcedEpoch[tid*PREFETCH_SIZE_WORDS].load(std::memory_order_relaxed);
//
//            // if our announced epoch is different from the current epoch, skip the following, since we're going to rotate limbo bags on our next operation, anyway
//            if (readEpoch != BITS_EPOCH(ann)) return;
//
//            // scan all threads (skipping any threads we've already checked) to see if we can advance the epoch (and subsequently reclaim memory)
//            for (; thread_data[tid].checked < this->NUM_PROCESSES; ++thread_data[tid].checked) {
//                const int otherTid = thread_data[tid].checked;
//                auto otherAnnounce = announcedEpoch[otherTid*PREFETCH_SIZE_WORDS].load(std::memory_order_relaxed);
//                if (!(BITS_EPOCH(otherAnnounce) == readEpoch || QUIESCENT(otherAnnounce))) return;
//            }
//            __sync_bool_compare_and_swap(&epoch, readEpoch, readEpoch+EPOCH_INCREMENT);
//        }
    }

    void debugPrintStatus(const int tid) {
        if (tid == 0) {
            std::cout<<"global_epoch_counter="<<epoch->read()/EPOCH_INCREMENT<<std::endl;
            epoch->debugPrint();
        }
    }

    void initThread(const int tid) {
#ifdef GSTATS_HANDLE_STATS
        GSTATS_CLEAR_TIMERS;
#endif
    }
    void deinitThread(const int tid) {}

    reclaimer_ebr_tree_q(const int numProcesses, Pool *_pool, debugInfo * const _debug, RecoveryMgr<void *> * const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE std::cout<<"constructor reclaimer_ebr_tree_q helping="<<this->shouldHelp()<<std::endl;// scanThreshold="<<scanThreshold<<std::endl;
        if (numProcesses > MAX_THREADS_POW2) {
            setbench_error("number of threads is greater than MAX_THREADS_POW2 = "<<MAX_THREADS_POW2);
        }
        epoch = new epoch_tree(numProcesses);
        for (int tid=0;tid<numProcesses;++tid) {
            for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
                thread_data[tid].epochbags[i] = new blockbag<T>(tid, this->pool->blockpools[tid]);
            }
            thread_data[tid].timesBagTooLargeSinceRotation = 0;
            thread_data[tid].currentBag = thread_data[tid].epochbags[0];
            thread_data[tid].index = 0;
            thread_data[tid].timeSinceTryAdvance = 0;
//            GSTATS_TIMER_RESET(tid, timersplit_guard);
//            GSTATS_TIMER_RESET(tid, timersplit_epoch);
        }
    }
    ~reclaimer_ebr_tree_q() {
        VERBOSE DEBUG std::cout<<"destructor reclaimer_ebr_tree_q"<<std::endl;
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            for (int i=0;i<NUMBER_OF_EPOCH_BAGS;++i) {
                this->pool->addMoveAll(tid, thread_data[tid].epochbags[i]);
                delete thread_data[tid].epochbags[i];
            }
        }
        delete epoch;
    }

};

#undef EPOCH_INCREMENT
#undef BITS_EPOCH
#undef QUIESCENT
#undef GET_WITH_QUIESCENT
#undef MIN_TIME_BEFORE_TRY_ADVANCE
#undef NUMBER_OF_EPOCH_BAGS
#undef NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS

#undef EBRT_LEFT_CHILD
#undef EBRT_RIGHT_CHILD
#undef EBRT_PARENT
#undef EBRT_LEAF

#endif

