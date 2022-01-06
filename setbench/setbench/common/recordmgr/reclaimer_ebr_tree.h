/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECLAIM_TREE_EBR_H
#define	RECLAIM_TREE_EBR_H

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

#define EPOCH_INCREMENT 2
#define BITS_EPOCH(ann) ((ann)&~(EPOCH_INCREMENT-1))
#define QUIESCENT_MASK (0x1)
#define QUIESCENT(ann) ((ann)&QUIESCENT_MASK)
#define GET_WITH_QUIESCENT(ann) ((ann)|QUIESCENT_MASK)

#ifdef RAPID_RECLAMATION
#   define MIN_TIME_BEFORE_TRY_ADVANCE 1
#else
#   define MIN_TIME_BEFORE_TRY_ADVANCE 50
#endif

#define NUMBER_OF_EPOCH_BAGS 3
#define NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS 0

template <typename T = void, class Pool = pool_interface<T> >
class reclaimer_ebr_tree : public reclaimer_interface<T, Pool> {
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
                long index;
                long timeSinceTryAdvance;
                long timesBagTooLargeSinceRotation;
            };
        };
    };
    struct epoch_node_t {
        volatile long v;
        volatile char padding[PREFETCH_SIZE_BYTES-sizeof(v)];
    } __attribute__((aligned(PREFETCH_SIZE_BYTES)));

#   define EBRT_LEFT_CHILD(ix) (2*(ix))
#   define EBRT_RIGHT_CHILD(ix) (2*(ix)+1)
#   define EBRT_PARENT(ix) ((ix)/2)
#   define EBRT_LEAF(tid) ((tid)+(numThreadsPow2))
#   define EBRT_IS_LEAF(ix) ((ix)>=(numThreadsPow2))
#   define EBRT_ROOT (1)

    class epoch_tree {
    private:
    PAD;
        const int numThreadsPow2;
        const int numNodes;
    PAD;
        epoch_node_t * const nodes; // note: nodes[EBRT_ROOT] contains the real epoch number
    PAD;

    private:
        void propagateQ(int currIx) {
            // preserve invariant: currIx is quiescent

            // get parent
            const int parentIx = EBRT_PARENT(currIx);

            // get sibling
            const int leftIx = EBRT_LEFT_CHILD(parentIx);
            const int siblingIx = (currIx == leftIx)
                    ? leftIx + 1 /* shortcut for computing right child from left */
                    : leftIx;
            const long siblingVal = nodes[siblingIx].v;

            if (QUIESCENT(siblingVal)) {
                nodes[parentIx].v = GET_WITH_QUIESCENT(0);
                propagateQ(parentIx);
            }
        }

    public:
        epoch_tree(const int numThreads)
        : numThreadsPow2(roundUpPow2(numThreads))
        , numNodes(2*numThreadsPow2)
        , nodes(new epoch_node_t[numNodes]) {
            for (int ix=0;ix<numNodes;++ix) {
                nodes[ix].v = 0; //GET_WITH_QUIESCENT(0); // maybe a problem (store "X" / ignore in EVERY node)
            }
            // set quiescence bit for each "fake" thread
            for (int ix=EBRT_LEAF(numThreads); ix<numNodes; ++ix) {
                nodes[ix].v = GET_WITH_QUIESCENT(0);
            }
            // propagate quiescence upwards for "fake" threads
            for (int ix=EBRT_LEAF(numThreads); ix<numNodes; ++ix) {
                propagateQ(ix);
            }

            nodes[EBRT_ROOT].v = EPOCH_INCREMENT;
        }
        ~epoch_tree() {
            delete[] nodes;
        }
        void announce(const int tid, const long val) {
            // announce new value
            const int currIx = EBRT_LEAF(tid);
            nodes[currIx].v = val;
            //__sync_synchronize();
//            GSTATS_SET(tid, thread_announced_epoch, val);
        }
        void tryAdvance(const int tid) {
//            GSTATS_ADD(tid, num_tryadvance, 1);

            /**
             * we propagate announcement values up the tree via a tournament
             * with 2 threads competing at each step.
             *
             * propagate our announcement up the tree as long as is appropriate.
             */
            int currIx = EBRT_LEAF(tid);
            const long val = nodes[currIx].v;
            while (currIx > EBRT_ROOT) {
//                GSTATS_ADD(tid, num_prop_nodes_visited, 1);
                // get parent
                const int parentIx = EBRT_PARENT(currIx);

                // get sibling
                const int leftIx = EBRT_LEFT_CHILD(parentIx);
                const int siblingIx = (currIx == leftIx)
                        ? leftIx + 1 /* shortcut for computing right child from left */
                        : leftIx;
                const long siblingVal = nodes[siblingIx].v;

                // we try to propagate if our sibling has the same value as us,
                // or if we see that its value is quiescent
                // (since then we can ignore it, just like we do in debra)
                if (siblingVal == val || QUIESCENT(siblingVal)) {
                    // if we are at the root, modifying the global epoch,
                    // then we want to increment the global epoch,
                    // ONLY IF we are propagating exactly the value at the root.
                    if (parentIx == EBRT_ROOT) {
//                        GSTATS_ADD(tid, num_prop_root, 1);
                        const long parentVal = nodes[parentIx].v;
                        if (parentVal != val) return; // cannot propagate (value already propagated. we know this because val cannot be > parentVal, since we read val from root)
//                        GSTATS_SET(tid, num_prop_root_update_time, get_server_clock());
                        if (CASB(&nodes[parentIx].v, parentVal, parentVal + EPOCH_INCREMENT)) {
//                            GSTATS_TIMER_APPEND_SPLIT(tid, timersplit_epoch, num_prop_epoch_latency);
//                            GSTATS_SET_IX(tid, num_prop_epoch_latency, GSTATS_TIMER_SPLIT(tid, timersplit_epoch), parentVal + EPOCH_INCREMENT);
                        }
//                        assert(nodes[parentIx].v == val+2); // note: need not hold if someone else also incremented the epoch (which can occur if we are quiescent)
//                        assert(nodes[parentIx].v == read());
                        // note: we use cas instead of fetch and add because more than one thread could be trying to change it.
                        // HOWEVER, if our cas fails, then another thread has incremented the epoch, so we are done.
                        return; // finished propagating

                    // otherwise, we want to propagate upward only if our value is greater than the parent's value
                    } else {
                        // note: we use a cas loop to ensure that we don't fail to propagate just because a competing thread propagated a smaller value.
                        const long parentVal = nodes[parentIx].v;
                        if (parentVal >= val) {
//                            GSTATS_ADD(tid, num_prop_parentlarge, 1);
                            return; // cannot propagate (value already propagated)
//                            break; // cannot propagate (value already propagated) -- but continue trying at parent!
                        }
                        if (CASB(&nodes[parentIx].v, parentVal, val)) {
                            // propagated successfully, so try again at the parent
                        } else {
                            return; // someone ELSE propagated. they will propagate further.
                        }
                    }
                } else {
                    return; // cannot propagate (sibling's value is stopping us)
                }

                // move to the parent
                currIx = parentIx;
            }
        }
        long read() {
            SOFTWARE_BARRIER;
            return nodes[EBRT_ROOT].v; // root
        }
        long readThread(const int tid) {
            return nodes[EBRT_LEAF(tid)].v;
        }
        void debugPrint() {
            int row = 0;
            int rowsize = 1;
            int ix = EBRT_ROOT;
            for (; rowsize <= numThreadsPow2; ++row, rowsize<<=1) {
                std::cout<<"level "<<row<<":";
                for (int i=0;i<rowsize;++i) {
                    std::cout<<" "<<nodes[ix].v;
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
    struct rebind { typedef reclaimer_ebr_tree<_Tp1, Pool> other; };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 { typedef reclaimer_ebr_tree<_Tp1, _Tp2> other; };

    inline void getSafeBlockbags(const int tid, blockbag<T> ** bags) {
        SOFTWARE_BARRIER;
        int ix = thread_data[tid].index;
        bags[0] = thread_data[tid].epochbags[ix];
        bags[1] = thread_data[tid].epochbags[((ix+NUMBER_OF_EPOCH_BAGS-1)%NUMBER_OF_EPOCH_BAGS)];
        bags[2] = thread_data[tid].epochbags[((ix+NUMBER_OF_EPOCH_BAGS-2)%NUMBER_OF_EPOCH_BAGS)];
        bags[3] = NULL;
        SOFTWARE_BARRIER;
    }

    long long getSizeInNodes() {
        long long sum = 0;
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

public:
    inline void rotateEpochBags(const int tid) {
        int nextIndex = (thread_data[tid].index+1) % NUMBER_OF_EPOCH_BAGS;
        blockbag<T> * const freeable = thread_data[tid].epochbags[((nextIndex+NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS) % NUMBER_OF_EPOCH_BAGS)];
#ifdef GSTATS_HANDLE_STATS
        GSTATS_APPEND(tid, limbo_reclamation_event_size, freeable->computeSize());
        GSTATS_ADD(tid, limbo_reclamation_event_count, 1);
#endif
        this->pool->addMoveFullBlocks(tid, freeable); // moves any full blocks (may leave one non-full block behind)
        SOFTWARE_BARRIER;
        thread_data[tid].index = nextIndex;
        thread_data[tid].currentBag = thread_data[tid].epochbags[nextIndex];
    }

private:
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

            ((reclaimer_ebr_tree<First, classPool> * const) reclaimers[i])->rotateEpochBags(tid);
            ((BagRotator<Rest...> *) this)->rotateAllEpochBags(tid, reclaimers, 1+i);
        }
    };

public:
    inline void endOp(const int tid) {
//        GSTATS_TIMER_APPEND_ELAPSED(tid, timersplit_guard, num_prop_guard_split);
        //epoch->announce(tid, GET_WITH_QUIESCENT(epoch->readThread(tid)));
    }

    template <typename First, typename... Rest>
    inline bool startOp(const int tid, void * const * const reclaimers, const int numReclaimers, const bool readOnly = false) {
        SOFTWARE_BARRIER; // prevent any bookkeeping from being moved after this point by the compiler.
//        GSTATS_TIMER_RESET(tid, timersplit_guard);
//        GSTATS_ADD(tid, num_getguard, 1);
        bool result = false;

//        GSTATS_SET(tid, num_prop_root_read_time, get_server_clock());
        const long readEpoch = epoch->read();
//        GSTATS_SET(tid, num_prop_hist_readepoch, readEpoch);
//        __sync_synchronize();
        const long ann = epoch->readThread(tid);

        // if our previous announced epoch is different from the current epoch
        if (readEpoch != BITS_EPOCH(ann)) {
            // announce new epoch
            epoch->announce(tid, readEpoch);
            // NOTE: we are sensitive to big delays caused by dirty page purging when bag rotation happens below!
            // rotate the epoch bags
            thread_data[tid].timesBagTooLargeSinceRotation = 0;
            BagRotator<First, Rest...> rotator;
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
//            for (int i=0;i<numReclaimers;++i) {
//                ((reclaimer_ebr_tree<T, Pool> * const) reclaimers[i])->rotateEpochBags(tid);
//            }
            result = true;
        }

#ifndef DEBRA_DISABLE_READONLY_OPT
        if (!readOnly) {
#endif
            // periodically try advancing the epoch
            if ((++thread_data[tid].timeSinceTryAdvance % MIN_TIME_BEFORE_TRY_ADVANCE) == 0) {
                epoch->tryAdvance(tid);
            }
#ifndef DEBRA_DISABLE_READONLY_OPT
        }
#endif
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
//            long readEpoch = epoch;
//            const long ann = announcedEpoch[tid*PREFETCH_SIZE_WORDS].load(std::memory_order_relaxed);
//
//            // if our announced epoch is different from the current epoch, skip the following, since we're going to rotate limbo bags on our next operation, anyway
//            if (readEpoch != BITS_EPOCH(ann)) return;
//
//            // scan all threads (skipping any threads we've already checked) to see if we can advance the epoch (and subsequently reclaim memory)
//            for (; thread_data[tid].checked < this->NUM_PROCESSES; ++thread_data[tid].checked) {
//                const int otherTid = thread_data[tid].checked;
//                long otherAnnounce = announcedEpoch[otherTid*PREFETCH_SIZE_WORDS].load(std::memory_order_relaxed);
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

    reclaimer_ebr_tree(const int numProcesses, Pool *_pool, debugInfo * const _debug, RecoveryMgr<void *> * const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE std::cout<<"constructor reclaimer_ebr_tree helping="<<this->shouldHelp()<<std::endl;// scanThreshold="<<scanThreshold<<std::endl;
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
    ~reclaimer_ebr_tree() {
        VERBOSE DEBUG std::cout<<"destructor reclaimer_ebr_tree"<<std::endl;
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

