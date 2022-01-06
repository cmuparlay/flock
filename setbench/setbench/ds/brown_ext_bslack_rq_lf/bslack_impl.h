/**
 * Implementation of the dictionary ADT with a lock-free B-slack tree.
 * Copyright (C) 2016 Trevor Brown
 * Contact (me [at] tbrown [dot] pro) with questions or comments.
 *
 * Details of the B-slack tree algorithm appear in the paper:
 *    Brown, Trevor. B-slack trees: space efficient B-trees. SWAT 2014.
 *
 * The paper leaves it up to the implementer to decide when and how to perform
 * rebalancing steps (i.e., Root-Zero, Root-Replace, Absorb, Split, Compress
 * and One-Child). In this implementation, we keep track of violations and fix
 * them using a recursive cleanup procedure, which is designed as follows.
 * After performing a rebalancing step that replaced a set R of nodes,
 * recursive invocations are made for every violation that appears at a newly
 * created node. Thus, any violations that were present at nodes in R are either
 * eliminated by the rebalancing step, or will be fixed by recursive calls.
 * This way, if an invocation I of this cleanup procedure is trying to fix a
 * violation at a node that has been replaced by another invocation I' of cleanup,
 * then I can hand off responsibility for fixing the violation to I'.
 * Designing the rebalancing procedure to allow responsibility to be handed
 * off in this manner is not difficult; it simply requires going through each
 * rebalancing step S and determining which nodes involved in S can have
 * violations after S (and then making a recursive call for each violation).
 *
 * -----------------------------------------------------------------------------
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef BSLACK_H
#define	BSLACK_H

/**
 * Implementation note:
 * The ptrs arrays of internal nodes may be modified by calls to
 * rqProvider->linearize_update_at_cas or ->linearize_update_at_write.
 * Consequently, we must access access entries in the ptrs arrays of INTERNAL
 * nodes by performing calls to read_addr and write_addr (and linearize_...).
 *
 * However, the ptrs arrays of leaves represent fundamentally different data:
 * specifically values, or pointers to values, and NOT pointers to nodes.
 * Thus, the ptrs arrays of leaves CANNOT be modified by such calls.
 * So, we do NOT use these functions to access entries in leaves' ptrs arrays.
 */

#define eassert(x, y) if ((x) != (y)) { std::cout<<"ERROR: "<<#x<<" != "<<#y<<" :: "<<#x<<"="<<x<<" "<<#y<<"="<<y<<std::endl; exit(-1); }

#include <string>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <set>
#include <unistd.h>
#include <sys/types.h>
#include "descriptors.h"
#include "record_manager.h"
#include "rq_provider.h"

namespace bslack_ns {

    #ifndef TRACE
    #define TRACE if(0)
    #endif
    #ifndef DEBUG
    #define DEBUG if(0)
    #endif
    #ifndef DEBUG1
    #define DEBUG1 if(0)
    #endif
    #ifndef DEBUG2
    #define DEBUG2 if(0)
    #endif

    //#define REBALANCING_NONE
    //#define REBALANCING_WEIGHT_ONLY

    //#define NO_NONROOT_SLACK_VIOLATION_FIXING

    //#define NO_VALIDATION

    //#define NO_HELPING

    #define OPTIMIZATION_PRECHECK_DEGREE_VIOLATIONS

    #define BSLACK_ENABLE_DESTRUCTOR

    //#define USE_SIMPLIFIED_ABTREE_REBALANCING

    template <int DEGREE, typename K>
    struct Node;

    template <int DEGREE, typename K>
    struct SCXRecord;

    template <int DEGREE, typename K>
    class wrapper_info {
    public:
        const static int MAX_NODES = DEGREE+2;
        Node<DEGREE,K> * nodes[MAX_NODES];
        SCXRecord<DEGREE,K> * scxPtrs[MAX_NODES];
        Node<DEGREE,K> * newNode;
        Node<DEGREE,K> * volatile * field;
        int state;
        char numberOfNodes;
        char numberOfNodesToFreeze;
        char numberOfNodesAllocated;

        // for rqProvider
        Node<DEGREE,K> * insertedNodes[MAX_NODES+1];
        Node<DEGREE,K> * deletedNodes[MAX_NODES+1];
    };

    template <int DEGREE, typename K>
    struct SCXRecord {
        const static int STATE_INPROGRESS = 0;
        const static int STATE_COMMITTED = 1;
        const static int STATE_ABORTED = 2;
        union {
            struct {
                volatile mutables_t mutables;

                int numberOfNodes;
                int numberOfNodesToFreeze;

                Node<DEGREE,K> * newNode;
                Node<DEGREE,K> * volatile * field;
                Node<DEGREE,K> * nodes[wrapper_info<DEGREE,K>::MAX_NODES];            // array of pointers to nodes
                SCXRecord<DEGREE,K> * scxPtrsSeen[wrapper_info<DEGREE,K>::MAX_NODES]; // array of pointers to scx records

                // for rqProvider
                Node<DEGREE,K> * insertedNodes[wrapper_info<DEGREE,K>::MAX_NODES+1];
                Node<DEGREE,K> * deletedNodes[wrapper_info<DEGREE,K>::MAX_NODES+1];
            } c;
            char bytes[2*PREFETCH_SIZE_BYTES];
        };
        const static int size = sizeof(c);
    } /*__attribute__((aligned (PREFETCH_SIZE_BYTES)))*/;

    template <int DEGREE, typename K>
    struct Node {
        SCXRecord<DEGREE,K> * volatile scxPtr;
        int leaf; // 0 or 1
        volatile int marked; // 0 or 1
        int weight; // 0 or 1
        int size; // degree of node
        K searchKey;
#if defined(RQ_LOCKFREE) || defined(RQ_RWLOCK) || defined(HTM_RQ_RWLOCK)
        volatile long long itime; // for use by range query algorithm
        volatile long long dtime; // for use by range query algorithm
#endif
        K keys[DEGREE];
        Node<DEGREE,K> * volatile ptrs[DEGREE];

        inline bool isLeaf() {
            return leaf;
        }
        inline int getKeyCount() {
            return isLeaf() ? size : size-1;
        }
        inline int getABDegree() {
            return size;
        }
        template <class Compare>
        inline int getChildIndex(const K& key, Compare cmp) {
            int nkeys = getKeyCount();
            int retval = 0;
            while (retval < nkeys && !cmp(key, (const K&) keys[retval])) {
                ++retval;
            }
            return retval;
        }
        template <class Compare>
        inline int getKeyIndex(const K& key, Compare cmp) {
            int nkeys = getKeyCount();
            int retval = 0;
            while (retval < nkeys && cmp((const K&) keys[retval], key)) {
                ++retval;
            }
            return retval;

    //        // useful if we have unordered key/value pairs in leaves
    //        for (int i=0;i<nkeys;++i) {
    //            if (!cmp(key, (const K&) keys[i]) && !cmp((const K&) keys[i], key)) return i;
    //        }
    //        return nkeys;
        }
        // somewhat slow version that detects cycles in the tree
        void printTreeFile(std::ostream& os, std::set<bslack_ns::Node<DEGREE,K> *> *seen) {
            int __state = scxPtr->state;
            //os<<"@"<<(long long)(void *)this;
            os<<"("<<((__state & bslack_ns::SCXRecord<DEGREE,K>::STATE_COMMITTED) ? "" : (__state & bslack_ns::SCXRecord<DEGREE,K>::STATE_ABORTED) ? "A" : "I")
                   <<(marked?"m":"")
    //               <<getKeyCount()
                   <<(weight ? "w1" : "w0")
                   <<(isLeaf() ? "L" : "");
            os<<"[";
    //        os<<",[";
            for (int i=0;i<getKeyCount();++i) {
                os<<(i?",":"")<<keys[i];
            }
            os<<"]"; //,["<<marked<<","<<scxPtr->state<<"]";
            if (isLeaf()) {
    //            for (int i=0;i<getKeyCount();++i) {
    //                os<<","<<(long long) ptrs[i];
    //            }
            } else {
                for (int i=0;i<1+getKeyCount();++i) {
                    Node<DEGREE,K> * __node = ptrs[i];
    //                if (getKeyCount()) os<<",";
                    os<<",";
                    if (__node == NULL) {
                        os<<"-";
                    } else if (seen->find(__node) != seen->end()) { // for finding cycles
                        os<<"!"; // cycle!                          // for finding cycles
                    } else {
                        seen->insert(__node);
                        __node->printTreeFile(os, seen);
                    }
                }
            }
            os<<")";
        }
        void printTreeFile(std::ostream& os) {
            std::set<bslack_ns::Node<DEGREE,K> *> seen;
            printTreeFile(os, &seen);
        }
    } /*__attribute__((aligned (PREFETCH_SIZE_BYTES)))*/;

    template <int DEGREE, typename K, class Compare, class RecManager>
    class bslack {

        // the following bool determines whether the optimization to guarantee
        // amortized constant rebalancing (at the cost of decreasing average degree
        // by at most one) is used.
        // if it is false, then an amortized logarithmic number of rebalancing steps
        // may be performed per operation, but average degree increases slightly.
        PAD;
        const bool ALLOW_ONE_EXTRA_SLACK_PER_NODE;

        const int b;
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
        const int a;
    #endif

//        PAD;
        RecManager * const recordmgr;
//        PAD;
        RQProvider<K, void *, Node<DEGREE,K>, bslack<DEGREE,K,Compare,RecManager>, RecManager, false, false> * const rqProvider;
//        PAD;
        Compare cmp;

        // descriptor reduction algorithm
        #ifndef comma
            #define comma ,
        #endif
        #define DESC1_ARRAY records
        #define DESC1_T SCXRecord<DEGREE comma K>
        #define MUTABLES1_OFFSET_ALLFROZEN 0
        #define MUTABLES1_OFFSET_STATE 1
        #define MUTABLES1_MASK_ALLFROZEN 0x1
        #define MUTABLES1_MASK_STATE 0x6
        #define MUTABLES1_NEW(mutables) \
            ((((mutables)&MASK1_SEQ)+(1<<OFFSET1_SEQ)) \
            | (SCXRecord<DEGREE comma K>::STATE_INPROGRESS<<MUTABLES1_OFFSET_STATE))
        #define MUTABLES1_INIT_DUMMY SCXRecord<DEGREE comma K>::STATE_COMMITTED<<MUTABLES1_OFFSET_STATE | MUTABLES1_MASK_ALLFROZEN<<MUTABLES1_OFFSET_ALLFROZEN
        #include "../descriptors/descriptors_impl.h"
        PAD;
        DESC1_T DESC1_ARRAY[LAST_TID1+1] __attribute__ ((aligned(64)));
        PAD;
        Node<DEGREE,K> * entry;
//        PAD;

        #define DUMMY       ((SCXRecord<DEGREE,K>*) (void*) TAGPTR1_STATIC_DESC(0))
        #define FINALIZED   ((SCXRecord<DEGREE,K>*) (void*) TAGPTR1_DUMMY_DESC(1))
        #define FAILED      ((SCXRecord<DEGREE,K>*) (void*) TAGPTR1_DUMMY_DESC(2))

        // the following variable is only useful for single threaded execution
        const bool SEQUENTIAL_STAT_TRACKING;
//        PAD;

        // these variables are only used by single threaded executions,
        // and only if SEQUENTIAL_STAT_TRACKING == true.
        // they simply track various events in the b-slack tree.
        int operationCount;
        int overflows;
        int weightChecks;
        int weightCheckSearches;
        int weightFixAttempts;
        int weightFixes;
        int weightEliminated;
        int slackChecks;
        int slackCheckTotaling;
        int slackCheckSearches;
        int slackFixTotaling;
        int slackFixAttempts;
        int slackFixSCX;
        int slackFixes;

        #define arraycopy(src, srcStart, dest, destStart, len) \
            for (int ___i=0;___i<(len);++___i) { \
                (dest)[(destStart)+___i] = (src)[(srcStart)+___i]; \
            }
        #define arraycopy_ptrs(src, srcStart, dest, destStart, len) \
            for (int ___i=0;___i<(len);++___i) { \
                rqProvider->write_addr(tid, &(dest)[(destStart)+___i], \
                        rqProvider->read_addr(tid, &(src)[(srcStart)+___i])); \
            }

    private:
        std::string tagptrToString(uintptr_t tagptr) {
            std::stringstream ss;
            if (tagptr) {
                if ((void*) tagptr == DUMMY) {
                    ss<<"dummy";
                } else {
                    SCXRecord<DEGREE,K> *ptr;
                    ss<<"<seq="<<UNPACK1_SEQ(tagptr)<<",tid="<<TAGPTR1_UNPACK_TID(tagptr)<<">";
                    ptr = TAGPTR1_UNPACK_PTR(tagptr);

                    // print contents of actual scx record
                    intptr_t mutables = ptr->c.mutables;
                    ss<<"[";
                    ss<<"state="<<MUTABLES1_UNPACK_FIELD(mutables, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
                    ss<<" ";
                    ss<<"allFrozen="<<MUTABLES1_UNPACK_FIELD(mutables, MUTABLES1_MASK_ALLFROZEN, MUTABLES1_OFFSET_ALLFROZEN);
                    ss<<" ";
                    ss<<"seq="<<UNPACK1_SEQ(mutables);
                    ss<<"]";
                }
            } else {
                ss<<"null";
            }
            return ss.str();
        }

        void * doInsert(const int tid, const K& key, void * const value, const bool replace);

        // returns true if the invocation of this method
        // (and not another invocation of a method performed by this method)
        // performed an scx, and false otherwise
        bool fixWeightViolation(const int tid, Node<DEGREE,K>* viol);

        // returns true if the invocation of this method
        // (and not another invocation of a method performed by this method)
        // performed an scx, and false otherwise
        bool fixDegreeOrSlackViolation(const int tid, Node<DEGREE,K>* viol);

        bool llx(const int tid, Node<DEGREE,K>* r, Node<DEGREE,K> ** snapshot, const int i, SCXRecord<DEGREE,K> ** ops, Node<DEGREE,K> ** nodes);
        SCXRecord<DEGREE,K>* llx(const int tid, Node<DEGREE,K>* r, Node<DEGREE,K> ** snapshot);
        bool scx(const int tid, wrapper_info<DEGREE,K> * info);
        void helpOther(const int tid, tagptr_t tagptr);
        int help(const int tid, const tagptr_t tagptr, SCXRecord<DEGREE,K> const * const snap, const bool helpingOther);

        SCXRecord<DEGREE,K>* createSCXRecord(const int tid, wrapper_info<DEGREE,K> * info);
        Node<DEGREE,K>* allocateNode(const int tid);

        void freeSubtree(Node<DEGREE,K>* node, int* nodes) {
            const int tid = 0;
            if (node == NULL) return;
            if (!node->isLeaf()) {
                for (int i=0;i<node->getABDegree();++i) {
                    freeSubtree(node->ptrs[i], nodes);
                }
            }
            ++(*nodes);
            recordmgr->deallocate(tid, node);
        }

        int init[MAX_THREADS_POW2] = {0,};
public:
//        PAD;
        void * const NO_VALUE;
        const int NUM_PROCESSES;
        PAD;
    #ifdef USE_DEBUGCOUNTERS
        debugCounters * const counters; // debug info
        PAD;
    #endif

        /**
         * This function must be called once by each thread that will
         * invoke any functions on this class.
         *
         * It must be okay that we do this with the main thread and later with another thread!
         */
        void initThread(const int tid) {
            if (init[tid]) return; else init[tid] = !init[tid];

            recordmgr->initThread(tid);
            rqProvider->initThread(tid);
        }
        void deinitThread(const int tid) {
            if (!init[tid]) return; else init[tid] = !init[tid];

            rqProvider->deinitThread(tid);
            recordmgr->deinitThread(tid);
        }

        /**
         * Creates a new B-slack tree wherein: <br>
         *      each internal node has up to <code>DEGREE</code> child pointers, and <br>
         *      each leaf has up to <code>DEGREE</code> key/value pairs, and <br>
         *      keys are ordered according to the provided comparator.
         */
        bslack(const int numProcesses,
                const K anyKey,
                int suspectedCrashSignal = SIGQUIT)
        : ALLOW_ONE_EXTRA_SLACK_PER_NODE(true)
        , b(DEGREE)
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
        , a(DEGREE/2 - 2)
    #endif
        , recordmgr(new RecManager(numProcesses, suspectedCrashSignal))
        , rqProvider(new RQProvider<K, void *, Node<DEGREE,K>, bslack<DEGREE,K,Compare,RecManager>, RecManager, false, false>(numProcesses, this, recordmgr))
        , SEQUENTIAL_STAT_TRACKING(false)
        , NO_VALUE((void *) -1LL)
        , NUM_PROCESSES(numProcesses)
    #ifdef USE_DEBUGCOUNTERS
        , counters(new debugCounters(numProcesses))
    #endif
        {
            cmp = Compare();

            const int tid = 0;
            initThread(tid);

            recordmgr->endOp(tid);

            DESC1_INIT_ALL(numProcesses);

            SCXRecord<DEGREE,K> *dummy = TAGPTR1_UNPACK_PTR(DUMMY);
            dummy->c.mutables = MUTABLES1_INIT_DUMMY;
            TRACE COUTATOMICTID("DUMMY mutables="<<dummy->c.mutables<<std::endl);

            // initial tree: entry is a sentinel node (with one pointer and no keys)
            //               that points to an empty node (no pointers and no keys)
            Node<DEGREE,K>* _entryLeft = allocateNode(tid);
            _entryLeft->scxPtr = DUMMY;
            _entryLeft->leaf = true;
            _entryLeft->marked = false;
            _entryLeft->weight = true;
            _entryLeft->size = 0;
            _entryLeft->searchKey = anyKey;

            Node<DEGREE,K>* _entry = allocateNode(tid);
            _entry = allocateNode(tid);
            _entry->scxPtr = DUMMY;
            _entry->leaf = false;
            _entry->marked = false;
            _entry->weight = true;
            _entry->size = 1;
            _entry->searchKey = anyKey;
            _entry->ptrs[0] = _entryLeft;

            // need to simulate real insertion of root and the root's child,
            // since range queries will actually try to add these nodes,
            // and we don't want blocking rq providers to spin forever
            // waiting for their itimes to be set to a positive number.
            Node<DEGREE,K>* insertedNodes[] = {_entry, _entryLeft, NULL};
            Node<DEGREE,K>* deletedNodes[] = {NULL};
            rqProvider->linearize_update_at_write(tid, &entry, _entry, insertedNodes, deletedNodes);

            operationCount = 0;
            overflows = 0;
            weightChecks = 0;
            weightCheckSearches = 0;
            weightFixAttempts = 0;
            weightFixes = 0;
            weightEliminated = 0;
            slackChecks = 0;
            slackCheckTotaling = 0;
            slackCheckSearches = 0;
            slackFixTotaling = 0;
            slackFixAttempts = 0;
            slackFixSCX = 0;
            slackFixes = 0;

    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
            COUTATOMIC("NOTICE: (a,b)-tree rebalancing enabled"<<std::endl);
    #else
            COUTATOMIC("NOTICE: B-slack tree rebalancing enabled"<<std::endl);
    #endif
        }

    #ifdef BSLACK_ENABLE_DESTRUCTOR
        ~bslack() {
            int nodes = 0;
            freeSubtree(entry, &nodes);
            COUTATOMIC("main thread: deleted tree containing "<<nodes<<" nodes"<<std::endl);
            delete rqProvider;
//            recordmgr->printStatus();
            delete recordmgr;
    #ifdef USE_DEBUGCOUNTERS
            delete counters;
    #endif
        }
    #endif

        Node<DEGREE,K> * debug_getEntryPoint() { return entry; }

    private:
        /*******************************************************************
         * Utility functions for integration with the test harness
         *******************************************************************/

        int sequentialSize(Node<DEGREE,K>* node) {
            if (node->isLeaf()) {
                return node->getKeyCount();
            }
            int retval = 0;
            for (int i=0;i<node->getABDegree();++i) {
                Node<DEGREE,K>* child = node->ptrs[i];
                retval += sequentialSize(child);
            }
            return retval;
        }
        int sequentialSize() {
            return sequentialSize(entry->ptrs[0]);
        }

        int getNumberOfLeaves(Node<DEGREE,K>* node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 1;
            int result = 0;
            for (int i=0;i<node->getABDegree();++i) {
                result += getNumberOfLeaves(node->ptrs[i]);
            }
            return result;
        }
        const int getNumberOfLeaves() {
            return getNumberOfLeaves(entry->ptrs[0]);
        }
        int getNumberOfInternals(Node<DEGREE,K>* node) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 0;
            int result = 1;
            for (int i=0;i<node->getABDegree();++i) {
                result += getNumberOfInternals(node->ptrs[i]);
            }
            return result;
        }
        const int getNumberOfInternals() {
            return getNumberOfInternals(entry->ptrs[0]);
        }
        const int getNumberOfNodes() {
            return getNumberOfLeaves() + getNumberOfInternals();
        }

        int getSumOfKeyDepths(Node<DEGREE,K>* node, int depth) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return depth * node->getKeyCount();
            int result = 0;
            for (int i=0;i<node->getABDegree();i++) {
                result += getSumOfKeyDepths(node->ptrs[i], 1+depth);
            }
            return result;
        }
        const int getSumOfKeyDepths() {
            return getSumOfKeyDepths(entry->ptrs[0], 0);
        }
        const double getAverageKeyDepth() {
            long sz = sequentialSize();
            return (sz == 0) ? 0 : getSumOfKeyDepths() / sz;
        }

        int getHeight(Node<DEGREE,K>* node, int depth) {
            if (node == NULL) return 0;
            if (node->isLeaf()) return 0;
            int result = 0;
            for (int i=0;i<node->getABDegree();i++) {
                int retval = getHeight(node->ptrs[i], 1+depth);
                if (retval > result) result = retval;
            }
            return result+1;
        }
        const int getHeight() {
            return getHeight(entry->ptrs[0], 0);
        }

        int getKeyCount(Node<DEGREE,K>* entry) {
            if (entry == NULL) return 0;
            if (entry->isLeaf()) return entry->getKeyCount();
            int sum = 0;
            for (int i=0;i<entry->getABDegree();++i) {
                sum += getKeyCount(entry->ptrs[i]);
            }
            return sum;
        }
        int getTotalDegree(Node<DEGREE,K>* entry) {
            if (entry == NULL) return 0;
            int sum = entry->getKeyCount();
            if (entry->isLeaf()) return sum;
            for (int i=0;i<entry->getABDegree();++i) {
                sum += getTotalDegree(entry->ptrs[i]);
            }
            return 1+sum; // one more children than keys
        }
        int getNodeCount(Node<DEGREE,K>* entry) {
            if (entry == NULL) return 0;
            if (entry->isLeaf()) return 1;
            int sum = 1;
            for (int i=0;i<entry->getABDegree();++i) {
                sum += getNodeCount(entry->ptrs[i]);
            }
            return sum;
        }
        double getAverageDegree() {
            return getTotalDegree(entry) / (double) getNodeCount(entry);
        }
        double getSpacePerKey() {
            return getNodeCount(entry)*2*b / (double) getKeyCount(entry);
        }

        long long getSumOfKeys(Node<DEGREE,K>* node) {
            TRACE COUTATOMIC("  getSumOfKeys("<<node<<"): isLeaf="<<node->isLeaf()<<std::endl);
            long long sum = 0;
            if (node->isLeaf()) {
                TRACE COUTATOMIC("      leaf sum +=");
                for (int i=0;i<node->getKeyCount();++i) {
                    sum += (long long) node->keys[i];
                    TRACE COUTATOMIC(node->keys[i]);
                }
                TRACE COUTATOMIC(std::endl);
            } else {
                for (int i=0;i<node->getABDegree();++i) {
                    sum += getSumOfKeys(node->ptrs[i]);
                }
            }
            TRACE COUTATOMIC("  getSumOfKeys("<<node<<"): sum="<<sum<<std::endl);
            return sum;
        }
        long long getSumOfKeys() {
            TRACE COUTATOMIC("getSumOfKeys()"<<std::endl);
            return getSumOfKeys(entry);
        }

        /**
         * Functions for verifying that the data structure is a B-slack tree
         */

        bool satisfiesP1(Node<DEGREE,K>* node, int height, int depth) {
            if (node->isLeaf()) return (height == depth);
            for (int i=0;i<node->getABDegree();++i) {
                if (!satisfiesP1(node->ptrs[i], height, depth+1)) return false;
            }
            return true;
        }
        bool satisfiesP1() {
            return satisfiesP1(entry->ptrs[0], getHeight(), 0);
        }

        bool satisfiesP2(Node<DEGREE,K>* node) {
            if (node->isLeaf()) return true;
            if (node->getABDegree() < 2) return false;
            if (node->getKeyCount() + 1 != node->getABDegree()) return false;
            for (int i=0;i<node->getABDegree();++i) {
                if (!satisfiesP2(node->ptrs[i])) return false;
            }
            return true;
        }
        bool satisfiesP2() {
            return satisfiesP2(entry->ptrs[0]);
        }

        bool noWeightViolations(Node<DEGREE,K>* node) {
            if (!node->weight) return false;
            if (!node->isLeaf()) {
                for (int i=0;i<node->getABDegree();++i) {
                    if (!noWeightViolations(node->ptrs[i])) return false;
                }
            }
            return true;
        }
        bool noWeightViolations() {
            return noWeightViolations(entry->ptrs[0]);
        }

    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
        bool abtree_noDegreeViolations(Node<DEGREE,K>* node) {
            if (!(node->size >= a || node == entry || node == entry->ptrs[0])) {
                std::cerr<<"degree violation found: node->size="<<node->size<<" a="<<a<<std::endl;
                return false;
            }
            if (!node->isLeaf()) {
                for (int i=0;i<node->getABDegree();++i) {
                    if (!abtree_noDegreeViolations(node->ptrs[i])) return false;
                }
            }
            return true;
        }
        bool abtree_noDegreeViolations() {
            return abtree_noDegreeViolations(entry->ptrs[0]);
        }
    #endif

        bool childrenAreAllLeavesOrInternal(Node<DEGREE,K>* node) {
            if (node->isLeaf()) return true;
            bool leafChild = false;
            for (int i=0;i<node->getABDegree();++i) {
                if (node->ptrs[i]->isLeaf()) leafChild = true;
                else if (leafChild) return false;
            }
            return true;
        }
        bool childrenAreAllLeavesOrInternal() {
            return childrenAreAllLeavesOrInternal(entry->ptrs[0]);
        }

        bool satisfiesP4(Node<DEGREE,K>* node) {
            // note: this function assumes that childrenAreAllLeavesOrInternal() = true
            if (node->isLeaf()) return true;
            int totalDegreeOfChildren = 0;
            for (int i=0;i<node->getABDegree();++i) {
                Node<DEGREE,K>* c = node->ptrs[i];
                if (!satisfiesP4(c)) return false;
                totalDegreeOfChildren += (c->isLeaf() ? c->getKeyCount() : c->getABDegree());
            }
            int slack = node->getABDegree() * b - totalDegreeOfChildren;
            if (slack >= b + (ALLOW_ONE_EXTRA_SLACK_PER_NODE ? node->getABDegree() : 0)) {
                return false;
            }
            return true;
        }
        bool satisfiesP4() {
            return satisfiesP4(entry->ptrs[0]);
        }

        void bslack_error(std::string s) {
            std::cerr<<"ERROR: "<<s<<std::endl;
            exit(-1);
        }

        bool isBSlackTree() {
            if (!satisfiesP1()) bslack_error("satisfiesP1() == false");
            if (!satisfiesP2()) bslack_error("satisfiesP2() == false");
            if (!noWeightViolations()) bslack_error("noWeightViolations() == false");
            if (!childrenAreAllLeavesOrInternal()) bslack_error("childrenAreAllLeavesOrInternal() == false");
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
            if (!abtree_noDegreeViolations()) bslack_error("abtree_noDegreeViolations() == false");
    #else
            if (!satisfiesP4()) bslack_error("satisfiesP4() == false");
    #endif
            return true;
        }

        void debugPrint() {
            if (SEQUENTIAL_STAT_TRACKING) {
                std::cout<<"overflows="<<overflows<<std::endl;
                std::cout<<"weightChecks="<<weightChecks<<std::endl;
                std::cout<<"weightCheckSearches="<<weightCheckSearches<<std::endl;
                std::cout<<"weightFixAttempts="<<weightFixAttempts<<std::endl;
                std::cout<<"weightFixes="<<weightFixes<<std::endl;
                std::cout<<"weightEliminated="<<weightEliminated<<std::endl;
                std::cout<<"slackChecks="<<slackChecks<<std::endl;
                std::cout<<"slackCheckTotaling="<<slackCheckTotaling<<std::endl;
                std::cout<<"slackCheckSearches="<<slackCheckSearches<<std::endl;
                std::cout<<"slackFixTotaling="<<slackFixTotaling<<std::endl;
                std::cout<<"slackFixAttempts="<<slackFixAttempts<<std::endl;
                std::cout<<"slackFixSCX="<<slackFixSCX<<std::endl;
                std::cout<<"slackFixes="<<slackFixes<<std::endl;
            }
            std::cout<<"averageDegree="<<getAverageDegree()<<std::endl;
            std::cout<<"averageDepth="<<getAverageKeyDepth()<<std::endl;
            std::cout<<"height="<<getHeight()<<std::endl;
            std::cout<<"internalNodes="<<getNumberOfInternals()<<std::endl;
            std::cout<<"leafNodes="<<getNumberOfLeaves()<<std::endl;
        }

    public:
        void * insert(const int tid, const K& key, void * const val) {
            return doInsert(tid, key, val, true);
        }
        void * insertIfAbsent(const int tid, const K& key, void * const val) {
            return doInsert(tid, key, val, false);
        }
        const std::pair<void*,bool> erase(const int tid, const K& key);
        const std::pair<void*,bool> find(const int tid, const K& key);
        bool contains(const int tid, const K& key);
        int rangeQuery(const int tid, const K& low, const K& hi, K * const resultKeys, void ** const resultValues);
        bool validate(const long long keysum, const bool checkkeysum) {
            if (checkkeysum) {
                long long treekeysum = getSumOfKeys();
                if (treekeysum != keysum) {
                    std::cerr<<"ERROR: tree keysum "<<treekeysum<<" did not match thread keysum "<<keysum<<std::endl;
                    return false;
                }
            }

            debugPrint();
            bool isbslack = isBSlackTree();
            return isbslack;
        }

        /**
         * BEGIN FUNCTIONS FOR RANGE QUERY SUPPORT
         */

        inline bool isLogicallyDeleted(const int tid, Node<DEGREE,K> * node) {
            return false;
        }

        inline int getKeys(const int tid, Node<DEGREE,K> * node, K * const outputKeys, void ** const outputValues) {
            if (node->isLeaf()) {
                // leaf ==> its keys are in the set.
                const int sz = node->getKeyCount();
                for (int i=0;i<sz;++i) {
                    outputKeys[i] = node->keys[i];
                    outputValues[i] = (void *) node->ptrs[i];
                }
                return sz;
            }
            // note: internal ==> its keys are NOT in the set
            return 0;
        }

        bool isInRange(const K& key, const K& lo, const K& hi) {
            return (!cmp(key, lo) && !cmp(hi, key));
        }

        /**
         * END FUNCTIONS FOR RANGE QUERY SUPPORT
         */

        long long getSizeInNodes() {
            return getNumberOfNodes();
        }
        std::string getSizeString() {
            std::stringstream ss;
            int preallocated = wrapper_info<DEGREE,K>::MAX_NODES * recordmgr->NUM_PROCESSES;
            ss<<getSizeInNodes()<<" nodes in tree";
            return ss.str();
        }
        long long getSize(Node<DEGREE,K> * node) {
            return sequentialSize(node);
        }
        long long getSize() {
            return sequentialSize();
        }
        RecManager * const debugGetRecMgr() {
            return recordmgr;
        }
        long long debugKeySum() {
            return getSumOfKeys();
        }
    #ifdef USE_DEBUGCOUNTERS
        debugCounters * const debugGetCounters() {
            return counters;
        }
    #endif
    //    void debugPrintTree() {
    //        entry->printTreeFile(cout);
    //    }
        void debugPrintToFile(std::string prefix, long id1, std::string infix, long id2, std::string suffix) {
            std::stringstream ss;
            ss<<prefix<<id1<<infix<<id2<<suffix;
            COUTATOMIC("print to filename \""<<ss.str()<<"\""<<std::endl);
            std::fstream fs (ss.str().c_str(), std::fstream::out);
            entry->printTreeFile(fs);
            fs.close();
        }
    #ifdef USE_DEBUGCOUNTERS
        void clearCounters() {
            counters->clear();
        }
    #endif
    };
} // namespace

template <int DEGREE, typename K, class Compare, class RecManager>
bslack_ns::SCXRecord<DEGREE,K> * bslack_ns::bslack<DEGREE,K,Compare,RecManager>::createSCXRecord(const int tid, wrapper_info<DEGREE,K> * info) {

    SCXRecord<DEGREE,K> * result = DESC1_NEW(tid);
    result->c.newNode = info->newNode;
    for (int i=0;i<info->numberOfNodes;++i) {
        result->c.nodes[i] = info->nodes[i];
    }
    for (int i=0;i<info->numberOfNodesToFreeze;++i) {
        result->c.scxPtrsSeen[i] = info->scxPtrs[i];
    }

    int i;
    for (i=0;info->insertedNodes[i];++i) result->c.insertedNodes[i] = info->insertedNodes[i];
    result->c.insertedNodes[i] = NULL;
    for (i=0;info->deletedNodes[i];++i) result->c.deletedNodes[i] = info->deletedNodes[i];
    result->c.deletedNodes[i] = NULL;

    result->c.field = info->field;
    result->c.numberOfNodes = info->numberOfNodes;
    result->c.numberOfNodesToFreeze = info->numberOfNodesToFreeze;
    DESC1_INITIALIZED(tid);
    return result;
}

template <int DEGREE, typename K, class Compare, class RecManager>
bslack_ns::Node<DEGREE,K> * bslack_ns::bslack<DEGREE,K,Compare,RecManager>::allocateNode(const int tid) {
    Node<DEGREE,K> *newnode = recordmgr->template allocate<Node<DEGREE,K> >(tid);
    if (newnode == NULL) {
        COUTATOMICTID("ERROR: could not allocate node"<<std::endl);
        exit(-1);
    }
    rqProvider->init_node(tid, newnode);
// #ifdef GSTATS_HANDLE_STATS
//     GSTATS_APPEND(tid, node_allocated_addresses, ((long long) newnode)%(1<<12));
// #endif
    return newnode;
}

/**
 * Returns the value associated with key, or NULL if key is not present.
 */
template <int DEGREE, typename K, class Compare, class RecManager>
const std::pair<void*,bool> bslack_ns::bslack<DEGREE,K,Compare,RecManager>::find(const int tid, const K& key) {
    std::pair<void*,bool> result;
    auto guard = recordmgr->getGuard(tid, true);
    Node<DEGREE,K> * l = rqProvider->read_addr(tid, &entry->ptrs[0]);
    while (!l->isLeaf()) {
        int ix = l->getChildIndex(key, cmp);
        l = rqProvider->read_addr(tid, &l->ptrs[ix]);
    }
    int index = l->getKeyIndex(key, cmp);
    if (index < l->getKeyCount() && l->keys[index] == key) {
        result.first = l->ptrs[index]; // this is a value, not a pointer, so it cannot be modified by rqProvider->linearize_update_at_..., so we do not use read_addr
        result.second = true;
    } else {
        result.first = NO_VALUE;
        result.second = false;
    }
    return result;
}

template <int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::contains(const int tid, const K& key) {
    return find(tid, key).second;
}

template<int DEGREE, typename K, class Compare, class RecManager>
int bslack_ns::bslack<DEGREE,K,Compare,RecManager>::rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, void ** const resultValues) {
    block<Node<DEGREE,K>> stack (NULL);
    auto guard = recordmgr->getGuard(tid, true);
    rqProvider->traversal_start(tid);

    // depth first traversal (of interesting subtrees)
    int size = 0;
    TRACE COUTATOMICTID("rangeQuery(lo="<<lo<<", hi="<<hi<<", size="<<(hi-lo+1)<<")"<<std::endl);

    stack.push(entry);
    while (!stack.isEmpty()) {
        Node<DEGREE,K> * node = stack.pop();
        assert(node);

        // if leaf node, check if we should add its keys to the traversal
        if (node->isLeaf()) {
            rqProvider->traversal_try_add(tid, node, resultKeys, resultValues, &size, lo, hi);

        // else if internal node, explore its children
        } else {
            // find right-most sub-tree that could contain a key in [lo, hi]
            int nkeys = node->getKeyCount();
            int r = nkeys;
            while (r > 0 && cmp(hi, (const K&) node->keys[r-1])) --r;           // subtree rooted at node->ptrs[r] contains only keys > hi

            // find left-most sub-tree that could contain a key in [lo, hi]
            int l = 0;
            while (l < nkeys && !cmp(lo, (const K&) node->keys[l])) ++l;        // subtree rooted at node->ptrs[l] contains only keys < lo

            // perform DFS from left to right (so push onto stack from right to left)
            for (int i=r;i>=l; --i) stack.push(rqProvider->read_addr(tid, &node->ptrs[i]));

//            // simply explore EVERYTHING
//            for (int i=0;i<node->getABDegree();++i) {
//                stack.push(rqProvider->read_addr(tid, &node->ptrs[i]));
//            }
        }
    }

    // success
    rqProvider->traversal_end(tid, resultKeys, resultValues, &size, lo, hi);
    return size;
}


template <int DEGREE, typename K, class Compare, class RecManager>
void* bslack_ns::bslack<DEGREE,K,Compare,RecManager>::doInsert(const int tid, const K& key, void * const value, const bool replace) {
    wrapper_info<DEGREE,K> _info;
    wrapper_info<DEGREE,K>* info = &_info;
    while (true) {
        /**
         * search
         */
        auto guard = recordmgr->getGuard(tid);
        Node<DEGREE,K>* gp = NULL;
        Node<DEGREE,K>* p = entry;
        Node<DEGREE,K>* l = rqProvider->read_addr(tid, &p->ptrs[0]);
        int ixToP = -1;
        int ixToL = 0;
        while (!l->isLeaf()) {
            ixToP = ixToL;
            ixToL = l->getChildIndex(key, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]);
        }

        /**
         * do the update
         */
        int keyIndex = l->getKeyIndex(key, cmp);
        if (keyIndex < l->getKeyCount() && l->keys[keyIndex] == key) {
            /**
             * if l already contains key, replace the existing value
             */
            void* const oldValue = l->ptrs[keyIndex]; // this is a value, not a pointer, so it cannot be modified by rqProvider->linearize_update_at_..., so we do not use read_addr
            if (!replace) {
                return oldValue;
            }

            // perform LLXs
            if (!llx(tid, p, NULL, 0, info->scxPtrs, info->nodes)
                     || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) {
                continue;    // retry the search
            }
            info->nodes[1] = l;

            // create new node(s)
            Node<DEGREE,K>* n = allocateNode(tid);
            arraycopy(l->keys, 0, n->keys, 0, l->getKeyCount());
            arraycopy(l->ptrs, 0, n->ptrs, 0, l->getABDegree());    // although we are copying l->ptrs, since l is a leaf, l->ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
            n->ptrs[keyIndex] = (Node<DEGREE,K>*) value;            // similarly, we don't use write_addr here
            n->leaf = true;
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = l->searchKey;
            n->size = l->size;
            n->weight = true;

            // construct info record to pass to SCX
            info->numberOfNodes = 2;
            info->numberOfNodesAllocated = 1;
            info->numberOfNodesToFreeze = 1;
            info->field = &p->ptrs[ixToL];
            info->newNode = n;
            info->insertedNodes[0] = n;
            info->insertedNodes[1] = NULL;
            info->deletedNodes[0] = l;
            info->deletedNodes[1] = NULL;

            if (scx(tid, info)) {
                TRACE COUTATOMICTID("replace std::pair ("<<key<<", "<<value<<"): SCX succeeded"<<std::endl);
#ifndef REBALANCING_NONE
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
                fixDegreeOrSlackViolation(tid, n);
    #endif
#endif
                return oldValue;
            }
            TRACE COUTATOMICTID("replace std::pair ("<<key<<", "<<value<<"): SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);

        } else {
            /**
             * if l does not contain key, we have to insert it
             */

            // perform LLXs
            if (!llx(tid, p, NULL, 0, info->scxPtrs, info->nodes) || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) {
                continue;    // retry the search
            }
            info->nodes[1] = l;

            if (l->getKeyCount() < b) {
                /**
                 * Insert pair
                 */

                // create new node(s)
                Node<DEGREE,K>* n = allocateNode(tid);
                arraycopy(l->keys, 0, n->keys, 0, keyIndex);
                arraycopy(l->keys, keyIndex, n->keys, keyIndex+1, l->getKeyCount()-keyIndex);
                n->keys[keyIndex] = key;
                arraycopy(l->ptrs, 0, n->ptrs, 0, keyIndex); // although we are copying the ptrs array, since the source node is a leaf, ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
                arraycopy(l->ptrs, keyIndex, n->ptrs, keyIndex+1, l->getABDegree()-keyIndex);
                n->ptrs[keyIndex] = (Node<DEGREE,K>*) value; // similarly, we don't use write_addr here
                n->leaf = l->leaf;
                n->marked = false;
                n->scxPtr = DUMMY;
                n->searchKey = l->searchKey;
                n->size = l->size+1;
                n->weight = l->weight;

                // construct info record to pass to SCX
                info->numberOfNodes = 2;
                info->numberOfNodesAllocated = 1;
                info->numberOfNodesToFreeze = 1;
                info->field = &p->ptrs[ixToL];
                info->newNode = n;
                info->insertedNodes[0] = n;
                info->insertedNodes[1] = NULL;
                info->deletedNodes[0] = l;
                info->deletedNodes[1] = NULL;

                if (scx(tid, info)) {
                    TRACE COUTATOMICTID("insert std::pair ("<<key<<", "<<value<<"): SCX succeeded"<<std::endl);
#ifndef REBALANCING_NONE
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
                    fixDegreeOrSlackViolation(tid, n);
    #endif
#endif
                    return NO_VALUE;
                }
                TRACE COUTATOMICTID("insert std::pair ("<<key<<", "<<value<<"): SCX FAILED"<<std::endl);
                this->recordmgr->deallocate(tid, n);

            } else { // assert: l->getKeyCount() == DEGREE == b)
                /**
                 * Overflow
                 */

                // first, we create a pair of large arrays
                // containing too many keys and pointers to fit in a single node
                K keys[DEGREE+1];
                Node<DEGREE,K>* ptrs[DEGREE+1];
                arraycopy(l->keys, 0, keys, 0, keyIndex);
                arraycopy(l->keys, keyIndex, keys, keyIndex+1, l->getKeyCount()-keyIndex);
                keys[keyIndex] = key;
                arraycopy(l->ptrs, 0, ptrs, 0, keyIndex); // although we are copying the ptrs array, since the source node is a leaf, ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
                arraycopy(l->ptrs, keyIndex, ptrs, keyIndex+1, l->getABDegree()-keyIndex);
                ptrs[keyIndex] = (Node<DEGREE,K>*) value;

                // create new node(s):
                // since the new arrays are too big to fit in a single node,
                // we replace l by a new subtree containing three new nodes:
                // a parent, and two leaves;
                // the array contents are then split between the two new leaves

                const int size1 = (DEGREE+1)/2;
                Node<DEGREE,K>* left = allocateNode(tid);
                arraycopy(keys, 0, left->keys, 0, size1);
                arraycopy(ptrs, 0, left->ptrs, 0, size1); // although we are copying the ptrs array, since the node is a leaf, ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
                left->leaf = true;
                left->marked = false;
                left->scxPtr = DUMMY;
                left->searchKey = keys[0];
                left->size = size1;
                left->weight = true;

                const int size2 = (DEGREE+1) - size1;
                Node<DEGREE,K>* right = allocateNode(tid);
                arraycopy(keys, size1, right->keys, 0, size2);
                arraycopy(ptrs, size1, right->ptrs, 0, size2); // although we are copying the ptrs array, since the node is a leaf, ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
                right->leaf = true;
                right->marked = false;
                right->scxPtr = DUMMY;
                right->searchKey = keys[size1];
                right->size = size2;
                right->weight = true;

                Node<DEGREE,K>* n = allocateNode(tid);
                n->keys[0] = keys[size1];
                rqProvider->write_addr(tid, &n->ptrs[0], left);
                rqProvider->write_addr(tid, &n->ptrs[1], right);
                n->leaf = false;
                n->marked = false;
                n->scxPtr = DUMMY;
                n->searchKey = keys[size1];
                n->size = 2;
                n->weight = p == entry;

                // note: weight of new internal node n will be zero,
                //       unless it is the root; this is because we test
                //       p == entry, above; in doing this, we are actually
                //       performing Root-Zero at the same time as this Overflow
                //       if n will become the root (of the B-slack tree)

                // construct info record to pass to SCX
                info->numberOfNodes = 2;
                info->numberOfNodesAllocated = 3;
                info->numberOfNodesToFreeze = 1;
                info->field = &p->ptrs[ixToL];
                info->newNode = n;
                info->insertedNodes[0] = n;
                info->insertedNodes[1] = left;
                info->insertedNodes[2] = right;
                info->insertedNodes[3] = NULL;
                info->deletedNodes[0] = l;
                info->deletedNodes[1] = NULL;

                if (scx(tid, info)) {
                    TRACE COUTATOMICTID("insert overflow ("<<key<<", "<<value<<"): SCX succeeded"<<std::endl);
                    if (SEQUENTIAL_STAT_TRACKING) ++overflows;

                    // after overflow, there may be a weight violation at n,
                    // and there may be a slack violation at p
#ifndef REBALANCING_NONE
                    fixWeightViolation(tid, n);
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
    #else
                    fixDegreeOrSlackViolation(tid, p);
    #endif
#endif
                    return NO_VALUE;
                }
                TRACE COUTATOMICTID("insert overflow ("<<key<<", "<<value<<"): SCX FAILED"<<std::endl);
                this->recordmgr->deallocate(tid, n);
                this->recordmgr->deallocate(tid, left);
                this->recordmgr->deallocate(tid, right);
            }
        }
    }
}

template <int DEGREE, typename K, class Compare, class RecManager>
const std::pair<void*,bool> bslack_ns::bslack<DEGREE,K,Compare,RecManager>::erase(const int tid, const K& key) {
    wrapper_info<DEGREE,K> _info;
    wrapper_info<DEGREE,K>* info = &_info;
    while (true) {
        /**
         * search
         */
        auto guard = recordmgr->getGuard(tid);
        Node<DEGREE,K>* gp = NULL;
        Node<DEGREE,K>* p = entry;
        Node<DEGREE,K>* l = rqProvider->read_addr(tid, &p->ptrs[0]);
        int ixToP = -1;
        int ixToL = 0;
        while (!l->isLeaf()) {
            ixToP = ixToL;
            ixToL = l->getChildIndex(key, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]);
        }

        /**
         * do the update
         */
        const int keyIndex = l->getKeyIndex(key, cmp);
        if (keyIndex == l->getKeyCount() || l->keys[keyIndex] != key) {
            /**
             * if l does not contain key, we are done.
             */
            return std::pair<void*,bool>(NO_VALUE,false);
        } else {
            /**
             * if l contains key, replace l by a new copy that does not contain key.
             */

            // perform LLXs
            if (!llx(tid, p, NULL, 0, info->scxPtrs, info->nodes) || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) {
                continue;    // retry the search
            }
            info->nodes[1] = l;
            // create new node(s)
            Node<DEGREE,K>* n = allocateNode(tid);
            //printf("keyIndex=%d getABDegree-keyIndex=%d\n", keyIndex, l->getABDegree()-keyIndex);
            arraycopy(l->keys, 0, n->keys, 0, keyIndex);
            arraycopy(l->keys, keyIndex+1, n->keys, keyIndex, l->getKeyCount()-(keyIndex+1));
            arraycopy(l->ptrs, 0, n->ptrs, 0, keyIndex); // although we are copying the ptrs array, since the node is a leaf, ptrs CANNOT contain modified by rqProvider->linearize_update_at_..., so we do not use arraycopy_ptrs.
            arraycopy(l->ptrs, keyIndex+1, n->ptrs, keyIndex, l->getABDegree()-(keyIndex+1));
            n->leaf = true;
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = l->keys[0]; // NOTE: WE MIGHT BE DELETING l->keys[0], IN WHICH CASE newL IS EMPTY. HOWEVER, newL CAN STILL BE LOCATED BY SEARCHING FOR l->keys[0], SO WE USE THAT AS THE searchKey FOR newL.
            n->size = l->size-1;
            n->weight = true;

            // construct info record to pass to SCX
            info->numberOfNodes = 2;
            info->numberOfNodesAllocated = 1;
            info->numberOfNodesToFreeze = 1;
            info->field = &p->ptrs[ixToL];
            info->newNode = n;
            info->insertedNodes[0] = n;
            info->insertedNodes[1] = NULL;
            info->deletedNodes[0] = l;
            info->deletedNodes[1] = NULL;

            void* oldValue = l->ptrs[keyIndex]; // since the node is a leaf, ptrs is not modified by any call to rqProvider->linearize_update_at_..., so we do not need to use read_addr to access it
            if (scx(tid, info)) {
                TRACE COUTATOMICTID("delete std::pair ("<<key<<", "<<oldValue<<"): SCX succeeded"<<std::endl);

                /**
                 * Compress may be needed at p after removing key from l.
                 */
#ifndef REBALANCING_NONE
    #ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
                fixDegreeOrSlackViolation(tid, n);
    #else
                fixDegreeOrSlackViolation(tid, p);
    #endif
#endif
                return std::pair<void*,bool>(oldValue, true);
            }
            TRACE COUTATOMICTID("delete std::pair ("<<key<<", "<<oldValue<<"): SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);
        }
    }
}

/**
 *  suppose there is a violation at node that is replaced by an update (specifically, a template operation that performs a successful scx).
 *  we want to hand off the violation to the update that replaced the node.
 *  so, for each update, we consider all the violations that could be present before the update, and determine where each violation can be moved by the update.
 *
 *  in the following we use several names to refer to nodes involved in the update:
 *    n is the topmost new node created by the update,
 *    leaf is the leaf replaced by the update,
 *    u is the node labeled in the figure showing the bslack updates in the paper,
 *    pi(u) is the parent of u,
 *    p is the node whose child pointer is changed by the update (and the parent of n after the update), and
 *    root refers to the root of the bslack tree (NOT the sentinel entry -- in this implementation it refers to entry->ptrs[0])
 *
 *  delete [check: slack@p]
 *      no weight at leaf
 *      no degree at leaf
 *      no slack at leaf
 *      [maybe create slack at p]
 *
 *  insert [check: none]
 *      no weight at leaf
 *      no degree at leaf
 *      no slack at leaf
 *
 *  overflow [check: weight@n, slack@p]
 *      no weight at leaf
 *      no degree at leaf
 *      no slack at leaf
 *      [create weight at n]
 *      [maybe create slack at p]
 *
 *  root-zero [check: degree@n, slack@n]
 *      weight at root -> eliminated
 *      degree at root -> degree at n
 *      slack at root -> slack at n
 *
 *  root-replace [check: degree@n, slack@n]
 *      no weight at root
 *      degree at root -> eliminated
 *      slack at root -> eliminated
 *      weight at child of root -> eliminated
 *      degree at child of root -> degree at n
 *      slack at child of root -> slack at n
 *
 *  absorb [check: slack@n]
 *      no weight at pi(u)
 *      degree at pi(u) -> eliminated
 *      slack at pi(u) -> eliminated or slack at n
 *      weight at u -> eliminated
 *      no degree at u
 *      slack at u -> slack at n
 *
 *  split [check: weight@n, slack@n, slack@n.p1, slack@n.p2, slack@p]
 *      no weight at pi(u)
 *      no degree at pi(u)
 *      slack at pi(u) and/or u -> slack at n and/or n.p1 and/or n.p2
 *      weight at u -> weight at n
 *      no degree at u (since u has exactly 2 pointers)
 *      [maybe create slack at p]
 *
 *  compress [check: slack@children(n), slack@p, degree@n]
 *      no weight at u
 *      no degree at u
 *      slack at u -> eliminated
 *      no weight at any child of u
 *      degree at a child of u -> eliminated
 *      slack at a child of u -> eliminated or slack at a child of n
 *      [maybe create slack at any or all children of n]
 *      [maybe create slack at p]
 *      [maybe create degree at n]
 *
 *  one-child [check: slack@children(n)]
 *      no weight at u
 *      degree at u -> eliminated
 *      slack at u -> eliminated or slack at a child of n
 *      no weight any sibling of u or pi(u)
 *      no degree at any sibling of u or pi(u)
 *      slack at a sibling of u -> eliminated or slack at a child of n
 *      no slack at pi(u)
 *      [maybe create slack at any or all children of n]
 */

// returns true if the invocation of this method
// (and not another invocation of a method performed by this method)
// performed an scx, and false otherwise
template <int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::fixWeightViolation(const int tid, Node<DEGREE,K>* viol) {
    if (SEQUENTIAL_STAT_TRACKING) ++weightChecks;
    if (viol->weight) return false;

    // assert: viol is internal (because leaves always have weight = 1)
    // assert: viol is not entry or root (because both always have weight = 1)

    // do an optimistic check to see if viol was already removed from the tree
    if (llx(tid, viol, NULL) == FINALIZED) {
        // recall that nodes are finalized precisely when
        // they are removed from the tree
        // we hand off responsibility for any violations at viol to the
        // process that removed it.
        return false;
    }

    wrapper_info<DEGREE,K> _info;
    wrapper_info<DEGREE,K>* info = &_info;

    // try to locate viol, and fix any weight violation at viol
    while (true) {
        if (SEQUENTIAL_STAT_TRACKING) ++weightCheckSearches;

        const K k = viol->searchKey;
        Node<DEGREE,K>* gp = NULL;
        Node<DEGREE,K>* p = entry;
        Node<DEGREE,K>* l = rqProvider->read_addr(tid, &p->ptrs[0]);
        int ixToP = -1;
        int ixToL = 0;
        while (!l->isLeaf() && l != viol) {
            ixToP = ixToL;
            ixToL = l->getChildIndex(k, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]);
        }

        if (l != viol) {
            // l was replaced by another update.
            // we hand over responsibility for viol to that update.
            return false;
        }
        if (SEQUENTIAL_STAT_TRACKING) ++weightFixAttempts;

        // we cannot apply this update if p has a weight violation
        // so, we check if this is the case, and, if so, try to fix it
        if (!p->weight) {
            fixWeightViolation(tid, p);
            continue;
        }

        // perform LLXs
        if (!llx(tid, gp, NULL, 0, info->scxPtrs, info->nodes) || rqProvider->read_addr(tid, &gp->ptrs[ixToP]) != p) continue;    // retry the search
        if (!llx(tid, p, NULL, 1, info->scxPtrs, info->nodes) || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) continue;      // retry the search
        if (!llx(tid, l, NULL, 2, info->scxPtrs, info->nodes)) continue;                             // retry the search

        const int c = p->getABDegree() + l->getABDegree();
        const int size = c-1;

        if (size <= b) {
            /**
             * Absorb
             */

            // create new node(s)
            // the new arrays are small enough to fit in a single node,
            // so we replace p by a new internal node.
            Node<DEGREE,K>* n = allocateNode(tid);
            arraycopy_ptrs(p->ptrs, 0, n->ptrs, 0, ixToL); // p and l are both internal, so we use arraycopy_ptrs
            arraycopy_ptrs(l->ptrs, 0, n->ptrs, ixToL, l->getABDegree());
            arraycopy_ptrs(p->ptrs, ixToL+1, n->ptrs, ixToL+l->getABDegree(), p->getABDegree()-(ixToL+1));
            arraycopy(p->keys, 0, n->keys, 0, ixToL);
            arraycopy(l->keys, 0, n->keys, ixToL, l->getKeyCount());
            arraycopy(p->keys, ixToL, n->keys, ixToL+l->getKeyCount(), p->getKeyCount()-ixToL);
            n->leaf = false; assert(!l->isLeaf());
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = n->keys[0];
            n->size = size;
            n->weight = true;

            // construct info record to pass to SCX
            info->numberOfNodes = 3;
            info->numberOfNodesAllocated = 1;
            info->numberOfNodesToFreeze = 3;
            info->field = &gp->ptrs[ixToP];
            info->newNode = n;
//            info->insertedNodes[0] = info->deletedNodes[0] = NULL;
            info->insertedNodes[0] = n;
            info->insertedNodes[1] = NULL;
            info->deletedNodes[0] = p;
            info->deletedNodes[1] = l;
            info->deletedNodes[2] = NULL;

            if (scx(tid, info)) {
                TRACE COUTATOMICTID("absorb: SCX succeeded"<<std::endl);
                if (SEQUENTIAL_STAT_TRACKING) ++weightFixes;
                if (SEQUENTIAL_STAT_TRACKING) ++weightEliminated;

                //    absorb [check: slack@n]
                //        no weight at pi(u)
                //        degree at pi(u) -> eliminated
                //        slack at pi(u) -> eliminated or slack at n
                //        weight at u -> eliminated
                //        no degree at u
                //        slack at u -> slack at n

                /**
                 * Compress may be needed at the new internal node we created
                 * (since we move grandchildren from two parents together).
                 */
                fixDegreeOrSlackViolation(tid, n);
                return true;
            }
            TRACE COUTATOMICTID("absorb: SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);

        } else {
            /**
             * Split
             */

            // merge keys of p and l into one big array (and similarly for children)
            // (we essentially replace the pointer to l with the contents of l)
            K keys[2*DEGREE];
            Node<DEGREE,K>* ptrs[2*DEGREE];
            arraycopy_ptrs(p->ptrs, 0, ptrs, 0, ixToL); // p and l are both internal, so we use arraycopy_ptrs
            arraycopy_ptrs(l->ptrs, 0, ptrs, ixToL, l->getABDegree());
            arraycopy_ptrs(p->ptrs, ixToL+1, ptrs, ixToL+l->getABDegree(), p->getABDegree()-(ixToL+1));
            arraycopy(p->keys, 0, keys, 0, ixToL);
            arraycopy(l->keys, 0, keys, ixToL, l->getKeyCount());
            arraycopy(p->keys, ixToL, keys, ixToL+l->getKeyCount(), p->getKeyCount()-ixToL);

            // the new arrays are too big to fit in a single node,
            // so we replace p by a new internal node and two new children.
            //
            // we take the big merged array and split it into two arrays,
            // which are used to create two new children u and v.
            // we then create a new internal node (whose weight will be zero
            // if it is not the root), with u and v as its children.

            // create new node(s)
            const int size1 = size / 2;
            Node<DEGREE,K>* left = allocateNode(tid);
            arraycopy(keys, 0, left->keys, 0, size1-1);
            arraycopy_ptrs(ptrs, 0, left->ptrs, 0, size1);
            left->leaf = false; assert(!l->isLeaf());
            left->marked = false;
            left->scxPtr = DUMMY;
            left->searchKey = keys[0];
            left->size = size1;
            left->weight = true;

            const int size2 = size - size1;
            Node<DEGREE,K>* right = allocateNode(tid);
            arraycopy(keys, size1, right->keys, 0, size2-1);
            arraycopy_ptrs(ptrs, size1, right->ptrs, 0, size2);
            right->leaf = false;
            right->marked = false;
            right->scxPtr = DUMMY;
            right->searchKey = keys[size1];
            right->size = size2;
            right->weight = true;

            Node<DEGREE,K>* n = allocateNode(tid);
            n->keys[0] = keys[size1-1];
            rqProvider->write_addr(tid, &n->ptrs[0], left);
            rqProvider->write_addr(tid, &n->ptrs[1], right);
            n->leaf = false;
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = keys[size1-1]; // note: should be the same as n->keys[0]
            n->size = 2;
            n->weight = (gp == entry);

            // note: weight of new internal node n will be zero,
            //       unless it is the root; this is because we test
            //       gp == entry, above; in doing this, we are actually
            //       performing Root-Zero at the same time as this Overflow
            //       if n will become the root (of the B-slack tree)

            // construct info record to pass to SCX
            info->numberOfNodes = 3;
            info->numberOfNodesAllocated = 3;
            info->numberOfNodesToFreeze = 3;
            info->field = &gp->ptrs[ixToP];
            info->newNode = n;
//            info->insertedNodes[0] = info->deletedNodes[0] = NULL;
            info->insertedNodes[0] = n;
            info->insertedNodes[1] = left;
            info->insertedNodes[2] = right;
            info->insertedNodes[3] = NULL;
            info->deletedNodes[0] = p;
            info->deletedNodes[1] = l;
            info->deletedNodes[2] = NULL;

            if (scx(tid, info)) {
                TRACE COUTATOMICTID("split: SCX succeeded"<<std::endl);
                if (SEQUENTIAL_STAT_TRACKING) ++weightFixes;
                if (SEQUENTIAL_STAT_TRACKING) if (gp == entry) ++weightEliminated;

#ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
                fixWeightViolation(tid, n);
                fixDegreeOrSlackViolation(tid, n);
#else
                //    split [check: weight@n, slack@n, slack@n.p1, slack@n.p2, slack@p]
                //        no weight at pi(u)
                //        no degree at pi(u)
                //        slack at pi(u) and/or u -> slack at n and/or n.p1 and/or n.p2
                //        weight at u -> weight at n
                //        no degree at u (since u has exactly 2 pointers)
                //        [maybe create slack at p]
                fixWeightViolation(tid, n);
                fixDegreeOrSlackViolation(tid, n);       // corresponds to node n using the terminology of the preceding comment
                fixDegreeOrSlackViolation(tid, left);    // corresponds to node n.p1 using the terminology of the preceding comment
                fixDegreeOrSlackViolation(tid, right);   // corresponds to node n.p2 using the terminology of the preceding comment
                fixDegreeOrSlackViolation(tid, gp);      // corresponds to node p using the terminology of the preceding comment
#endif
                return true;
            }
            TRACE COUTATOMICTID("split: SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);
            this->recordmgr->deallocate(tid, left);
            this->recordmgr->deallocate(tid, right);
        }
    }
}

#ifdef USE_SIMPLIFIED_ABTREE_REBALANCING
template <int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::fixDegreeOrSlackViolation(const int tid, Node<DEGREE,K>* viol) {
#ifdef REBALANCING_WEIGHT_ONLY
    return false;
#else
    if (viol->getABDegree() >= a || viol == entry || viol == rqProvider->read_addr(tid, &entry->ptrs[0])) {
        return false; // no degree violation at viol
    }

    // do an optimistic check to see if viol was already removed from the tree
    if (llx(tid, viol, NULL) == FINALIZED) {
        // recall that nodes are finalized precisely when
        // they are removed from the tree.
        // we hand off responsibility for any violations at viol to the
        // process that removed it.
        return false;
    }

    wrapper_info<DEGREE,K> _info;
    wrapper_info<DEGREE,K>* info = &_info;

    // we search for viol and try to fix any violation we find there
    // this entails performing AbsorbSibling or Distribute.
    while (true) {
        if (SEQUENTIAL_STAT_TRACKING) ++slackCheckSearches;
        /**
         * search for viol
         */
        const K k = viol->searchKey;
        Node<DEGREE,K>* gp = NULL;
        Node<DEGREE,K>* p = entry;
        Node<DEGREE,K>* l = rqProvider->read_addr(tid, &p->ptrs[0]);
        int ixToP = -1;
        int ixToL = 0;
        while (!l->isLeaf() && l != viol) {
            ixToP = ixToL;
            ixToL = l->getChildIndex(k, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]);
        }

        if (l != viol) {
            // l was replaced by another update.
            // we hand over responsibility for viol to that update.
            return false;
        }

        // assert: gp != NULL (because if AbsorbSibling or Distribute can be applied, then p is not the root)

        // perform LLXs
        if (!llx(tid, gp, NULL, 0, info->scxPtrs, info->nodes)
                 || rqProvider->read_addr(tid, &gp->ptrs[ixToP]) != p) continue;   // retry the search
        if (!llx(tid, p, NULL, 1, info->scxPtrs, info->nodes)
                 || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) continue;     // retry the search

        int ixToS = (ixToL > 0 ? ixToL-1 : 1);
        Node<DEGREE,K>* s = rqProvider->read_addr(tid, &p->ptrs[ixToS]);

        // we can only apply AbsorbSibling or Distribute if there are no
        // weight violations at p, l or s.
        // so, we first check for any weight violations,
        // and fix any that we see.
        bool foundWeightViolation = false;
        if (!p->weight) {
            foundWeightViolation = true;
            fixWeightViolation(tid, p);
        }
        if (!l->weight) {
            foundWeightViolation = true;
            fixWeightViolation(tid, l);
        }
        if (!s->weight) {
            foundWeightViolation = true;
            fixWeightViolation(tid, s);
        }
        // if we see any weight violations, then either we fixed one,
        // removing one of these nodes from the tree,
        // or one of the nodes has been removed from the tree by another
        // rebalancing step, so we retry the search for viol
        if (foundWeightViolation) continue;

        // assert: there are no weight violations at p, l or s
        // assert: l and s are either both leaves or both internal nodes
        //         (because there are no weight violations at these nodes)

        // also note that p->size >= a >= 2

        Node<DEGREE,K>* left;
        Node<DEGREE,K>* right;
        int leftindex;
        int rightindex;

        if (ixToL < ixToS) {
            if (!llx(tid, l, NULL, 2, info->scxPtrs, info->nodes)) continue; // retry the search
            if (!llx(tid, s, NULL, 3, info->scxPtrs, info->nodes)) continue; // retry the search
            left = l;
            right = s;
            leftindex = ixToL;
            rightindex = ixToS;
        } else {
            if (!llx(tid, s, NULL, 2, info->scxPtrs, info->nodes)) continue; // retry the search
            if (!llx(tid, l, NULL, 3, info->scxPtrs, info->nodes)) continue; // retry the search
            left = s;
            right = l;
            leftindex = ixToS;
            rightindex = ixToL;
        }

        int sz = left->getABDegree() + right->getABDegree();
        assert(left->weight && right->weight);

        if (sz < 2*a) {
            /**
             * AbsorbSibling
             */

            // create new node(s))
            Node<DEGREE,K>* newl = allocateNode(tid);
            int k1=0, k2=0;
            for (int i=0;i<left->getKeyCount();++i) {
                newl->keys[k1++] = left->keys[i];
            }
            for (int i=0;i<left->getABDegree();++i) {
                if (left->isLeaf()) {
                    newl->ptrs[k2++] = left->ptrs[i];
                } else {
                    //assert(left->getKeyCount() != left->getABDegree());
                    rqProvider->write_addr(tid, &newl->ptrs[k2++], rqProvider->read_addr(tid, &left->ptrs[i]));
                }
            }
            if (!left->isLeaf()) newl->keys[k1++] = p->keys[leftindex];
            for (int i=0;i<right->getKeyCount();++i) {
                newl->keys[k1++] = right->keys[i];
            }
            for (int i=0;i<right->getABDegree();++i) {
                if (right->isLeaf()) {
                    newl->ptrs[k2++] = right->ptrs[i];
                } else {
                    rqProvider->write_addr(tid, &newl->ptrs[k2++], rqProvider->read_addr(tid, &right->ptrs[i]));
                }
            }
            newl->leaf = left->isLeaf();
            newl->marked = false;
            newl->scxPtr = DUMMY;
            newl->searchKey = l->searchKey;
            newl->size = l->getABDegree() + s->getABDegree();
            newl->weight = true; assert(left->weight && right->weight && p->weight);

            // now, we atomically replace p and its children with the new nodes.
            // if appropriate, we perform RootAbsorb at the same time.
            if (gp == entry && p->getABDegree() == 2) {

                // construct info record to pass to SCX
                info->numberOfNodes = 4; // gp + p + l + s
                info->numberOfNodesAllocated = 1; // newl
                info->numberOfNodesToFreeze = 4; // gp + p + l + s
                info->field = &gp->ptrs[ixToP];
                info->newNode = newl;
                info->insertedNodes[0] = newl;
                info->insertedNodes[1] = NULL;
                info->deletedNodes[0] = p;
                info->deletedNodes[1] = l;
                info->deletedNodes[2] = s;
                info->deletedNodes[3] = NULL;

                if (scx(tid, info)) {
                    TRACE COUTATOMICTID("absorbsibling AND rootabsorb: SCX succeeded"<<std::endl);
                    if (SEQUENTIAL_STAT_TRACKING) ++slackFixes;

                    fixDegreeOrSlackViolation(tid, newl);
                    return true;
                }
                TRACE COUTATOMICTID("absorbsibling AND rootabsorb: SCX FAILED"<<std::endl);
                this->recordmgr->deallocate(tid, newl);

            } else {
                assert(gp != entry || p->getABDegree() > 2);

                // create n from p by:
                // 1. skipping the key for leftindex and child pointer for ixToS
                // 2. replacing l with newl
                Node<DEGREE,K>* n = allocateNode(tid);
                for (int i=0;i<leftindex;++i) {
                    n->keys[i] = p->keys[i];
                }
                for (int i=0;i<ixToS;++i) {
                    rqProvider->write_addr(tid, &n->ptrs[i], rqProvider->read_addr(tid, &p->ptrs[i]));      // n and p are internal, so their ptrs arrays might have entries that are being modified by rqProvider->linearize_update_at_..., so we use read_addr and write_addr
                }
                for (int i=leftindex+1;i<p->getKeyCount();++i) {
                    n->keys[i-1] = p->keys[i];
                }
                for (int i=ixToL+1;i<p->getABDegree();++i) {
                    rqProvider->write_addr(tid, &n->ptrs[i-1], rqProvider->read_addr(tid, &p->ptrs[i]));    // n and p are internal, so their ptrs arrays might have entries that are being modified by rqProvider->linearize_update_at_..., so we use read_addr and write_addr
                }
                // replace l with newl
                rqProvider->write_addr(tid, &n->ptrs[ixToL - (ixToL > ixToS)], newl);
                n->leaf = false;
                n->marked = false;
                n->scxPtr = DUMMY;
                n->searchKey = p->searchKey;
                n->size = p->getABDegree()-1;
                n->weight = true;

                // construct info record to pass to SCX
                info->numberOfNodes = 4; // gp + p + l + s
                info->numberOfNodesAllocated = 2; // n + newl
                info->numberOfNodesToFreeze = 4; // gp + p + l + s
                info->field = &gp->ptrs[ixToP];
                info->newNode = n;
                info->insertedNodes[0] = n;
                info->insertedNodes[1] = newl;
                info->insertedNodes[2] = NULL;
                info->deletedNodes[0] = p;
                info->deletedNodes[1] = l;
                info->deletedNodes[2] = s;
                info->deletedNodes[3] = NULL;

#ifdef NO_NONROOT_SLACK_VIOLATION_FIXING
this->recordmgr->deallocate(tid, n);
this->recordmgr->deallocate(tid, newl);
return false;
#endif
                if (scx(tid, info)) {
                    TRACE COUTATOMICTID("absorbsibling: SCX succeeded"<<std::endl);
                    if (SEQUENTIAL_STAT_TRACKING) ++slackFixes;

                    fixDegreeOrSlackViolation(tid, newl);
                    fixDegreeOrSlackViolation(tid, n);
                    return true;
                }
                TRACE COUTATOMICTID("absorbsibling: SCX FAILED"<<std::endl);
                this->recordmgr->deallocate(tid, newl);
                this->recordmgr->deallocate(tid, n);
            }

        } else {
            /**
             * Distribute
             */

            int leftsz = sz/2;
            int rightsz = sz-leftsz;

            // create new node(s))
            Node<DEGREE,K>* n = allocateNode(tid);
            Node<DEGREE,K>* newleft = allocateNode(tid);
            Node<DEGREE,K>* newright = allocateNode(tid);

            // combine the contents of l and s (and one key from p if l and s are internal)
            K keys[2*DEGREE];
            Node<DEGREE,K>* ptrs[2*DEGREE];
            int k1=0, k2=0;
            for (int i=0;i<left->getKeyCount();++i) {
                keys[k1++] = left->keys[i];
            }
            for (int i=0;i<left->getABDegree();++i) {
                if (left->isLeaf()) {
                    ptrs[k2++] = left->ptrs[i];
                } else {
                    ptrs[k2++] = rqProvider->read_addr(tid, &left->ptrs[i]);
                }
            }
            if (!left->isLeaf()) keys[k1++] = p->keys[leftindex];
            for (int i=0;i<right->getKeyCount();++i) {
                keys[k1++] = right->keys[i];
            }
            for (int i=0;i<right->getABDegree();++i) {
                if (right->isLeaf()) {
                    ptrs[k2++] = right->ptrs[i];
                } else {
                    ptrs[k2++] = rqProvider->read_addr(tid, &right->ptrs[i]);
                }
            }

            // distribute contents between newleft and newright
            k1=0;
            k2=0;
            for (int i=0;i<leftsz - !left->isLeaf();++i) {
                newleft->keys[i] = keys[k1++];
            }
            for (int i=0;i<leftsz;++i) {
                if (left->isLeaf()) {
                    newleft->ptrs[i] = ptrs[k2++];
                } else {
                    rqProvider->write_addr(tid, &newleft->ptrs[i], ptrs[k2++]);
                }
            }
            newleft->leaf = left->isLeaf();
            newleft->marked = false;
            newleft->scxPtr = DUMMY;
            newleft->searchKey = newleft->keys[0];
            newleft->size = leftsz;
            newleft->weight = true;

            // reserve one key for the parent (to go between newleft and newright)
            K keyp = keys[k1];
            if (!left->isLeaf()) ++k1;
            for (int i=0;i<rightsz - !left->isLeaf();++i) {
                newright->keys[i] = keys[k1++];
            }
            for (int i=0;i<rightsz;++i) {
                if (right->isLeaf()) {
                    newright->ptrs[i] = ptrs[k2++];
                } else {
                    rqProvider->write_addr(tid, &newright->ptrs[i], ptrs[k2++]);
                }
            }
            newright->leaf = right->isLeaf();
            newright->marked = false;
            newright->scxPtr = DUMMY;
            newright->searchKey = newright->keys[0];
            newright->size = rightsz;
            newright->weight = true;

            // create n from p by replacing left with newleft and right with newright,
            // and replacing one key (between these two pointers)
            for (int i=0;i<p->getKeyCount();++i) {
                n->keys[i] = p->keys[i];
            }
            for (int i=0;i<p->getABDegree();++i) {
                rqProvider->write_addr(tid, &n->ptrs[i], rqProvider->read_addr(tid, &p->ptrs[i])); // n and p are internal, so their ptrs arrays might have entries that are being modified by rqProvider->linearize_update_at_..., so we use read_addr and write_addr
            }
            n->keys[leftindex] = keyp;
            rqProvider->write_addr(tid, &n->ptrs[leftindex], newleft);
            rqProvider->write_addr(tid, &n->ptrs[rightindex], newright);
            n->leaf = false;
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = p->searchKey;
            n->size = p->size;
            n->weight = true;

            // construct info record to pass to SCX
            info->numberOfNodes = 4; // gp + p + l + s
            info->numberOfNodesAllocated = 3; // n + newleft + newright
            info->numberOfNodesToFreeze = 4; // gp + p + l + s
            info->field = &gp->ptrs[ixToP];
            info->newNode = n;
            info->insertedNodes[0] = n;
            info->insertedNodes[1] = newleft;
            info->insertedNodes[2] = newright;
            info->insertedNodes[3] = NULL;
            info->deletedNodes[0] = p;
            info->deletedNodes[1] = l;
            info->deletedNodes[2] = s;
            info->deletedNodes[3] = NULL;

#ifdef NO_NONROOT_SLACK_VIOLATION_FIXING
this->recordmgr->deallocate(tid, n);
this->recordmgr->deallocate(tid, newleft);
this->recordmgr->deallocate(tid, newright);
return false;
#endif
            if (scx(tid, info)) {
                TRACE COUTATOMICTID("distribute: SCX succeeded"<<std::endl);
                if (SEQUENTIAL_STAT_TRACKING) ++slackFixes;

                fixDegreeOrSlackViolation(tid, n);
                return true;
            }
            TRACE COUTATOMICTID("distribute: SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);
            this->recordmgr->deallocate(tid, newleft);
            this->recordmgr->deallocate(tid, newright);
        }
    }
#endif
}
#else
// returns true if the invocation of this method
// (and not another invocation of a method performed by this method)
// performed an scx, and false otherwise
template <int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::fixDegreeOrSlackViolation(const int tid, Node<DEGREE,K>* viol) {
#ifdef REBALANCING_WEIGHT_ONLY
    return false;
#else
    /**
     * at a high level, in this function, we search for viol,
     * and then try to fix any degree or slack violations that we see there.
     * it is possible that viol has already been removed from the tree,
     * in which case we hand off responsibility for any violations at viol
     * to the process that removed it from the tree.
     * however, searching to determine whether viol is in the tree
     * before we know whether there are any violations to fix is expensive.
     * so, we first do an optimistic check to see if there is a violation
     * to fix at viol. if not, we can stop.
     * however, if there is a violation, then we go ahead and do the search
     * to find viol and check whether it is still in the tree.
     * once we've determined that viol is in the tree,
     * we follow the tree update template to perform a rebalancing step.
     * of course, any violation at viol might have been fixed between when
     * we found the violation with our optimistic check,
     * and when our search arrived at viol.
     * so, as part of the template (specifically, the Conflict procedure),
     * after performing llx on viol, we verify that there is still a
     * violation at viol. if not, then we are done.
     * if so, we use SCX to perform an update to fix the violation.
     * if that SCX succeeds, then a violation provably occurred at viol when
     * the SCX occurred.
     */

    // if viol is a leaf, then no violation occurs at viol
    if (SEQUENTIAL_STAT_TRACKING) ++slackChecks;
    if (viol->isLeaf()) return false;
    //assert(viol->weight); // TODO: DETERMINE IF THIS ASSERT IS INCORRECT

    // do an optimistic check to see if viol was already removed from the tree
    if (llx(tid, viol, NULL) == FINALIZED) {
        // recall that nodes are finalized precisely when
        // they are removed from the tree.
        // we hand off responsibility for any violations at viol to the
        // process that removed it.
        return false;
    }

    wrapper_info<DEGREE,K> _info;
    wrapper_info<DEGREE,K>* info = &_info;

    // optimistically check if there is no violation at viol before doing
    // a full search to try to locate viol and fix any violations at it
    if (viol->getABDegree() == 1) {
        // found a degree violation at viol
    } else {
#ifdef OPTIMIZATION_PRECHECK_DEGREE_VIOLATIONS
        // note: to determine whether there is a slack violation at viol,
        //       we must look at all of the children of viol.
        //       we use llx to get an atomic snapshot of the child pointers.
        //       if the llx returns FINALIZED, then viol was removed from
        //       the tree, so we hand off responsibility for any violations
        //       at viol to the process that removed it.
        //       if the llx returns FAILED, indicating that the llx was
        //       concurrent with an SCX that changed, or will change, viol,
        //       then we abort the optimistic violation check.
        if (SEQUENTIAL_STAT_TRACKING) ++slackCheckTotaling;
        Node<DEGREE,K>* ptrs[DEGREE];
        SCXRecord<DEGREE,K>* result = llx(tid, viol, ptrs);
        if (result == FINALIZED) {
            // viol was removed from the tree, so we hand off responsibility
            // for any violations at viol to the process that removed it.
            return false;
        } else if (result == FAILED) {
            // llx failed: go ahead and do the full search to find viol
        } else {
            // we have a snapshot of the child pointers
            // determine whether there is a slack violation at viol
            int slack = 0;
            int numLeaves = 0;
            int sz = viol->size;
            for (int i=0;i<sz;++i) {
                slack += b - ptrs[i]->getABDegree();
                if (ptrs[i]->isLeaf()) ++numLeaves;
            }
            if (numLeaves > 0 && numLeaves < viol->getABDegree()) {
                // some children are internal and some are leaves
                // consequently, there is a weight violation among the children.
                // thus, we can't fix any degree or slack violation until
                // the weight violation is fixed, so we continue with the
                // procedure,  which will find and repair any weight violations.
            } else if (slack >= b + (ALLOW_ONE_EXTRA_SLACK_PER_NODE ? viol->getABDegree() : 0)) {
                // found a slack violation at viol
            } else {
                // no slack violation or degree violation at viol
                return false;
            }
        }
#endif
    }

    // we found a degree violation or slack violation at viol
    // note: it is easy/efficient to determine which type we found.
    //       if we found a degree violation above, then,
    //       since the number of children in a node does not change,
    //       viol will always satisfy viol->getABDegree() == 1.
    //       however, if viol->getABDegree() > 1,
    //       then we know we found a slack violation, above.

    // we search for viol and try to fix any violation we find there
    while (true) {
        if (SEQUENTIAL_STAT_TRACKING) ++slackCheckSearches;
        /**
         * search for viol
         */
        const K k = viol->searchKey;
        Node<DEGREE,K>* gp = NULL;
        Node<DEGREE,K>* p = entry;
        Node<DEGREE,K>* l = rqProvider->read_addr(tid, &p->ptrs[0]);
        int ixToP = -1;
        int ixToL = 0;
        while (!l->isLeaf() && l != viol) {
            ixToP = ixToL;
            ixToL = l->getChildIndex(k, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]);
        }

        if (l != viol) {
            // l was replaced by another update.
            // we hand over responsibility for viol to that update.
            return false;
        }

        /**
         * observe that Compress and One-Child can be implemented in
         * exactly the same way.
         * consider the figure in the paper that shows these updates.
         * since k = ceil(c/b) when kb >= c > kb-b,
         * One-child actually has exactly the same effect as Compress.
         * thus, the code for Compress can be used fix both
         * degree and slack violations.
         *
         * the only difference is that, in Compress,
         * the (slack) violation occurs at the topmost node, top,
         * that is replaced by the update, and in One-Child,
         * the (degree) violation occurs at a child of top.
         * thus, if the violation we found at viol is a slack violation,
         * then the leaf l that we found in our search is top.
         * otherwise, the violation we found was a degree violation,
         * so l is a child of top.
         *
         * to facilitate the use of the same code in both cases,
         * if the violation we found was a slack violation,
         * then we take one extra step in the search,
         * so that l is a child of top.
         * this way, in each case, l is a child of top, p is top,
         * and gp is the parent of top
         * (so gp is the node whose child pointer will be changed).
         */
        if (viol->getABDegree() > 1) {
            // there is no degree violation at viol,
            // so we must have found a slack violation there, earlier.
            // we take one extra step in the search, so that p is top.
            ixToP = ixToL;
            ixToL = l->getChildIndex(k, cmp);
            gp = p;
            p = l;
            l = rqProvider->read_addr(tid, &l->ptrs[ixToL]); // TODO: verify l is not a leaf, or skip read_addr
        }
        // note: p is now top
        // assert: gp != NULL (because if Compress or One-Child can be applied, then p is not the root)

        // perform LLXs
        Node<DEGREE,K>* pChildren[DEGREE];
        if (!llx(tid, gp, NULL, 0, info->scxPtrs, info->nodes)
                 || rqProvider->read_addr(tid, &gp->ptrs[ixToP]) != p) continue;    // retry the search
        if (!llx(tid, p, pChildren, 1, info->scxPtrs, info->nodes)
                 || rqProvider->read_addr(tid, &p->ptrs[ixToL]) != l) continue; // retry the search

        // we can only apply Compress (or One-Child) if there are no
        // weight violations at p or its children.
        // so, we first check for any weight violations,
        // and fix any that we see.
        bool foundWeightViolation = false;
        for (int i=0;i<p->getABDegree();++i) {
            if (!pChildren[i]->weight) {
                foundWeightViolation = true;
                fixWeightViolation(tid, pChildren[i]);
            }
        }
        if (!p->weight) {
            foundWeightViolation = true;
            fixWeightViolation(tid, p);
        }
        // if we see any weight violations, then either we fixed one,
        // removing one of these nodes from the tree,
        // or one of the nodes has been removed from the tree by another
        // rebalancing step, so we retry the search for viol
        if (foundWeightViolation) continue;

        // assert: there are no weight violations at p or any nodes in pChildren

        // assert: pChildren consists entirely of leaves or entirely of internal nodes
        // (this is because there are no weight violations any nodes in pChildren)
        bool pChildrenAreLeaves = (pChildren[0]->isLeaf());

        // get the numbers of keys and pointers in the nodes of pChildren
        if (SEQUENTIAL_STAT_TRACKING) ++slackFixTotaling;
        int pGrandDegree = 0;
        for (int i=0;i<p->getABDegree();++i) {
            pGrandDegree += pChildren[i]->getABDegree();
        }
        int slack = p->getABDegree() * b - pGrandDegree;
        if (!(slack >= b + (ALLOW_ONE_EXTRA_SLACK_PER_NODE ? p->getABDegree() : 0))
                && !(viol->getABDegree() == 1)) {
            // there is no violation at viol
            return false;
        }
        if (SEQUENTIAL_STAT_TRACKING) ++slackFixAttempts;

        /**
         * replace the children of p with new nodes that evenly share
         * the keys/pointers originally contained in the children of p.
         */

        // perform LLXs on the children of p
        bool failedllx = false;
        for (int i=0;i<p->getABDegree();++i) {
            if (!llx(tid, pChildren[i], NULL, 1+1+i, info->scxPtrs, info->nodes)) {
                failedllx = true;
                break;
            }
        }
        if (failedllx) continue; // retry the search

        // combine keys and pointers of all children into big arrays
        K keys[DEGREE*DEGREE];
        Node<DEGREE,K>* ptrs[DEGREE*DEGREE];
        pGrandDegree = 0;
        for (int i=0;i<p->getABDegree();++i) {
            arraycopy(pChildren[i]->keys, 0, keys, pGrandDegree, pChildren[i]->getKeyCount());
            if (pChildrenAreLeaves) {
                arraycopy(pChildren[i]->ptrs, 0, ptrs, pGrandDegree, pChildren[i]->getABDegree());
            } else {
                arraycopy_ptrs(pChildren[i]->ptrs, 0, ptrs, pGrandDegree, pChildren[i]->getABDegree());
            }
            pGrandDegree += pChildren[i]->getABDegree();
            // if the children of p are internal,
            // then we have one fewer keys than pointers.
            // so, we fill the hole with the key of p to the right of this
            // child pointer.
            if (!pChildrenAreLeaves && i < p->getKeyCount()) {
                keys[pGrandDegree-1] = p->keys[i];
            }
        }

        int numberOfNewChildren;
        Node<DEGREE,K>* newChildren[DEGREE];

        // determine how to divide keys&values into new nodes as evenly as possible.
        // specifically, we divide them into nodesWithCeil + nodesWithFloor leaves,
        // containing keysPerNodeCeil and keysPerNodeFloor keys, respectively.
        if (ALLOW_ONE_EXTRA_SLACK_PER_NODE) {
            numberOfNewChildren = (pGrandDegree + (b-2)) / (b-1); // how many new nodes?
        } else {
            numberOfNewChildren = (pGrandDegree + (b-1)) / b;
        }
        int degreePerNodeCeil = (pGrandDegree + (numberOfNewChildren-1)) / numberOfNewChildren;
        int degreePerNodeFloor = pGrandDegree / numberOfNewChildren;
        int nodesWithCeil = pGrandDegree % numberOfNewChildren;
        int nodesWithFloor = numberOfNewChildren - nodesWithCeil;

        // create new node(s)
        // divide keys&values into new nodes of degree keysPerNodeCeil
        for (int i=0;i<nodesWithCeil;++i) {
            Node<DEGREE,K>* child = allocateNode(tid);
            arraycopy(keys, degreePerNodeCeil*i, child->keys, 0, degreePerNodeCeil - !pChildrenAreLeaves);
            if (pChildrenAreLeaves) {
                arraycopy(ptrs, degreePerNodeCeil*i, child->ptrs, 0, degreePerNodeCeil);
            } else {
                arraycopy_ptrs(ptrs, degreePerNodeCeil*i, child->ptrs, 0, degreePerNodeCeil);
            }
            child->leaf = pChildrenAreLeaves;
            child->marked = false;
            child->scxPtr = DUMMY;

            // note: the following search key exists because, if we enter this loop,
            // then there are at least two new children, which means each
            // contains at least floor(b/2) > 0 keys
            // (or floor(b/2)-2 > 0 when ALLOW_ONE_EXTRA_SLACK_PER_NODE is true)
            child->searchKey = keys[degreePerNodeCeil*i];

            child->size = degreePerNodeCeil;
            child->weight = true;
            newChildren[i] = child;
        }

        // create new node(s)
        // divide remaining keys&values into new nodes of degree keysPerNodeFloor
        for (int i=0;i<nodesWithFloor;++i) {
            Node<DEGREE,K>* child = allocateNode(tid);
            arraycopy(keys, degreePerNodeCeil*nodesWithCeil+degreePerNodeFloor*i, child->keys, 0, degreePerNodeFloor - !pChildrenAreLeaves);
            if (pChildrenAreLeaves) {
                arraycopy(ptrs, degreePerNodeCeil*nodesWithCeil+degreePerNodeFloor*i, child->ptrs, 0, degreePerNodeFloor);
            } else {
                arraycopy_ptrs(ptrs, degreePerNodeCeil*nodesWithCeil+degreePerNodeFloor*i, child->ptrs, 0, degreePerNodeFloor);
            }
            child->leaf = pChildrenAreLeaves;
            child->marked = false;
            child->scxPtr = DUMMY;

            // let me explain why the following search key assignment makes sense.
            //
            // if there are two or more new children,
            // then each contains contains at least floor(b/2) > 0 keys
            // (or floor(b/2)-2 > 0 when ALLOW_ONE_EXTRA_SLACK_PER_NODE is true),
            // so child will contain at least 1 key, and the first key is
            // keys[keysPerNodeCeil*nodesWithCeil+keysPerNodeFloor*i].
            //
            // if there is only one new child, then the new child will still be
            // reachable by searching for the same key as the old first child of p.
            // (we use pChildren[0]->searchKey instead of keys[0] because keys
            //  might in fact contain ZERO keys!)
            child->searchKey = (numberOfNewChildren == 1) ? pChildren[0]->searchKey : keys[degreePerNodeCeil*nodesWithCeil+degreePerNodeFloor*i];

            child->size = degreePerNodeFloor;
            child->weight = true;
            newChildren[i+nodesWithCeil] = child;
        }

        if (SEQUENTIAL_STAT_TRACKING) ++slackFixSCX;

        // now, we atomically replace p and its children with the new nodes.
        // if appropriate, we perform Root-Replace at the same time.
        if (gp == entry && numberOfNewChildren == 1) {
            /**
             * Compress/One-Child AND Root-Replace.
             */

            // construct info record to pass to SCX
            info->numberOfNodes = 1+1+p->getABDegree(); // gp + p + children of p
            info->numberOfNodesAllocated = 1; // newChildren[0]
            info->numberOfNodesToFreeze = 1+1+(pChildrenAreLeaves ? 0 : p->getABDegree()); // gp + p (since leaves cannot change, there is no need to freeze the children of p)
            info->field = &gp->ptrs[ixToP];
            info->newNode = newChildren[0];

//            info->insertedNodes[0] = info->deletedNodes[0] = NULL;
            info->insertedNodes[0] = newChildren[0];
            info->insertedNodes[1] = NULL;
            info->deletedNodes[0] = p;
            int i;
            for (i=1;i<=p->getABDegree();++i) {
                info->deletedNodes[i] = p->ptrs[i-1];
            }
            info->deletedNodes[i] = NULL;

            if (scx(tid, info)) {
                TRACE COUTATOMICTID("compress/one-child AND root-replace: SCX succeeded"<<std::endl);
                if (SEQUENTIAL_STAT_TRACKING) ++slackFixes;

                //    compress [check: slack@children(n), slack@p, degree@n]
                //        no weight at u
                //        no degree at u
                //        slack at u -> eliminated
                //        no weight at any child of u
                //        degree at a child of u -> eliminated
                //        slack at a child of u -> eliminated or slack at a child of n
                //        [maybe create slack at any or all children of n]
                //        [maybe create slack at p]
                //        [maybe create degree at n]

                //    root-replace [check: degree@n, slack@n]
                //        no weight at root
                //        degree at root -> eliminated
                //        slack at root -> eliminated
                //        weight at child of root -> eliminated
                //        degree at child of root -> degree at n
                //        slack at child of root -> slack at n

                // in this case, the compress creates a node n with exactly
                // one child. this child may have a slack violation, and
                // n may have a degree violation. additionally, p may have
                // a slack violation.
                // however, after we also perform root-replace, n is removed
                // altogether, so there are no violations at n.
                // note that the n in the root-replace comment above refers
                // to the single child of the node n referred to by the
                // compress comment.
                // thus, the only possible violations after the root-replace
                // are a slack violation at the child, a degree violation
                // at the child, and a slack violation at p.
                // we check for (and attempt to fix) each of these.
                fixDegreeOrSlackViolation(tid, newChildren[0]);
                // note: it is impossible for there to be a weight violation at childrenNewP or p, since these nodes must have weight=true for the compress/one-child+root-replace operation to be applicable, and we consequently CREATE childrenNewP[0] and p with weight=true above
                return true;
            }
            TRACE COUTATOMICTID("compress/one-child AND root-replace: SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, newChildren[0]);

        } else {
            /**
             * Compress/One-Child.
             */

            // construct the new parent node n
            Node<DEGREE,K>* n = allocateNode(tid);
            arraycopy_ptrs(newChildren, 0, n->ptrs, 0, numberOfNewChildren);

            // build array of keys (note that this is a bit tricky for internal n)
            if (pChildrenAreLeaves) {
                for (int i=1;i<numberOfNewChildren;++i) {
                    n->keys[i-1] = newChildren[i]->keys[0];
                }
            } else {
                for (int i=0;i<nodesWithCeil;++i) {
                    n->keys[i] = keys[degreePerNodeCeil*i + degreePerNodeCeil-1];
                }
                for (int i=0;i<nodesWithFloor-1;++i) { // this is nodesWithFloor - 1 because we want to go up to numberOfNewChildren - 1, not numberOfNewChildren.
                    n->keys[i+nodesWithCeil] = keys[degreePerNodeCeil*nodesWithCeil + degreePerNodeFloor*i + degreePerNodeFloor-1];
                }
            }
            n->leaf = false;
            n->marked = false;
            n->scxPtr = DUMMY;
            n->searchKey = p->searchKey;
            n->size = numberOfNewChildren;
            n->weight = true;

            // construct info record to pass to SCX
            info->numberOfNodes = 1+1+p->getABDegree(); // gp + p + children of p
            info->numberOfNodesAllocated = 1+numberOfNewChildren; // n + new children
            info->numberOfNodesToFreeze = 1+1+(pChildrenAreLeaves ? 0 : p->getABDegree()); // gp + p (since leaves cannot change, there is no need to freeze the children of p)
            info->field = &gp->ptrs[ixToP];
            info->newNode = n;

//            info->insertedNodes[0] = info->deletedNodes[0] = NULL;
            int i;
            info->insertedNodes[0] = n;
            for (i=1;i<=numberOfNewChildren;++i) {
                info->insertedNodes[i] = newChildren[i-1];
            }
            info->insertedNodes[i] = NULL;

            info->deletedNodes[0] = p;
            for (i=1;i<=p->getABDegree();++i) {
                info->deletedNodes[i] = p->ptrs[i-1];
            }
            info->deletedNodes[i] = NULL;

#ifdef NO_NONROOT_SLACK_VIOLATION_FIXING
this->recordmgr->deallocate(tid, n);
for (int i=0;i<numberOfNewChildren;++i) {
    this->recordmgr->deallocate(tid, newChildren[i]);
}
return false;
#endif

            if (scx(tid, info)) {
                TRACE COUTATOMICTID("compress/one-child: SCX succeeded"<<std::endl);
                if (SEQUENTIAL_STAT_TRACKING) ++slackFixes;

                //    compress [check: slack@children(n), slack@p, degree@n]
                //        no weight at u
                //        no degree at u
                //        slack at u -> eliminated
                //        no weight at any child of u
                //        degree at a child of u -> eliminated
                //        slack at a child of u -> eliminated or slack at a child of n
                //        [maybe create slack at any or all children of n]
                //        [maybe create slack at p]
                //        [maybe create degree at n]
                //
                //    one-child [check: slack@children(n)]
                //        no weight at u
                //        degree at u -> eliminated
                //        slack at u -> eliminated or slack at a child of n
                //        no weight any sibling of u or pi(u)
                //        no degree at any sibling of u or pi(u)
                //        slack at a sibling of u -> eliminated or slack at a child of n
                //        no slack at pi(u)
                //        [maybe create slack at any or all children of n]

                // note that, in the above comment, slack@p actually refers to
                // a possible slack violation at the parent of the topmost node
                // that is replaced by the update. here, the topmost node
                // replaced by the update is p, so we must actually check
                // for a slack violation at gp.

                for (int i=0;i<numberOfNewChildren;++i) {
                    fixDegreeOrSlackViolation(tid, newChildren[i]);
                }
                fixDegreeOrSlackViolation(tid, n);
                fixDegreeOrSlackViolation(tid, gp);
                return true;
            }
            TRACE COUTATOMICTID("compress/one-child: SCX FAILED"<<std::endl);
            this->recordmgr->deallocate(tid, n);
            for (int i=0;i<numberOfNewChildren;++i) {
                this->recordmgr->deallocate(tid, newChildren[i]);
            }
        }
    }
#endif
}
#endif

template <int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::llx(const int tid, Node<DEGREE,K>* r, Node<DEGREE,K> ** snapshot, const int i, SCXRecord<DEGREE,K> ** ops, Node<DEGREE,K> ** nodes) {
    SCXRecord<DEGREE,K>* result = llx(tid, r, snapshot);
    if (result == FAILED || result == FINALIZED) return false;
    ops[i] = result;
    nodes[i] = r;
    return true;
}

template <int DEGREE, typename K, class Compare, class RecManager>
bslack_ns::SCXRecord<DEGREE,K>* bslack_ns::bslack<DEGREE,K,Compare,RecManager>::llx(const int tid, Node<DEGREE,K>* r, Node<DEGREE,K> ** snapshot) {
    const bool marked = r->marked;
    SOFTWARE_BARRIER;
    tagptr_t tagptr = (tagptr_t) r->scxPtr;

    // read mutable state field of descriptor
    bool succ;
    TRACE COUTATOMICTID("tagged ptr seq="<<UNPACK1_SEQ(tagptr)<<" descriptor seq="<<UNPACK1_SEQ(TAGPTR1_UNPACK_PTR(tagptr)->c.mutables)<<std::endl);
    int state = DESC1_READ_FIELD(succ, TAGPTR1_UNPACK_PTR(tagptr)->c.mutables, tagptr, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
    if (!succ) state = SCXRecord<DEGREE,K>::STATE_COMMITTED;
    TRACE { mutables_t debugmutables = TAGPTR1_UNPACK_PTR(tagptr)->c.mutables; COUTATOMICTID("llx scxrecord succ="<<succ<<" state="<<state<<" mutables="<<debugmutables<<" desc-seq="<<UNPACK1_SEQ(debugmutables)<<std::endl); }
    // note: special treatment for alg in the case where the descriptor has already been reallocated (impossible before the transformation, assuming safe memory reclamation)
    SOFTWARE_BARRIER;

    if (state == SCXRecord<DEGREE,K>::STATE_ABORTED || ((state == SCXRecord<DEGREE,K>::STATE_COMMITTED) && !r->marked)) {
        // read snapshot fields
        if (snapshot != NULL) {
            if (r->isLeaf()) {
                arraycopy(r->ptrs, 0, snapshot, 0, r->getABDegree());
            } else {
                arraycopy_ptrs(r->ptrs, 0, snapshot, 0, r->getABDegree());
            }
        }
        if ((tagptr_t) r->scxPtr == tagptr) return (SCXRecord<DEGREE,K> *) tagptr; // we have a snapshot
    }

    if (state == SCXRecord<DEGREE,K>::STATE_INPROGRESS) {
        helpOther(tid, tagptr);
    }
    return (marked ? FINALIZED : FAILED);
}

template<int DEGREE, typename K, class Compare, class RecManager>
bool bslack_ns::bslack<DEGREE,K,Compare,RecManager>::scx(const int tid, wrapper_info<DEGREE,K> * info) {
    const int init_state = SCXRecord<DEGREE,K>::STATE_INPROGRESS;
    SCXRecord<DEGREE,K> * newdesc = createSCXRecord(tid, info);
    tagptr_t tagptr = TAGPTR1_NEW(tid, newdesc->c.mutables);
    info->state = help(tid, tagptr, newdesc, false);
    return info->state & SCXRecord<DEGREE,K>::STATE_COMMITTED;
}

// returns true if we executed help, and false otherwise
template<int DEGREE, typename K, class Compare, class RecManager>
void bslack_ns::bslack<DEGREE,K,Compare,RecManager>::helpOther(const int tid, tagptr_t tagptr) {
    if ((void*) tagptr == DUMMY) {
        TRACE COUTATOMICTID("helpOther dummy descriptor"<<std::endl);
        return; // deal with the dummy descriptor
    }
    SCXRecord<DEGREE,K> snap;
    if (DESC1_SNAPSHOT(&snap, tagptr, SCXRecord<DEGREE comma K>::size)) {
        TRACE COUTATOMICTID("helpOther obtained snapshot of "<<tagptrToString(tagptr)<<std::endl);
        help(tid, tagptr, &snap, true);
    } else {
        TRACE COUTATOMICTID("helpOther unable to get snapshot of "<<tagptrToString(tagptr)<<std::endl);
    }
}

template<int DEGREE, typename K, class Compare, class RecManager>
int bslack_ns::bslack<DEGREE,K,Compare,RecManager>::help(const int tid, const tagptr_t tagptr, SCXRecord<DEGREE,K> const * const snap, const bool helpingOther) {
#ifdef NO_HELPING
    int IGNORED_RETURN_VALUE = -1;
    if (helpingOther) return IGNORED_RETURN_VALUE;
#endif
    TRACE COUTATOMICTID("help "<<tagptrToString(tagptr)<<" helpingOther="<<helpingOther<<" numNodes="<<snap->c.numberOfNodes<<" numToFreeze="<<snap->c.numberOfNodesToFreeze<<std::endl);
    SCXRecord<DEGREE,K> *ptr = TAGPTR1_UNPACK_PTR(tagptr);
    //if (helpingOther) { eassert(UNPACK1_SEQ(snap->c.mutables), UNPACK1_SEQ(tagptr)); /*assert(UNPACK1_SEQ(snap->c.mutables) == UNPACK1_SEQ(tagptr));*/ }
    // freeze sub-tree
    for (int i=helpingOther; i<snap->c.numberOfNodesToFreeze; ++i) {
        if (snap->c.nodes[i]->isLeaf()) {
            TRACE COUTATOMICTID((helpingOther?"    ":"")<<"help "<<"nodes["<<i<<"]@"<<"0x"<<((uintptr_t)(snap->c.nodes[i]))<<" is a leaf\n");
            assert(i > 0); // nodes[0] cannot be a leaf...
            continue; // do not freeze leaves
        }

        bool successfulCAS = __sync_bool_compare_and_swap(&snap->c.nodes[i]->scxPtr, snap->c.scxPtrsSeen[i], tagptr);
        SCXRecord<DEGREE,K> *exp = snap->c.nodes[i]->scxPtr;
        TRACE if (successfulCAS) COUTATOMICTID((helpingOther?"    ":"")<<"help froze nodes["<<i<<"]@0x"<<((uintptr_t)snap->c.nodes[i])<<" with tagptr="<<tagptrToString((tagptr_t) snap->c.nodes[i]->scxPtr)<<std::endl);
        if (successfulCAS || exp == (void*) tagptr) continue; // if node is already frozen for our operation

        // note: we can get here only if:
        // 1. the state is inprogress, and we just failed a cas, and every helper will fail that cas (or an earlier one), so the scx must abort, or
        // 2. the state is committed or aborted
        // (this suggests that it might be possible to get rid of the allFrozen bit)

        // read mutable allFrozen field of descriptor
        bool succ;
        bool allFrozen = DESC1_READ_FIELD(succ, ptr->c.mutables, tagptr, MUTABLES1_MASK_ALLFROZEN, MUTABLES1_OFFSET_ALLFROZEN);
        if (!succ) return SCXRecord<DEGREE,K>::STATE_ABORTED;

        if (allFrozen) {
            TRACE COUTATOMICTID((helpingOther?"    ":"")<<"help return state "<<SCXRecord<DEGREE comma K>::STATE_COMMITTED<<" after failed freezing cas on nodes["<<i<<"]"<<std::endl);
            return SCXRecord<DEGREE,K>::STATE_COMMITTED;
        } else {
            const int newState = SCXRecord<DEGREE,K>::STATE_ABORTED;
            TRACE COUTATOMICTID((helpingOther?"    ":"")<<"help return state "<<newState<<" after failed freezing cas on nodes["<<i<<"]"<<std::endl);
            MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, newState, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
            return newState;
        }
    }

    MUTABLES1_WRITE_BIT(ptr->c.mutables, snap->c.mutables, MUTABLES1_MASK_ALLFROZEN);
    SOFTWARE_BARRIER;
    for (int i=1; i<snap->c.numberOfNodesToFreeze; ++i) {
        if (snap->c.nodes[i]->isLeaf()) continue; // do not mark leaves
        snap->c.nodes[i]->marked = true; // finalize all but first node
    }

    // CAS in the new sub-tree (update CAS)
    rqProvider->linearize_update_at_cas(tid, snap->c.field, snap->c.nodes[1], snap->c.newNode, snap->c.insertedNodes, snap->c.deletedNodes);
//    __sync_bool_compare_and_swap(snap->c.field, snap->c.nodes[1], snap->c.newNode);
    TRACE COUTATOMICTID((helpingOther?"    ":"")<<"help CAS'ed to newNode@0x"<<((uintptr_t)snap->c.newNode)<<std::endl);

    MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, SCXRecord<DEGREE comma K>::STATE_COMMITTED, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);

    TRACE COUTATOMICTID((helpingOther?"    ":"")<<"help return COMMITTED after performing update cas"<<std::endl);
    return SCXRecord<DEGREE,K>::STATE_COMMITTED; // success
}

#endif	/* BSLACK_IMPL_H */
