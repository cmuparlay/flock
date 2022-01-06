/**
 * C++ implementation of unbalanced binary search tree using LLX/SCX.
 *
 * Copyright (C) 2018 Trevor Brown
 */

#ifndef BST_H
#define	BST_H

#include <string>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <set>
#include <csignal>
#include <cassert>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <stdexcept>
#include <bitset>
#include "descriptors.h"
#include "record_manager.h"
#include "scxrecord.h"
#include "node.h"
#include "rq_provider.h"

#ifdef NOREBALANCING
#define IFREBALANCING if (0)
#else
#define IFREBALANCING if (1)
#endif

namespace bst_ns {

    template <class K, class V>
    class ReclamationInfo {
    public:
        int type;
        void *llxResults[MAX_NODES];
        Node<K,V> *nodes[MAX_NODES];
        int state;
        int numberOfNodes;
        int numberOfNodesToFreeze;
        int numberOfNodesToReclaim;
        int numberOfNodesAllocated;
        int path;
        //bool capacityAborted[NUMBER_OF_PATHS];
        int lastAbort;
    };

    template <class K, class V, class Compare, class RecManager>
    class bst {
    private:
        PAD;
        RecManager * const recmgr;
//        PAD;
        RQProvider<K, V, Node<K,V>, bst<K,V,Compare,RecManager>, RecManager, false, false> * const rqProvider;
//        PAD;

        const int N; // number of violations to allow on a search path before we fix everything on it
//        PAD;
        Node<K,V> *root;        // actually const
//        PAD;
        Compare cmp;
//        PAD;

        Node<K,V> **allocatedNodes;
//        PAD;
        #define GET_ALLOCATED_NODE_PTR(tid, i) allocatedNodes[tid*(PREFETCH_SIZE_WORDS+MAX_NODES)+i]
        #define REPLACE_ALLOCATED_NODE(tid, i) { GET_ALLOCATED_NODE_PTR(tid, i) = allocateNode(tid); /*GET_ALLOCATED_NODE_PTR(tid, i)->left.store((uintptr_t) NULL, std::memory_order_relaxed);*/ }

    #ifdef USE_DEBUGCOUNTERS
        // debug info
        debugCounters * const counters;
//        PAD;
    #endif

        // descriptor reduction algorithm
        #define DESC1_ARRAY records
        #define DESC1_T SCXRecord<K,V>
        #define MUTABLES_OFFSET_ALLFROZEN 0
        #define MUTABLES_OFFSET_STATE 1
        #define MUTABLES_MASK_ALLFROZEN 0x1
        #define MUTABLES_MASK_STATE 0x6
        #define MUTABLES1_NEW(mutables) \
            ((((mutables)&MASK1_SEQ)+(1<<OFFSET1_SEQ)) \
            | (SCXRecord<K comma1 V>::STATE_INPROGRESS<<MUTABLES_OFFSET_STATE))
        #define MUTABLES_INIT_DUMMY SCXRecord<K comma1 V>::STATE_COMMITTED<<MUTABLES_OFFSET_STATE | MUTABLES_MASK_ALLFROZEN<<MUTABLES_OFFSET_ALLFROZEN
        #include "descriptors_impl.h"
        PAD;
        DESC1_T DESC1_ARRAY[LAST_TID1+1] __attribute__ ((aligned(64)));
//        PAD;

        /**
         * this is what LLX returns when it is performed on a leaf.
         * the important qualities of this value are:
         *      - it is not NULL
         *      - it cannot be equal to any pointer to an scx record
         */
        #define LLX_RETURN_IS_LEAF ((void*) TAGPTR1_DUMMY_DESC(0))
        #define DUMMY_SCXRECORD ((void*) TAGPTR1_STATIC_DESC(0))

        // private function declarations

        inline Node<K,V>* allocateNode(const int tid);
        inline Node<K,V>* initializeNode(
                    const int,
                    Node<K,V> * const,
                    const K&,
                    const V&,
                    Node<K,V> * const,
                    Node<K,V> * const);
        inline SCXRecord<K,V>* initializeSCXRecord(
                    const int,
                    SCXRecord<K,V> * const,
                    ReclamationInfo<K,V> * const,
                    std::atomic_uintptr_t * const,
                    Node<K,V> * const);
        int rangeQuery_lock(ReclamationInfo<K,V> * const, const int, void **input, void **output);
        int rangeQuery_vlx(ReclamationInfo<K,V> * const, const int, void **input, void **output);
        bool updateInsert_search_llx_scx(ReclamationInfo<K,V> * const, const int, void **input, void **output); // input consists of: const K& key, const V& val, const bool onlyIfAbsent
        bool updateErase_search_llx_scx(ReclamationInfo<K,V> * const, const int, void **input, void **output); // input consists of: const K& key, const V& val, const bool onlyIfAbsent
        void reclaimMemoryAfterSCX(
                    const int tid,
                    ReclamationInfo<K,V> * info);
        void helpOther(const int tid, tagptr_t tagptr);
        int help(const int tid, tagptr_t tagptr, SCXRecord<K,V> *ptr, bool helpingOther);
        inline void* llx(
                const int tid,
                Node<K,V> *node,
                Node<K,V> **retLeft,
                Node<K,V> **retRight);
        inline bool scx(
                    const int tid,
                    ReclamationInfo<K,V> * const,
                    Node<K,V> * volatile * field,         // pointer to a "field pointer" that will be changed
                    Node<K,V> *newNode,
                    Node<K,V> * const * const insertedNodes,
                    Node<K,V> * const * const deletedNodes);
        inline int computeSize(Node<K,V>* node);

        long long debugKeySum(Node<K,V> * node);
        bool validate(Node<K,V> * const node, const int currdepth, const int leafdepth);

        const V doInsert(const int tid, const K& key, const V& val, bool onlyIfAbsent);

        int init[MAX_THREADS_POW2] = {0,};
//        PAD;

public:
        const K NO_KEY;
        const V NO_VALUE;
        PAD;

        /**
         * This function must be called once by each thread that will
         * invoke any functions on this class.
         *
         * It must be okay that we do this with the main thread and later with another thread!!!
         */
        void initThread(const int tid) {
            if (init[tid]) return; else init[tid] = !init[tid];

            recmgr->initThread(tid);
            rqProvider->initThread(tid);

            for (int i=0;i<MAX_NODES;++i) {
                REPLACE_ALLOCATED_NODE(tid, i);
            }
        }
        void deinitThread(const int tid) {
            if (!init[tid]) return; else init[tid] = !init[tid];

            rqProvider->deinitThread(tid);
            recmgr->deinitThread(tid);
        }

        bst(const K _NO_KEY,
                    const V _NO_VALUE,
                    const int numProcesses,
                    int suspectedCrashSignal = SIGQUIT,
                    int allowedViolationsPerPath = 6)
            : N(allowedViolationsPerPath)
                    , NO_KEY(_NO_KEY)
                    , NO_VALUE(_NO_VALUE)
                    , recmgr(new RecManager(numProcesses, suspectedCrashSignal))
                    , rqProvider(new RQProvider<K, V, Node<K,V>, bst<K,V,Compare,RecManager>, RecManager, false, false>(numProcesses, this, recmgr))
    #ifdef USE_DEBUGCOUNTERS
                    , counters(new debugCounters(numProcesses))
    #endif
        {

            VERBOSE DEBUG COUTATOMIC("constructor bst"<<std::endl);
            allocatedNodes = new Node<K,V>*[numProcesses*(PREFETCH_SIZE_WORDS+MAX_NODES)];
            cmp = Compare();

            const int tid = 0;
            initThread(tid);

            DESC1_INIT_ALL(numProcesses);
            SCXRecord<K,V> *dummy = TAGPTR1_UNPACK_PTR(DUMMY_SCXRECORD);
            dummy->c.mutables = MUTABLES_INIT_DUMMY;

            recmgr->endOp(tid); // block crash recovery signal for this thread, and enter an initial quiescent state.
            Node<K,V> *rootleft = initializeNode(tid, allocateNode(tid), NO_KEY, NO_VALUE, NULL, NULL);
            Node<K,V> *_root = initializeNode(tid, allocateNode(tid), NO_KEY, NO_VALUE, rootleft, NULL);

            // need to simulate real insertion of root and the root's child,
            // since range queries will actually try to add these nodes,
            // and we don't want blocking rq providers to spin forever
            // waiting for their itimes to be set to a positive number.
            Node<K,V>* insertedNodes[] = {_root, rootleft, NULL};
            Node<K,V>* deletedNodes[] = {NULL};
            rqProvider->linearize_update_at_write(tid, &root, _root, insertedNodes, deletedNodes);
        }

        Node<K,V> * debug_getEntryPoint() { return root; }

        long long getSizeInNodes(Node<K,V> * const u) {
            if (u == NULL) return 0;
            return 1 + getSizeInNodes(u->left)
                     + getSizeInNodes(u->right);
        }
        long long getSizeInNodes() {
            return getSizeInNodes(root);
        }
        std::string getSizeString() {
            std::stringstream ss;
            int preallocated = MAX_NODES * recmgr->NUM_PROCESSES;
            ss<<getSizeInNodes()<<" nodes in tree and "<<preallocated<<" preallocated but unused";
            return ss.str();
        }
        long long getSize(Node<K,V> * const u) {
            if (u == NULL) return 0;
            if (u->left == NULL) return 1; // is leaf
            return getSize(u->left)
                 + getSize(u->right);
        }
        long long getSize() {
            return getSize(root);
        }

        void dfsDeallocateBottomUp(Node<K,V> * const u, int *numNodes) {
            if (u == NULL) return;
            if (u->left != NULL) {
                dfsDeallocateBottomUp(u->left, numNodes);
                dfsDeallocateBottomUp(u->right, numNodes);
            }
            MEMORY_STATS ++(*numNodes);
            recmgr->deallocate(0 /* tid */, u);
        }
        ~bst() {
            VERBOSE DEBUG COUTATOMIC("destructor bst");
            // free every node and scx record currently in the data structure.
            // an easy DFS, freeing from the leaves up, handles all nodes.
            // cleaning up scx records is a little bit harder if they are in progress or aborted.
            // they have to be collected and freed only once, since they can be pointed to by many nodes.
            // so, we keep them in a set, then free each set element at the end.
            int numNodes = 0;
            dfsDeallocateBottomUp(root, &numNodes);
            VERBOSE DEBUG COUTATOMIC(" deallocated nodes "<<numNodes<<std::endl);
            for (int tid=0;tid<recmgr->NUM_PROCESSES;++tid) {
                for (int i=0;i<MAX_NODES;++i) {
                    recmgr->deallocate(tid, GET_ALLOCATED_NODE_PTR(tid, i));
                }
            }
            delete[] allocatedNodes;
            delete rqProvider;
//            recmgr->printStatus();
            delete recmgr;
    #ifdef USE_DEBUGCOUNTERS
            delete counters;
    #endif
        }

        Node<K,V> *getRoot(void) { return root; }
        const V insert(const int tid, const K& key, const V& val);
        const V insertIfAbsent(const int tid, const K& key, const V& val);
        const std::pair<V,bool> erase(const int tid, const K& key);
        const std::pair<V,bool> find(const int tid, const K& key);
        int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues);
        bool contains(const int tid, const K& key);
        int size(void); /** warning: size is a LINEAR time operation, and does not return consistent results with concurrency **/

        /**
         * BEGIN FUNCTIONS FOR RANGE QUERY SUPPORT
         */

        inline bool isLogicallyDeleted(const int tid, Node<K,V> * node) {
            return false;
        }

        inline int getKeys(const int tid, Node<K,V> * node, K * const outputKeys, V * const outputValues) {
            if (rqProvider->read_addr(tid, &node->left) == NULL) {
                // leaf ==> its key is in the set.
                outputKeys[0] = node->key;
                outputValues[0] = node->value;
                return 1;
            }
            // note: internal ==> its key is NOT in the set
            return 0;
        }

        bool isInRange(const K& key, const K& lo, const K& hi) {
            return (key != NO_KEY && !cmp(key, lo) && !cmp(hi, key));
        }

        /**
         * END FUNCTIONS FOR RANGE QUERY SUPPORT
         */

        void debugPrintAllocatorStatus() {
            recmgr->printStatus();
        }
        void debugPrintToFile(std::string prefix, long id1, std::string infix, long id2, std::string suffix) {
            std::stringstream ss;
            ss<<prefix<<id1<<infix<<id2<<suffix;
            COUTATOMIC("print to filename \""<<ss.str()<<"\""<<std::endl);
            std::fstream fs (ss.str().c_str(), std::fstream::out);
            root->printTreeFile(fs);
            fs.close();
        }

        std::string tagptrToString(uintptr_t tagptr) {
            std::stringstream ss;
            if (tagptr) {
                if ((void*) tagptr == DUMMY_SCXRECORD) {
                    ss<<"dummy";
                } else {
                    SCXRecord<K,V> *ptr;
    //                if (TAGPTR_TEST(tagptr)) {
                        ss<<"<seq="<<UNPACK1_SEQ(tagptr)<<",tid="<<TAGPTR1_UNPACK_TID(tagptr)<<">";
                        ptr = TAGPTR1_UNPACK_PTR(tagptr);
    //                }

                    // print contents of actual scx record
                    intptr_t mutables = ptr->c.mutables;
                    ss<<"[";
                    ss<<"state="<<MUTABLES1_UNPACK_FIELD(mutables, MUTABLES_MASK_STATE, MUTABLES_OFFSET_STATE);
                    ss<<" ";
                    ss<<"allFrozen="<<MUTABLES1_UNPACK_FIELD(mutables, MUTABLES_MASK_ALLFROZEN, MUTABLES_OFFSET_ALLFROZEN);
                    ss<<" ";
                    ss<<"seq="<<UNPACK1_SEQ(mutables);
                    ss<<"]";
                }
            } else {
                ss<<"null";
            }
            return ss.str();
        }

    //    friend ostream& operator<<(ostream& os, const SCXRecord<K,V>& obj) {
    //        ios::fmtflags f( os.flags() );
    ////        std::cout<<"obj.type = "<<obj.type<<std::endl;
    //        intptr_t mutables = obj.mutables;
    //        os<<"["//<<"type="<<NAME_OF_TYPE[obj.type]
    //          <<" state="<<SCX_READ_STATE(mutables)//obj.state
    //          <<" allFrozen="<<SCX_READ_ALLFROZEN(mutables)//obj.allFrozen
    ////          <<"]";
    ////          <<" nodes="<<obj.nodes
    ////          <<" ops="<<obj.ops
    //          <<"]" //" subtree="+subtree+"]";
    //          <<"@0x"<<hex<<(long)(&obj);
    //        os.flags(f);
    //        return os;
    //    }

    #ifdef USE_DEBUGCOUNTERS
        void clearCounters() {
            counters->clear();
    //        recmgr->clearCounters();
        }
        debugCounters * const debugGetCounters() {
            return counters;
        }
    #endif
        RecManager * const debugGetRecMgr() {
            return recmgr;
        }

        bool validate(const long long keysum, const bool checkkeysum);
        long long debugKeySum() {
            return debugKeySum((root->left)->left);
        }
    };

}

template<class K, class V, class Compare, class RecManager>
bst_ns::Node<K,V>* bst_ns::bst<K,V,Compare,RecManager>::allocateNode(const int tid) {
    //this->recmgr->getDebugInfo(NULL)->addToPool(tid, 1);
    Node<K,V> *newnode = recmgr->template allocate<Node<K,V> >(tid);
    if (newnode == NULL) {
        COUTATOMICTID("ERROR: could not allocate node"<<std::endl);
        exit(-1);
    }
// #ifdef GSTATS_HANDLE_STATS
//     GSTATS_APPEND(tid, node_allocated_addresses, (long long) newnode);
// #endif
    return newnode;
}

template<class K, class V, class Compare, class RecManager>
long long bst_ns::bst<K,V,Compare,RecManager>::debugKeySum(Node<K,V> * node) {
    if (node == NULL) return 0;
    if ((void*) node->left == NULL) return (long long) node->key;
    return debugKeySum(node->left)
         + debugKeySum(node->right);
}

template<class K, class V, class Compare, class RecManager>
bool bst_ns::bst<K,V,Compare,RecManager>::validate(Node<K,V> * const node, const int currdepth, const int leafdepth) {
    return true;
}

template<class K, class V, class Compare, class RecManager>
bool bst_ns::bst<K,V,Compare,RecManager>::validate(const long long keysum, const bool checkkeysum) {
    return true;
}

template<class K, class V, class Compare, class RecManager>
inline int bst_ns::bst<K,V,Compare,RecManager>::size() {
    return computeSize((root->left)->left);
}

template<class K, class V, class Compare, class RecManager>
inline int bst_ns::bst<K,V,Compare,RecManager>::computeSize(Node<K,V> * const root) {
    if (root == NULL) return 0;
    if (root->left != NULL) { // if internal node
        return computeSize(root->left)
                + computeSize(root->right);
    } else { // if leaf
        return 1;
//        printf(" %d", root->key);
    }
}

template<class K, class V, class Compare, class RecManager>
bool bst_ns::bst<K,V,Compare,RecManager>::contains(const int tid, const K& key) {
    std::pair<V,bool> result = find(tid, key);
    return result.second;
}

template<class K, class V, class Compare, class RecManager>
int bst_ns::bst<K,V,Compare,RecManager>::rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
    block<Node<K,V> > stack (NULL);
    auto guard = recmgr->getGuard(tid, true);
    rqProvider->traversal_start(tid);

    // depth first traversal (of interesting subtrees)
    int size = 0;
    stack.push(root);
    while (!stack.isEmpty()) {
        Node<K,V> * node = stack.pop();
        assert(node);
        Node<K,V> * left = rqProvider->read_addr(tid, &node->left);

        // if internal node, explore its children
        if (left != NULL) {
            if (node->key != this->NO_KEY && !cmp(hi, node->key)) {
                Node<K,V> * right = rqProvider->read_addr(tid, &node->right);
                assert(right);
                stack.push(right);
            }
            if (node->key == this->NO_KEY || cmp(lo, node->key)) {
                assert(left);
                stack.push(left);
            }

        // else if leaf node, check if we should add its key to the traversal
        } else {
            rqProvider->traversal_try_add(tid, node, resultKeys, resultValues, &size, lo, hi);
        }
    }
    rqProvider->traversal_end(tid, resultKeys, resultValues, &size, lo, hi);
    return size;
}

template<class K, class V, class Compare, class RecManager>
const std::pair<V,bool> bst_ns::bst<K,V,Compare,RecManager>::find(const int tid, const K& key) {
    std::pair<V,bool> result;
    Node<K,V> *p;
    Node<K,V> *l;
    for (;;) {
        TRACE COUTATOMICTID("find(tid="<<tid<<" key="<<key<<")"<<std::endl);
        auto guard = recmgr->getGuard(tid, true);
        p = rqProvider->read_addr(tid, &root->left);
        l = rqProvider->read_addr(tid, &p->left);
        if (l == NULL) {
            result = std::pair<V,bool>(NO_VALUE, false); // no keys in data structure
            return result; // success
        }

        while (rqProvider->read_addr(tid, &l->left) != NULL) {
            TRACE COUTATOMICTID("traversing tree; l="<<*l<<std::endl);
            p = l; // note: the new p is currently protected
            assert(p->key != NO_KEY);
            if (cmp(key, p->key)) {
                l = rqProvider->read_addr(tid, &p->left);
            } else {
                l = rqProvider->read_addr(tid, &p->right);
            }
        }
        if (key == l->key) {
            result = std::pair<V,bool>(l->value, true);
        } else {
            result = std::pair<V,bool>(NO_VALUE, false);
        }
        return result; // success
    }
    assert(0);
    return std::pair<V,bool>(NO_VALUE, false);
}

//template<class K, class V, class Compare, class RecManager>
//const V bst_ns::bst<K,V,Compare,RecManager>::insert(const int tid, const K& key, const V& val) {
//    bool onlyIfAbsent = false;
//    V result = NO_VALUE;
//    void *input[] = {(void*) &key, (void*) &val, (void*) &onlyIfAbsent};
//    void *output[] = {(void*) &result};
//
//    ReclamationInfo<K,V> info;
//    bool finished = 0;
//    for (;;) {
//        auto guard = recmgr->getGuard(tid);
//        finished = updateInsert_search_llx_scx(&info, tid, input, output);
//        if (finished) {
//            break;
//        }
//    }
//    return result;
//}

template<class K, class V, class Compare, class RecManager>
const V bst_ns::bst<K,V,Compare,RecManager>::doInsert(const int tid, const K& key, const V& val, bool onlyIfAbsent) {
    V result = NO_VALUE;
    void *input[] = {(void*) &key, (void*) &val, (void*) &onlyIfAbsent};
    void *output[] = {(void*) &result};

    ReclamationInfo<K,V> info;
    bool finished = 0;
    for (;;) {
        auto guard = recmgr->getGuard(tid);
        finished = updateInsert_search_llx_scx(&info, tid, input, output);
        if (finished) {
            break;
        }
    }
    return result;
}

template<class K, class V, class Compare, class RecManager>
const V bst_ns::bst<K,V,Compare,RecManager>::insertIfAbsent(const int tid, const K& key, const V& val) {
    return doInsert(tid, key, val, true);
}

template<class K, class V, class Compare, class RecManager>
const V bst_ns::bst<K,V,Compare,RecManager>::insert(const int tid, const K& key, const V& val) {
    return doInsert(tid, key, val, false);
}

template<class K, class V, class Compare, class RecManager>
const std::pair<V,bool> bst_ns::bst<K,V,Compare,RecManager>::erase(const int tid, const K& key) {
    V result = NO_VALUE;
    void *input[] = {(void*) &key};
    void *output[] = {(void*) &result};

    ReclamationInfo<K,V> info;
    bool finished = 0;
    for (;;) {
        auto guard = recmgr->getGuard(tid);
        finished = updateErase_search_llx_scx(&info, tid, input, output);
        if (finished) {
            break;
        }
    }
    return std::pair<V,bool>(result, (result != NO_VALUE));
}

template<class K, class V, class Compare, class RecManager>
inline bool bst_ns::bst<K,V,Compare,RecManager>::updateInsert_search_llx_scx(
            ReclamationInfo<K,V> * const info, const int tid, void **input, void **output) {
    const K& key = *((const K*) input[0]);
    const V& val = *((const V*) input[1]);
    const bool onlyIfAbsent = *((const bool*) input[2]);
    V *result = (V*) output[0];

    TRACE COUTATOMICTID("updateInsert_search_llx_scx(tid="<<tid<<", key="<<key<<")"<<std::endl);

    Node<K,V> *p = root, *l;
    l = rqProvider->read_addr(tid, &root->left);
    if (rqProvider->read_addr(tid, &l->left) != NULL) { // the tree contains some node besides sentinels...
        p = l;
        l = rqProvider->read_addr(tid, &l->left);    // note: l must have key infinity, and l->left must not.
        while (rqProvider->read_addr(tid, &l->left) != NULL) {
            p = l;
            if (cmp(key, p->key)) {
                l = rqProvider->read_addr(tid, &p->left);
            } else {
                l = rqProvider->read_addr(tid, &p->right);
            }
        }
    }
    // if we find the key in the tree already
    if (key == l->key) {
        if (onlyIfAbsent) {
            TRACE COUTATOMICTID("return true5\n");
            *result = val; // for insertIfAbsent, we don't care about the particular value, just whether we inserted or not. so, we use val to signify not having inserted (and NO_VALUE to signify having inserted).
            return true; // success
        }
        Node<K,V> *pleft, *pright;
        if ((info->llxResults[0] = llx(tid, p, &pleft, &pright)) == NULL) {
            return false;
        } //RETRY;
        if (l != pleft && l != pright) {
            return false;
        } //RETRY;
        *result = l->value;
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), key, val, NULL, NULL);
        info->numberOfNodes = 2;
        info->numberOfNodesToFreeze = 1;
        info->numberOfNodesToReclaim = 1; // only reclaim l (reclamation starts at nodes[1])
        info->numberOfNodesAllocated = 1;
        info->type = SCXRecord<K,V>::TYPE_REPLACE;
        info->nodes[0] = p;
        info->nodes[1] = l;
        assert(l);

        bool isInsertingKey = false;
        K insertedKey = NO_KEY;
        Node<K,V> * insertedNodes[] = {GET_ALLOCATED_NODE_PTR(tid, 0), NULL};
        bool isDeletingKey = false;
        K deletedKey = NO_KEY;
        Node<K,V> * deletedNodes[] = {l, NULL};

        bool retval = scx(tid, info, (l == pleft ? &p->left : &p->right), GET_ALLOCATED_NODE_PTR(tid, 0), insertedNodes, deletedNodes);
        if (retval) {
        }
        return retval;
    } else {
        Node<K,V> *pleft, *pright;
        if ((info->llxResults[0] = llx(tid, p, &pleft, &pright)) == NULL) {
            return false;
        } //RETRY;
        if (l != pleft && l != pright) {
            return false;
        } //RETRY;
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), key, val, NULL, NULL);
//        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), l->key, l->value, /*1,*/ NULL, NULL);
        // TODO: change all equality comparisons with NO_KEY to use cmp()
        if (l->key == NO_KEY || cmp(key, l->key)) {
            initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), l->key, l->value, GET_ALLOCATED_NODE_PTR(tid, 0), l);
        } else {
            initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), key, val, l, GET_ALLOCATED_NODE_PTR(tid, 0));
        }
        *result = NO_VALUE;
        info->numberOfNodes = 2;
        info->numberOfNodesToReclaim = 0;
        info->numberOfNodesToFreeze = 1; // only freeze nodes[0]
        info->numberOfNodesAllocated = 2;
        info->type = SCXRecord<K,V>::TYPE_INS;
        info->nodes[0] = p;
        info->nodes[1] = l; // note: used as OLD value for CAS that changes p's child pointer (but is not frozen or marked)

        Node<K,V> * insertedNodes[] = {GET_ALLOCATED_NODE_PTR(tid, 0), GET_ALLOCATED_NODE_PTR(tid, 1), NULL};
        Node<K,V> * deletedNodes[] = {NULL};

        bool retval = scx(tid, info, (l == pleft ? &p->left : &p->right), GET_ALLOCATED_NODE_PTR(tid, 1), insertedNodes, deletedNodes);
        return retval;
    }
}

template<class K, class V, class Compare, class RecManager>
inline bool bst_ns::bst<K,V,Compare,RecManager>::updateErase_search_llx_scx(
            ReclamationInfo<K,V> * const info, const int tid, void **input, void **output) { // input consists of: const K& key
    const K& key = *((const K*) input[0]);
    V *result = (V*) output[0];

    TRACE COUTATOMICTID("updateErase_search_llx_scx(tid="<<tid<<", key="<<key<<")"<<std::endl);

    Node<K,V> *gp, *p, *l;
    l = rqProvider->read_addr(tid, &root->left);
    if (rqProvider->read_addr(tid, &l->left) == NULL) {
        *result = NO_VALUE;
        return true;
    } // only sentinels in tree...
    gp = root;
    p = l;
    l = rqProvider->read_addr(tid, &p->left);    // note: l must have key infinity, and l->left must not.
    while (rqProvider->read_addr(tid, &l->left) != NULL) {
        gp = p;
        p = l;
        if (cmp(key, p->key)) {
            l = rqProvider->read_addr(tid, &p->left);
        } else {
            l = rqProvider->read_addr(tid, &p->right);
        }
    }
    // if we fail to find the key in the tree
    if (key != l->key) {
        *result = NO_VALUE;
        return true; // success
    } else {
        Node<K,V> *gpleft, *gpright;
        Node<K,V> *pleft, *pright;
        Node<K,V> *sleft, *sright;
        if ((info->llxResults[0] = llx(tid, gp, &gpleft, &gpright)) == NULL) return false;
        if (p != gpleft && p != gpright) return false;
        if ((info->llxResults[1] = llx(tid, p, &pleft, &pright)) == NULL) return false;
        if (l != pleft && l != pright) return false;
        *result = l->value;
        // Read fields for the sibling s of l
        Node<K,V> *s = (l == pleft ? pright : pleft);
        if ((info->llxResults[2] = llx(tid, s, &sleft, &sright)) == NULL) return false;
        // Now, if the op. succeeds, all structure is guaranteed to be just as we verified
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), s->key, s->value, /*newWeight,*/ sleft, sright);
        info->numberOfNodes = 4;
        info->numberOfNodesToReclaim = 3; // reclaim p, s, l (reclamation starts at nodes[1])
        info->numberOfNodesToFreeze = 3;
        info->numberOfNodesAllocated = 1;
        info->type = SCXRecord<K,V>::TYPE_DEL;
        info->nodes[0] = gp;
        info->nodes[1] = p;
        info->nodes[2] = s;
        info->nodes[3] = l;
        assert(gp); assert(p); assert(s); assert(l);

        Node<K,V> * insertedNodes[] = {GET_ALLOCATED_NODE_PTR(tid, 0), NULL};
        Node<K,V> * deletedNodes[] = {p, s, l, NULL};
        bool retval = scx(tid, info, (p == gpleft ? &gp->left : &gp->right), GET_ALLOCATED_NODE_PTR(tid, 0), insertedNodes, deletedNodes);
        return retval;
    }
}

template<class K, class V, class Compare, class RecManager>
bst_ns::Node<K,V>* bst_ns::bst<K,V,Compare,RecManager>::initializeNode(
            const int tid,
            Node<K,V> * const newnode,
            const K& key,
            const V& value,
            Node<K,V> * const left,
            Node<K,V> * const right) {
    newnode->key = key;
    newnode->value = value;
    rqProvider->init_node(tid, newnode);
    // note: synchronization is not necessary for the following accesses,
    // since a memory barrier will occur before this object becomes reachable
    // from an entry point to the data structure.
    rqProvider->write_addr(tid, &newnode->left, left);
    rqProvider->write_addr(tid, &newnode->right, right);
    newnode->scxRecord.store((uintptr_t) DUMMY_SCXRECORD, std::memory_order_relaxed);
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
    newnode->marked.store(false, std::memory_order_relaxed);
#else
    // taken care of by scxRecord store above
#endif
    return newnode;
}

// you may call this only in a quiescent state.
// the scx records in scxRecordsSeen must be protected (or we must know no one can have freed them--this is the case in this implementation).
// if this is being called from crash recovery, all nodes in nodes[] and the scx record must be Qprotected.
template<class K, class V, class Compare, class RecManager>
void bst_ns::bst<K,V,Compare,RecManager>::reclaimMemoryAfterSCX(
            const int tid,
            ReclamationInfo<K,V> * info) {

    Node<K,V> ** const nodes = info->nodes;
    SCXRecord<K,V> * const * const scxRecordsSeen = (SCXRecord<K,V> * const * const) info->llxResults;
    const int state = info->state;
    const int operationType = info->type;

    // NOW, WE ATTEMPT TO RECLAIM ANY RETIRED NODES
    int highestIndexReached = (state == SCXRecord<K,V>::STATE_COMMITTED
            ? info->numberOfNodesToFreeze
            : 0);
    const int maxNodes = MAX_NODES;
    assert(highestIndexReached>=0);
    assert(highestIndexReached<=maxNodes);

    const int state_aborted = SCXRecord<K,V>::STATE_ABORTED;
    const int state_inprogress = SCXRecord<K,V>::STATE_INPROGRESS;
    const int state_committed = SCXRecord<K,V>::STATE_COMMITTED;
    if (highestIndexReached == 0) {
        assert(state == state_aborted || state == state_inprogress);
        return;
    } else {
        assert(!recmgr->supportsCrashRecovery() || recmgr->isQuiescent(tid));
        // if the state was COMMITTED, then we cannot reuse the nodes the we
        // took from allocatedNodes[], either, so we must replace these nodes.
        if (state == SCXRecord<K,V>::STATE_COMMITTED) {
            //cout<<"replacing allocated nodes"<<std::endl;
            for (int i=0;i<info->numberOfNodesAllocated;++i) {
                REPLACE_ALLOCATED_NODE(tid, i);
            }
//            // nodes[1], nodes[2], ..., nodes[nNodes-1] are now retired
//            for (int j=0;j<info->numberOfNodesToReclaim;++j) {
//                recmgr->retire(tid, nodes[1+j]);
//            }
        } else {
            assert(state >= state_aborted); /* is ABORTED */
        }
    }
}

// you may call this only if each node in nodes is protected by a call to recmgr->protect
template<class K, class V, class Compare, class RecManager>
bool bst_ns::bst<K,V,Compare,RecManager>::scx(
            const int tid,
            ReclamationInfo<K,V> * const info,
            Node<K,V> * volatile * field,        // pointer to a "field pointer" that will be changed
            Node<K,V> *newNode,
            Node<K,V> * const * const insertedNodes,
            Node<K,V> * const * const deletedNodes) {
    TRACE COUTATOMICTID("scx(tid="<<tid<<" type="<<info->type<<")"<<std::endl);

    SCXRecord<K,V> *newdesc = DESC1_NEW(tid);
    newdesc->c.newNode = newNode;
    for (int i=0;i<info->numberOfNodes;++i) {
        newdesc->c.nodes[i] = info->nodes[i];
    }
    for (int i=0;i<info->numberOfNodesToFreeze;++i) {
        newdesc->c.scxRecordsSeen[i] = (SCXRecord<K,V> *) info->llxResults[i];
    }

    int i;
    for (i=0;insertedNodes[i];++i) newdesc->c.insertedNodes[i] = insertedNodes[i];
    newdesc->c.insertedNodes[i] = NULL;
    for (i=0;deletedNodes[i];++i) newdesc->c.deletedNodes[i] = deletedNodes[i];
    newdesc->c.deletedNodes[i] = NULL;

    newdesc->c.field = field;
    newdesc->c.numberOfNodes = (char) info->numberOfNodes;
    newdesc->c.numberOfNodesToFreeze = (char) info->numberOfNodesToFreeze;

    // note: writes equivalent to the following two are already done by DESC1_NEW()
    //rec->state.store(SCXRecord<K,V>::STATE_INPROGRESS, std::memory_order_relaxed);
    //rec->allFrozen.store(false, std::memory_order_relaxed);
    DESC1_INITIALIZED(tid); // mark descriptor as being in a consistent state

    SOFTWARE_BARRIER;
    int state = help(tid, TAGPTR1_NEW(tid, newdesc->c.mutables), newdesc, false);
    info->state = state; // rec->state.load(std::memory_order_relaxed);
    reclaimMemoryAfterSCX(tid, info);
    return state & SCXRecord<K,V>::STATE_COMMITTED;
}

template <class K, class V, class Compare, class RecManager>
void bst_ns::bst<K,V,Compare,RecManager>::helpOther(const int tid, tagptr_t tagptr) {
    if ((void*) tagptr == DUMMY_SCXRECORD) {
        TRACE COUTATOMICTID("helpOther dummy descriptor"<<std::endl);
        return; // deal with the dummy descriptor
    }
    SCXRecord<K,V> newdesc;
    //cout<<"sizeof(newrec)="<<sizeof(newrec)<<" computed size="<<SCXRecord<K,V>::size<<std::endl;
    if (DESC1_SNAPSHOT(&newdesc, tagptr, SCXRecord<K comma1 V>::size /* sizeof(newrec) /*- sizeof(newrec.padding)*/)) {
        help(tid, tagptr, &newdesc, true);
    } else {
        TRACE COUTATOMICTID("helpOther unable to get snapshot of "<<tagptrToString(tagptr)<<std::endl);
    }
}

// returns the state field of the scx record "scx."
template <class K, class V, class Compare, class RecManager>
int bst_ns::bst<K,V,Compare,RecManager>::help(const int tid, tagptr_t tagptr, SCXRecord<K,V> *snap, bool helpingOther) {
    TRACE COUTATOMICTID("help "<<tagptrToString(tagptr)<<std::endl);
    SCXRecord<K,V> *ptr = TAGPTR1_UNPACK_PTR(tagptr);

    // TODO: make SCX_WRITE_STATE into regular write for the owner of this descriptor
    // (might not work in general, but here we can prove that allfrozen happens
    //  before state changes, and the sequence number does not change until the
    //  owner is finished with his operation; in general, seems like the last
    //  write done by helpers/owner can be a write, not a CAS when done
    //  by the owner)
    for (int i=helpingOther; i<snap->c.numberOfNodesToFreeze; ++i) { // freeze sub-tree
        if (snap->c.scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) {
            TRACE COUTATOMICTID("nodes["<<i<<"] is a leaf");
            assert(i > 0); // nodes[0] cannot be a leaf...
            continue; // do not freeze leaves
        }

        uintptr_t exp = (uintptr_t) snap->c.scxRecordsSeen[i];
        bool successfulCAS = snap->c.nodes[i]->scxRecord.compare_exchange_strong(exp, tagptr); // MEMBAR ON X86/64
        if (successfulCAS || exp == tagptr) continue; // if node is already frozen for our operation

        // read mutable allFrozen field of descriptor
        bool succ;
        bool allFrozen = DESC1_READ_FIELD(succ, ptr->c.mutables, tagptr, MUTABLES_MASK_ALLFROZEN, MUTABLES_OFFSET_ALLFROZEN);
        if (!succ) return SCXRecord<K,V>::STATE_ABORTED;

        int newState = (allFrozen) ? SCXRecord<K,V>::STATE_COMMITTED : SCXRecord<K,V>::STATE_ABORTED;
        TRACE COUTATOMICTID("help return state "<<newState<<" after failed freezing cas on nodes["<<i<<"]"<<std::endl);
        MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, newState, MUTABLES_MASK_STATE, MUTABLES_OFFSET_STATE);
        return newState;
    }

    MUTABLES1_WRITE_BIT(ptr->c.mutables, snap->c.mutables, MUTABLES_MASK_ALLFROZEN);
    for (int i=1; i<snap->c.numberOfNodesToFreeze; ++i) {
        if (snap->c.scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not mark leaves
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
        snap->c.nodes[i]->marked.store(true, std::memory_order_relaxed); // finalize all but first node
#else
        snap->c.nodes[i]->scxRecord.fetch_or(0x1, std::memory_order_relaxed);
        // could this be done more efficiently with bit-test-and-set?
#endif
    }

    // CAS in the new sub-tree (update CAS)
//    uintptr_t expected = (uintptr_t) snap->nodes[1];
    rqProvider->linearize_update_at_cas(tid, snap->c.field, snap->c.nodes[1], snap->c.newNode, snap->c.insertedNodes, snap->c.deletedNodes);

    // todo: add #ifdef CASing scx record pointers to set an "invalid" bit,
    // and add to llx a test that determines whether a pointer is invalid.
    // if so, the record no longer exists and no longer needs help.
    //
    // the goal of this is to prevent a possible ABA problem that can occur
    // if there is VERY long lived data in the tree.
    // specifically, if a node whose scx record pointer is CAS'd by this scx
    // was last modified by the owner of this scx, and the owner performed this
    // modification exactly 2^SEQ_WIDTH (of its own) operations ago,
    // then an operation running now may confuse a pointer to an old version of
    // this scx record with the current version (since the sequence #'s match).
    //
    // actually, there may be another solution. a problem arises only if a
    // helper can take an effective action not described by an active operation.
    // even if a thread erroneously believes an old pointer reflects an active
    // operation, if it simply helps the active operation, there is no issue.
    // the issue arises when the helper believes it sees an active operation,
    // and begins helping by looking at the scx record, but the owner of that
    // operation is currently in the process of initializing the scx record,
    // so the helper sees an inconsistent scx record and takes invalid steps.
    // (recall that a helper can take a snapshot of an scx record whenever
    //  the sequence # it sees in a tagged pointer matches the sequence #
    //  it sees in the scx record.)
    // the solution here seems to be to ensure scx records can be helped only
    // when they are consistent.

    MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, SCXRecord<K comma1 V>::STATE_COMMITTED, MUTABLES_MASK_STATE, MUTABLES_OFFSET_STATE);

    TRACE COUTATOMICTID("help return COMMITTED after performing update cas"<<std::endl);
    return SCXRecord<K,V>::STATE_COMMITTED; // success
}

// you may call this only if node is protected by a call to recmgr->protect
template<class K, class V, class Compare, class RecManager>
void * bst_ns::bst<K,V,Compare,RecManager>::llx(
            const int tid,
            Node<K,V> *node,
            Node<K,V> **retLeft,
            Node<K,V> **retRight) {
    TRACE COUTATOMICTID("llx(tid="<<tid<<", node="<<*node<<")"<<std::endl);

#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
    tagptr_t tagptr1 = node->scxRecord.load(std::memory_order_relaxed);
#else
    tagptr_t tagptr1 = node->scxRecord.load(std::memory_order_relaxed) & ~0x1;
#endif

    // read mutable state field of descriptor
    bool succ;
    int state = DESC1_READ_FIELD(succ, TAGPTR1_UNPACK_PTR(tagptr1)->c.mutables, tagptr1, MUTABLES_MASK_STATE, MUTABLES_OFFSET_STATE);
    if (!succ) state = SCXRecord<K,V>::STATE_COMMITTED;
    // note: special treatment for alg in the case where the descriptor has already been reallocated (impossible before the transformation, assuming safe memory reclamation)

    SOFTWARE_BARRIER;       // prevent compiler from moving the read of marked before the read of state (no hw barrier needed on x86/64, since there is no read-read reordering)
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
    bool marked = node->marked.load(std::memory_order_relaxed);
#else
    bool marked = node->scxRecord.load(std::memory_order_relaxed) & 0x1;
#endif
    SOFTWARE_BARRIER;       // prevent compiler from moving the reads tagptr2=node->scxRecord or tagptr3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
    if ((state & SCXRecord<K,V>::STATE_COMMITTED && !marked) || state & SCXRecord<K,V>::STATE_ABORTED) {
        SOFTWARE_BARRIER;       // prevent compiler from moving the reads tagptr2=node->scxRecord or tagptr3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
        *retLeft = rqProvider->read_addr(tid, &node->left);
        *retRight = rqProvider->read_addr(tid, &node->right);
        if (*retLeft == NULL) {
            TRACE COUTATOMICTID("llx return2.a (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n");
            return LLX_RETURN_IS_LEAF;
        }
        SOFTWARE_BARRIER; // prevent compiler from moving the read of node->scxRecord before the read of left or right
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
        tagptr_t tagptr2 = node->scxRecord.load(std::memory_order_relaxed);
#else
        tagptr_t tagptr2 = node->scxRecord.load(std::memory_order_relaxed) & ~0x1;
#endif
        if (tagptr1 == tagptr2) {
            TRACE COUTATOMICTID("llx return2 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<" desc1="<<tagptr1<<")\n");
            // on x86/64, we do not need any memory barrier here to prevent mutable fields of node from being moved before our read of desc1, because the hardware does not perform read-read reordering. on another platform, we would need to ensure no read from after this point is reordered before this point (technically, before the read that becomes desc1)...
            return (void*) tagptr1;    // success
        } else {
            if (recmgr->shouldHelp()) {
                TRACE COUTATOMICTID("llx help 1 tid="<<tid<<std::endl);
                helpOther(tid, tagptr2);
            }
        }
    } else if (state == SCXRecord<K,V>::STATE_INPROGRESS) {
        if (recmgr->shouldHelp()) {
            TRACE COUTATOMICTID("llx help 2 tid="<<tid<<std::endl);
            helpOther(tid, tagptr1);
        }
    } else {
        // state committed and marked
        assert(state == 1); /* SCXRecord<K,V>::STATE_COMMITTED */
        assert(marked);
        if (recmgr->shouldHelp()) {
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
            tagptr_t tagptr3 = node->scxRecord.load(std::memory_order_relaxed);
#else
            tagptr_t tagptr3 = node->scxRecord.load(std::memory_order_relaxed) & ~0x1;
#endif
            TRACE COUTATOMICTID("llx help 3 tid="<<tid<<" tagptr3="<<tagptrToString(tagptr3)<<std::endl);
            helpOther(tid, tagptr3);
        }
    }
    TRACE COUTATOMICTID("llx return5 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n");
    return NULL;            // fail
}

#endif