/**
 * Preliminary C++ implementation of chromatic tree using LLX/SCX and DEBRA(+).
 * 
 * Copyright (C) 2017 Trevor Brown
 * This preliminary implementation is CONFIDENTIAL and may not be distributed.
 */

#ifndef CHROMATIC_IMPL_H
#define	CHROMATIC_IMPL_H

#include "chromatic.h"
using namespace std;

#ifdef NOREBALANCING
#define IFREBALANCING if (0)
#else
#define IFREBALANCING if (1)
#endif

#define IF_FAIL_TO_PROTECT_SCX(info, tid, _obj, arg2, arg3) \
    info.obj = _obj; \
    info.ptrToObj = arg2; \
    info.nodeContainingPtrToObjIsMarked = arg3; \
    if (_obj != dummy && !recordmgr->protect(tid, _obj, callbackCheckNotRetired, (void*) &info))
#define IF_FAIL_TO_PROTECT_NODE(info, tid, _obj, arg2, arg3) \
    info.obj = _obj; \
    info.ptrToObj = arg2; \
    info.nodeContainingPtrToObjIsMarked = arg3; \
    if (_obj != root && !recordmgr->protect(tid, _obj, callbackCheckNotRetired, (void*) &info))

inline CallbackReturn callbackCheckNotRetired(CallbackArg arg) {
    Chromatic_retired_info *info = (Chromatic_retired_info*) arg;
    if ((void*) info->ptrToObj->load(memory_order_relaxed) == info->obj) {
        // we insert a compiler barrier (not a memory barrier!)
        // to prevent these if statements from being merged or reordered.
        // we care because we need to see that ptrToObj == obj
        // and THEN see that ptrToObject is a field of an object
        // that is not marked. seeing both of these things,
        // in this order, implies that obj is in the data structure.
        SOFTWARE_BARRIER;
        if (!info->nodeContainingPtrToObjIsMarked->load(memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

#define allocateSCXRecord(tid) recordmgr->template allocate<SCXRecord<K,V> >((tid))
#define allocateNode(tid) recordmgr->template allocate<Node<K,V> >((tid))

#define initializeSCXRecord(_tid, _newop, _type, _nodes, _llxResults, _field, _newNode) { \
    (_newop)->type = (_type); \
    (_newop)->newNode = (_newNode); \
    for (int i=0;i<NUM_OF_NODES[(_type)];++i) { \
        (_newop)->nodes[i] = (_nodes)[i]; \
    } \
    for (int i=0;i<NUM_TO_FREEZE[(_type)];++i) { \
        (_newop)->scxRecordsSeen[i] = (SCXRecord<K,V>*) (_llxResults)[i]; \
    } \
    /* note: synchronization is not necessary for the following accesses, \
       since a memory barrier will occur before this object becomes reachable \
       from an entry point to the data structure. */ \
    (_newop)->state.store(SCXRecord<K,V>::STATE_INPROGRESS, memory_order_relaxed); \
    (_newop)->allFrozen.store(false, memory_order_relaxed); \
    (_newop)->field = (_field); \
}
#define initializeNode(_tid, _newnode, _key, _value, _weight, _left, _right) \
(_newnode); \
{ \
    (_newnode)->key = (_key); \
    (_newnode)->value = (_value); \
    (_newnode)->weight = (_weight); \
    /* note: synchronization is not necessary for the following accesses, \
       since a memory barrier will occur before this object becomes reachable \
       from an entry point to the data structure. */ \
    (_newnode)->left.store((uintptr_t) (_left), memory_order_relaxed); \
    (_newnode)->right.store((uintptr_t) (_right), memory_order_relaxed); \
    (_newnode)->scxRecord.store((uintptr_t) dummy, memory_order_relaxed); \
    (_newnode)->marked.store(false, memory_order_relaxed); \
}

template<class K, class V, class Compare, class MasterRecordMgr>
Chromatic<K,V,Compare,MasterRecordMgr>::Chromatic(const K& _NO_KEY,
            const V& _NO_VALUE,
            // const V& _RETRY,
            const int numProcesses,
            int neutralizeSignal,
            int allowedViolationsPerPath)
    : N(allowedViolationsPerPath)
    , NO_KEY(_NO_KEY)
    , NO_VALUE(_NO_VALUE)
    // , RETRY(_RETRY)
    , recordmgr(new MasterRecordMgr(numProcesses, neutralizeSignal)) {

    VERBOSE DEBUG COUTATOMIC("constructor chromatic"<<endl);
    const int tid = 0;

    allocatedSCXRecord = new SCXRecord<K,V>*[numProcesses*PREFETCH_SIZE_WORDS];
    allocatedNodes = new Node<K,V>*[numProcesses*(PREFETCH_SIZE_WORDS+MAX_NODES-1)];
    for (int tid=0;tid<numProcesses;++tid) {
        GET_ALLOCATED_SCXRECORD_PTR(tid) = NULL;
    }
    cmp = Compare();

    initThread(tid);

    auto guard = recordmgr->getGuard(tid);

    dummy = allocateSCXRecord(tid);
    dummy->type = SCXRecord<K,V>::TYPE_NOOP;
    dummy->state.store(SCXRecord<K,V>::STATE_ABORTED, memory_order_relaxed); // this is a NO-OP, so it shouldn't start as InProgress; aborted is just more efficient than committed, since we won't try to help marked leaves, which always have the dummy scx record...
    Node<K,V> *rootleft = allocateNode(tid);
    initializeNode(tid, rootleft, NO_KEY, NO_VALUE, 1, NULL, NULL);
    root = allocateNode(tid);
    initializeNode(tid, root, NO_KEY, NO_VALUE, 1, rootleft, NULL);
}

template<class K, class V, class Compare, class MasterRecordMgr>
long long Chromatic<K,V,Compare,MasterRecordMgr>::debugKeySum(Node<K,V> * node) {
    if (node == NULL) return 0;
    if ((void*) node->left.load(memory_order_relaxed) == NULL) {
        long long key = (long long) node->key;
        return (key == NO_KEY) ? 0 : key;
    }
    return debugKeySum((Node<K,V> *) node->left.load(memory_order_relaxed))
         + debugKeySum((Node<K,V> *) node->right.load(memory_order_relaxed));
}

template<class K, class V, class Compare, class MasterRecordMgr>
int Chromatic<K,V,Compare,MasterRecordMgr>::computeSize(Node<K,V> * const root) {
    if (root == NULL) return 0;
    if ((Node<K,V> *) root->left.load(memory_order_relaxed) != NULL) { // if internal node
        return computeSize((Node<K,V> *) root->left.load(memory_order_relaxed))
                + computeSize((Node<K,V> *) root->right.load(memory_order_relaxed));
    } else { // if leaf
        return 1;
//        printf(" %d", root->key);
    }
}

template<class K, class V, class Compare, class MasterRecordMgr>
int Chromatic<K,V,Compare,MasterRecordMgr>::size() {
    return computeSize((Node<K,V> *) ((Node<K,V> *) root->left.load(memory_order_relaxed))->left.load(memory_order_relaxed));
}

/**
 * This function must be called once by each thread that will
 * invoke any functions on this class.
 * 
 * It must be okay that we do this with the main thread and later with another thread!!!
 */
template<class K, class V, class Compare, class MasterRecordMgr>
void Chromatic<K,V,Compare,MasterRecordMgr>::initThread(const int tid) {
    if (init[tid]) return; else init[tid] = !init[tid];

    recordmgr->initThread(tid);
    if (GET_ALLOCATED_SCXRECORD_PTR(tid) == NULL) {
        REPLACE_ALLOCATED_SCXRECORD(tid);
        for (int i=0;i<MAX_NODES-1;++i) {
            REPLACE_ALLOCATED_NODE(tid, i);
        }
    }
}

template<class K, class V, class Compare, class MasterRecordMgr>
void Chromatic<K,V,Compare,MasterRecordMgr>::deinitThread(const int tid) {
    if (!init[tid]) return; else init[tid] = !init[tid];

    recordmgr->deinitThread(tid);
}

template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::contains(const int tid, const K& key) {
    pair<V,bool> result = find(tid, key);
    return result.second;
}

template<class K, class V, class Compare, class MasterRecordMgr>
const pair<V,bool> Chromatic<K,V,Compare,MasterRecordMgr>::find(const int tid, const K& key) {
    pair<V,bool> result;
    // Chromatic_retired_info info;
    Node<K,V> *p;
    Node<K,V> *l;
    for (;;) {
        // TRACE COUTATOMICTID("find(tid="<<tid<<" key="<<key<<")"<<endl);
        // CHECKPOINT_AND_RUN_QUERY(tid) {
            auto guard = recordmgr->getGuard(tid, true);
            // root is never retired, so we don't need to call
            // protectPointer before accessing its child pointers
            p = (Node<K,V>*) root->left.load(memory_order_relaxed);
            // IF_FAIL_TO_PROTECT_NODE(info, tid, p, &root->left, &root->marked) {
            //     // ++counters[tid]->findRestart;
            //     continue; /* retry */ 
            // }
            // assert(p != root);
            // assert(recordmgr->isProtected(tid, p));
            l = (Node<K,V>*) p->left.load(memory_order_relaxed);
            if (l == NULL) {
                result = pair<V,bool>(NO_VALUE, false); // no keys in data structure
                return result; // success
            }
            // IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) { 
            //     // ++counters[tid]->findRestart;
            //     continue; /* retry */ 
            // }

            // assert(recordmgr->isProtected(tid, l));
            while ((Node<K,V>*) l->left.load(memory_order_relaxed) != NULL) {
                // TRACE COUTATOMICTID("traversing tree; l="<<*l<<endl);
                // assert(recordmgr->isProtected(tid, p));
                // recordmgr->unprotect(tid, p);
                p = l; // note: the new p is currently protected
                // assert(recordmgr->isProtected(tid, p));
                // assert(p->key != NO_KEY);
                if (cmp(key, p->key)) {
                    // assert(recordmgr->isProtected(tid, p));
                    l = (Node<K,V>*) p->left.load(memory_order_relaxed);
                    // IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) {
                    //     // ++counters[tid]->findRestart;
                    //     continue; /* retry */ 
                    // }
                } else {
                    // assert(recordmgr->isProtected(tid, p));
                    l = (Node<K,V>*) p->right.load(memory_order_relaxed);
                    // IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->right, &p->marked) { 
                    //     // ++counters[tid]->findRestart;
                    //     continue; /* retry */ 
                    // }
                }
                // assert(recordmgr->isProtected(tid, l));
            }
            // assert(recordmgr->isProtected(tid, l));
            if (key == l->key) {
                // assert(recordmgr->isProtected(tid, l));
                result = pair<V,bool>(l->value, true);
            } else {
                result = pair<V,bool>(NO_VALUE, false);
            }
            return result; // success
        // }
    }
    return pair<V,bool>(NO_VALUE, false);
}

template<class K, class V, class Compare, class MasterRecordMgr>
const V Chromatic<K,V,Compare,MasterRecordMgr>::insert(const int tid, const K& key, const V& val) {
    bool onlyIfAbsent = false;
    V result = NO_VALUE;
    bool shouldRebalance = false;
    bool finished = false;
    while (!finished) {
        CHECKPOINT_AND_RUN_UPDATE(tid, finished) {
            auto guard = recordmgr->getGuard(tid);
            finished = updateInsert(tid, key, val, onlyIfAbsent, &result, &shouldRebalance);
            // if (!finished) ++counters[tid]->insertRestart;
        }
    }
    // call another routine to handle rebalancing.
    // this is considered to be a whole new operation (possibly many, in fact).
    IFREBALANCING if (shouldRebalance) {
        fixAllToKey(tid, key);
    }
    return result;
}

template<class K, class V, class Compare, class MasterRecordMgr>
const V Chromatic<K,V,Compare,MasterRecordMgr>::insertIfAbsent(const int tid, const K& key, const V& val) {
    bool onlyIfAbsent = true;
    V result = NO_VALUE;
    bool shouldRebalance = false;
    bool finished = false;
    while (!finished) {
        CHECKPOINT_AND_RUN_UPDATE(tid, finished) {
            auto guard = recordmgr->getGuard(tid);
            finished = updateInsert(tid, key, val, onlyIfAbsent, &result, &shouldRebalance);
            // if (!finished) ++counters[tid]->insertRestart;
        }
    }
    // call another routine to handle rebalancing.
    // this is considered to be a whole new operation (possibly many, in fact).
    IFREBALANCING if (shouldRebalance) {
        fixAllToKey(tid, key);
    }
    return result;
}

template<class K, class V, class Compare, class MasterRecordMgr>
const pair<V,bool> Chromatic<K,V,Compare,MasterRecordMgr>::erase(const int tid, const K& key) {
    V result = NO_VALUE;
    bool shouldRebalance = false;
    bool finished = false;
    while (!finished) {
        CHECKPOINT_AND_RUN_UPDATE(tid, finished) {
            auto guard = recordmgr->getGuard(tid);
            finished = updateErase(tid, key, &result, &shouldRebalance);
            // if (!finished) ++counters[tid]->eraseRestart;
        }
    }
    // call another routine to handle rebalancing.
    // this is considered to be a whole new operation (possibly many, in fact).
    IFREBALANCING if (shouldRebalance) {
        fixAllToKey(tid, key);
    }
    return pair<V,bool>(result, (result != NO_VALUE));
}

template<class K, class V, class Compare, class MasterRecordMgr>
void Chromatic<K,V,Compare,MasterRecordMgr>::fixAllToKey(const int tid, const K& key) {
    bool finished = false;
    while (!finished) {
        // we use CHECKPOINT_AND_RUN_QUERY here because rebalancing does not need to be helped if a process is neutralized
        CHECKPOINT_AND_RUN_QUERY(tid) {
            auto guard = recordmgr->getGuard(tid);
            finished = updateRebalancingStep(tid, key);
        }
    }
}

// RULE: ANY OUTPUT OF updateXXXXX MUST BE FULLY WRITTEN BEFORE SCX IS INVOKED!
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::updateInsert(
            const int tid,
            const K& key,
            const V& val,
            const bool onlyIfAbsent,
            V * const result,
            bool * const shouldRebalance) {
    TRACE COUTATOMICTID("insert(tid="<<tid<<", key="<<key<<")"<<endl);
    
    int debugLoopCount = 0;
    Node<K,V> *p = root, *l;
    int count = 0;
    
    // root is never retired, so we don't need to call
    // protect before accessing its child pointers
    l = (Node<K,V>*) root->left.load(memory_order_relaxed);

    Chromatic_retired_info info;
    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &root->left, &root->marked) {
        TRACE COUTATOMICTID("return false1\n");
        return false;
    } // return and retry
    assert(recordmgr->isProtected(tid, l));
    if ((Node<K,V>*) l->left.load(memory_order_relaxed) != NULL) { // the tree contains some node besides sentinels...
        p = l;          // note: p is protected by the above call to protect(..., l, ...)
        assert(recordmgr->isProtected(tid, l));
        l = (Node<K,V>*) l->left.load(memory_order_relaxed);    // note: l must have key infinity, and l->left must not.

        // loop invariant: p and l are protected by calls to protect(tid, ...)
        IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) {
            TRACE COUTATOMICTID("return false2\n");
            return false;
        } // return and retry
        assert(recordmgr->isProtected(tid, l));
        while ((Node<K,V>*) l->left.load(memory_order_relaxed) != NULL) {
            TRACE COUTATOMICTID("traversing tree; l="<<*l<<endl);
            DEBUG if (++debugLoopCount > 10000) { COUTATOMICTID("tree extremely likely to contain a cycle."<<endl); raise(SIGTERM); }
            assert(recordmgr->isProtected(tid, l));
            assert(recordmgr->isProtected(tid, p));
            if (l->weight > 1 || l->weight == 0 && p->weight == 0) ++count; // count violations on this path
            recordmgr->unprotect(tid, p);
//            assert(!recordmgr->isProtected(tid, p));
            p = l;  // note: p is protected by the call to protect made in the last iteration of this loop (or above the loop)

            assert(p->key != NO_KEY);
            if (cmp(key, p->key)) {
                assert(recordmgr->isProtected(tid, p));
                l = (Node<K,V>*) p->left.load(memory_order_relaxed);
                IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) {
                    TRACE COUTATOMICTID("return false3\n");
                    return false;
                } // return and retry
            } else {
                assert(recordmgr->isProtected(tid, p));
                l = (Node<K,V>*) p->right.load(memory_order_relaxed);
                IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->right, &p->marked) {
                    TRACE COUTATOMICTID("return false4\n");
                    return false;
                } // return and retry
            }
            assert(recordmgr->isProtected(tid, l));
        }
    }

    // if we find the key in the tree already
    assert(recordmgr->isProtected(tid, l));
    if (key == l->key) {
        if (onlyIfAbsent) {
            assert(recordmgr->isProtected(tid, l));
            *result = l->value;
            TRACE COUTATOMICTID("return true5\n");
            return true; // success
        }

        void *llxResults[NUM_TO_FREEZE[SCXRecord<K,V>::TYPE_REPLACE]];
        Node<K,V> *nodes[] = {p, l};
        Node<K,V> *pleft, *pright;
        // note: p is already protected by a call to protect in the search phase, above
        assert(recordmgr->isProtected(tid, p) || p == root);
        if ((llxResults[0] = LLX(tid, p, &pleft, &pright)) == NULL) {
            TRACE COUTATOMICTID("return false6\n");
            return false;
        } //RETRY;
        if (l != pleft && l != pright) {
            TRACE COUTATOMICTID("return false7\n");
            return false;
        } //RETRY;
        assert(recordmgr->isProtected(tid, l));
        *result = l->value;

        assert(recordmgr->isProtected(tid, l));
        assert((Node<K,V>*) l->left.load(memory_order_relaxed) == NULL);
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), key, val, l->weight, NULL, NULL);

        assert(recordmgr->isProtected(tid, p) || p == root);
        assert(recordmgr->isProtected(tid, l));
        if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_REPLACE, nodes, llxResults, (l == pleft ? &p->left : &p->right), GET_ALLOCATED_NODE_PTR(tid, 0))) {
            TRACE COUTATOMICTID("return true8\n");
            return true;
        } else {
            TRACE COUTATOMICTID("return false9\n");
            return false; //RETRY;
        }
    } else {
        void *llxResults[NUM_TO_FREEZE[SCXRecord<K,V>::TYPE_INS]];
        Node<K,V> *nodes[] = {p, l};
        Node<K,V> *pleft, *pright;
        // note: p is already protected by a call to protect in the search phase, above
        assert(recordmgr->isProtected(tid, p) || p == root);
        if ((llxResults[0] = LLX(tid, p, &pleft, &pright)) == NULL) {
            TRACE COUTATOMICTID("return false10\n");
            return false;
        } //RETRY;
        if (l != pleft && l != pright) {
            TRACE COUTATOMICTID("return false11\n");
            return false;
        } //RETRY;

        // Compute the weight for the new parent node.
        // If l is a sentinel then we must set its weight to one.
        assert(recordmgr->isProtected(tid, l));
        assert(recordmgr->isProtected(tid, p) || p == root);
        int newWeight = (IS_SENTINEL(l, p) ? 1 : l->weight - 1);

        /*Node<K,V> *newLeaf =*/
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), key, val, 1, NULL, NULL);
        assert(recordmgr->isProtected(tid, l));
        /*Node<K,V> *newCopyOfL =*/
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), l->key, l->value, 1, NULL, NULL);
        /*Node<K,V> *newInternal;*/
        assert(recordmgr->isProtected(tid, l));
        if (l->key == NO_KEY || cmp(key, l->key)) {
            assert(recordmgr->isProtected(tid, l));
            /*newInternal = */
            initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), l->key, l->value, newWeight, GET_ALLOCATED_NODE_PTR(tid, 0), GET_ALLOCATED_NODE_PTR(tid, 1));
        } else {
            /*newInternal = */
            initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), key, val, newWeight, GET_ALLOCATED_NODE_PTR(tid, 1), GET_ALLOCATED_NODE_PTR(tid, 0));
        }
        *result = NO_VALUE;
        *shouldRebalance = (count > N);

        if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_INS, nodes, llxResults, (l == pleft ? &p->left : &p->right), GET_ALLOCATED_NODE_PTR(tid, 2))) {
            TRACE COUTATOMICTID("return true12\n");
            return true; // success
        } else {
            TRACE COUTATOMICTID("return false13\n"); 
            return false; //RETRY;
        }
    }
}

// RULE: ANY OUTPUT OF updateXXXXX MUST BE FULLY WRITTEN BEFORE SCX IS INVOKED!
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::updateErase(
            const int tid,
            const K& key,
            V * const result,
            bool * const shouldRebalance) {
    TRACE COUTATOMICTID("erase(tid="<<tid<<", key="<<key<<")"<<endl);

    int debugLoopCount = 0;
    Node<K,V> *gp, *p, *l;


    // root is never retired, so we don't need to call
    // protect before accessing its child pointers
    l = (Node<K,V>*) root->left.load(memory_order_relaxed);

    Chromatic_retired_info info;
    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &root->left, &root->marked) {
        TRACE COUTATOMICTID("return false1\n");
        return false;
    } // return and retry
    assert(recordmgr->isProtected(tid, l));
    if ((Node<K,V>*) l->left.load(memory_order_relaxed) == NULL) {
        TRACE COUTATOMICTID("return true2\n");
        return true;
    } // only sentinels in tree...
    
    int count = 0;
    gp = root;      // note: gp is protected because it is the root
    p = l;          // note: p is protected by the above call to protect(..., l, ...)
    assert(recordmgr->isProtected(tid, p));
    l = (Node<K,V>*) p->left.load(memory_order_relaxed);    // note: l must have key infinity, and l->left must not.
    
    // loop invariant: gp, p, l are all protected by calls to protect(tid, ...) (and no other nodes are)
    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) {
        TRACE COUTATOMICTID("return false3\n");
        return false;
    } // return and retry
    assert(recordmgr->isProtected(tid, l));
    while ((Node<K,V>*) l->left.load(memory_order_relaxed) != NULL) {
        TRACE COUTATOMICTID("traversing tree; l="<<*l<<endl);
        DEBUG if (++debugLoopCount > 10000) { COUTATOMICTID("tree extremely likely to contain a cycle."<<endl); raise(SIGTERM); }
        assert(recordmgr->isProtected(tid, l));
        assert(recordmgr->isProtected(tid, p));
        if (l->weight > 1 || l->weight == 0 && p->weight == 0) ++count; // count violations on this path
        if (gp != root) recordmgr->unprotect(tid, gp);
        gp = p; // note: gp is protected by the call to protect made in the second last iteration of this loop (or above the loop)
        p = l;  // note: p is protected by the call to protect made in the last iteration of this loop (or above the loop)
        assert(recordmgr->isProtected(tid, gp));
        assert(recordmgr->isProtected(tid, p));
        
        assert(p->key != NO_KEY);
        if (cmp(key, p->key)) {
            assert(recordmgr->isProtected(tid, p));
            l = (Node<K,V>*) p->left.load(memory_order_relaxed);
            IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) {
                TRACE COUTATOMICTID("return false4\n");
                return false;
            } // return and retry
        } else {
            assert(recordmgr->isProtected(tid, p));
            l = (Node<K,V>*) p->right.load(memory_order_relaxed);
            IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->right, &p->marked) {
                TRACE COUTATOMICTID("return false5\n");
                return false;
            } // return and retry
        }
        assert(recordmgr->isProtected(tid, l));
    }

    // if we fail to find the key in the tree
    assert(recordmgr->isProtected(tid, l));
    if (key != l->key) {
        *result = NO_VALUE;
        return true; // success
    } else {
        assert(key != NO_KEY);
        void *llxResults[NUM_TO_FREEZE[SCXRecord<K,V>::TYPE_DEL]];
        Node<K,V> *nodes[] = {gp, p, NULL, l};
        Node<K,V> *gpleft, *gpright;
        Node<K,V> *pleft, *pright;
        Node<K,V> *sleft, *sright;
        // note: gp is already protected
        assert(recordmgr->isProtected(tid, gp) || gp == root);
        if ((llxResults[0] = LLX(tid, gp, &gpleft, &gpright)) == NULL) {
            TRACE COUTATOMICTID("return false6\n");
            return false;
        }
        if (p != gpleft && p != gpright) {
            TRACE COUTATOMICTID("return false7\n");
            return false;
        }
        // note: p is already protected
        assert(recordmgr->isProtected(tid, p));
        if ((llxResults[1] = LLX(tid, p, &pleft, &pright)) == NULL) {
            TRACE COUTATOMICTID("return false8\n");
            return false;
        }
        if (l != pleft && l != pright) {
            TRACE COUTATOMICTID("return false9\n");
            return false;
        }
        assert(recordmgr->isProtected(tid, l));
        *result = l->value;

        // Read fields for the sibling s of l
        // note: we must call protect(..., s, ...) because LLX will read its fields.
        Node<K,V> *s = (l == pleft ? pright : pleft);
        IF_FAIL_TO_PROTECT_NODE(info, tid, s, (l == pleft ? &p->right : &p->left), &p->marked) {
            TRACE COUTATOMICTID("return false10\n");
            return false;
        } // return and retry
        assert(recordmgr->isProtected(tid, s));
        if ((llxResults[2] = LLX(tid, s, &sleft, &sright)) == NULL) {
            TRACE COUTATOMICTID("return false11\n");
            return false;
        }
        nodes[2] = s;

        // Now, if the op. succeeds, all structure is guaranteed to be just as we verified

        // Compute weight for the new node that replaces p (and l)
        // If p is a sentinel then we must set the new node's weight to one.
        assert(recordmgr->isProtected(tid, p));
        assert(recordmgr->isProtected(tid, s));
        int newWeight = (IS_SENTINEL(p, gp) ? 1 : p->weight + s->weight);

        assert(recordmgr->isProtected(tid, s));
        initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), s->key, s->value, newWeight, sleft, sright);
        assert(recordmgr->isProtected(tid, gp) || gp == root);
        assert(recordmgr->isProtected(tid, p));
        assert(recordmgr->isProtected(tid, l));
        assert(recordmgr->isProtected(tid, s));
        *shouldRebalance = (count > N);
        
        if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_DEL, nodes, llxResults, (p == gpleft ? &gp->left : &gp->right), GET_ALLOCATED_NODE_PTR(tid, 0))) {
            TRACE COUTATOMICTID("return true12\n");
            return true; // success
        } else {
            TRACE COUTATOMICTID("return false13\n");
            return false; // retry
        }
    }
}

template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::updateRebalancingStep(
            const int tid,
            const K& key) {
    TRACE COUTATOMICTID("rebalancing(tid="<<tid<<", key="<<key<<")"<<endl);
    Node<K,V> *ggp, *gp, *p, *l;

    // root is never retired, so we don't need to call
    // protect before accessing its child pointers
    l = (Node<K,V>*) root->left.load(memory_order_relaxed);

    Chromatic_retired_info info;
    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &root->left, &root->marked) return false; // return and retry
    if ((Node<K,V>*) l->left.load(memory_order_relaxed) == NULL) return true; // only sentinels in tree...
    
    ggp = gp = root;
    p = l; // note: p is protected by the above call to protect(..., l, ...)
    assert(recordmgr->isProtected(tid, p));
    l = (Node<K,V>*) l->left.load(memory_order_relaxed); // note: l must have key infinity, and l->left must not.
    
    // loop invariant: ggp, gp, p, l are all protected by calls to protect(tid, ...) (and no other nodes are)
    assert(recordmgr->isProtected(tid, p));
    IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) return false; // return and retry
    assert(recordmgr->isProtected(tid, l));
    while ((Node<K,V>*) l->left.load(memory_order_relaxed) != NULL && l->weight <= 1 && (l->weight != 0 || p->weight != 0)) {
        if (ggp != root) recordmgr->unprotect(tid, ggp);
        ggp = gp;
        gp = p;
        p = l; // note: p is still protected by the call to protect made in the last iteration (or above the loop, where p = l)
        assert(recordmgr->isProtected(tid, ggp) || ggp == root);
        assert(recordmgr->isProtected(tid, gp));
        assert(recordmgr->isProtected(tid, p));
        assert(recordmgr->isProtected(tid, l));
        
        assert(p->key != NO_KEY);
        if (cmp(key, p->key)) {
            l = (Node<K,V>*) p->left.load(memory_order_relaxed);
            IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->left, &p->marked) return false; // return and retry
        } else {
            l = (Node<K,V>*) p->right.load(memory_order_relaxed);
            IF_FAIL_TO_PROTECT_NODE(info, tid, l, &p->right, &p->marked) return false; // return and retry
        }
    }
    assert(recordmgr->isProtected(tid, l));
    if (l->weight == 1) {
        return true; // (if no violation, then we hit a leaf, so we can stop)
    }

    // a few aliases to make the code more uniform
    // note: these nodes have already been passed to protect() in the
    //       search phase, above. they are currently protected.
    //       thus, we don't need to call protect() before LLXing them.
    // the following variable names follow a specific convention that encodes
    // their ancestry relative to one another.
    //       u is the topmost node.
    //       uX is a child of u, but we don't know/care which child it is.
    //       uXL is the left child of uX. uXR is the right child of uX.
    // more complicated example:
    //       uXXRLR is the right child of uXXRL,
    //       which is the left child of uXXR,
    //       which is the right child of uXX,
    //       which is a child of uX,
    //       which is a child of u.
    Node<K,V> * const u = ggp;
    Node<K,V> * const uX = gp;
    Node<K,V> * const uXX = p;
    Node<K,V> * const uXXX = l;

    void *iu;       Node<K,V> *uL, *uR;
    void *iuX;      Node<K,V> *uXL, *uXR;
    void *iuXX;     Node<K,V> *uXXL, *uXXR;
    void *iuXL;     Node<K,V> *uXLL, *uXLR;
    void *iuXR;     Node<K,V> *uXRL, *uXRR;
    void *iuXXL;    Node<K,V> *uXXLL, *uXXLR;
    void *iuXXR;    Node<K,V> *uXXRL, *uXXRR;
    void *iuXXLL;   Node<K,V> *uXXLLL, *uXXLLR;
    void *iuXXLR;   Node<K,V> *uXXLRL, *uXXLRR;
    void *iuXXRL;   Node<K,V> *uXXRLL, *uXXRLR;
    void *iuXXRR;   Node<K,V> *uXXRRL, *uXXRRR;
    void *iuXXLRL;  Node<K,V> *uXXLRLL, *uXXLRLR;
    void *iuXXLRR;  Node<K,V> *uXXLRRL, *uXXLRRR;
    void *iuXXRLL;  Node<K,V> *uXXRLLL, *uXXRLLR;
    void *iuXXRLR;  Node<K,V> *uXXRLRL, *uXXRLRR;

    assert(recordmgr->isProtected(tid, u) || u == root);
    if ((iu = LLX(tid, u, &uL, &uR)) == NULL) return false; // return and retry
    bool uXleft = (uX == uL);
    if (!uXleft && uX != uR) return false; // return and retry

    assert(recordmgr->isProtected(tid, uX) || uX == root);
    if ((iuX = LLX(tid, uX, &uXL, &uXR)) == NULL) return false; // return and retry
    bool uXXleft = (uXX == uXL);
    if (!uXXleft && uXX != uXR) return false; // return and retry

    assert(recordmgr->isProtected(tid, uXX));
    if ((iuXX = LLX(tid, uXX, &uXXL, &uXXR)) == NULL) return false; // return and retry
    bool uXXXleft = (uXXX == uXXL);
    if (!uXXXleft && uXXX != uXXR) return false; // return and retry

    // any further nodes we LLX will have to have protect calls first
    
    /**
     * Overweight violation
     */

    IF_FAIL_TO_PROTECT_NODE(info, tid, uXXL, &uXX->left, &uXX->marked) return false; // return and retry
    if ((iuXXL = LLX(tid, uXXL, &uXXLL, &uXXLR)) == NULL) return false; // return and retry

    IF_FAIL_TO_PROTECT_NODE(info, tid, uXXR, &uXX->right, &uXX->marked) return false; // return and retry
    if ((iuXXR = LLX(tid, uXXR, &uXXRL, &uXXRR)) == NULL) return false; // return and retry

    if (uXXX->weight > 1) {
        if (uXXXleft) {
            /**
             * Rebalance left overweight violation
             */
            if (uXXR->weight == 0) {
                if (uXX->weight == 0) {
                    if (uXXleft) {
                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXR, &uX->right, &uX->marked) return false; // return and retry
                        if ((iuXR = LLX(tid, uXR, &uXRL, &uXRR)) == NULL) return false; // return and retry
                        
                        if (uXR->weight == 0) {
                            void *llxResults[] = {iu, iuX, iuXX, iuXR}; Node<K,V> *nodes[] = {u, uX, uXX, uXR};
                            return doBlk(tid, nodes, llxResults, uXleft);
                        } else { // assert: uXR->weight > 0
                            void *llxResults[] = {iu, iuX, iuXX, iuXXR}; Node<K,V> *nodes[] = {u, uX, uXX, uXXR};
                            return doRb2(tid, nodes, llxResults, uXleft);
                        }
                    } else { // assert: uXX == uXR
                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXL, &uX->left, &uX->marked) return false; // return and retry
                        if ((iuXL = LLX(tid, uXL, &uXLL, &uXLR)) == NULL) return false; // return and retry
                        
                        if (uXL->weight == 0) {
                            void *llxResults[] = {iu, iuX, iuXL, iuXX}; Node<K,V> *nodes[] = {u, uX, uXL, uXX};
                            return doBlk(tid, nodes, llxResults, uXleft);
                        } else {
                            void *llxResults[] = {iu, iuX, iuXX}; Node<K,V> *nodes[] = {u, uX, uXX};
                            return doRb1Sym(tid, nodes, llxResults, uXleft);
                        }
                    }
                } else { // assert: uXX->weight > 0
                    IF_FAIL_TO_PROTECT_NODE(info, tid, uXXRL, &uXXR->left, &uXXR->marked) return false; // return and retry
                    if ((iuXXRL = LLX(tid, uXXRL, &uXXRLL, &uXXRLR)) == NULL) return false; // return and retry

                    if (uXXRL->weight > 1) {
                        void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRL};
                        return doW1(tid, nodes, llxResults, uXXleft);
                    } else if (uXXRL->weight == 0) {
                        void *llxResults[] = {iuX, iuXX, iuXXR, iuXXRL}; Node<K,V> *nodes[] = {uX, uXX, uXXR, uXXRL};
                        return doRb2Sym(tid, nodes, llxResults, uXXleft);
                    } else { // assert: uXXRL->weight == 1
                        if (uXXRLR == NULL) {
                            TRACE COUTATOMICTID("rebalancing return because uXXRLR == NULL"<<endl);
                            return false; // return and retry
                        }
                        
                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXXRLR, &uXXRL->right, &uXXRL->marked) return false; // return and retry
                        if ((iuXXRLR = LLX(tid, uXXRLR, &uXXRLRL, &uXXRLRR)) == NULL) return false; // return and retry
                        
                        if (uXXRLR->weight == 0) {
                            void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRL, iuXXRLR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRL, uXXRLR};
                            return doW4(tid, nodes, llxResults, uXXleft);
                        } else { // assert: uXXRLR->weight > 0
                            if (uXXRLL == NULL) {
                                TRACE COUTATOMICTID("rebalancing return because uXXRLL == NULL"<<endl);
                                return false; // return and retry
                            }
                            
                            IF_FAIL_TO_PROTECT_NODE(info, tid, uXXRLL, &uXXRL->left, &uXXRL->marked) return false; // return and retry
                            if ((iuXXRLL = LLX(tid, uXXRLL, &uXXRLLL, &uXXRLLR)) == NULL) return false; // return and retry
                            
                            if (uXXRLL->weight == 0) {
                                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRL, iuXXRLL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRL, uXXRLL};
                                return doW3(tid, nodes, llxResults, uXXleft);
                            } else { // assert: uXXRLL->weight > 0
                                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRL};
                                return doW2(tid, nodes, llxResults, uXXleft);
                            }
                        }
                    }
                }
            } else if (uXXR->weight == 1) {
                if (uXXRL == NULL) {
                    TRACE COUTATOMICTID("rebalancing return because uXXRL == NULL"<<endl);
                    return false; // return and retry
                }
                
                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXRR, &uXXR->right, &uXXR->marked) return false; // return and retry
                if ((iuXXRR = LLX(tid, uXXRR, &uXXRRL, &uXXRRR)) == NULL) return false; // return and retry

                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXRL, &uXXR->left, &uXXR->marked) return false; // return and retry
                if ((iuXXRL = LLX(tid, uXXRL, &uXXRLL, &uXXRLR)) == NULL) return false; // return and retry
                
                if (uXXRR->weight == 0) {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRR};
                    return doW5(tid, nodes, llxResults, uXXleft);
                } else if (uXXRL->weight == 0) {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXRL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXRL};
                    return doW6(tid, nodes, llxResults, uXXleft);
                } else {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR};
                    return doPush(tid, nodes, llxResults, uXXleft);
                }
            } else {
                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR};
                return doW7(tid, nodes, llxResults, uXXleft);
            }
        } else {
            /**
             * Rebalance right overweight violation
             */
            if (uXXL->weight == 0) {
                if (uXX->weight == 0) {
                    if (!uXXleft) {
                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXL, &uX->left, &uX->marked) return false; // return and retry
                        if ((iuXL = LLX(tid, uXL, &uXLL, &uXLR)) == NULL) return false; // return and retry
                        
                        if (uXL->weight == 0) {
                            void *llxResults[] = {iu, iuX, iuXL, iuXX}; Node<K,V> *nodes[] = {u, uX, uXL, uXX};
                            return doBlk(tid, nodes, llxResults, uXleft);
                        } else { // assert: uXL->weight > 0
                            void *llxResults[] = {iu, iuX, iuXX, iuXXL}; Node<K,V> *nodes[] = {u, uX, uXX, uXXL};
                            return doRb2Sym(tid, nodes, llxResults, uXleft);
                        }
                    } else { // assert: uXX == uXL
                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXR, &uX->right, &uX->marked) return false; // return and retry
                        if ((iuXR = LLX(tid, uXR, &uXRL, &uXRR)) == NULL) return false; // return and retry
                        
                        if (uXR->weight == 0) {
                            void *llxResults[] = {iu, iuX, iuXX, iuXR}; Node<K,V> *nodes[] = {u, uX, uXX, uXR};
                            return doBlk(tid, nodes, llxResults, uXleft);
                        } else {
                            void *llxResults[] = {iu, iuX, iuXX}; Node<K,V> *nodes[] = {u, uX, uXX};
                            return doRb1(tid, nodes, llxResults, uXleft);
                        }
                    }
                } else { // assert: uXX->weight > 0
                    IF_FAIL_TO_PROTECT_NODE(info, tid, uXXL, &uXX->left, &uXX->marked) return false; // return and retry
                    if ((iuXXL = LLX(tid, uXXL, &uXXLL, &uXXLR)) == NULL) return false; // return and retry

                    IF_FAIL_TO_PROTECT_NODE(info, tid, uXXLR, &uXXL->right, &uXXL->marked) return false; // return and retry
                    if ((iuXXLR = LLX(tid, uXXLR, &uXXLRL, &uXXLRR)) == NULL) return false; // return and retry

                    if (uXXLR->weight > 1) {
                        void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLR};
                        return doW1Sym(tid, nodes, llxResults, uXXleft);
                    } else if (uXXLR->weight == 0) {
                        void *llxResults[] = {iuX, iuXX, iuXXL, iuXXLR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXLR};
                        return doRb2(tid, nodes, llxResults, uXXleft);
                    } else { // assert: uXXLR->weight == 1
                        if (uXXLRL == NULL) {
                            TRACE COUTATOMICTID("rebalancing return because uXXLRL == NULL"<<endl);
                            return false; // return and retry
                        }

                        IF_FAIL_TO_PROTECT_NODE(info, tid, uXXLRL, &uXXLR->left, &uXXLR->marked) return false; // return and retry
                        if ((iuXXLRL = LLX(tid, uXXLRL, &uXXLRLL, &uXXLRLR)) == NULL) return false; // return and retry
                        
                        if (uXXLRL->weight == 0) {
                            void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLR, iuXXLRL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLR, uXXLRL};
                            return doW4Sym(tid, nodes, llxResults, uXXleft);
                        } else { // assert: uXXLRL->weight > 0
                            if (uXXLRR == NULL) {
                                TRACE COUTATOMICTID("rebalancing return because uXXLRR == NULL"<<endl);
                                return false; // return and retry
                            }

                            IF_FAIL_TO_PROTECT_NODE(info, tid, uXXLRR, &uXXLR->right, &uXXLR->marked) return false; // return and retry
                            if ((iuXXLRR = LLX(tid, uXXLRR, &uXXLRRL, &uXXLRRR)) == NULL) return false; // return and retry
                            
                            if (uXXLRR->weight == 0) {
                                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLR, iuXXLRR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLR, uXXLRR};
                                return doW3Sym(tid, nodes, llxResults, uXXleft);
                            } else { // assert: uXXLRR->weight > 0
                                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLR};
                                return doW2Sym(tid, nodes, llxResults, uXXleft);
                            }
                        }
                    }
                }
            } else if (uXXL->weight == 1) {
                if (uXXLR == NULL) {
                    TRACE COUTATOMICTID("rebalancing return because uXXLR == NULL"<<endl);
                    return false; // return and retry // note: if uXXLL is NULL, then uXXLR is NULL, since tree is always a full binary tree, and children of leaves don't change
                }

                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXLL, &uXXL->left, &uXXL->marked) return false; // return and retry
                if ((iuXXLL = LLX(tid, uXXLL, &uXXLLL, &uXXLLR)) == NULL) return false; // return and retry

                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXLR, &uXXL->right, &uXXL->marked) return false; // return and retry
                if ((iuXXLR = LLX(tid, uXXLR, &uXXLRL, &uXXLRR)) == NULL) return false; // return and retry
                
                if (uXXLL->weight == 0) {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLL}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLL};
                    return doW5Sym(tid, nodes, llxResults, uXXleft);
                } else if (uXXLR->weight == 0) {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR, iuXXLR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR, uXXLR};
                    return doW6Sym(tid, nodes, llxResults, uXXleft);
                } else {
                    void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR};
                    return doPushSym(tid, nodes, llxResults, uXXleft);
                }
            } else {
                void *llxResults[] = {iuX, iuXX, iuXXL, iuXXR}; Node<K,V> *nodes[] = {uX, uXX, uXXL, uXXR};
                return doW7Sym(tid, nodes, llxResults, uXXleft);
            }
        }
    /**
     * Red-red violation
     */
    } else {
        if (uXXleft) {
            /**
             * Rebalance left red-red violation
             */
            IF_FAIL_TO_PROTECT_NODE(info, tid, uXR, &uX->right, &uX->marked) return false; // return and retry
            if ((iuXR = LLX(tid, uXR, &uXRL, &uXRR)) == NULL) return false; // return and retry

            if (uXR->weight == 0) {
                void *llxResults[] = {iu, iuX, iuXX, iuXR}; Node<K,V> *nodes[] = {u, uX, uXX, uXR};
                return doBlk(tid, nodes, llxResults, uXleft);
            } else if (uXXXleft) {
                void *llxResults[] = {iu, iuX, iuXX}; Node<K,V> *nodes[] = {u, uX, uXX};
                return doRb1(tid, nodes, llxResults, uXleft);
            } else {
                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXR, &uXX->right, &uXX->marked) return false; // return and retry
                if ((iuXXR = LLX(tid, uXXR, &uXXRL, &uXXRR)) == NULL) return false; // return and retry
                
                void *llxResults[] = {iu, iuX, iuXX, iuXXR}; Node<K,V> *nodes[] = {u, uX, uXX, uXXR};
                return doRb2(tid, nodes, llxResults, uXleft);
            }
        } else {
            /**
             * Rebalance right red-red violation
             */
            IF_FAIL_TO_PROTECT_NODE(info, tid, uXL, &uX->left, &uX->marked) return false; // return and retry
            if ((iuXL = LLX(tid, uXL, &uXLL, &uXLR)) == NULL) return false; // return and retry

            if (uXL->weight == 0) {
                void *llxResults[] = {iu, iuX, iuXL, iuXX}; Node<K,V> *nodes[] = {u, uX, uXL, uXX};
                return doBlk(tid, nodes, llxResults, uXleft);
            } else if (!uXXXleft) {
                void *llxResults[] = {iu, iuX, iuXX}; Node<K,V> *nodes[] = {u, uX, uXX};
                return doRb1Sym(tid, nodes, llxResults, uXleft);
            } else {
                IF_FAIL_TO_PROTECT_NODE(info, tid, uXXL, &uXX->left, &uXX->marked) return false; // return and retry
                if ((iuXXL = LLX(tid, uXXL, &uXXLL, &uXXLR)) == NULL) return false; // return and retry
                
                void *llxResults[] = {iu, iuX, iuXX, iuXXL}; Node<K,V> *nodes[] = {u, uX, uXX, uXXL};
                return doRb2Sym(tid, nodes, llxResults, uXleft);
            }
        }
    }
    COUTATOMICTID("THE IMPOSSIBLE HAPPENED!!!!!"<<endl);
    exit(-1);
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doBlk(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, 1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, 1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = (IS_SENTINEL(nodes[1], nodes[0]) ? 1 : nodes[1]->weight-1); // root of old subtree is a sentinel
    Node<K,V> *nodeX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[1]->key, nodes[1]->value, weight, nodeXL, nodeXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_BLK, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doRb1(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 0, (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed), (Node<K,V>*) nodes[1]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), nodeXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_RB1, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doRb2(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, 0, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[1]->key, nodes[1]->value, 0, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed), (Node<K,V>*) nodes[1]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXL, nodeXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_RB2, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doPush(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, 0, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = (IS_SENTINEL(nodes[1], nodes[0]) ? 1 : nodes[1]->weight+1); // root of old subtree is a sentinel
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[1]->key, nodes[1]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_PUSH, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW1(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXLR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[4]->key, nodes[4]->value, nodes[4]->weight-1, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXLL, nodeXXLR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXXL, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W1, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW2(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXLR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[4]->key, nodes[4]->value, 0, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXLL, nodeXXLR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXXL, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W2, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW3(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 4), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[1]->key, nodes[1]->value, 1, nodeXXLLL, (Node<K,V>*) nodes[5]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXLR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[4]->key, nodes[4]->value, 1, (Node<K,V>*) nodes[5]->right.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[5]->key, nodes[5]->value, 0, nodeXXLL, nodeXXLR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXXL, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W3, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW4(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXLL, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXRL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 4), nodes[5]->key, nodes[5]->value, 1, (Node<K,V>*) nodes[5]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[5]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, 0, nodeXXRL, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[4]->key, nodes[4]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W4, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW5(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXLL, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[4]->key, nodes[4]->value, 1, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W5, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW6(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXLL, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, 1, (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[4]->key, nodes[4]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W6, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW7(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = (IS_SENTINEL(nodes[1], nodes[0]) ? 1 : nodes[1]->weight+1); // root of old subtree is a sentinel
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[1]->key, nodes[1]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W7, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doRb1Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 0, (Node<K,V>*) nodes[1]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, nodeXL, (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_RB1SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doRb2Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 0, (Node<K,V>*) nodes[1]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[2]->key, nodes[2]->value, 0, (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    int weight = nodes[1]->weight;
    Node<K,V> *nodeX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[3]->key, nodes[3]->value, weight, nodeXL, nodeXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_RB2SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doPushSym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, 0, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = (IS_SENTINEL(nodes[1], nodes[0]) ? 1 : nodes[1]->weight+1); // root of old subtree is a sentinel
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[1]->key, nodes[1]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_PUSHSYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW1Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXRL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[4]->key, nodes[4]->value, nodes[4]->weight-1, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXRL, nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W1SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW2Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXRL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[4]->key, nodes[4]->value, 0, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[1]->key, nodes[1]->value, 1, nodeXXRL, nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W2SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW3Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXRL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[4]->key, nodes[4]->value, 1, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[5]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXRRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 4), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[1]->key, nodes[1]->value, 1, (Node<K,V>*) nodes[5]->right.load(memory_order_relaxed), nodeXXRRR);
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[5]->key, nodes[5]->value, 0, nodeXXRL, nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W3SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW4Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXLR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[5]->key, nodes[5]->value, 1, (Node<K,V>*) nodes[5]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[5]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, 0, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), nodeXXLR);
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 4), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[1]->key, nodes[1]->value, 1, (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed), nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[4]->key, nodes[4]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W4SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW5Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[4]->key, nodes[4]->value, 1, (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[1]->key, nodes[1]->value, 1, (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed), nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[2]->key, nodes[2]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W5SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW6Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, 1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[4]->left.load(memory_order_relaxed));
    Node<K,V> *nodeXXRR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 3), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[1]->key, nodes[1]->value, 1, (Node<K,V>*) nodes[4]->right.load(memory_order_relaxed), nodeXXRR);
    int weight = nodes[1]->weight;
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[4]->key, nodes[4]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W6SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

template<class K, class V, class Compare, class MasterRecordMgr> bool Chromatic<K,V,Compare,MasterRecordMgr>::doW7Sym(const int tid, Node<K,V> **nodes, void **llxResults, bool fieldIsLeft) {
    Node<K,V> *nodeXXL = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 1), nodes[2]->key, nodes[2]->value, nodes[2]->weight-1, (Node<K,V>*) nodes[2]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[2]->right.load(memory_order_relaxed));
    Node<K,V> *nodeXXR = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 2), nodes[3]->key, nodes[3]->value, nodes[3]->weight-1, (Node<K,V>*) nodes[3]->left.load(memory_order_relaxed), (Node<K,V>*) nodes[3]->right.load(memory_order_relaxed));
    int weight = (IS_SENTINEL(nodes[1], nodes[0]) ? 1 : nodes[1]->weight+1); // root of old subtree is a sentinel
    Node<K,V> *nodeXX = initializeNode(tid, GET_ALLOCATED_NODE_PTR(tid, 0), nodes[1]->key, nodes[1]->value, weight, nodeXXL, nodeXXR);
    if (SCXAndEnterQuiescentState(tid, SCXRecord<K,V>::TYPE_W7SYM, nodes, llxResults, (fieldIsLeft ? &nodes[0]->left : &nodes[0]->right), nodeXX)) {
        return true;
    } else {
        return false;
    }
}

// THIS CAN ONLY BE INVOKED IN A QUIESCENT STATE.
// continues any scx that was started by this thread, and returns the result of that scx.
// if there was no scx started by this thread, this returns false.
// thus, a false return value could either represent an aborted scx, or no scx for this thread.
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::recoverAnyAttemptedSCX(const int tid, const int location) {
    assert(recordmgr->supportsCrashRecovery());
    SCXRecord<K,V> * const myscx = allocatedSCXRecord[tid*PREFETCH_SIZE_WORDS];
    if (recordmgr->isQProtected(tid, myscx)) {
        assert(recordmgr->isQuiescent(tid));
        const int operationType = myscx->type;
        Node<K,V> ** nodes = myscx->nodes;
        SCXRecord<K,V> ** scxRecordsSeen = myscx->scxRecordsSeen;
        const int n = NUM_OF_NODES[operationType];
        const int nFreeze = NUM_TO_FREEZE[operationType];
        for (int i=0;i<n;++i) {
            if (!recordmgr->isQProtected(tid, nodes[i])) {
                COUTATOMICTID("ERROR: nodes["<<i<<"] WAS NOT Q PROTECTED!!!!!!!"<<endl);
                assert(false); exit(-1);
            }
        }
        for (int i=0;i<nFreeze;++i) {
            if (!recordmgr->isQProtected(tid, scxRecordsSeen[i])) {
                COUTATOMICTID("ERROR: scxRecordsSeen["<<i<<"] WAS NOT Q PROTECTED!!!!!!!"<<endl);
                assert(false); exit(-1);
            }
        }
        // we started an scx using the scx record that was allocated for this operation,
        // so we must determine whether we have to complete it.

        // the remarkable thing here is that we don't need to
        // leave a Q state to complete our SCX!
        TRACE COUTATOMICTID("    finishing attempted scx."<<endl);
        const int state = help(tid, myscx, false);
        assert(recordmgr->isQuiescent(tid));
        bool result = reclaimMemoryAfterSCX(tid, operationType, nodes, scxRecordsSeen, state);
        recordmgr->qUnprotectAll(tid);
        return result;
    } else {
        recordmgr->qUnprotectAll(tid);
    }
    return false;
}

/* 2-bit state | 5-bit highest index reached | 24-bit frozen flags for each element of nodes[] on which a freezing CAS was performed = total 31 bits (highest bit unused) */
#define ABORT_STATE_INIT(i, flags) (SCXRecord<K,V>::STATE_ABORTED | ((i)<<2) | ((flags)<<7))
#define STATE_GET_FLAGS(state) ((state) & 0x7FFFFF80)
#define STATE_GET_HIGHEST_INDEX_REACHED(state) (((state) & 0x7C)>>2)
#define STATE_GET_WITH_FLAG_OFF(state, i) ((state) & ~(1<<(i+7)))
//#define STATE_GET_FLAG(state, i) ((state>>(i+7)) & 1)

// this internal function is called only by scx(), and only when otherSCX is protected by a call to recordmgr->protect
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::tryRetireSCXRecord(const int tid, SCXRecord<K,V> * const otherSCX, Node<K,V> * const node) {
    if (otherSCX == dummy) return false; // never retire the dummy scx record!
    if (otherSCX->state.load(memory_order_relaxed) == SCXRecord<K,V>::STATE_COMMITTED) {
        // in this tree, committed scx records are only pointed to by one node.
        // so, when this function is called, the scx record is already retired.
        recordmgr->retire(tid, otherSCX);
        return true;
    } else { // assert: scx->state >= STATE_ABORTED
        assert(otherSCX->state.load(memory_order_relaxed) >= 2); /* state >= aborted */
        // node->scxRecord no longer points to scx, so we set
        // the corresponding bit in scx->state to 0.
        // when state == ABORT_STATE_NO_FLAGS(state), scx is retired.
        const int n = NUM_TO_FREEZE[otherSCX->type];
        Node<K,V> ** const otherNodes = otherSCX->nodes;
        bool casSucceeded = false;
        int stateNew = -1;
        for (int i=0;i<n;++i) {
            if (otherNodes[i] == node) {
                while (!casSucceeded) {
                    TRACE COUTATOMICTID("attemping state CAS..."<<endl);
                    int stateOld = otherSCX->state.load(memory_order_relaxed);
                    stateNew = STATE_GET_WITH_FLAG_OFF(stateOld, i);
                    DEBUG assert(stateOld >= 2);
                    DEBUG assert(stateNew >= 2);
                    assert(stateNew < stateOld);
                    casSucceeded = otherSCX->state.compare_exchange_weak(stateOld, stateNew);       // MEMBAR ON X86/64
                }
                break;
            }
        }
        // many scxs can all be CASing state and trying to retire this node.
        // the one who gets to invoke retire() is the one whose CAS sets
        // the flag subfield of scx->state to 0.
        if (casSucceeded && STATE_GET_FLAGS(stateNew) == 0) {
            recordmgr->retire(tid, otherSCX);
            return true;
        }
    }
    return false;
}

// you may call this only in a quiescent state.
// the scx records in scxRecordsSeen must be protected (or we must know no one can have freed them--this is the case in this implementation).
// if this is being called from crash recovery, all nodes in nodes[] and the scx record must be Qprotected.
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::reclaimMemoryAfterSCX(
            const int tid,
            const int operationType,
            Node<K,V> **nodes,
            SCXRecord<K,V> * const * const scxRecordsSeen,
            const int state) {
    
    // NOW, WE ATTEMPT TO RECLAIM ANY RETIRED NODES AND SCX RECORDS
    // first, we determine how far we got in the loop in help()
    int highestIndexReached = (state == SCXRecord<K,V>::STATE_COMMITTED 
            ? NUM_TO_FREEZE[operationType]
            : STATE_GET_HIGHEST_INDEX_REACHED(state));
    assert(highestIndexReached>=0);
    assert(highestIndexReached<=MAX_NODES);
    
    SCXRecord<K,V> *debugSCXRecord = allocatedSCXRecord[tid*PREFETCH_SIZE_WORDS];
    
    if (highestIndexReached == 0) {
        assert(state == 2); /* aborted but only got to help() loop iteration 0 */
        // scx was never inserted into the data structure,
        // so we can reuse it for our next operation.
        return false; // aborted = failed, so return false
    } else {
        assert(highestIndexReached > 0);
        // now that we're in a quiescent state,
        // it's safe to perform non-restartable operations on bookkeeping data structures
        // (since no other thread will force us to restart in a quiescent state).

        // we wrote a pointer to newscxrecord into the data structure,
        // so we cannot reuse it immediately for our next operation.
        // instead, we allocate a new scx record for our next operation.
        assert(recordmgr->isQuiescent(tid));
        allocatedSCXRecord[tid*PREFETCH_SIZE_WORDS] = allocateSCXRecord(tid);

        // if the state was COMMITTED, then we cannot reuse the nodes the we
        // took from allocatedNodes[], either, so we must replace these nodes.
        // for the chromatic tree, the number of nodes can be found in
        // NUM_INSERTS[opreationType].
        // in general, we have to add a parameter, specified when you call SCX,
        // that says how large the replacement subtree of new nodes is.
        // alternatively, we could just move this out into the data structure code,
        // to be performed AFTER an scx completes.
        if (state == SCXRecord<K,V>::STATE_COMMITTED) {
            for (int i=0;i<NUM_INSERTED[operationType];++i) {
                REPLACE_ALLOCATED_NODE(tid, i);
            }
        }
        
        // consider the set of scx records for which we will invoke tryRetireSCXRecords,
        // in the following code block.
        // we don't need to call protect object for any of these scx records,
        // because none of them can be retired until we've invoked tryRetireSCXRecord!
        // this is because we changed pointers that pointed to each of these
        // scx records when we performed help(), above.
        // thus, we know they are not retired.
    
        // the scx records in scxRecordsSeen[] may now be retired
        // (since this scx changed each nodes[i]->scxRecord so that it does not
        //  point to any scx record in scxRecordsSeen[].)
        // we start at j=1 because nodes[0] may have been retired and freed
        // since we entered a quiescent state.
        // furthermore, we don't need to check if nodes[0]->left == NULL, since
        // we know nodes[0] is never a leaf.
        for (int j=0;j<highestIndexReached;++j) {
            DEBUG { // some debug invariant checking
                if (j>0) { // nodes[0] could be reclaimed already, so nodes[0]->left.load(...) is not safe
                    if (state == SCXRecord<K,V>::STATE_COMMITTED) {
                        // NOTE: THE FOLLOWING ACCESSES TO NODES[J]'S FIELDS ARE
                        //       ONLY SAFE BECAUSE STATE IS NOT ABORTED!
                        //       (also, we are the only one who can invoke retire[j],
                        //        and we have not done so yet.)
                        //       IF STATE == ABORTED, THE NODE MAY BE RECLAIMED ALREADY!
                        if ((Node<K,V>*) nodes[j]->left.load(memory_order_relaxed) == NULL) {
                            if (!((SCXRecord<K,V>*) nodes[j]->scxRecord.load(memory_order_relaxed) == dummy)) {
                                COUTATOMICTID("(SCXRecord<K,V>*) nodes["<<j<<"]->scxRecord.load(...)="<<((SCXRecord<K,V>*) nodes[j]->scxRecord.load(memory_order_relaxed))<<endl);
                                COUTATOMICTID("dummy="<<dummy<<endl);
                                assert(false);
                            }
                            if (scxRecordsSeen[j] != LLX_RETURN_IS_LEAF) {
                                if (nodes[j]) { COUTATOMICTID("nodes["<<j<<"]="<<*nodes[j]<<endl); }
                                else { COUTATOMICTID("nodes["<<j<<"]=NULL"<<endl); }
                                //if (newNode) { COUTATOMICTID("newNode="<<*newNode<<endl); }
                                //else { COUTATOMICTID("newNode=NULL"<<endl); }
                                COUTATOMICTID("scxRecordsSeen["<<j<<"]="<<scxRecordsSeen[j]<<endl);
                                COUTATOMICTID("dummy="<<dummy<<endl);
                                assert(false);
                            }
                        }
                    }
                } else {
                    assert(scxRecordsSeen[j] != LLX_RETURN_IS_LEAF);
                }
            }
            // if nodes[j] is not a leaf, then we froze it, changing the scx record
            // that nodes[j] points to. so, we try to retire the scx record is
            // no longer pointed to by nodes[j].
            // note: we know scxRecordsSeen[j] is not retired, since we have not
            //       zeroed out its flag representing an incoming pointer
            //       from nodes[j] until we execute tryRetireSCXRecord() below.
            //       (it follows that we don't need to invoke protect().)
            if (scxRecordsSeen[j] != LLX_RETURN_IS_LEAF) {
                bool success = tryRetireSCXRecord(tid, scxRecordsSeen[j], nodes[j]);
                DEBUG2 {
                    // note: tryRetireSCXRecord returns whether it retired an scx record.
                    //       this code checks that we don't retire the same scx record twice!
                    if (success && scxRecordsSeen[j] != dummy) {
                        for (int k=j+1;k<highestIndexReached;++k) {
                            assert(scxRecordsSeen[j] != scxRecordsSeen[k]);
                        }
                    }
                }
            }
        }
        SOFTWARE_BARRIER; // prevent compiler from moving retire() calls before tryRetireSCXRecord() calls above
        if (state == SCXRecord<K,V>::STATE_COMMITTED) {
            const int nNodes = NUM_OF_NODES[operationType];
            // nodes[1], nodes[2], ..., nodes[nNodes-1] are now retired
            for (int j=1;j<nNodes;++j) {
                DEBUG if (j < highestIndexReached) {
                    if ((void*) scxRecordsSeen[j] != LLX_RETURN_IS_LEAF) {
                        assert(nodes[j]->scxRecord.load(memory_order_relaxed) == (uintptr_t) debugSCXRecord);
                        assert(nodes[j]->marked.load(memory_order_relaxed));
                    }
                }
                recordmgr->retire(tid, nodes[j]);
            }
            return true; // committed = successful, so return true
        } else {
            assert(state >= 2); /* is >= SCXRecord<K,V>::STATE_ABORTED */
            return false;
        }
    }
}

// you may call this only if each node in nodes is protected by a call to recordmgr->protect
template<class K, class V, class Compare, class MasterRecordMgr>
bool Chromatic<K,V,Compare,MasterRecordMgr>::scx(
            const int tid,
            const int operationType,
            Node<K,V> **nodes,
            void **llxResults,
            atomic_uintptr_t *field,        // pointer to a "field pointer" that will be changed
            Node<K,V> *newNode) {
    TRACE COUTATOMICTID("scx(tid="<<tid<<" operationType="<<operationType<<")"<<endl);

    SCXRecord<K,V> *newscxrecord = allocatedSCXRecord[tid*PREFETCH_SIZE_WORDS];
    initializeSCXRecord(tid, newscxrecord, operationType, nodes, llxResults, field, newNode);
    
    // if this memory reclamation scheme supports crash recovery, it's important
    // that we protect the scx record and its nodes so we can help the scx complete
    // once we've recovered from the crash.
    if (recordmgr->supportsCrashRecovery()) {
        // it is important that initializeSCXRecord is performed before qProtect
        // because if we are neutralized, we use the fact that isQProtected = true
        // to decide that we should finish our scx, and the results will be bogus
        // if our scx record is not initialized properly.
        SOFTWARE_BARRIER;
        for (int i=0;i<NUM_OF_NODES[operationType];++i) {
            if (!recordmgr->qProtect(tid, nodes[i], callbackReturnTrue, NULL, false)) {
                assert(false);
                COUTATOMICTID("ERROR: failed to qProtect node"<<endl);
                exit(-1);
            }
        }
        for (int i=0;i<NUM_TO_FREEZE[operationType];++i) {
            if (!recordmgr->qProtect(tid, (SCXRecord<K,V>*) llxResults[i], callbackReturnTrue, NULL, false)) {
                assert(false);
                COUTATOMICTID("ERROR: failed to qProtect scx record in scxRecordsSeen / llxResults"<<endl);
                exit(-1);
            }
        }

        // it is important that we qprotect everything else before qprotecting our new
        // scx record, because the scx record is used to determine whether we should
        // help this scx once we've been neutralized and have restarted,
        // and helping requires the nodes to be protected.
        // (we know the scx record is qprotected before the first freezing cas,
        //  so we know that no pointer to the scx record has been written to the 
        //  data structure if it is not qprotected when we execute the crash handler.)
        SOFTWARE_BARRIER;
        if (!recordmgr->qProtect(tid, newscxrecord, callbackReturnTrue, NULL, false)) {
            COUTATOMICTID("ERROR: failed to qProtect scx record"<<endl);
            assert(false); exit(-1);
        }
        // memory barriers are not needed for these qProtect() calls on x86/64
        // because there's no write-write reordering, and nothing can be
        // reordered over the first freezing CAS in help().
        
    // if we don't have crash recovery, then we only need to protect our scx
    // record, so that it's not retired and freed out from under us by someone
    // who helps us.
    } else {
        SOFTWARE_BARRIER;
        if (!recordmgr->protect(tid, newscxrecord, callbackReturnTrue, NULL, false)) {
            COUTATOMICTID("ERROR: failed to protect scx record"<<endl);
            assert(false); exit(-1);
        }
        // no membar is needed for this protect call,
        // because newscxrecord is not inserted into the data structure
        // (and, hence, cannot be retired),
        // until the first freezing CAS in help().
        // since this freezing CAS implies a membar on x86/64, we don't need
        // one here to make sure newscxrecord is protected before it is retired.
    }
    SOFTWARE_BARRIER;
    int state = help(tid, newscxrecord, false);
    recordmgr->endOp(tid);
    bool result = reclaimMemoryAfterSCX(tid, operationType, nodes, (SCXRecord<K,V> * const * const) llxResults, state);
    recordmgr->qUnprotectAll(tid);
    return result;
}

// you may call this only if scx is protected by a call to recordmgr->protect.
// each node in scx->nodes must be protected by a call to recordmgr->protect.
// returns the state field of the scx record "scx."
template<class K, class V, class Compare, class MasterRecordMgr>
int Chromatic<K,V,Compare,MasterRecordMgr>::help(const int tid, SCXRecord<K,V> *scx, bool helpingOther) {
    assert(recordmgr->isProtected(tid, scx));
    assert(scx != dummy);
//    bool updateCAS = false;
    const int type                          = scx->type;
    const int nFreeze                       = NUM_TO_FREEZE[type];
    Node<K,V> ** const nodes                = scx->nodes;
    SCXRecord<K,V> ** const scxRecordsSeen  = scx->scxRecordsSeen;
    Node<K,V> * const newNode               = scx->newNode;
    TRACE COUTATOMICTID("help(tid="<<tid<<" scx="<<*scx<<" helpingOther="<<helpingOther<<"), nFreeze="<<nFreeze<<endl);
    //SOFTWARE_BARRIER; // prevent compiler from reordering read(state) before read(nodes), read(scxRecordsSeen), read(newNode). an x86/64 cpu will not reorder these reads.
    int __state = scx->state.load(memory_order_relaxed);
    if (__state != SCXRecord<K,V>::STATE_INPROGRESS) {
        TRACE COUTATOMICTID("help return 0 after state != in progress"<<endl);
        return __state;
    }
    // note: the above cannot cause us to leak the memory allocated for scx,
    // since, if !helpingOther, then we created the SCX record,
    // and did not write it into the data structure.
    // so, no one could have helped us, and state must be INPROGRESS.
    
    DEBUG {
        for (int i=0;i<NUM_OF_NODES[type];++i) {
            assert(nodes[i] == root || recordmgr->isProtected(tid, nodes[i]));
        }
    }
    
    // a note about reclaiming SCX records:
    // IN THEORY, there are exactly three cases in which an SCX record passed
    // to help() is not in the data structure and can be retired.
    //    1. help was invoked directly by SCX, and it failed its first
    //       CAS. in this case the SCX record can be immediately freed.
    //    2. a pointer to an SCX record U with state == COMMITTED is
    //       changed by a CAS to point to a different SCX record.
    //       in this case, the SCX record is retired, but cannot
    //       immediately be freed.
    //     - intuitively, we can retire it because,
    //       after the SCX that created U commits, only the node whose
    //       pointer was changed still points to U. so, when a pointer
    //       that points to U is changed, U is no longer pointed to by
    //       any node in the tree.
    //     - however, a helper or searching process might still have
    //       a local pointer to U, or a local pointer to a
    //       retired node that still points to U.
    //     - so, U can only be freed safely after no process has a
    //       pointer to a retired node that points to U.
    //     - in other words, U can be freed only when all retired nodes
    //       that point to it can be freed.
    //     - if U is retired when case 2 occurs, then it will be retired
    //       AFTER all nodes that point to it are retired. thus, it will
    //       be freed at the same time as, or after, those nodes.
    //    3. a pointer to an SCX record U with state == ABORTED is
    //       changed by a CAS to point to a different SCX record.
    //       this is the hard case, because several nodes in the tree may
    //       point to U.
    //     - in this case, we store the number of pointers from nodes in the
    //       tree to this SCX record in the state field of this SCX record.
    // [NOTE: THE FOLLOWING THREE BULLET POINTS ARE FOR AN OLD IDEA;
    //  THE CURRENT IDEA IS SLIGHTLY DIFFERENT.]    
    //     - when the state of an SCX record becomes STATE_ABORTED, we store
    //       STATE_ABORTED + i in the state field, where i is the number of
    //       incoming pointers from nodes in the tree. (STATE_INPROGRESS and
    //       STATE_COMMITTED are both less than STATE_ABORTED.)
    //     - every time we change a pointer from an SCX record U to another
    //       SCX record U', and U.state > STATE_ABORTED, we decrement U.state.
    //     - if U.state == STATE_ABORTED, then we know there are no incoming
    //       pointers to U from nodes in the tree, so we can retire U.
    //
    // HOWEVER, in practice, we don't freeze leaves for insert and delete,
    // so we have to be careful to deal with a possible memory leak.
    // if some operations (e.g., rebalancing steps) DO freeze leaves, then
    // we can wind up in a situation where a rebalancing step freezes a leaf
    // and is aborted, then a successful insertion or deletion retires
    // that leaf without freezing it. in this scenario, the scx record
    // for the rebalancing step will never be retired, since no further
    // freezing CAS will modify it's scx record pointer (which means it will
    // never trigger case 3, above).
    // there are three (easy) possible fixes for this problem.
    //   1. make sure all operations freeze leaves
    //   2. make sure no operation freezes leaves
    //   3. when retiring a node, if it points to an scx record with
    //      state aborted, then respond as if we were in case 3, above.
    //      (note: since the dummy scx record has state ABORTED,
    //       we have to be a little bit careful; we ignore the dummy.)
    // in this implementation, we choose option 2. this is viable because
    // leaves are immutable, and, hence, do not need to be frozen.
    
    // freeze sub-tree
    int flags = 1; // bit i is 1 if nodes[i] is not a leaf, and 0 otherwise.
    // note that flags bit 0 is always set, since nodes[0] is never a leaf.
    // (technically, if we abort in the first iteration,
    //  flags=1 makes no sense (since it suggests there is one pointer to scx
    //  from a node in the tree), but in this case we ignore the flags variable.)
    for (int i=helpingOther; i<nFreeze; ++i) {
        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) {
            TRACE COUTATOMICTID("nodes["<<i<<"] is a leaf");
            assert(i > 0); // nodes[0] cannot be a leaf...
            continue; // do not freeze leaves
        }
        
        uintptr_t exp = (uintptr_t) scxRecordsSeen[i];
        bool successfulCAS = nodes[i]->scxRecord.compare_exchange_strong(exp, (uintptr_t) scx);     // MEMBAR ON X86/64
        
        if (!successfulCAS && (SCXRecord<K,V>*) exp != scx) { // if work was not done
            if (scx->allFrozen.load(memory_order_relaxed)) {
                assert(scx->state.load(memory_order_relaxed) == 1); /*STATE_COMMITTED*/
                TRACE COUTATOMICTID("help return COMMITTED after failed freezing cas on nodes["<<i<<"]"<<endl);
                return SCXRecord<K,V>::STATE_COMMITTED; // success
            } else {
                if (i == 0) {
                    // if i == 0, then our scx record was never in the tree, and,
                    // consequently, no one else can have a pointer to it.
                    // so, there is no need to change scx->state.
                    // (recall that helpers start with helpingOther == true,
                    //  so i>0 for every helper. thus, if and only if i==0,
                    //  we created this scx record and failed our first CAS.)
                    assert(!helpingOther);
                    TRACE COUTATOMICTID("help return ABORTED after failed freezing cas on nodes["<<i<<"]"<<endl);
                    return ABORT_STATE_INIT(0, 0); // scx is aborted (but no one else will ever know)
                } else {
                    // if this is the first failed freezing CAS to occur for this SCX,
                    // then flags encodes the pointers to this scx record from nodes IN the tree.
                    // (the following CAS will succeed only the first time it is performed
                    //  by any thread running help() for this scx.)
                    int expectedState = SCXRecord<K,V>::STATE_INPROGRESS;
                    int newState = ABORT_STATE_INIT(i, flags);
                    bool success = scx->state.compare_exchange_strong(expectedState, newState);     // MEMBAR ON X86/64
                    assert(expectedState != 1); /* not committed */
                    // note2: a regular write will not do, here, since two people can start helping, one can abort at i>0, then after a long time, the other can fail to CAS i=0, so they can get different i values.
                    assert(scx->state >= 2); /* SCXRecord<K,V>::STATE_ABORTED */
                    // ABORTED THE SCX AFTER PERFORMING ONE OR MORE SUCCESSFUL FREEZING CASs
                    if (success) {
                        TRACE COUTATOMICTID("help return ABORTED(changed to "<<newState<<") after failed freezing cas on nodes["<<i<<"]"<<endl);
                        return newState;
                    } else {
                        TRACE COUTATOMICTID("help return ABORTED(failed to change to "<<newState<<" because encountered "<<expectedState<<" instead of in progress) after failed freezing cas on nodes["<<i<<"]"<<endl);
                        return expectedState; // this has been overwritten by compare_exchange_strong with the value that caused the CAS to fail.
                    }
                }
            }
        } else {
            flags |= (1<<i); // nodes[i] was frozen for scx
            assert((SCXRecord<K,V>*) exp == scx || ((SCXRecord<K,V> *) exp)->state != 0); // not in progress
        }
    }
    scx->allFrozen.store(true, memory_order_relaxed);
    // note: i think the sequential consistency memory model is not actually needed here...
    // why? in an execution where no reads are moved before allFrozen by the
    // compiler/cpu (because we added a barrier here), any process that sees
    // allFrozen = true has also just seen that nodes[i]->op != &op,
    // which means that the operation it is helping has already completed!
    // in particular, the child CAS will already have been done, which implies
    // that allFrozen will have been set to true, since the compiler/cpu cannot
    // move the (first) child CAS before the (first) write to allFrozen.
    SOFTWARE_BARRIER;
    for (int i=1; i<nFreeze; ++i) {
        if (scxRecordsSeen[i] == LLX_RETURN_IS_LEAF) continue; // do not mark leaves
        assert(recordmgr->isProtected(tid, scx));
        assert(nodes[i] == root || recordmgr->isProtected(tid, nodes[i]));
        nodes[i]->marked.store(true, memory_order_relaxed); // finalize all but first node
    }

    // CAS in the new sub-tree (update CAS)
    uintptr_t expected = (uintptr_t) nodes[1];
    assert(nodes[1] == root || recordmgr->isProtected(tid, nodes[1]));
    scx->field->compare_exchange_strong(expected, (uintptr_t) newNode);                             // MEMBAR ON X86/64
    assert(scx->state.load(memory_order_relaxed) < 2); // not aborted
    scx->state.store(SCXRecord<K,V>::STATE_COMMITTED, memory_order_relaxed);
    
    TRACE COUTATOMICTID("help return COMMITTED after performing update cas"<<endl);
    return SCXRecord<K,V>::STATE_COMMITTED; // success
}

// you may call this only if node is protected by a call to recordmgr->protect
template<class K, class V, class Compare, class MasterRecordMgr>
void *Chromatic<K,V,Compare,MasterRecordMgr>::llx(
            const int tid,
            Node<K,V> *node,
            Node<K,V> **retLeft,
            Node<K,V> **retRight) {
    TRACE COUTATOMICTID("llx(tid="<<tid<<", node="<<*node<<")"<<endl);
    assert(node == root || recordmgr->isProtected(tid, node));
    Chromatic_retired_info info;
    SCXRecord<K,V> *scx1 = (SCXRecord<K,V>*) node->scxRecord.load(memory_order_relaxed);
    IF_FAIL_TO_PROTECT_SCX(info, tid, scx1, &node->scxRecord, &node->marked) {
        TRACE COUTATOMICTID("llx return1 (tid="<<tid<<" key="<<node->key<<")\n");
        // DEBUG ++counters[tid]->llxFail;
        return NULL;
    } // return and retry
    assert(scx1 == dummy || recordmgr->isProtected(tid, scx1));
    int state = scx1->state.load(memory_order_relaxed);
    SOFTWARE_BARRIER;       // prevent compiler from moving the read of marked before the read of state (no hw barrier needed on x86/64, since there is no read-read reordering)
    bool marked = node->marked.load(memory_order_relaxed);
    SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
    if ((state == SCXRecord<K,V>::STATE_COMMITTED && !marked) || state >= SCXRecord<K,V>::STATE_ABORTED) {
        SOFTWARE_BARRIER;       // prevent compiler from moving the reads scx2=node->scxRecord or scx3=node->scxRecord before the read of marked. (no h/w barrier needed on x86/64 since there is no read-read reordering)
        *retLeft = (Node<K,V>*) node->left.load(memory_order_relaxed);
        *retRight = (Node<K,V>*) node->right.load(memory_order_relaxed);
        if (*retLeft == NULL) {
            TRACE COUTATOMICTID("llx return2.a (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n"); 
            return (void*) LLX_RETURN_IS_LEAF;
        }
        SOFTWARE_BARRIER; // prevent compiler from moving the read of node->scxRecord before the read of left or right
        SCXRecord<K,V> *scx2 = (SCXRecord<K,V>*) node->scxRecord.load(memory_order_relaxed);
        if (scx1 == scx2) {
            DEBUG {
                if (marked && state >= SCXRecord<K,V>::STATE_ABORTED) {
                    // since scx1 == scx2, the two claims in the antecedent hold simultaneously.
                    assert(scx1 == dummy || recordmgr->isProtected(tid, scx1));
                    assert(node == root || recordmgr->isProtected(tid, node));
                    assert(node->marked.load(memory_order_relaxed));
                    assert(scx1->state.load(memory_order_relaxed) >= 2 /* aborted */);
                    assert(false);
                }
            }
            TRACE COUTATOMICTID("llx return2 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<" scx1="<<scx1<<")\n"); 
            // DEBUG ++counters[tid]->llxSuccess;
            // on x86/64, we do not need any memory barrier here to prevent mutable fields of node from being moved before our read of scx1, because the hardware does not perform read-read reordering. on another platform, we would need to ensure no read from after this point is reordered before this point (technically, before the read that becomes scx1)...
            return scx1;    // success
        } else {
            DEBUG {
                IF_FAIL_TO_PROTECT_SCX(info, tid, scx2, &node->scxRecord, &node->marked) {
                    TRACE COUTATOMICTID("llx return1.b (tid="<<tid<<" key="<<node->key<<")\n");
                    // DEBUG ++counters[tid]->llxFail;
                    return NULL;
                } else {
                    assert(scx1 == dummy || recordmgr->isProtected(tid, scx1));
                    assert(recordmgr->isProtected(tid, scx2));
                    assert(node == root || recordmgr->isProtected(tid, node));
                    int __state = scx2->state.load(memory_order_relaxed);
                    SCXRecord<K,V>* __scx = (SCXRecord<K,V>*) node->scxRecord.load(memory_order_relaxed);
                    if ((marked && __state >= 2 && __scx == scx2)) {
                        COUTATOMICTID("ERROR: marked && state aborted! raising signal SIGTERM..."<<endl);
                        COUTATOMICTID("node      = "<<*node<<endl);
                        COUTATOMICTID("scx2      = "<<*scx2<<endl);
                        COUTATOMICTID("state     = "<<state<<" bits="<<bitset<32>(state)<<endl);
                        COUTATOMICTID("marked    = "<<marked<<endl);
                        COUTATOMICTID("__state   = "<<__state<<" bits="<<bitset<32>(__state)<<endl);
                        assert(node->marked.load(memory_order_relaxed));
                        assert(scx2->state.load(memory_order_relaxed) >= 2 /* aborted */);
                        raise(SIGTERM);
                    }
                }
            }
            if (recordmgr->shouldHelp()) {
                IF_FAIL_TO_PROTECT_SCX(info, tid, scx2, &node->scxRecord, &node->marked) {
                    TRACE COUTATOMICTID("llx return3 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n");
                    // DEBUG ++counters[tid]->llxFail;
                    return NULL;
                } // return and retry
                assert(scx2 != dummy);
                assert(recordmgr->isProtected(tid, scx2));
                TRACE COUTATOMICTID("llx help 1 tid="<<tid<<endl);
                help(tid, scx2, true);
            }
        }
    } else if (state == SCXRecord<K,V>::STATE_INPROGRESS) {
        if (recordmgr->shouldHelp()) {
            assert(scx1 != dummy);
            assert(recordmgr->isProtected(tid, scx1));
            TRACE COUTATOMICTID("llx help 2 tid="<<tid<<endl);
            help(tid, scx1, true);
        }
    } else {
        // state committed and marked
        assert(state == 1); /* SCXRecord<K,V>::STATE_COMMITTED */
        assert(marked);
        if (recordmgr->shouldHelp()) {
            SCXRecord<K,V> *scx3 = (SCXRecord<K,V>*) node->scxRecord.load(memory_order_relaxed);
            IF_FAIL_TO_PROTECT_SCX(info, tid, scx3, &node->scxRecord, &node->marked) {
                TRACE COUTATOMICTID("llx return4 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n");
                // DEBUG ++counters[tid]->llxFail;
                return NULL;
            } // return and retry
            assert(scx3 != dummy);
            assert(recordmgr->isProtected(tid, scx3));
            TRACE COUTATOMICTID("llx help 3 tid="<<tid<<endl);
            help(tid, scx3, true);
        } else {
        }
    }
    TRACE COUTATOMICTID("llx return5 (tid="<<tid<<" state="<<state<<" marked="<<marked<<" key="<<node->key<<")\n");
    // DEBUG ++counters[tid]->llxFail;
    return NULL;            // fail
}

#endif	/* CHROMATIC_IMPL_H */

