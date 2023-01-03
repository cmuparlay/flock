/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ca_tree.h
 * Author: ginns
 *
 * Created on April 7, 2021, 5:05 PM
 */

#ifndef CA_TREE_H
#define CA_TREE_H

#include "base_node.h"
#include "util.h"
#include <queue>        /* used in BFS */
#include "avl_tree.h"
// #include "linkedlist.h"
// #include "redblack_tree.h"

#define DEBUG_PRINT      0

template <class RecordManager, typename K, typename V>
class CATree {
private:
    K minKey;
    K maxKey;
    K numThreads;
    volatile char padding0[PADDING_BYTES];
    RecordManager* const recmgr;
    volatile char padding1[PADDING_BYTES];
    CA_Node* volatile root;
    volatile char padding2[PADDING_BYTES];

    /* Contention Adapting */
    void highContentionSplit(const int tid, BaseNodePtr baseNode);
    void lowContentionJoin(const int tid, BaseNodePtr baseNode);
    void adaptIfNeeded(const int tid, BaseNodePtr baseNode);
    
    /* Helpers */
    BaseNodePtr getBaseNode(const K & key);
    BaseNodePtr leftmostBaseNode(CA_Node * node);
    BaseNodePtr rightmostBaseNode(CA_Node * node);
    RouteNodePtr parentOf(RouteNodePtr node);
    
    long calcSubtreeKeySum(CA_Node * node);

    void freeSubtree(const int tid, CA_Node* node);
    
public:
    CATree(int totalThreads, K minKey, K maxKey, OrderedSetType type);
    ~CATree();
    
    /* Set Operations */
    V find(const int tid, const K & key);
    V insert(const int tid, const K & key, const V& val);
    V erase(const int tid, const K & key);
    
    void printDebuggingDetails();
    long getSumOfKeys();
    CA_Node* getRoot();

    void initThread(const int tid);
    void deinitThread(const int tid);
    
};

template <class RecordManager, typename K, typename V>
CATree<RecordManager, K, V>::CATree(int _numThreads, K _minKey, K _maxKey, OrderedSetType type): 
    numThreads(_numThreads), minKey(_minKey), maxKey(_maxKey), recmgr(new RecordManager(numThreads)) {
    const int tid = 0;
    initThread(tid);
    BaseNodePtr baseRoot = recmgr->template allocate<BaseNode<K, V>>(tid);
    IOSet * set;
    switch ( type ) {
        case OrderedSetType::AVL:
            set = new AVLTree<K, V>();
            // cout << "using AVL TREE" << endl;
            break;
        // case OrderedSetType::LINKEDLIST:
        //     set = new LinkedList<K, V>();
        //     cout << "using LINKED LIST" << endl;
        //     break;
        // case OrderedSetType::REDBLACK:
        //     set = new RedBlackTree<K, V>();
        //     cout << "using REDBLACK TREE" << endl;
        //     break;
        default:
            assert(false);
            break;
    }
    baseRoot->setOrderedSet(set);
    root = baseRoot;
}

template <class RecordManager, typename K, typename V>
CATree<RecordManager, K, V>::~CATree() {
    const int tid = 0;
    initThread(tid);
    freeSubtree(tid, root);
    deinitThread(tid);
    delete recmgr;
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::freeSubtree(const int tid, CA_Node* node) {
    if (node == nullptr) return;
    if (!node->isBaseNode) {
        RouteNodePtr r = (RouteNodePtr) node;
        freeSubtree(tid, r->getLeft());
        freeSubtree(tid, r->getRight());
        recmgr->deallocate(tid, r);
    }
    else {
        recmgr->deallocate(tid, (BaseNodePtr)node);
    }
}

template <class RecordManager, typename K, typename V>
BaseNodePtr CATree<RecordManager, K, V>::getBaseNode(const K & key) {
    CA_Node * currNode = root;
    while (!currNode->isBaseNode) {
        RouteNodePtr currNodeR = (RouteNodePtr) currNode;
        if ( key < currNodeR->getKey() ) {
            currNode = currNodeR->getLeft();
        }
        else {
            currNode = currNodeR->getRight();
        }
    }
    return (BaseNodePtr)currNode;
}

template <class RecordManager, typename K, typename V>
BaseNodePtr CATree<RecordManager, K, V>::leftmostBaseNode(CA_Node * node) {
    CA_Node * currNode = node;
    while (!currNode->isBaseNode) {
        currNode = ((RouteNodePtr)currNode)->getLeft();
    }
    /* Drop out when we reach base node, cast and return */
    return (BaseNodePtr)currNode;
}

template <class RecordManager, typename K, typename V>
BaseNodePtr CATree<RecordManager, K, V>::rightmostBaseNode(CA_Node* node) {
    CA_Node * currNode = node;
    while (!currNode->isBaseNode) {
        currNode = ((RouteNodePtr)currNode)->getRight();
    }
    /* Drop out when we reach base node, cast and return */
    return (BaseNodePtr)currNode;
}

template <class RecordManager, typename K, typename V>
RouteNodePtr CATree<RecordManager, K, V>::parentOf(RouteNodePtr node) {
    RouteNodePtr prevNode = nullptr;
    assert(!root->isBaseNode);
    RouteNodePtr currNode = (RouteNodePtr) root;
    K targetKey = node->getKey();
    while (currNode != node) {
        prevNode = currNode;
        if (targetKey < currNode->getKey() )
            currNode = (RouteNodePtr) currNode->getLeft();
        else
            currNode = (RouteNodePtr) currNode->getRight();
    }

    return prevNode;
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::adaptIfNeeded(const int tid, BaseNodePtr baseNode) {
    if (baseNode->isHighContentionLimitReached()) {
        TRACE TPRINT("Attempting high contention split");
        highContentionSplit(tid, baseNode);
    }
    else if (baseNode->isLowContentionLimitReached()) {
        TRACE TPRINT("Attempting low contention join");
        lowContentionJoin(tid, baseNode);
    }
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::lowContentionJoin(const int tid, BaseNodePtr baseNode) {
    RouteNodePtr parent = baseNode->getParent();
    if (parent == NULL) {
        baseNode->resetStatistics();
    }
    else if (parent->getLeft() == baseNode) {
        BaseNodePtr neighborBase = leftmostBaseNode( parent->getRight() );
        if ( !neighborBase->tryLock(tid) ){
            baseNode->resetStatistics();
            return;
        }
        else if ( !neighborBase->isValid(tid) ) {
            neighborBase->unlock(tid);
            baseNode->resetStatistics();
            return;
        }
        else {
            IOSet * baseSet = baseNode->getOrderedSet();
            IOSet * neighborSet = neighborBase->getOrderedSet();

#if DEBUG_PRINT            
            cout << "[JOIN]";
            cout << "Base Set: ";
            baseSet->printKeys();
            cout << "Neightbor Set: ";
            neighborSet->printKeys();
#endif
            IOSet * joinedSet = baseSet->join(tid, neighborSet);
                        
            BaseNodePtr newBase = recmgr->template allocate<BaseNode<K, V>>(tid);
            newBase->setOrderedSet(joinedSet);
            parent->lock(tid);
            RouteNodePtr gparent = NULL;
            
            /* Unlink parent route node */
            do {
                if (gparent != NULL) {
                    gparent->unlock(tid);
                }
                gparent = parentOf(parent);
                if (gparent != NULL) {
                    gparent->lock(tid);
                }
            } while (gparent != NULL && !gparent->isValid(tid));
            if (gparent == NULL) {
                root = parent->getRight();
            }
            else if ( gparent->getLeft() == parent) {
                gparent->setLeft( parent->getRight() ); 
            }
            else {
                gparent->setRight( parent->getRight() );
            }
            
            parent->invalidate(tid);
            parent->unlock(tid);
            if (gparent != NULL) {
                gparent->unlock(tid);
            }
            
            /* Link new base node */
            RouteNodePtr neighborBaseParent = NULL;
            if (parent->getRight() == neighborBase) {
                neighborBaseParent = gparent;
            }
            else  {
                neighborBaseParent = neighborBase->getParent(); 
            }
            newBase->setParent(neighborBaseParent);
            if ( neighborBaseParent == NULL ) {
                root = newBase;
            }
            else if ( neighborBaseParent->getLeft() == neighborBase ) {
                neighborBaseParent->setLeft(newBase);
            }
            else {
                neighborBaseParent->setRight(newBase);
            }
            neighborBase->invalidate(tid);
            neighborBase->unlock(tid);
            baseNode->invalidate(tid);

            recmgr->retire(tid, baseNode);
            recmgr->retire(tid, neighborBase);
            recmgr->retire(tid, parent);
#if DEBUG_PRINT             
            cout << "Joined Set: ";
            joinedSet->printKeys();

            TRACE TPRINT("Executed low contention join");
#endif
        }
    }
    else { /* Symmetric case */
        BaseNodePtr neighborBase = rightmostBaseNode( parent->getLeft() );
        if ( !neighborBase->tryLock(tid) ){
            baseNode->resetStatistics();
            return;
        }
        else if ( !neighborBase->isValid(tid) ) {
            neighborBase->unlock(tid);
            baseNode->resetStatistics();
            return;
        }
        else {
            IOSet * baseSet = baseNode->getOrderedSet();
            IOSet * neighborSet = neighborBase->getOrderedSet();
#if DEBUG_PRINT              
            cout << "[JOIN]";
            cout << "Base Set: ";
            baseSet->printKeys();
            cout << "Neightbor Set: ";
            neighborSet->printKeys();
#endif            
            IOSet * joinedSet = neighborSet->join(tid, baseSet);
            BaseNodePtr newBase = recmgr->template allocate<BaseNode<K, V>>(tid);
            newBase->setOrderedSet(joinedSet);
            parent->lock(tid);
            RouteNodePtr gparent = NULL;
            
             /* Unlink parent route node */
            do {
                if (gparent != NULL) {
                    gparent->unlock(tid);
                }
                gparent = parentOf(parent);
                if (gparent != NULL) {
                    gparent->lock(tid);
                }
            } while (gparent != NULL && !gparent->isValid(tid));
            if (gparent == NULL) {
                root = parent->getLeft();
            }
            else if ( gparent->getLeft() == parent) {
                gparent->setLeft( parent->getLeft() ); 
            }
            else {
                gparent->setRight( parent->getLeft() );
            }
            
            parent->invalidate(tid);
            parent->unlock(tid);
            if (gparent != NULL) {
                gparent->unlock(tid);
            }
            
            /* Link new base node */
            RouteNodePtr neighborBaseParent = NULL;
            if (parent->getLeft() == neighborBase) {
                neighborBaseParent = gparent;
            }
            else  {
                neighborBaseParent = neighborBase->getParent(); 
            }
            newBase->setParent(neighborBaseParent);
            if ( neighborBaseParent == NULL ) {
                root = newBase;
            }
            else if ( neighborBaseParent->getLeft() == neighborBase ) {
                neighborBaseParent->setLeft(newBase);
            }
            else {
                neighborBaseParent->setRight(newBase);
            }
            neighborBase->invalidate(tid);
            neighborBase->unlock(tid);
            baseNode->invalidate(tid);

            recmgr->retire(tid, baseNode);
            recmgr->retire(tid, neighborBase);
            recmgr->retire(tid, parent);
#if DEBUG_PRINT              
            cout << "Joined Set: ";
            joinedSet->printKeys();
            
            TRACE TPRINT("Executed low contention join");
#endif
        }
    }
} // end lowContentionJoin

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::highContentionSplit(int tid, BaseNodePtr baseNode) {
    RouteNodePtr parent = baseNode->getParent();
    IOSet * baseSet = baseNode->getOrderedSet();
    
#if DEBUG_PRINT
    cout << "[SPLIT]";
    cout << "Original List: ";
    baseSet->printKeys();
#endif
    
    std::tuple<K, IOSet *, IOSet *> tuple = baseSet->split(tid);
    K splitKey = std::get<0>(tuple);
    IOSet * leftSet = std::get<1>(tuple);
    IOSet * rightSet = std::get<2>(tuple);
    
    if (leftSet == nullptr) {
        /* Occurs when split is not possible (e.g. one node set) */
        baseNode->resetStatistics();
        return;
    }

    BaseNodePtr newLeftBase = recmgr->template allocate<BaseNode<K, V>>(tid);
    BaseNodePtr newRightBase = recmgr->template allocate<BaseNode<K, V>>(tid);
    newLeftBase->setOrderedSet(leftSet);
    newRightBase->setOrderedSet(rightSet);
    
#if DEBUG_PRINT
    cout << "Left List: ";
    leftSet->printKeys();
    cout << "Right List: ";
    rightSet->printKeys();
#endif
    RouteNodePtr newRoute = recmgr->template allocate<RouteNode<K, V>>(tid);
    newRoute->setKey(splitKey);
    newRoute->setLeft(newLeftBase);
    newRoute->setRight(newRightBase);
    newLeftBase->setParent(newRoute);
    newRightBase->setParent(newRoute);
    
    
    if ( parent == NULL ) {
        root = newRoute;
    }
    else {
        if ( parent->getLeft() == baseNode ) {
            parent->setLeft(newRoute);
        }
        else {
            parent->setRight(newRoute);
        }
    }
    baseNode->invalidate(tid);
    recmgr->retire(tid, baseNode);
    
#if DEBUG_PRINT
    TRACE TPRINT("Executed high contention split, splitKey: " << splitKey);   
#endif
} // end highContentionSplit


/**
 * Public Methods
 */
template <class RecordManager, typename K, typename V>
V CATree<RecordManager, K, V>::find(const int tid, const K & key) {
    while (true) {
        auto guard = recmgr->getGuard(tid, true);
        BaseNodePtr baseNode = getBaseNode(key);
        baseNode->lock(tid);
        
        if (baseNode->isValid(tid) == false) {
            baseNode->unlock(tid);
            continue;
        }
        IOSet * set = baseNode->getOrderedSet();
        V result = set->find(tid, key);
        adaptIfNeeded(tid, baseNode);
        baseNode->unlock(tid);
        return result;
    }
}

template <class RecordManager, typename K, typename V>
V CATree<RecordManager, K, V>::insert(const int tid, const K & key, const V & val) {
    assert( (key >= minKey) && (key <= maxKey) );
    
    while (true) {
        auto guard = recmgr->getGuard(tid, true);
        BaseNodePtr baseNode = getBaseNode(key);
        baseNode->lock(tid);
        
        if (baseNode->isValid(tid) == false) {
            baseNode->unlock(tid);
            continue;
        }
        IOSet * set = baseNode->getOrderedSet();
        V result = set->insert(tid, key, val);
        adaptIfNeeded(tid, baseNode);
        baseNode->unlock(tid);
        return result;
    }
}

template <class RecordManager, typename K, typename V>
V CATree<RecordManager, K, V>::erase(const int tid, const K & key) {
    assert( (key >= minKey) && (key <= maxKey) );
    while (true) {
        auto guard = recmgr->getGuard(tid, true);
        BaseNodePtr baseNode = getBaseNode(key);
        baseNode->lock(tid);
        
        if (baseNode->isValid(tid) == false) {
            baseNode->unlock(tid);
            continue;
        }
        IOSet * set = baseNode->getOrderedSet();
        V result = set->erase(tid, key);
        adaptIfNeeded(tid, baseNode);
        baseNode->unlock(tid);
        return result;
    }
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::printDebuggingDetails() {
    /* Tree walk the CA Tree and count the number of Base Nodes and Route Nodes */
    // int numBaseNodes = 0;
    // int numRouteNodes = 0;
    // queue<CA_Node* volatile> q;
    // q.push(root);
    // while ( !q.empty() ) {
    //     CA_Node * curr = q.front();
    //     q.pop();
    //     if (curr->isBaseNode) {
    //         /* curr is a base node */
    //         numBaseNodes++;
    //     }
    //     else {
    //         /* curr is a route node */
    //         numRouteNodes++;
    //         RouteNodePtr routeNode = (RouteNodePtr)curr;
    //         if ( routeNode->getLeft() != NULL )
    //             q.push(routeNode->getLeft());
            
    //         if ( routeNode->getRight() != NULL )
    //             q.push(routeNode->getRight());
    //     }
    // }
    
    // cout << "Number of base nodes: " << numBaseNodes << endl;
    // cout << "Number of route nodes: " << numRouteNodes << endl;
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::initThread(const int tid) {
    recmgr->initThread(tid);
}

template <class RecordManager, typename K, typename V>
void CATree<RecordManager, K, V>::deinitThread(const int tid) {
    recmgr->deinitThread(tid);
}

template <class RecordManager, typename K, typename V>
CA_Node* CATree<RecordManager, K, V>::getRoot() {
    return root;
}

#endif /* CA_TREE_H */

