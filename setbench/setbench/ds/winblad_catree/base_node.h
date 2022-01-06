/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   base_node.h
 * Author: ginns
 *
 * Created on April 7, 2021, 4:47 PM
 */

#ifndef BASE_NODE_H
#define BASE_NODE_H

#include "route_node.h"
#include "avl_tree.h"

#define BaseNodePtr BaseNode<K, V>*

template <typename K, typename V>
class BaseNode : public CA_Node {
private:
    IOSet * set; 
    
    /* For statistic locking */
    std::mutex m;
    int statLockStatistics;
    volatile int owner = -1;
    volatile bool valid;

    RouteNodePtr volatile parent;

    /* Contention heuristics */
    static const int STAT_LOCK_HIGH_CONTENTION_LIMIT = 1000;
    static const int STAT_LOCK_LOW_CONTENTION_LIMIT = -1000;
    static const int STAT_LOCK_FAILURE_CONTRIB = 250;
    static const int STAT_LOCK_SUCCESS_CONTRIB = 1;
    
public:

    BaseNode();
    ~BaseNode();
    void setParent(RouteNodePtr newParent);
    RouteNodePtr getParent();
    
    /* Valid Status functions */
    bool isValid(const int tid) override;
    void invalidate(const int tid) override;
    
    /* Statistic lock methods */
    void lock(const int tid) override;
    void unlock(const int tid) override;
    bool tryLock(const int tid);
    int getStatistics();
    void resetStatistics();
    int getHighContentionLimit();
    int getLowContentionLimit();
    bool isHighContentionLimitReached();
    bool isLowContentionLimitReached();
    
    /* */
    void setOrderedSet(IOSet * set);
    IOSet * getOrderedSet();
};

template <typename K, typename V>
BaseNode<K, V>::BaseNode() {
    valid = true;
    isBaseNode = true;
    statLockStatistics = 0;
    parent = NULL; 
}

template <typename K, typename V>
BaseNode<K, V>::~BaseNode() {
    delete ((AVLTree<K,V>*)set);
}

template <typename K, typename V>
void BaseNode<K, V>::setParent(RouteNodePtr node) {
    parent = node;
}

template <typename K, typename V>
RouteNodePtr BaseNode<K, V>::getParent() {
    return parent;
}

template <typename K, typename V>
void BaseNode<K, V>::setOrderedSet(IOSet * set) {
    this->set = set;
}

template <typename K, typename V>
IOSet * BaseNode<K, V>::getOrderedSet() {
    return set;
}

/**
 * Valid status methods
 */
template <typename K, typename V>
void BaseNode<K, V>::invalidate(const int tid) {
    assert(valid);
    assert(owner == tid);
    valid = false;
}

template <typename K, typename V>
bool BaseNode<K, V>::isValid(const int tid) {
    assert(owner == tid);
    return valid;
}


/**
 * Statistic Lock Methods
 * 
 */
template <typename K, typename V>
bool BaseNode<K, V>::tryLock(const int tid) {
    if (m.try_lock()) {
        assert(owner == -1);
        owner = tid;
        return true;
    }
    return false;
}

template <typename K, typename V>
void BaseNode<K, V>::lock(const int tid) {
    if ( m.try_lock() ) {
        assert(owner == -1);
        owner = tid;
        statLockStatistics -= STAT_LOCK_SUCCESS_CONTRIB;
        return;
    }
    m.lock(); /* Wait and lock */
    assert(owner == -1);
    owner = tid;
    statLockStatistics += STAT_LOCK_FAILURE_CONTRIB;
}

template <typename K, typename V>
void BaseNode<K, V>::unlock(const int tid) {
    assert(owner == tid);
    owner = -1;
    m.unlock();
}

template <typename K, typename V>
int BaseNode<K, V>::getStatistics() {
    return statLockStatistics;
}

template <typename K, typename V>
void BaseNode<K, V>::resetStatistics() {
    statLockStatistics = 0;
}

template <typename K, typename V>
int BaseNode<K, V>::getHighContentionLimit() {
    return STAT_LOCK_HIGH_CONTENTION_LIMIT;
}

template <typename K, typename V>
int BaseNode<K, V>::getLowContentionLimit() {
    return STAT_LOCK_LOW_CONTENTION_LIMIT;
}

template <typename K, typename V>
bool BaseNode<K, V>::isHighContentionLimitReached() {
    return statLockStatistics > STAT_LOCK_HIGH_CONTENTION_LIMIT;
}

template <typename K, typename V>
bool BaseNode<K, V>::isLowContentionLimitReached() {
    return statLockStatistics < STAT_LOCK_LOW_CONTENTION_LIMIT;
}


#endif /* BASE_NODE_H */

