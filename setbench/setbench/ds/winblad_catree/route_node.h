/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   route_node.h
 * Author: ginns
 *
 * Created on March 22, 2021, 12:06 AM
 */

#ifndef ROUTE_NODE_H
#define ROUTE_NODE_H

#include <mutex>
#include "interfaces.h"

#define RouteNodePtr RouteNode<K, V>*

template <typename K, typename V>
class RouteNode : public CA_Node {
private:
    K key;
    CA_Node* volatile left; // TODO volatile?
    CA_Node* volatile right; //TODO volatile?
    std::mutex m;
    volatile int owner = -1;
    volatile bool valid;
    //RouteNode parent;
    
public:
    RouteNode();
    ~RouteNode();
    K getKey();
    CA_Node * getLeft();
    CA_Node * getRight();
    
    void setKey(K key);
    void setLeft(CA_Node * node);
    void setRight(CA_Node * node);
    
    /* lock methods */
    void lock(const int tid) override;
    void unlock(const int tid) override;

    /* valid status methods */
    void invalidate(const int tid) override;
    bool isValid(const int tid) override;
};

template <typename K, typename V>
RouteNode<K, V>::RouteNode() {
    isBaseNode = false;
    valid = true;
}

template <typename K, typename V>
RouteNode<K, V>::~RouteNode() {
    
}

template <typename K, typename V>
CA_Node * RouteNode<K, V>::getLeft() {
    return left;
}

template <typename K, typename V>
void RouteNode<K, V>::setLeft(CA_Node * node) {
    left = node;
}

template <typename K, typename V>
CA_Node * RouteNode<K, V>::getRight() {
    return right;
}

template <typename K, typename V>
void RouteNode<K, V>::setRight(CA_Node * node) {
    right = node;
}

template <typename K, typename V>
void RouteNode<K, V>::setKey(K _key) {
    key = _key;
}

template <typename K, typename V>
K RouteNode<K, V>::getKey() {
    return key;
}

template <typename K, typename V>
void RouteNode<K, V>::invalidate(const int tid) {
    assert(owner == tid);
    assert(valid);
    valid = false;
}

template <typename K, typename V>
bool RouteNode<K, V>::isValid(const int tid) {
    assert(owner == tid);
    return valid;
}


template <typename K, typename V>
void RouteNode<K, V>::lock(const int tid) {
    m.lock();
    assert(owner == -1);
    owner = tid;
}

template <typename K, typename V>
void RouteNode<K, V>::unlock(const int tid) {
    assert(owner == tid);
    owner = -1;
    m.unlock();
}


#endif /* ROUTE_NODE_H */

