/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
#ifndef INTERFACES_H
#define INTERFACES_H

#include <cassert>

#define IOSet IOrderedSet<K, V>

/**
 * Base class for CA Tree? AVL Tree, Red-Black Tree and Splay Tree should inherit
 */
template <typename K, typename V>
class IOrderedSet {
public:
    virtual V find(const int tid, const K & key) = 0;
    virtual V insert(const int tid, const K & key, const V& val) = 0;
    virtual V erase(const int tid, const K & key) = 0;
    //virtual void printKeys() = 0;
    virtual size_t numKeys() = 0;
    virtual size_t sumOfKeys() = 0;
    virtual IOSet * join(const int tid, IOSet * rightSet) = 0;
    virtual std::tuple<K, IOSet *, IOSet *> split(const int tid) = 0;
};

class CA_Node {
    /* TODO add something here? */
public:
    bool isBaseNode;
    virtual void lock(const int tid) = 0;
    virtual void unlock(const int tid) = 0;
    virtual bool isValid(const int tid) = 0;
    virtual void invalidate(const int tid) = 0;
};

enum OrderedSetType { AVL, LINKEDLIST, REDBLACK };

#endif /* INTERFACES_H */
