/**
 * @TODO: ADD COMMENT HEADER
 */

#ifndef BST_ADAPTER_H
#define BST_ADAPTER_H

#include <iostream>
#include <csignal>
#include "errors.h"
#include "record_manager.h"
#ifdef USE_TREE_STATS
#   include "tree_stats.h"
#endif
#include "optik_btree.h"

#define DEGREE 11

// Why can't I use simple_record_manager here?? (not declared in scope??)
#define NODE_T Node<K, V, DEGREE>
#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, NODE_T>
#define DATA_STRUCTURE_T ABTreeOptik<RECORD_MANAGER_T, K, V, DEGREE, std::less<K> >


template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    const void * NO_VALUE;
    DATA_STRUCTURE_T * const ds;

public:
     ds_adapter(const int NUM_THREADS,
               const K& KEY_ANY,
               const K& KEY_MAX,
               const V& unused2,
               Random64 * const unused3)
    : ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_ANY, KEY_MAX))
    { }

    ~ds_adapter() {
        delete ds;
    }

    long int getNoValue() {
        return ds->NO_VALUE;
    }

    void initThread(const int tid) {
        ds->initThread(tid);
    }

    void deinitThread(const int tid) {
        ds->deinitThread(tid);
    }

    V insert(const int tid, const K& key, const V& val) {
        setbench_error("insert-replace functionality not implemented for this data structure");
    }

    V insertIfAbsent(const int tid, const K& key, const V& val) {
        return ds->tryInsert(tid, key, val);
    }

    V erase(const int tid, const K& key) {
        return ds->tryErase(tid, key);
    }

    V find(const int tid, const K& key) {
        return ds->find(tid, key);
    }

    bool contains(const int tid, const K& key) {
        return ds->contains(tid, key);
    }

    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V* const resultValues) {
        setbench_error("Range query functionality not implemented for this data structure");
        return -1; //TODO
    }
    void printSummary() {
        ds->printDebuggingDetails();
    }
    bool validateStructure() {
  return ds->validate();
    }

    void printObjectSizes() {
        std::cout<<"sizes: node="
                 <<(sizeof(Node<K, V, DEGREE>))
                 <<std::endl;
    }
    // try to clean up: must only be called by a single thread as part of the test harness!
    void debugGCSingleThreaded() {
        ds->debugGetRecMgr()->debugGCSingleThreaded();
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef Node<K, V, DEGREE> * NodePtrType;

        K minKey;
        K maxKey;

        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }

        class ChildIterator {
        private:
            int ix;
            NodePtrType node; // node being iterated over
        public:
            ChildIterator(NodePtrType _node) { node = _node; ix = 0; }
            bool hasNext() { return ix < node->size; }
            NodePtrType next() { return node->ptrs[ix++]; }
        };

        static bool isLeaf(NodePtrType node) { return node->leaf; }
        static ChildIterator getChildIterator(NodePtrType node) { return ChildIterator(node); }
        static size_t getNumChildren(NodePtrType node) { return node->size; }
        static size_t getNumKeys(NodePtrType node) { return node->leaf ? node->size : 0; }
        static size_t getSumOfKeys(NodePtrType node) {
            size_t result = 0;
            if (node->leaf) {
                for (size_t i=0;i<DEGREE;++i) {
                    result += (size_t) node->keys[i];
                }
            }
            return result;
        }
        static size_t getSizeInBytes(NodePtrType node) { return sizeof(*node); }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->getRoot(), true);
    }
#endif
};

#endif
