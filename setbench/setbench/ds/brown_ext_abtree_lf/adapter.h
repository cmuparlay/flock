/**
 * Implementation of a lock-free relaxed (a,b)-tree using LLX/SCX.
 * Trevor Brown, 2018.
 */

#ifndef DS_ADAPTER_H
#define DS_ADAPTER_H

#include <iostream>
#include "errors.h"
#include "brown_ext_abtree_lf_impl.h"
#ifdef USE_TREE_STATS
#   define TREE_STATS_BYTES_AT_DEPTH
#   include "tree_stats.h"
#endif

#if !defined FAT_NODE_DEGREE
    #define FAT_NODE_DEGREE 11
#endif

#define NODE_T abtree_ns::Node<FAT_NODE_DEGREE, K>
#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, NODE_T>
#define DATA_STRUCTURE_T abtree_ns::abtree<FAT_NODE_DEGREE, K, std::less<K>, RECORD_MANAGER_T>

template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    DATA_STRUCTURE_T * const ds;

public:
    ds_adapter(const int NUM_THREADS,
               const K& KEY_ANY,
               const K& unused1,
               const V& unused2,
               Random64 * const unused3)
    : ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_ANY))
    {
        if (sizeof(V) > sizeof(void *)) {
            setbench_error("Value type V is too large to fit in void *. This data structure stores all values in fields of type void *, so this is a problem.");
        }
        if (NUM_THREADS > MAX_THREADS_POW2) {
            setbench_error("NUM_THREADS exceeds MAX_THREADS_POW2");
        }
    }
    ~ds_adapter() {
        delete ds;
    }

    void * getNoValue() {
        return ds->NO_VALUE;
    }

    void initThread(const int tid) {
        ds->initThread(tid);
    }
    void deinitThread(const int tid) {
        ds->deinitThread(tid);
    }

    bool contains(const int tid, const K& key) {
        return ds->contains(tid, key);
    }
    V insert(const int tid, const K& key, const V& val) {
        return (V) ds->insert(tid, key, val);
    }
    V insertIfAbsent(const int tid, const K& key, const V& val) {
        return (V) ds->insertIfAbsent(tid, key, (void*) val);
    }
    V erase(const int tid, const K& key) {
        return (V) ds->erase(tid, key).first;
    }
    V find(const int tid, const K& key) {
        return (V) ds->find(tid, key).first;
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        return ds->rangeQuery(tid, lo, hi, resultKeys, (void ** const) resultValues);
    }
    void printSummary() {
        ds->debugGetRecMgr()->printStatus();
    }
    bool validateStructure() {
        return true;
    }
    void printObjectSizes() {
        std::cout<<"size_node="<<(sizeof(abtree_ns::Node<FAT_NODE_DEGREE, K>))<<std::endl;
    }
    // try to clean up: must only be called by a single thread as part of the test harness!
    void debugGCSingleThreaded() {
        ds->debugGetRecMgr()->debugGCSingleThreaded();
    }

    size_t size() {
        return ds->sequentialSize();
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef NODE_T * NodePtrType;

        K minKey;
        K maxKey;

        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }

        class ChildIterator {
        private:
            size_t ix;
            NodePtrType node; // node being iterated over
        public:
            ChildIterator(NodePtrType _node) { node = _node; ix = 0; }
            bool hasNext() { return ix < node->size; }
            NodePtrType next() { return node->ptrs[ix++]; }
        };

        static bool isLeaf(NodePtrType node) { return node->leaf; }
        static ChildIterator getChildIterator(NodePtrType node) { return ChildIterator(node); }
        static size_t getNumChildren(NodePtrType node) { return node->size; }
        static size_t getNumKeys(NodePtrType node) { return isLeaf(node) ? node->size : 0; }
        static size_t getSumOfKeys(NodePtrType node) {
            size_t sz = getNumKeys(node);
            size_t result = 0;
            for (size_t i=0;i<sz;++i) {
                result += (size_t) node->keys[i];
            }
            return result;
        }
        static size_t getSizeInBytes(NodePtrType node) { return sizeof(*node); }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->debug_getEntryPoint(), true);
    }
#endif

private:
    template<typename... Arguments>
    void iterate_helper_fn(int depth, void (*callback)(K key, V value, Arguments... args)
            , NODE_T * node, Arguments... args) {
        if (node == NULL) return;

        if (node->leaf) {
            for (int i=0;i<node->getABDegree();++i) {
                K key = node->keys[i];
                V val = (V) node->ptrs[i];
                callback(key, val, args...);
            }
            return;
        }

        for (int i=0;i<node->getABDegree();++i) {
            if (depth == 4) {
                #pragma omp task
                iterate_helper_fn(1+depth, callback, (NODE_T *) node->ptrs[i], args...);
            } else {
                iterate_helper_fn(1+depth, callback, (NODE_T *) node->ptrs[i], args...);
            }
        }
    }

public:
    #define DS_ADAPTER_SUPPORTS_TERMINAL_ITERATE
    template<typename... Arguments>
    void iterate(void (*callback)(K key, V value, Arguments... args), Arguments... args) {
        #pragma omp parallel
        {
            #pragma omp single
            iterate_helper_fn(0, callback, ds->debug_getEntryPoint(), args...);
        }
    }

};

#endif
