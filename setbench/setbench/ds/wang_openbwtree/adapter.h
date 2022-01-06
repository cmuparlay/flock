/**
 * Copyright Trevor Brown, 2020.
 */

#pragma once

#include "errors.h"
#ifdef USE_TREE_STATS
#   include "tree_stats.h"
#endif

#include <vector>
#include "bwtree.h"

using namespace wangziqi2013::bwtree;

class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }
  KeyComparator(int dummy) {
    (void)dummy;
    return;
  }
  KeyComparator() = delete;
};
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }
  KeyEqualityChecker(int dummy) {
    (void)dummy;
    return;
  }
  KeyEqualityChecker() = delete;
};


#define TREE_TYPE BwTree<K,V,KeyComparator,KeyEqualityChecker>

template <typename K, typename V, class Reclaim = void *, class Alloc = void *, class Pool = void *>
class ds_adapter {
struct PaddedValset {
    std::vector<V> __thr_valset;
    char padding[128-sizeof(std::vector<V>)];
};

private:
    const V NO_VALUE;
    const V YES_VALUE;
    TREE_TYPE * ds;
    PAD;
    PaddedValset* valsets;

public:
    ds_adapter(const int NUM_THREADS,
               const K& KEY_RESERVED,
               const K& unused1,
               const V& VALUE_RESERVED,
               Random64 * const unused2)
    : NO_VALUE(VALUE_RESERVED), YES_VALUE((V)(((uintptr_t) VALUE_RESERVED)+1)) {
        ds = new TREE_TYPE{true, KeyComparator{1}, KeyEqualityChecker{1}};
        ds->UpdateThreadLocal(NUM_THREADS);
        ds->AssignGCID(0); // dummy TID for main thread
        valsets = new PaddedValset[NUM_THREADS];
    }
    ~ds_adapter() {
        delete ds;
        delete[] valsets;
    }

    V getNoValue() {
        return NO_VALUE;
    }

    void initThread(const int tid) {
        ds->AssignGCID(tid);
        valsets[tid].__thr_valset.reserve(100);
    }
    void deinitThread(const int tid) {
        ds->UnregisterThread(tid);
    }

    bool contains(const int tid, const K& key) {
        ds->GetValue(key, valsets[tid].__thr_valset);
        bool ret = valsets[tid].__thr_valset.size() > 0;
        valsets[tid].__thr_valset.clear();
        return ret;
    }
    V insert(const int tid, const K& key, const V& val) {
        setbench_error("not implemented");
    }
    V insertIfAbsent(const int tid, const K& key, const V& val) {
        // WARNING: interface for open bw tree can't implement this one exactly
        // (it doesn't return the value that exists in place of this one...)
        bool changed = ds->Insert(key, val);
        return (changed) ? NO_VALUE : YES_VALUE;
    }
    V erase(const int tid, const K& key) {
        // WARNING: interface for open bw tree can't implement this one exactly
        // (it doesn't return the value that exists in place of this one...)
        // setbench_error("not implemented");
        bool changed = ds->Delete(key, YES_VALUE);
        return (changed) ? YES_VALUE : NO_VALUE;
    }
    V find(const int tid, const K& key) {
        ds->GetValue(key, valsets[tid].__thr_valset);
        // cout << "Size: " << valsets[tid].__thr_valset.size() << endl;
        V retval = valsets[tid].__thr_valset[0];
        valsets[tid].__thr_valset.pop_back();
        return retval;
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }
    void printSummary() {}
    bool validateStructure() {
        return true;
    }
    void printObjectSizes() {}
    // try to clean up: must only be called by a single thread as part of the test harness!
    void debugGCSingleThreaded() {}

#if 0
#   ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef void * NodePtrType;
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return NULL;
    }
#   endif
#else
#   ifdef USE_TREE_STATS

    // NOTE: this is a very strange NodeHandler... it just traverses the ENTIRE DATA STRUCTURE.
    //       this is because we don't know how to traverse the BwTree, except to use its iterator...
    //       so from the perspective of TreeStats, the BwTree is just a giant root node!
    //
    // additionally, we use NodeHandler as the node type (weird),
    // and ChildIterator actually just returns the singular NodeHandler.
    // this is because the node handler has all of the info we need, i.e., the RESULTS of the iteration.
    //
    class NodeHandler {
    public:
        typedef NodeHandler * NodePtrType;
        size_t num_keys;
        size_t sum_of_keys;
        NodeHandler(TREE_TYPE * ds) {
            printf("Open BwTree: createTreeStats iterating over all kv-pairs...\n");
            num_keys = 0;
            sum_of_keys = 0;
            auto it = ds->Begin();
            for ( ;!it.IsEnd(); it++) {
                ++num_keys;
                sum_of_keys += (size_t) it->first;
            }
            printf("Open BwTree: createTreeStats finished iterating.\n");
        }
        class ChildIterator {
        public:
            ChildIterator(NodeHandler * unused) {}
            bool hasNext() { return false; }
            NodeHandler * next() { return NULL; }
        };
        static bool isLeaf(NodeHandler * unused) { return true; }
        static ChildIterator getChildIterator(NodeHandler * handler) { return ChildIterator(handler); }
        static size_t getNumChildren(NodeHandler * unused) { return 0; }
        static size_t getNumKeys(NodeHandler * handler) { return handler->num_keys; }
        static size_t getSumOfKeys(NodeHandler * handler) { return handler->sum_of_keys; }
        static size_t getSizeInBytes(NodeHandler * unused) { return 0; }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        NodeHandler * handler = new NodeHandler(ds);
        return new TreeStats<NodeHandler>(handler, handler, false);
    }
#   endif
#endif
};
