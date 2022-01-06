/**
 * @TODO: ADD COMMENT HEADER
 */

#ifndef CATREE_ADAPTER_H
#define CATREE_ADAPTER_H

#include <iostream>
#include <csignal>
#include <bits/stdc++.h>
using namespace std;

#include "errors.h"
#include "record_manager.h"
#ifdef USE_TREE_STATS
#   include "tree_stats.h"
#endif
#include "Tree.h"
#include "N.h"

#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, ART_OLC::N4, ART_OLC::N16, ART_OLC::N48, ART_OLC::N256>
#define DATA_STRUCTURE_T ART_OLC::Tree<RECORD_MANAGER_T>


void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen(sizeof(tid));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    const V NO_VALUE;
    DATA_STRUCTURE_T * const ds;

public:
    ds_adapter(const int NUM_THREADS,
               const K& KEY_MIN,
               const K& KEY_MAX,
               const V& VALUE_RESERVED,
               Random64 * const unused2)
            : NO_VALUE(VALUE_RESERVED)
            , ds(new DATA_STRUCTURE_T(NUM_THREADS, loadKey))
    { }

    ~ds_adapter() {
        delete ds;
    }

    V getNoValue() {
        return NO_VALUE;
    }

    void initThread(const int threadID) {
        ds->initThread(threadID);
    }

    void deinitThread(const int threadID) {
        ds->deinitThread(threadID);
    }

    V insert(const int threadID, const K& key, const V& val) {
        setbench_error("insert-replace functionality not implemented for this data structure");
    }

    V insertIfAbsent(const int threadID, const K& key, const V& val) {
        Key treeKey;
        loadKey(key, treeKey);
        if (ds->insert(threadID, treeKey, key)) return (V)0;
        return (V)1;
    }

    V erase(const int threadID, const K& key) {
        Key treeKey;
        loadKey(key, treeKey);
        if (ds->remove(threadID, treeKey, key)) return (V)1;
        return (V)0;
    }

    V find(const int threadID, const K& key) {
        Key treeKey;
        loadKey(key, treeKey);
        return (V)ds->lookup(threadID, treeKey);
    }

    bool contains(const int threadID, const K& key) {
        return find(threadID, key) != NO_VALUE;
    }

    int rangeQuery(const int threadID, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }

    void printSummary() {
//        ds->printDebuggingDetails();
    }

    void printObjectSizes() {
    }

    bool validateStructure() {
        return true;
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef ART_OLC::N* NodePtrType;

        NodeHandler() {}

    //             std::tuple<uint8_t, N *> children[256];
    //             uint32_t childrenCount = 0;
    //             N::getChildren(node, 0u, 255u, children, childrenCount);
    //             for (uint32_t i = 0; i < childrenCount; ++i) {
    //                 const N *n = std::get<1>(children[i]);
    //                 copy(n);
    //                 if (toContinue != 0) {
    //                     break;
    //                 }
    //             }
        class ChildIterator {
        private:
            int idx = 0;
            tuple<uint8_t, NodePtrType> children[256];
            uint32_t numChildren;
        public:
            ChildIterator(NodePtrType node) {
                ART_OLC::N::getChildren(node, 0u, 255u, children, numChildren);
                idx = 0; 
            }
            bool hasNext() { return idx < numChildren; }
            NodePtrType next() { 
                return get<1>(children[idx++]);
            }
        };

        bool isLeaf(NodePtrType node) {
            return ART_OLC::N::isLeaf(node);
        }

        size_t getNumKeys(NodePtrType node) {
            return ART_OLC::N::isLeaf(node) ? 1 : 0;
        }

        size_t getSumOfKeys(NodePtrType node) {
            assert(node != nullptr);
            if (ART_OLC::N::isLeaf(node)) {
                return ART_OLC::N::getLeaf(node);
            }
            return 0;
        }

        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(), ds->getRoot(), false);
    }
#endif
};

#endif
