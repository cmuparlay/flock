/**
 * @TODO: ADD COMMENT HEADER
 */

#ifndef BST_ADAPTER_H
#define BST_ADAPTER_H

#ifndef KCAS_TYPE
    #define KCAS_HTM
    #define KCAS_TYPE "KCAS_HTM"
#endif

#include <iostream>
#include <csignal>
#include "errors.h"
#include "record_manager.h"
#ifdef USE_TREE_STATS
#   include "tree_stats.h"
#endif
#include "internal_kcas.h"



#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, Node<K, V> >
#define DATA_STRUCTURE_T InternalKCAS<RECORD_MANAGER_T, K, V>


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
    , ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_MIN, KEY_MAX))
    { }

    ~ds_adapter() {
        delete ds;
    }

    V getNoValue() {
        return 0;
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
        return ds->insertIfAbsent(tid, key, val);
    }

    V erase(const int tid, const K& key) {
        return ds->erase(tid, key);
    }

    V find(const int tid, const K& key) {
        setbench_error("find functionality not implemented for this data structure");
    }

    bool contains(const int tid, const K& key) {
        return ds->contains(tid, key);
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }
    void printSummary() {
        ds->printDebuggingDetails();
    }
    bool validateStructure() {
#ifdef USE_TREE_STATS
	return ds->validate();
#endif
	return true;
    }

    void printObjectSizes() {
        std::cout<<"sizes: node="
                 <<(sizeof(Node<K, V>))
                 <<std::endl;
    }
    // try to clean up: must only be called by a single thread as part of the test harness!
    void debugGCSingleThreaded() {
        ds->debugGetRecMgr()->debugGCSingleThreaded();
    }

#ifdef USE_TREE_STATS
class NodeHandler {
    public:
        typedef Node<K, V> * NodePtrType;
        K minKey;
        K maxKey;

        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }

        class ChildIterator {
        private:
            bool leftDone;
            bool rightDone;
            NodePtrType node; // node being iterated over
        public:
            ChildIterator(NodePtrType _node) {
                node = _node;
                leftDone = node->left == NULL;
                rightDone = node->right  == NULL;
            }
            bool hasNext() {
                return !(leftDone && rightDone);
            }
            NodePtrType next() {
                if (!leftDone) {
                    leftDone = true;
                    return node->left;
                }
                if (!rightDone) {
                    rightDone = true;
                    return  node->right;
                }
                setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
            }
        };

        bool isLeaf(NodePtrType node) {
            return node->left == NULL && node->right == NULL;
        }
        size_t getNumChildren(NodePtrType node) {
            if (isLeaf(node)) return 0;
            return node->left != NULL + node->right != NULL;
        }
        size_t getNumKeys(NodePtrType node) {
            if (node->key == minKey || node->key == maxKey) return 0;
            return 1;
        }
        size_t getSumOfKeys(NodePtrType node) {
            if (getNumKeys(node) == 0) return 0;
            return (size_t) node->key;
        }
        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->getRoot(), true);
    }
#endif
};

#endif
