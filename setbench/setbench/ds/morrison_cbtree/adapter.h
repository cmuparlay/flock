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
#include "cbtree.h"


#define NODE_T Node<K, V>
#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, NODE_T>
#define DATA_STRUCTURE_T CBTree<RECORD_MANAGER_T, K, V, std::less<K> >


template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    DATA_STRUCTURE_T * const ds;

public:
    static constexpr void * NO_VALUE = nullptr;
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

    void * getNoValue() {
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
        return ds->insert(tid, key, val);
    }

    V erase(const int tid, const K& key) {
        return ds->remove(tid, key);
    }

    V find(const int tid, const K& key) {
        return ds->get(tid, key);
    }

    bool contains(const int tid, const K& key) {
        return ds->contains(tid, key);
    }

    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V* const resultValues) {
        setbench_error("Range query functionality not implemented for this data structure");
        return -1; //TODO
    }
    void printSummary() {
        // ds->printDebuggingDetails();
    }

    bool validateStructure() {
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
        typedef Node<K, V>* NodePtrType;

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
                leftDone = (node->left == NULL);
                rightDone = (node->right == NULL);
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
                    return node->right;
                }
                setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
            }
        };

        static bool isLeaf(NodePtrType node) {
            return (node->left == nullptr) && (node->right == nullptr);
        }
        static size_t getNumChildren(NodePtrType node) {
            return (node->left != nullptr) + (node->right != nullptr);
        }
        static size_t getNumKeys(NodePtrType node) {
            return (node->val == NO_VALUE) ? 0 : 1;
        }
        static size_t getSumOfKeys(NodePtrType node) {
            if (node->val == NO_VALUE) return 0;
            return (size_t) node->key;
        }
        static ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->getRoot()->right, true);
    }
#endif
};

#endif
