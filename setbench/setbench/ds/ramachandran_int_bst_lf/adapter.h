/**
 * Implementation of the internal lock-free BST of Ramachandran and Mittal.
 * This is a heavily modified version of the original authors' implementation.
 * (See copyright notice in intlf.h)
 * The modifications are copyrighted (consistent with the original license)
 *   by Maya Arbel-Raviv and Trevor Brown, 2018.
 */

#ifndef BST_ADAPTER_H
#define BST_ADAPTER_H

#include <csignal>
#include "errors.h"
#ifdef USE_TREE_STATS
#   define TREE_STATS_BYTES_AT_DEPTH
#   include "tree_stats.h"
#endif
#include "intlf_impl.h"

#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, node_t<K,V>, stateRecord<K,V>>
#define DATA_STRUCTURE_T intlf<K, V, RECORD_MANAGER_T>

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
    , ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_MIN, KEY_MAX, NO_VALUE, 0 /* unused */))
    {}
    ~ds_adapter() {
        delete ds;
    }

    V getNoValue() {
        return NO_VALUE;
    }

    void initThread(const int tid) {
        ds->initThread(tid);
    }
    void deinitThread(const int tid) {
        ds->deinitThread(tid);
    }

    V insert(const int tid, const K& key, const V& val) {
        setbench_error("not implemented");
    }
    V insertIfAbsent(const int tid, const K& key, const V& val) {
        return ds->insert(tid, key, val);
    }
    V erase(const int tid, const K& key) {
        return ds->remove(tid, key) ? (V) key : NO_VALUE; // dirty hack because i can't see how to modify this tree to return the deleted value when a deletion occurs
    }
    V find(const int tid, const K& key) {
        return ds->find(tid, key);
    }
    bool contains(const int tid, const K& key) {
        return find(tid, key) != getNoValue();
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }
    void printSummary() {
        auto recmgr = ds->debugGetRecMgr();
        recmgr->printStatus();
    }
    bool validateStructure() {
        return true;
    }
    void printObjectSizes() {
        std::cout<<"sizes: node="
                 <<(sizeof(node_t<K,V>))
                 <<std::endl;
    }
    // try to clean up: must only be called by a single thread as part of the test harness!
    void debugGCSingleThreaded() {
        ds->debugGetRecMgr()->debugGCSingleThreaded();
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef node_t<K,V> * NodePtrType;
        K minKey;
        K maxKey;

        typedef K skey_t; // for getAddress()
        typedef V sval_t; // for getAddress()

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
                leftDone = isNull(node->child[LEFT]);
                rightDone = isNull(node->child[RIGHT]);
            }
            bool hasNext() {
                return !(leftDone && rightDone);
            }
            NodePtrType next() {
                if (!leftDone) {
                    leftDone = true;
                    return getAddress(node->child[LEFT]);
                }
                if (!rightDone) {
                    rightDone = true;
                    return getAddress(node->child[RIGHT]);
                }
                setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
            }
        };

        bool isLeaf(NodePtrType node) {
            return isNull(node->child[LEFT]) && isNull(node->child[RIGHT]);
        }
        size_t getNumChildren(NodePtrType node) {
            return !isNull(node->child[LEFT]) + !isNull(node->child[RIGHT]);
        }
        size_t getNumKeys(NodePtrType node) {
            if (getKey(node->markAndKey) == minKey || getKey(node->markAndKey) == maxKey) return 0;
            return 1;
        }
        size_t getSumOfKeys(NodePtrType node) {
            if (getNumKeys(node) == 0) return 0;
            return (size_t) getKey(node->markAndKey);
        }
        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
        static size_t getSizeInBytes(NodePtrType node) { return sizeof(*node); }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->get_root(), true);
    }
#endif
};

#endif
