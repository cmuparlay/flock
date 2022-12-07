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
#include "ca_tree.h"

#include <parlay/random.h>
#include <parlay/primitives.h>

#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, RouteNode<K,V>, BaseNode<K,V>>
#define DATA_STRUCTURE_T CATree<RECORD_MANAGER_T, K, V>


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
            , ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_MIN, KEY_MAX, OrderedSetType::AVL))
    { }

    ~ds_adapter() {
        delete ds;
    }

    V getNoValue() {
        return NO_VALUE;
    }

    template<typename _T>
    static void shuffleHelper(size_t n) {
      auto ptrs = parlay::tabulate(n, [&] (size_t i) {return parlay::type_allocator<_T>::alloc();});
      ptrs = parlay::random_shuffle(ptrs);
      parlay::parallel_for(0, n, [&] (size_t i) {parlay::type_allocator<_T>::free(ptrs[i]);});
    }

    static void shuffle(size_t n) {
      shuffleHelper<RouteNode<K,V>>(n);
      shuffleHelper<BaseNode<K,V>>(n);
    }
    // static void shuffle(size_t n) {}

    static void reserve(size_t n) {}

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
        return ds->erase(tid, key);
    }

    V find(const int tid, const K& key) {
        return ds->find(tid, key);
    }

    bool contains(const int tid, const K& key) {
        return ds->find(tid, key) != NO_VALUE;
    }

    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }

    void printSummary() {
//        ds->printDebuggingDetails();
    }

    void printObjectSizes() {
        std::cout<<"sizes: RouteNode="
                 <<(sizeof(RouteNode<K, V>))
                 <<std::endl;
        std::cout<<"sizes: BaseNode="
                 <<(sizeof(BaseNode<K, V>))
                 <<std::endl;
    }

    bool validateStructure() {
        return true;
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef CA_Node* NodePtrType;
        K minKey;
        K maxKey;

        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }

        class ChildIterator {
        private:
            NodePtrType node; // node being iterated over
            bool done;
        public:
            ChildIterator(NodePtrType _node) {
                assert(false);
            }

            bool hasNext() {
                assert(false);
                return false;
            }

            NodePtrType next() {
                assert(false);
                return nullptr;
            }
        };

        bool isLeaf(NodePtrType node) {
            return true; // avoids any ChildIterator stuff
        }

        size_t getNumChildren(NodePtrType node) {
            assert(false);
            return 0;
        }

        size_t getNumKeys(NodePtrType node) {
            assert(node != NULL);
            long sum = 0;
            if (BaseNode<K, V>* baseNode = dynamic_cast<BaseNode<K, V>*>(node)) {
                /* node is a base node, get ordered set */
                sum = baseNode->getOrderedSet()->numKeys();
            }
            else {
                RouteNode<K, V>* routeNode = dynamic_cast<RouteNode<K, V>*>(node); //use static_cast?
                if ( routeNode->getLeft() != NULL ) 
                    sum += getNumKeys(routeNode->getLeft());
                if (routeNode->getRight() != NULL )
                    sum += getNumKeys(routeNode->getRight());
            }
            return sum;
        }

        size_t getSumOfKeys(NodePtrType node) {
            assert(node != NULL);
            long sum = 0;
            if (BaseNode<K, V>* baseNode = dynamic_cast<BaseNode<K, V>*>(node)) {
                /* node is a base node, get ordered set */
                sum = baseNode->getOrderedSet()->sumOfKeys();
            }
            else {
                RouteNode<K, V>* routeNode = dynamic_cast<RouteNode<K, V>*>(node); //use static_cast?
                if ( routeNode->getLeft() != NULL ) 
                    sum += getSumOfKeys(routeNode->getLeft());
                if (routeNode->getRight() != NULL )
                    sum += getSumOfKeys(routeNode->getRight());
            }
            return sum;
        }

        ChildIterator getChildIterator(NodePtrType node) {
            assert(false);
            return ChildIterator(node);
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->getRoot(), false);
    }
#endif
};

#endif
