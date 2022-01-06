/**
 * Preliminary C++ implementation of binary search tree using LLX/SCX.
 *
 * Copyright (C) 2014 Trevor Brown
 * This preliminary implementation is CONFIDENTIAL and may not be distributed.
 */

#ifndef NODE_H
#define	NODE_H

#include <iostream>
#include <iomanip>
#include <atomic>
#include <set>
#include "scxrecord.h"
#include "rq_provider.h"
#ifdef USE_RECLAIMER_RCU
#include <urcu.h>
#define RECLAIM_RCU_RCUHEAD_DEFN struct rcu_head rcuHeadField
#else
#define RECLAIM_RCU_RCUHEAD_DEFN
#endif

namespace bst_ns {

    #define nodeptr Node<K,V> * volatile

    template <class K, class V>
    class Node {
    public:
        V value;
        K key;
        std::atomic_uintptr_t scxRecord;
#if !defined(BROWN_EXT_BST_LF_COLOCATE_MARKED_BIT)
        std::atomic_bool marked; // might be able to combine this elegantly with scx record pointer... (maybe we can piggyback on the version number mechanism, using the same bit to indicate ver# OR marked)
#endif
        nodeptr left;
        nodeptr right;
#if defined(RQ_LOCKFREE) || defined(RQ_RWLOCK) || defined(HTM_RQ_RWLOCK)
        volatile long long itime; // for use by range query algorithm
        volatile long long dtime; // for use by range query algorithm
#endif
        RECLAIM_RCU_RCUHEAD_DEFN;

        Node() {}
        Node(const Node& node) {}

        friend std::ostream& operator<<(std::ostream& os, const Node<K,V>& obj) { return os; }
        void printTreeFile(std::ostream& os) {}
        void printTreeFileWeight(std::ostream& os, std::set< Node<K,V>* > *seen) {}
        void printTreeFileWeight(std::ostream& os) {}

    };
}

#endif	/* NODE_H */

