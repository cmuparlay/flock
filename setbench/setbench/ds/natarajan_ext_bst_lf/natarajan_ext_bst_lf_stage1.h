/**
 * Implementation of the lock-free external BST of Natarajan and Mittal.
 * Trevor Brown, 2017. (Based on Natarajan's original code.)
 *
 * I made several changes/fixes to the data structure,
 * and there are four different versions: baseline, stage 0, stage 1, stage 1.
 *
 * Baseline is functionally the same as the original implementation by Natarajan
 * (but converted to a class, and with templates/generics).
 *
 * Stage 0:
 *  - Fixed a concurrency bug (missing volatiles)
 *
 * Delta from stage 0 to stage 1:
 *  - Added proper node allocation
 *    (The original implementation explicitly allocated arrays of 2 nodes at a time,
 *     which effectively prevents any real memory reclamation.
 *     [You can't free the nodes individually. Rather, you must free the arrays,
 *      and only after BOTH nodes are safe to free. This is hard to solve.])
 *
 * Delta from stage 1 to stage 2:
 *  - Added proper memory reclamation
 *    (The original implementation leaked all memory.
 *     The implementation in ASCYLIB tried to reclaim memory (using epoch
 *     based reclamation?), but still leaked a huge amount of memory.
 *     It turns out the original algorithm lacks a necessary explanation of
 *     how you should reclaim memory in a system without garbage collection.
 *     Because of the way deletions are aggregated and performed at once,
 *     after you delete a single key, you may have to reclaim many nodes.)
 *    To my knowledge, this is the only correct, existing implementation
 *    of this algorithm (as of Mar 2018).
 *
 * To compile baseline, add compilation arguments:
 *  -DDS_H_FILE=ds/natarajan_ext_bst_lf/natarajan_ext_bst_lf_baseline_impl.h
 *  -DBASELINE
 *
 * To compile stage 0, add compilation argument:
 *  -DDS_H_FILE=ds/natarajan_ext_bst_lf/natarajan_ext_bst_lf_baseline_impl.h
 *
 * To compile stage 1, add compilation argument:
 *  -DDS_H_FILE=ds/natarajan_ext_bst_lf/natarajan_ext_bst_lf_stage1_impl.h
 *
 * To compile stage 2, add compilation argument:
 *  -DDS_H_FILE=ds/natarajan_ext_bst_lf/natarajan_ext_bst_lf_stage2_impl.h
 */

/*A Lock Free Binary Search Tree
 * File:
 *   wfrbt.cpp
 * Author(s):
 *   Aravind Natarajan <natarajan.aravind@gmail.com>
 * Description:
 *   A Lock Free Binary Search Tree
 *
 * Copyright (c) 20013-2014.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

Please cite our PPoPP 2014 paper - Fast Concurrent Lock-Free Binary Search Trees by Aravind Natarajan and Neeraj Mittal if you use our code in your experiments

Features:
1. Insert operations directly install their window without injecting the operation into the tree. They help any conflicting operation at the injection point,
before executing their window txn.
2. Delete operations are the same as that of the original algorithm.

 */

/*
 * File:   wfrbt.h
 * Author: Maya Arbel-Raviv
 *
 * Created on June 8, 2017, 10:45 AM
 */

#ifndef NATARAJAN_EXT_BST_LF_H
#define NATARAJAN_EXT_BST_LF_H

#include "record_manager.h"
#include "atomic_ops.h"

#if     (INDEX_STRUCT == IDX_NATARAJAN_EXT_BST_LF)
#elif   (INDEX_STRUCT == IDX_NATARAJAN_EXT_BST_LF_BASELINE)
#error cannot support baseline with int keys and no value.
#else
#error
#endif

// Most of these macros are not used in this algorithm

#define MARK_BIT 1
#define FLAG_BIT 0

#define atomic_cas_full(addr, old_val, new_val) __sync_bool_compare_and_swap(addr, old_val, new_val);
#define create_child_word(addr, mark, flag) (((uintptr_t) addr << 2) + (mark << 1) + (flag))
#define is_marked(x) ( ((x >> 1) & 1)  == 1 ? true:false)
#define is_flagged(x) ( (x & 1 )  == 1 ? true:false)
#define get_addr(x) (x >> 2)
#define add_mark_bit(x) (x + 4UL)
#define is_free(x) (((x) & 3) == 0? true:false)

enum {
    INSERT, DELETE
};

enum {
    UNMARK, MARK
};

enum {
    UNFLAG, FLAG
};

typedef uintptr_t Word;

template <typename skey_t, typename sval_t>
struct node_t {
    union {
        struct {
            skey_t key;
            sval_t value;
            volatile AO_double_t child;
        };
#ifdef MIN_NODE_SIZE
        char bytes[MIN_NODE_SIZE];
#endif
    };
};


template <typename skey_t, typename sval_t>
struct seekRecord_t {
    skey_t leafKey;
    sval_t leafValue;
    struct node_t<skey_t, sval_t>* leaf;
    struct node_t<skey_t, sval_t>* parent;
    AO_t pL;
    bool isLeftL; // is L the left child of P?
    struct node_t<skey_t, sval_t>* lum;
    AO_t lumC;
    bool isLeftUM; // is  last unmarked node's child on access path the left child of  the last unmarked node?
};

template <typename skey_t, typename sval_t>
struct thread_data_t {
    int id;
    struct node_t<skey_t, sval_t>* rootOfTree;
    seekRecord_t<skey_t, sval_t>* sr; // seek record
    seekRecord_t<skey_t, sval_t> * ssr; // secondary seek record
};

//static __thread thread_data_t<skey_t, sval_t> * data = NULL;

template <typename skey_t, typename sval_t, class RecMgr, class Compare = std::less<skey_t> >
class natarajan_ext_bst_lf {
private:
    PAD;
    RecMgr * const recmgr;
    Compare cmp;
//    PAD;
    node_t<skey_t, sval_t> * root;
//    PAD;

    seekRecord_t<skey_t, sval_t>* insseek(thread_data_t<skey_t, sval_t>* data, skey_t key, int op);
    seekRecord_t<skey_t, sval_t>* delseek(thread_data_t<skey_t, sval_t>* data, skey_t key, int op);
    seekRecord_t<skey_t, sval_t>* secondary_seek(thread_data_t<skey_t, sval_t>* data, skey_t key, seekRecord_t<skey_t, sval_t>* sr);
    sval_t delete_node(thread_data_t<skey_t, sval_t>* data, skey_t key);
    sval_t insertIfAbsent(thread_data_t<skey_t, sval_t>* data, skey_t key, sval_t value);
    sval_t search(thread_data_t<skey_t, sval_t>* data, skey_t key);
    int help_conflicting_operation (thread_data_t<skey_t, sval_t>* data, seekRecord_t<skey_t, sval_t>* R);
    int inject(thread_data_t<skey_t, sval_t>* data, seekRecord_t<skey_t, sval_t>* R, int op);
    int perform_one_delete_window_operation(thread_data_t<skey_t, sval_t>* data, seekRecord_t<skey_t, sval_t>* R, skey_t key);
    int perform_one_insert_window_operation(thread_data_t<skey_t, sval_t>* data, seekRecord_t<skey_t, sval_t>* R, skey_t newKey, sval_t value);

    void retireDeletedNodes(thread_data_t<skey_t, sval_t>* data, node_t<skey_t, sval_t> * node, node_t<skey_t, sval_t> * targetNode, bool pointerFlagged);

    int init[MAX_THREADS_POW2] = {0,};
public:
    const skey_t MAX_KEY;
    const sval_t NO_VALUE;
    const int NUM_PROCESSES;
    PAD;

    natarajan_ext_bst_lf(const skey_t& _MAX_KEY, const sval_t& _NO_VALUE, const int numProcesses)
    : MAX_KEY(_MAX_KEY)
    , NO_VALUE(_NO_VALUE)
    , NUM_PROCESSES(numProcesses)
    , recmgr(new RecMgr(numProcesses, SIGQUIT)) {
        const int tid = 0;
        initThread(tid);

        cmp = Compare();

        recmgr->endOp(tid); // block crash recovery signal for this thread, and enter an initial quiescent state.

        root = recmgr->template allocate<node_t<skey_t, sval_t>>(tid);
        node_t<skey_t, sval_t> * newLC = recmgr->template allocate<node_t<skey_t, sval_t>>(tid);
        node_t<skey_t, sval_t> * newRC = recmgr->template allocate<node_t<skey_t, sval_t>>(tid);

        memset(newLC, 0, sizeof (struct node_t<skey_t, sval_t>));
        memset(newRC, 0, sizeof (struct node_t<skey_t, sval_t>));

        root->key =  _MAX_KEY;
        newLC->key = _MAX_KEY - 1;
        newRC->key = _MAX_KEY;

        root->value = NO_VALUE;
        newLC->value = NO_VALUE;
        newRC->value = NO_VALUE;

        root->child.AO_val1 = create_child_word(newLC, UNMARK, UNFLAG);
        root->child.AO_val2 = create_child_word(newRC, UNMARK, UNFLAG);
    }

    ~natarajan_ext_bst_lf() {
        delete recmgr;
        // TODO: traverse the tree and retire all nodes
    }

    void initThread(const int tid) {
        if (init[tid]) return; else init[tid] = !init[tid];

        recmgr->initThread(tid);
    }

    void deinitThread(const int tid) {
        if (!init[tid]) return; else init[tid] = !init[tid];

        recmgr->deinitThread(tid);
    }

    sval_t insertIfAbsent(const int tid, skey_t key, sval_t item) {
        assert(cmp(key, MAX_KEY-1));
        thread_data_t<skey_t, sval_t> data;
        seekRecord_t<skey_t, sval_t> sr;
        seekRecord_t<skey_t, sval_t> ssr;
        data.id = tid;
        data.sr = &sr;
        data.ssr = &ssr;
        data.rootOfTree = root;
        return insertIfAbsent(&data,key,item);
    }

    sval_t erase(const int tid, skey_t key) {
        assert(cmp(key, MAX_KEY-1));
        thread_data_t<skey_t, sval_t> data;
        seekRecord_t<skey_t, sval_t> sr;
        seekRecord_t<skey_t, sval_t> ssr;
        data.id = tid;
        data.sr = &sr;
        data.ssr = &ssr;
        data.rootOfTree = root;
        return delete_node(&data,key);
    }

    sval_t find(const int tid, skey_t key) {
        thread_data_t<skey_t, sval_t> data;
        seekRecord_t<skey_t, sval_t> sr;
        seekRecord_t<skey_t, sval_t> ssr;
        data.id = tid;
        data.sr = &sr;
        data.ssr = &ssr;
        data.rootOfTree = root;
        return search(&data,key);
    }

    node_t<skey_t, sval_t> * get_root() {
        return root;
    }

    static node_t<skey_t, sval_t> * get_left(node_t<skey_t, sval_t> * curr) {
        return (node_t<skey_t, sval_t> *)get_addr(curr->child.AO_val1);
    }

    static node_t<skey_t, sval_t> * get_right(node_t<skey_t, sval_t> * curr) {
        return (node_t<skey_t, sval_t> *)get_addr(curr->child.AO_val2);
    }

    long long getKeyChecksum(node_t<skey_t, sval_t> * curr) {
        if (curr == NULL) return 0;
        node_t<skey_t, sval_t> * left = get_left(curr);
        node_t<skey_t, sval_t> * right = get_right(curr);
        if (!left && !right) return (long long) curr->key; // leaf
        return getKeyChecksum(left) + getKeyChecksum(right);
    }

    long long getKeyChecksum() {
        return getKeyChecksum(get_left(get_left(root)));
    }

    long long getSize(node_t<skey_t, sval_t> * curr) {
        if (curr == NULL) return 0;
        node_t<skey_t, sval_t> * left = get_left(curr);
        node_t<skey_t, sval_t> * right = get_right(curr);
        if (!left && !right) return 1; // leaf
        return getSize(left) + getSize(right);
    }

    bool validateStructure() {
        return true;
    }

    long long getSize() {
        return getSize(get_left(get_left(root)));
    }

    long long getSizeInNodes(node_t<skey_t, sval_t> * const curr) {
        if (curr == NULL) return 0;
        return 1 + getSizeInNodes(get_left(curr))
                 + getSizeInNodes(get_right(curr));
    }

    long long getSizeInNodes() {
        return getSizeInNodes(root);
    }

    void printSummary() {
//        std::stringstream ss;
//        ss<<getSizeInNodes()<<" nodes in tree";
//        std::cout<<ss.str()<<std::endl;
//
        recmgr->printStatus();
    }

    RecMgr * debugGetRecMgr() {
        return recmgr;
    }
};
#endif /* NATARAJAN_EXT_BST_LF_H */
