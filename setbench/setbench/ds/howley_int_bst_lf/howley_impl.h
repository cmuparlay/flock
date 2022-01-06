/*   
 *   File: bst_howley.c
 *   Author: Balmau Oana <oana.balmau@epfl.ch>, 
 *  	     Zablotchi Igor <igor.zablotchi@epfl.ch>, 
 *  	     Tudor David <tudor.david@epfl.ch>
 *   Description: Shane V Howley and Jeremy Jones. 
 *   A non-blocking internal binary search tree. SPAA 2012
 *   bst_howley.c is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

/*
 * Author: Trevor Brown
 *
 * Substantial improvements to interface, memory reclamation and bug fixing.
 *
 * Some changes by Trevor:
 * - The memory reclamation in the ASCYLIB implementation was wrong.
 *   It both leaked AND segfaulted. It is now correct.
 *   (Howley et al. didn't say how to reclaim memory at all!)
 * - There was a subtle bug with some incorrect NULL that should have been NULL_NODEPTR.
 * - Fixed incorrect volatile usage.
 * - Added proper padding on data structure globals (root, etc)
 *   to avoid false sharing with the test harness / enclosing program.
 * - Fixed value types so they don't need to be numeric.
 * - Fixed a bug that corrupted inserted numeric values (not keys, but values)
 *   that are smaller than 4.
 */

/* 
 * File:   howley.h
 * Author: Maya Arbel-Raviv
 *
 * Created on June 1, 2017, 10:57 AM
 */

#ifndef HOWLEY_H
#define HOWLEY_H

#include "record_manager.h"

#define USE_PADDING
#define LARGE_DES

//Encoded in the operation pointer
#define STATE_OP_NONE 0
#define STATE_OP_MARK 1
#define STATE_OP_CHILDCAS 2
#define STATE_OP_RELOCATE 3

//In the relocate_op struct
#define STATE_OP_ONGOING 0
#define STATE_OP_SUCCESSFUL 1
#define STATE_OP_FAILED 2

//States for the result of a search operation
#define FOUND 0x0
#define NOT_FOUND_L 0x1
#define NOT_FOUND_R 0x2
#define ABORT 0x3

#define GETFLAG(ptr) (((uint64_t) (ptr)) & 3)
#define FLAG(ptr, flag) ((operation_t<skey_t, sval_t> *) ((((uint64_t) (ptr)) & 0xfffffffffffffffc) | (flag)))
#define UNFLAG(ptr) ((operation_t<skey_t, sval_t> *) (((uint64_t) (ptr)) & 0xfffffffffffffffc))
//Last bit of the node pointer will be set to 1 if the pointer is null 
#define ISNULL(node) (((node) == NULL) || (((uint64_t) (node)) & 1))
#define SETNULL(node) ((node_t<skey_t, sval_t> *) ((((uintptr_t) (node)) & 0xfffffffffffffffe) | 1))
#define NULL_NODEPTR ((node_t<skey_t, sval_t> *) 1)

const unsigned int val_mask = ~(0x3);

template <typename skey_t, typename sval_t>
struct node_t;

template <typename skey_t, typename sval_t>
union operation_t;

template <typename skey_t, typename sval_t>
struct child_cas_op_t {
    bool is_left;
    node_t<skey_t, sval_t>* expected;
    node_t<skey_t, sval_t>* update;

};

template <typename skey_t, typename sval_t>
struct relocate_op_t {
    int volatile state; // initialize to ONGOING every time a relocate operation is created
    node_t<skey_t, sval_t>* dest;
    operation_t<skey_t, sval_t>* dest_op;
    skey_t remove_key;
    sval_t remove_value;
    skey_t replace_key;
    sval_t replace_value;

};

template <typename skey_t, typename sval_t>
struct node_t {
    skey_t key;
    sval_t value;
    operation_t<skey_t, sval_t> * volatile op;
    node_t<skey_t, sval_t> * volatile left;
    node_t<skey_t, sval_t> * volatile right;
#ifdef USE_PADDING
    volatile char pad[64 - sizeof(key) - sizeof(value) - sizeof(op) - sizeof(left) - sizeof(right)];
#endif
};

template <typename skey_t, typename sval_t>
union operation_t {
    child_cas_op_t<skey_t, sval_t> child_cas_op;
    relocate_op_t<skey_t, sval_t> relocate_op;
#if defined(LARGE_DES)
    uint8_t padding[112]; //unique for both TPCC and YCSB
#else
    uint8_t padding[64];
#endif
};

template <typename sval_t>
struct find_result {
    sval_t val;
    int code;
};

template <typename skey_t, typename sval_t, class RecMgr>
class howley {
private:
PAD;
    const unsigned int idx_id;
PAD;
    node_t<skey_t, sval_t> * root;
PAD;
    const int NUM_THREADS;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
PAD;
    RecMgr * const recmgr;
PAD;
    int init[MAX_THREADS_POW2] = {0,}; // this suffers from false sharing, but is only touched once per thread! so no worries.
PAD;
    
    node_t<skey_t, sval_t> * create_node(const int tid, skey_t key, sval_t value, node_t<skey_t, sval_t> * left, node_t<skey_t, sval_t> * right) {
        auto result = recmgr->template allocate<node_t<skey_t, sval_t>>(tid);
        if (result == NULL) {
            perror("malloc in bst create node");
            exit(1);
        }
        result->key = key;
        result->value = value;
        result->op = NULL;
        result->left = left;
        result->right = right;
        return result;
    }

    operation_t<skey_t, sval_t> * alloc_op(const int tid) {
        auto result = recmgr->template allocate<operation_t<skey_t, sval_t>>(tid);
        if (result == NULL) {
            perror("malloc in bst create node");
            exit(1);
        }
        return result;
    }
    
    void bst_help_child_cas(const int tid, operation_t<skey_t, sval_t>* op, node_t<skey_t, sval_t>* dest);
    bool bst_help_relocate(const int tid, operation_t<skey_t, sval_t>* op, node_t<skey_t, sval_t>* pred, operation_t<skey_t, sval_t>* pred_op, node_t<skey_t, sval_t>* curr);
    void bst_help_marked(const int tid, node_t<skey_t, sval_t>* pred, operation_t<skey_t, sval_t>* pred_op, node_t<skey_t, sval_t>* curr);
    void bst_help(const int tid, node_t<skey_t, sval_t>* pred, operation_t<skey_t, sval_t>* pred_op, node_t<skey_t, sval_t>* curr, operation_t<skey_t, sval_t>* curr_op);
    find_result<sval_t> bst_find(const int tid, skey_t k, node_t<skey_t, sval_t>** pred, operation_t<skey_t, sval_t>** pred_op, node_t<skey_t, sval_t>** curr, operation_t<skey_t, sval_t>** curr_op, node_t<skey_t, sval_t>* aux_root, node_t<skey_t, sval_t>* root);
public:

    howley(const int _NUM_THREADS, const skey_t& _KEY_MIN, const skey_t& _KEY_MAX, const sval_t& _VALUE_RESERVED, unsigned int id)
    : idx_id(id), NUM_THREADS(_NUM_THREADS), KEY_MIN(_KEY_MIN), KEY_MAX(_KEY_MAX), NO_VALUE(_VALUE_RESERVED), recmgr(new RecMgr(NUM_THREADS)) {
        const int tid = 0;
        initThread(tid);

        recmgr->endOp(tid); // enter an initial quiescent state.

        root = create_node(tid, KEY_MAX, NO_VALUE, NULL_NODEPTR, NULL_NODEPTR);
    }

    ~howley() {
        recmgr->printStatus();
        delete recmgr;
    }

    void initThread(const int tid) {
        if (init[tid]) return;
        else init[tid] = !init[tid];
        recmgr->initThread(tid);
    }

    void deinitThread(const int tid) {
        if (!init[tid]) return;
        else init[tid] = !init[tid];
        recmgr->deinitThread(tid);
    }

    sval_t bst_contains(const int tid, skey_t k);
    sval_t bst_add(const int tid, skey_t k, sval_t v);
    sval_t bst_remove(const int tid, skey_t k);

    node_t<skey_t, sval_t> * get_root() {
        return root;
    }
    
    RecMgr * debugGetRecMgr() {
        return recmgr;
    }
};

template <typename skey_t, typename sval_t, class RecMgr>
sval_t howley<skey_t, sval_t, RecMgr>::bst_contains(const int tid, skey_t k) {
    node_t<skey_t, sval_t> * pred, * curr;
    operation_t<skey_t, sval_t> * pred_op, * curr_op;
    auto res = bst_find(tid, k, &pred, &pred_op, &curr, &curr_op, root, root);
    if (res.code == FOUND) return res.val;
    return NO_VALUE;
}

template <typename skey_t, typename sval_t, class RecMgr>
find_result<sval_t> howley<skey_t, sval_t, RecMgr>::bst_find(const int tid, skey_t k, node_t<skey_t, sval_t> ** pred, operation_t<skey_t, sval_t> ** pred_op, node_t<skey_t, sval_t> ** curr, operation_t<skey_t, sval_t> ** curr_op, node_t<skey_t, sval_t> * aux_root, node_t<skey_t, sval_t> * root) {
    find_result<sval_t> result;
    skey_t curr_key;
    node_t<skey_t, sval_t> * next;
    node_t<skey_t, sval_t> * last_right;
    operation_t<skey_t, sval_t> * last_right_op;
retry:
    auto guard = recmgr->getGuard(tid, true);
    
    result.val = NO_VALUE;
    result.code = NOT_FOUND_R;
    *curr = aux_root;
    *curr_op = (*curr)->op;
    if (GETFLAG(*curr_op) != STATE_OP_NONE) {
        if (aux_root == root) {
            bst_help_child_cas(tid, UNFLAG(*curr_op), *curr);
            goto retry;
        } else {
            result.code = ABORT;
            return result;
        }
    }
    next = (*curr)->right;
    last_right = *curr;
    last_right_op = *curr_op;
    while (!ISNULL(next)) {
        *pred = *curr;
        *pred_op = *curr_op;
        *curr = next;
        *curr_op = (*curr)->op;

        if (GETFLAG(*curr_op) != STATE_OP_NONE) {
            bst_help(tid, *pred, *pred_op, *curr, *curr_op);
            goto retry;
        }
        curr_key = (*curr)->key;
        if (k < curr_key) {
            result.code = NOT_FOUND_L;
            next = (*curr)->left;
        } else if (k > curr_key) {
            result.code = NOT_FOUND_R;
            next = (*curr)->right;
            last_right = *curr;
            last_right_op = *curr_op;
        } else {
            result.val = (*curr)->value;
            result.code = FOUND;
            break;
        }
    }
    if ((!(result.code == FOUND)) && (last_right_op != last_right->op)) goto retry;
    if ((*curr)->op != *curr_op) goto retry;
    return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t howley<skey_t, sval_t, RecMgr>::bst_add(const int tid, skey_t k, sval_t v) {
    node_t<skey_t, sval_t> * pred;
    node_t<skey_t, sval_t> * curr;
    operation_t<skey_t, sval_t> * pred_op;
    operation_t<skey_t, sval_t> * curr_op;
    while (true) {
        auto guard = recmgr->getGuard(tid);
        
        auto result = bst_find(tid, k, &pred, &pred_op, &curr, &curr_op, root, root);
        if (result.code == FOUND) return result.val;
        bool is_left = (result.code == NOT_FOUND_L);
        auto new_node = create_node(tid, k, v, NULL_NODEPTR, NULL_NODEPTR);
        auto old = is_left ? curr->left : curr->right;
        auto cas_op = alloc_op(tid);
        cas_op->child_cas_op.is_left = is_left;
        cas_op->child_cas_op.expected = old;
        cas_op->child_cas_op.update = new_node;

        if (CASB(&curr->op, curr_op, FLAG(cas_op, STATE_OP_CHILDCAS))) {
            bst_help_child_cas(tid, cas_op, curr);
            return NO_VALUE;
        } else {
            recmgr->deallocate(tid, new_node);
            recmgr->deallocate(tid, cas_op);
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t howley<skey_t, sval_t, RecMgr>::bst_remove(const int tid, skey_t k) {
    node_t<skey_t, sval_t> * pred;
    node_t<skey_t, sval_t> * curr;
    node_t<skey_t, sval_t> * replace;
    operation_t<skey_t, sval_t> * pred_op;
    operation_t<skey_t, sval_t> * curr_op;
    operation_t<skey_t, sval_t> * replace_op;
    while (true) {
        auto guard = recmgr->getGuard(tid);
        
        auto result = bst_find(tid, k, &pred, &pred_op, &curr, &curr_op, root, root);
        if (!(result.code == FOUND)) return NO_VALUE;
        if (ISNULL(curr->right) || ISNULL(curr->left)) {
            if (CASB(&curr->op, curr_op, FLAG(curr_op, STATE_OP_MARK))) {
                bst_help_marked(tid, pred, pred_op, curr);
                recmgr->retire(tid, curr);
                return result.val;
            }
        } else {
            auto result2 = bst_find(tid, k, &pred, &pred_op, &replace, &replace_op, curr, root);
            if ((result2.code == ABORT) || (curr->op != curr_op)) continue;
            auto reloc_op = alloc_op(tid);
            reloc_op->relocate_op.state = STATE_OP_ONGOING;
            reloc_op->relocate_op.dest = curr;
            reloc_op->relocate_op.dest_op = curr_op;
            reloc_op->relocate_op.remove_key = k;
            reloc_op->relocate_op.remove_value = result.val;
            reloc_op->relocate_op.replace_key = replace->key;
            reloc_op->relocate_op.replace_value = replace->value;

            if (CASB(&replace->op, replace_op, FLAG(reloc_op, STATE_OP_RELOCATE))) {
                if (bst_help_relocate(tid, reloc_op, pred, pred_op, replace)) {
                    return result.val;
                }
                recmgr->retire(tid, reloc_op);
            } else {
                recmgr->deallocate(tid, reloc_op);
            }
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
bool howley<skey_t, sval_t, RecMgr>::bst_help_relocate(const int tid, operation_t<skey_t, sval_t> * op, node_t<skey_t, sval_t> * pred, operation_t<skey_t, sval_t> * pred_op, node_t<skey_t, sval_t> * curr) {
    int seen_state = op->relocate_op.state;
    if (seen_state == STATE_OP_ONGOING) {
        auto seen_op = CASV(&op->relocate_op.dest->op, op->relocate_op.dest_op, FLAG(op, STATE_OP_RELOCATE));
        if ((seen_op == op->relocate_op.dest_op) || (seen_op == FLAG(op, STATE_OP_RELOCATE))) {
            CASV(&op->relocate_op.state, STATE_OP_ONGOING, STATE_OP_SUCCESSFUL);
            seen_state = STATE_OP_SUCCESSFUL;
        } else {
            seen_state = CASV(&op->relocate_op.state, STATE_OP_ONGOING, STATE_OP_FAILED);
        }
    }
    if (seen_state == STATE_OP_SUCCESSFUL) {
        CASB(&op->relocate_op.dest->key, op->relocate_op.remove_key, op->relocate_op.replace_key);
        CASB(&op->relocate_op.dest->value, op->relocate_op.remove_value, op->relocate_op.replace_value);
        CASB(&op->relocate_op.dest->op, FLAG(op, STATE_OP_RELOCATE), FLAG(op, STATE_OP_NONE));
    }
    bool result = (seen_state == STATE_OP_SUCCESSFUL);
    if (op->relocate_op.dest == curr) return result;
    CASB(&curr->op, FLAG(op, STATE_OP_RELOCATE), FLAG(op, result ? STATE_OP_MARK : STATE_OP_NONE));
    if (result) {
        if (op->relocate_op.dest == pred) pred_op = FLAG(op, STATE_OP_NONE);
        bst_help_marked(tid, pred, pred_op, curr);
    }
    return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
void howley<skey_t, sval_t, RecMgr>::bst_help_child_cas(const int tid, operation_t<skey_t, sval_t> * op, node_t<skey_t, sval_t> * dest) {
    auto address = (op->child_cas_op.is_left) ? &dest->left : &dest->right;
    CASB(address, op->child_cas_op.expected, op->child_cas_op.update);
    if (CASB(&dest->op, FLAG(op, STATE_OP_CHILDCAS), FLAG(op, STATE_OP_NONE))) {
        recmgr->retire(tid, op);
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void howley<skey_t, sval_t, RecMgr>::bst_help_marked(const int tid, node_t<skey_t, sval_t> * pred, operation_t<skey_t, sval_t> * pred_op, node_t<skey_t, sval_t> * curr) {
    auto new_ref = ISNULL(curr->left) ? (ISNULL(curr->right) ? SETNULL(curr) : curr->right) : curr->left;
    auto cas_op = alloc_op(tid);
    cas_op->child_cas_op.is_left = (curr == pred->left);
    cas_op->child_cas_op.expected = curr;
    cas_op->child_cas_op.update = new_ref;
    if (CASB(&pred->op, pred_op, FLAG(cas_op, STATE_OP_CHILDCAS))) {
        bst_help_child_cas(tid, cas_op, pred);
    } else {
        recmgr->deallocate(tid, cas_op);
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void howley<skey_t, sval_t, RecMgr>::bst_help(const int tid, node_t<skey_t, sval_t> * pred, operation_t<skey_t, sval_t> * pred_op, node_t<skey_t, sval_t> * curr, operation_t<skey_t, sval_t> * curr_op) {
    if (GETFLAG(curr_op) == STATE_OP_CHILDCAS) {
        bst_help_child_cas(tid, UNFLAG(curr_op), curr);
    } else if (GETFLAG(curr_op) == STATE_OP_RELOCATE) {
        bst_help_relocate(tid, UNFLAG(curr_op), pred, pred_op, curr);
    } else if (GETFLAG(curr_op) == STATE_OP_MARK) {
        bst_help_marked(tid, pred, pred_op, curr);
    }
}

#endif /* HOWLEY_H */
