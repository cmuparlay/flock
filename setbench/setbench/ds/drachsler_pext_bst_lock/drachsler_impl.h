/*
 *   File: bst-drachsler.c
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: Dana Drachsler, Martin Vechev, and Eran Yahav.
 *   Practical Concurrent Binary Search Trees via Logical Ordering. PPoPP 2014.
 *   bst-drachsler.c is part of ASCYLIB
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
 * File:   drachsler.h
 * Author: Trevor Brown
 *
 * Substantial improvements to interface, memory reclamation and bug fixing.
 *
 * Sadly, there still *seems* to be a bug that is a holdover from ASCYLIB...
 * i think it involving lock acquisition and deadlock, or at least highly aggressive livelock...
 *
 * symptom: 16000 inserts in TOTAL by 256 threads takes almost TEN SECONDS... this is not reasonable.
 * This is THOUSANDS of TIMES slower than single threaded!
 *
 * Command to reproduce (might need MANY threads, and to run a few times, to expose it...):
 *      LD_PRELOAD=../../setbench/lib/libmimalloc.so ./drachsler_pext_bst_lock.none -nwork 256 -nprefill 256 -insdel 0 0 -k 2000 -t 100 -pin 0-63,128-191,64-127,192-255
 *
 * Created on June 7, 2017, 3:25 PM
 */

#ifndef DANA_H
#define DANA_H

#include <pthread.h>
#include "record_manager.h"

#define FIELDS_ORDER
#define SPIN_LOCK
#define USE_PADDING

#ifndef SPIN_LOCK
#error only spin lock is currently supported
#endif

typedef pthread_spinlock_t ptlock_t;
#define PTLOCK_SIZE         sizeof(ptlock_t)
#define INIT_LOCK(lock)     pthread_spin_init(lock, PTHREAD_PROCESS_PRIVATE);
#define DESTROY_LOCK(lock)  pthread_spin_destroy(lock)
#define LOCK(lock)          pthread_spin_lock(lock)
#define UNLOCK(lock)        pthread_spin_unlock(lock)
#define TRYLOCK(lock)       pthread_spin_trylock(lock)

#ifdef BASELINE //make sure that small key is defined and no padding
    #ifdef USE_PADDING
        #undef USE_PADDING
    #endif
    #ifdef FIELDS_ORDER
        #undef FIELDS_ORDER
    #endif
#endif


#define ALIGNED(N) __attribute__ ((aligned (N)))

typedef uint8_t bool_t;

template <typename skey_t, typename sval_t>
struct node_t {
#ifdef USE_PADDING
    union {
        volatile char pad[192];
        struct {
#endif
    #ifdef FIELDS_ORDER
        skey_t key;
        #ifndef NO_VOLATILE
            node_t<skey_t, sval_t>* volatile left;
            node_t<skey_t, sval_t>* volatile right;
            node_t<skey_t, sval_t>* volatile succ;
            node_t<skey_t, sval_t>* volatile pred;
        #else
            volatile node_t<skey_t, sval_t>* left;
            volatile node_t<skey_t, sval_t>* right;
            volatile node_t<skey_t, sval_t>* succ;
            volatile node_t<skey_t, sval_t>* pred;
        #endif
        bool_t mark;
        sval_t value;
        node_t<skey_t, sval_t>* volatile parent;
        ptlock_t tree_lock;
        ptlock_t succ_lock;
    #else
        volatile node_t<skey_t, sval_t>* left;
        volatile node_t<skey_t, sval_t>* right;
        volatile node_t<skey_t, sval_t>* parent;
        volatile node_t<skey_t, sval_t>* succ;
        volatile node_t<skey_t, sval_t>* pred;
        ptlock_t tree_lock;
        ptlock_t succ_lock;
        skey_t key;
        sval_t value;
        bool_t mark;
    #endif
#ifdef USE_PADDING
        };
    };
#endif
#ifdef BASELINE
    char padding[96-5*sizeof(uintptr_t)-2*sizeof(ptlock_t)-sizeof(sval_t)-sizeof(skey_t)-sizeof(bool_t)];
  #endif
};

template <typename skey_t, typename sval_t, class RecMgr>
class drachsler {
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
    int init[MAX_THREADS_POW2] = {0,};
PAD;

    node_t<skey_t, sval_t>* create_node(const int tid, skey_t k, sval_t value, int initializing);
    node_t<skey_t, sval_t>* initialize_tree(const int tid);
    node_t<skey_t, sval_t>* bst_search(const int tid, skey_t key);
    node_t<skey_t, sval_t>* choose_parent(const int tid, node_t<skey_t, sval_t>* pred, node_t<skey_t, sval_t>* succ, node_t<skey_t, sval_t>* first_cand);
    void insert_to_tree(const int tid, node_t<skey_t, sval_t>* parent, node_t<skey_t, sval_t>* new_node);
    node_t<skey_t, sval_t>* lock_parent(const int tid, node_t<skey_t, sval_t>* node);
    bool_t acquire_tree_locks(const int tid, node_t<skey_t, sval_t>* n);
    void remove_from_tree(const int tid, node_t<skey_t, sval_t>* n, bool_t has_two_children);
    void update_child(const int tid, node_t<skey_t, sval_t> *parent, node_t<skey_t, sval_t>* old_ch, node_t<skey_t, sval_t>* new_ch);

public:

    drachsler(const int _NUM_THREADS, const skey_t& _KEY_MIN, const skey_t& _KEY_MAX, const sval_t& _VALUE_RESERVED, unsigned int id)
    : NUM_THREADS(_NUM_THREADS), KEY_MIN(_KEY_MIN), KEY_MAX(_KEY_MAX), NO_VALUE(_VALUE_RESERVED), idx_id(id), recmgr(new RecMgr(NUM_THREADS)) {
        const int tid = 0;
        initThread(tid);

        recmgr->endOp(tid); // enter an initial quiescent state.
        root = initialize_tree(tid);
    }

    ~drachsler() {
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
    sval_t bst_insert(const int tid, skey_t k, sval_t v, bool onlyIfAbsent = true);
    sval_t bst_remove(const int tid, skey_t key);

    node_t<skey_t, sval_t> * get_root() {
        return root;
    }

    RecMgr * debugGetRecMgr() {
        return recmgr;
    }
};

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * drachsler<skey_t, sval_t, RecMgr>::create_node(const int tid, skey_t k, sval_t value, int initializing) {
    auto new_node = recmgr->template allocate<node_t<skey_t, sval_t >> (tid);
    if (new_node == NULL) {
        perror("malloc in bst create node");
        exit(1);
    }
    new_node->left = NULL;
    new_node->right = NULL;
    new_node->parent = NULL;
    new_node->succ = NULL;
    new_node->pred = NULL;
    INIT_LOCK(&(new_node->tree_lock));
    INIT_LOCK(&(new_node->succ_lock));
    new_node->key = k;
    new_node->value = value;
    new_node->mark = false;
    //asm volatile("" :: : "memory");
    return new_node;
}

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * drachsler<skey_t, sval_t, RecMgr>::initialize_tree(const int tid) {
    node_t<skey_t, sval_t> * parent = create_node(tid, KEY_MIN, NO_VALUE, 1);
    node_t<skey_t, sval_t> * root = create_node(tid, KEY_MAX, NO_VALUE, 1);
    root->pred = parent;
    root->succ = parent;
    root->parent = parent;
    parent->right = root;
    parent->succ = root;
    return root;
}

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * drachsler<skey_t, sval_t, RecMgr>::bst_search(const int tid, skey_t k) {
    node_t<skey_t, sval_t> * n = root;
    node_t<skey_t, sval_t> * child;
    skey_t curr_key;
    while (1) {
        curr_key = n->key;
        if (curr_key == k) {
            return n;
        }
        if (curr_key < k) {
            child = n->right;
        } else {
            child = n->left;
        }
        if (child == NULL) {
            return n;
        }
        n = child;
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t drachsler<skey_t, sval_t, RecMgr>::bst_contains(const int tid, skey_t k) {
    auto guard = recmgr->getGuard(tid, true);

    auto n = bst_search(tid, k);
    while (n->key > k) {
        n = n->pred;
    }
    while (n->key < k) {
        n = n->succ;
    }
    if ((n->key == k) && (n->mark == false)) {
        return n->value;
    }
    return NO_VALUE;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t drachsler<skey_t, sval_t, RecMgr>::bst_insert(const int tid, skey_t k, sval_t v, bool onlyIfAbsent) {
    while (1) {
        auto guard = recmgr->getGuard(tid);

        auto node = bst_search(tid, k);
#if defined(NO_VOLATILE) || defined(BASELINE)
        volatile node_t<skey_t, sval_t> * p;
#else
        node_t<skey_t, sval_t> * p;
#endif
        if (node->key >= k) {
            p = node->pred;
        } else {
            p = node;
        }

        // if (onlyIfAbsent) {
        //     auto n = node;
        //     while (n->key > k) {
        //         n = n->pred;
        //     }
        //     while (n->key < k) {
        //         n = n->succ;
        //     }
        //     if ((n->key == k) && (n->mark == false)) {
        //         return n->value;
        //     }
        // }

        LOCK(&(p->succ_lock));
#if defined(NO_VOLATILE) || defined(BASELINE)
        volatile node_t<skey_t, sval_t> * s = p->succ;
#else
        auto s = p->succ;
#endif
        if ((k > p->key) && (k <= s->key) && (p->mark == false)) {
            if (s->key == k) {
                sval_t res = s->value;
                if (!onlyIfAbsent) {
                    s->value = v; // actually set the new value!
                }
                UNLOCK(&(p->succ_lock));
                return res;
            }
            auto new_node = create_node(tid, k, v, 0);
            node_t<skey_t, sval_t> * parent = choose_parent(tid, p, s, node);
            new_node->succ = s;
            new_node->pred = p;
            new_node->parent = parent;
#ifdef __tile__
            MEM_BARRIER;
#endif
            SOFTWARE_BARRIER;
            p->succ = new_node;
            SOFTWARE_BARRIER;
            s->pred = new_node;
            UNLOCK(&(p->succ_lock));
            insert_to_tree(tid, parent, new_node);
            return NO_VALUE;
        }
        UNLOCK(&(p->succ_lock));
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * drachsler<skey_t, sval_t, RecMgr>::choose_parent(const int tid, node_t<skey_t, sval_t> * p, node_t<skey_t, sval_t> * s, node_t<skey_t, sval_t> * first_cand) {
    node_t<skey_t, sval_t> * candidate;
    if ((first_cand == p) || (first_cand == s)) {
        candidate = first_cand;
    } else {
        candidate = p;
    }
    while (1) {
        LOCK(&(candidate->tree_lock));
        if (candidate == p) {
            if (candidate->right == NULL) {
                return candidate;
            }
            UNLOCK(&(candidate->tree_lock));
            candidate = s;
        } else {
            if (candidate->left == NULL) {
                return candidate;
            }
            UNLOCK(&(candidate->tree_lock));
            candidate = p;
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void drachsler<skey_t, sval_t, RecMgr>::insert_to_tree(const int tid, node_t<skey_t, sval_t> * parent, node_t<skey_t, sval_t> * new_node) {
    new_node->parent = parent;
    if (parent->key < new_node->key) {
        parent->right = new_node;
    } else {
        parent->left = new_node;
    }

    UNLOCK(&(parent->tree_lock));
}

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * drachsler<skey_t, sval_t, RecMgr>::lock_parent(const int tid, node_t<skey_t, sval_t> * node) {
    while (1) {
        auto p = node->parent;
        LOCK(&(p->tree_lock));
        if ((node->parent == p) && (p->mark == false)) {
            return p;
        }
        UNLOCK(&(p->tree_lock));
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t drachsler<skey_t, sval_t, RecMgr>::bst_remove(const int tid, skey_t k) {
    while (1) {
        auto guard = recmgr->getGuard(tid);

        auto node = bst_search(tid, k);
        node_t<skey_t, sval_t> * p;
        if (node->key >= k) {
            p = node->pred;
        } else {
            p = node;
        }

#if DRACHSLER_RO_FAIL == 1
        auto n = node;
        while (n->key > k) {
            n = n->pred;
        }
        while (n->key < k) {
            n = n->succ;
        }
        if ((n->key != k) && (n->mark == false)) {
            return NO_VALUE;
        }
#endif

        LOCK(&(p->succ_lock));
        auto s = p->succ;
        if ((k > p->key) && (k <= s->key) && (p->mark == false)) {
            if (s->key > k) {
                UNLOCK(&(p->succ_lock));
                return NO_VALUE;
            }
            LOCK(&(s->succ_lock));
            bool_t has_two_children = acquire_tree_locks(tid, s);
            lock_parent(tid, s);
            s->mark = true;
            auto s_succ = s->succ;
            s_succ->pred = p;
            p->succ = s_succ;
            sval_t v = s->value;
            UNLOCK(&(s->succ_lock));
            UNLOCK(&(p->succ_lock));
            remove_from_tree(tid, s, has_two_children);
            return v;
        }
        UNLOCK(&(p->succ_lock));
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
bool_t drachsler<skey_t, sval_t, RecMgr>::acquire_tree_locks(const int tid, node_t<skey_t, sval_t> * n) {
    while (1) {
        LOCK(&(n->tree_lock));
        auto left = n->left;
        auto right = n->right;
        //lock_parent(n);
        if ((right == NULL) || (left == NULL)) {
            return false;
        } else {
            auto s = n->succ;
            int l = 0;
            node_t<skey_t, sval_t> * parent;
            auto sp = s->parent;
            if (sp != n) {
                parent = sp;
                //TRYLOCK failure returns non-zero value!
                //if (trylock failure) {...}
                if (TRYLOCK(&(parent->tree_lock)) != 0) {
                    UNLOCK(&(n->tree_lock));
                    //UNLOCK(&(n->parent->tree_lock));
                    continue;
                }
                l = 1;
                if ((parent != s->parent) || (parent->mark == true)) {
                    UNLOCK(&(n->tree_lock));
                    UNLOCK(&(parent->tree_lock));
                    //UNLOCK(&(n->parent->tree_lock));
                    continue;
                }
            }
            //TRYLOCK failure returns non-zero value!
            //if (trylock failure) {...}
            if (TRYLOCK(&(s->tree_lock)) != 0) {
                UNLOCK(&(n->tree_lock));
                //UNLOCK(&(n->parent->tree_lock));
                if (l) {
                    UNLOCK(&(parent->tree_lock));
                }
                continue;
            }
            return true;
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void drachsler<skey_t, sval_t, RecMgr>::remove_from_tree(const int tid, node_t<skey_t, sval_t> * n, bool_t has_two_children) {
    node_t<skey_t, sval_t> * child;
    node_t<skey_t, sval_t> * parent;
    node_t<skey_t, sval_t> * s;
    //int l=0;
    if (has_two_children == false) {
        if (n->right == NULL) {
            child = n->left;
        } else {
            child = n->right;
        }
        parent = n->parent;
        update_child(tid, parent, n, child);
    } else {
        s = n->succ;
        child = s->right;
        parent = s->parent;
        //if (parent != n ) l=1;
        update_child(tid, parent, s, child);
        s->left = n->left;
        s->right = n->right;
        n->left->parent = s;
        if (n->right != NULL) {
            n->right->parent = s;
        }
        update_child(tid, n->parent, n, s);
        if (parent == n) {
            parent = s;
        } else {
            UNLOCK(&(s->tree_lock));
        }
        UNLOCK(&(parent->tree_lock));
    }
    UNLOCK(&(n->parent->tree_lock));
    UNLOCK(&(n->tree_lock));

    recmgr->retire(tid, n);
}

template <typename skey_t, typename sval_t, class RecMgr>
void drachsler<skey_t, sval_t, RecMgr>::update_child(const int tid, node_t<skey_t, sval_t> * parent, node_t<skey_t, sval_t> * old_ch, node_t<skey_t, sval_t> * new_ch) {
    if (parent->left == old_ch) {
        parent->left = new_ch;
    } else {
        parent->right = new_ch;
    }
    if (new_ch != NULL) {
        new_ch->parent = parent;
    }
}

#endif /* DANA_H */
