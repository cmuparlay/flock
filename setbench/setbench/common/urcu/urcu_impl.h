/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   urcu_impl.h
 * Author: Maya Arbel-Raviv
 *
 * Created on June 10, 2017, 12:09 PM
 */

#ifndef URCU_IMPL_H
#define URCU_IMPL_H

#include "urcu.h"
#include "tsc.h"

namespace urcu {

int threads;
volatile char padding0[256];
rcu_node** urcu_table;
volatile char padding1[256];

__thread long* times; 
__thread int i; 

void init(const int numThreads) {
    rcu_node** result = (rcu_node**) malloc(sizeof(rcu_node*) * numThreads);
    int i;
    rcu_node* nnode;
    threads = numThreads;
    for (i = 0; i < threads; i++) {
        nnode = (rcu_node*) malloc(sizeof (rcu_node));
        nnode->time = 1;
        result[i] = nnode;
    }
    urcu_table = result;
    printf("initializing URCU finished, node_size: %zd\n", sizeof (rcu_node));
    return;
}

void deinit(const int numThreads) {
    for (int i=0;i<numThreads;++i) {
        free(urcu_table[i]);
    }
    free(urcu_table);
}

void registerThread(int id) {
    times = (long*) malloc(sizeof (long)*threads);
    i = id;
    if (times == NULL) {
        printf("malloc failed\n");
        exit(1);
    }
}

void unregisterThread() {
    free(times);
    times = NULL;
}

void readLock() {
    assert(urcu_table[i] != NULL);
#ifdef RCU_USE_TSC
    __sync_lock_test_and_set(&urcu_table[i]->time, read_tsc() << 1);
#else
    __sync_add_and_fetch(&urcu_table[i]->time, 1);
#endif
}

static inline void set_bit(int nr, volatile unsigned long *addr) {
    asm("btsl %1,%0" : "+m" (*addr) : "Ir" (nr));
}

void readUnlock() {
    assert(urcu_table[i] != NULL);
    set_bit(0, &urcu_table[i]->time);
}



#ifdef RCU_USE_TSC

void synchronize() {

    /* fence to order against previous writes by updater, and the
     * subsequent loads.
     */
    asm volatile("mfence":: : "memory");

    uint64_t now = read_tsc() << 1;
    int i;
    for (i = 0; i < threads; i++) {
        while (1) {
            unsigned long t = urcu_table[i]->time;
            if (t & 1 || t > now) {
                break;
            }
        }
    }
}

#else

void synchronize() {
    int i;
    //read old counters
    for (i = 0; i < threads; i++) {
        times[i] = urcu_table[i]->time;
    }
    for (i = 0; i < threads; i++) {
        if (times[i] & 1) continue;
        while (1) {
            unsigned long t = urcu_table[i]->time;
            if (t & 1 || t > times[i]) {
                break;
            }
        }
    }
}

#endif  /* RCU_USE_TSC */

};

#endif /* URCU_IMPL_H */

