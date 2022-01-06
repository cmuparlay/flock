/**
 * Simple intrusive lock-free stack using preexisting block objects.
 * operates on elements of the block<T> type defined in blockbag.h.
 * this class does NOT allocate any memory.
 * instead, it simply chains existing blocks together using their next pointers.
 * 
 * Copyright (C) 2019 Trevor Brown
 */

#ifndef LFBSTACK_H
#define	LFBSTACK_H

#include <atomic>
#include <iostream>
#include "blockbag.h"

template <typename T>
class lfbstack {
private:
    PAD;
    block<T> * volatile head;
    PAD;
public:
    lfbstack() : head(NULL) {}
    block<T> * getBlock() {
        while (true) {
            auto exp = head;
            if (!exp) return NULL;
            if (__sync_bool_compare_and_swap(&head, exp, exp->next)) {
                exp->next = NULL;
                return exp;
            }
        }
    }
    int addBlock(block<T> * b) { // returns size
        while (true) {
            b->next = head;
            auto sz = b->nextCount = (b->next ? b->next->nextCount + 1: 0);
            if (__sync_bool_compare_and_swap(&head, b->next, b)) return sz;
        }
    }
    int sizeInBlocks() {
        auto h = head;
        return (h ? h->nextCount + 1: 0);
    }
};

#endif	/* LFBSTACK_H */
