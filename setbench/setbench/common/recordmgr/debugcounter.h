/* 
 * File:   debugcounter.h
 * Author: trbot
 *
 * Created on September 27, 2015, 4:43 PM
 */

#ifndef DEBUGCOUNTER_H
#define	DEBUGCOUNTER_H

#include <string>
#include <sstream>
#include "plaf.h"

class debugCounter {
private:
    PAD;
    const int NUM_PROCESSES;
    volatile long long * data; // data[tid*PREFETCH_SIZE_WORDS] = count for thread tid (padded to avoid false sharing)
    PAD;
public:
    void add(const int tid, const long long val) {
        data[tid*PREFETCH_SIZE_WORDS] += val;
    }
    void inc(const int tid) {
        add(tid, 1);
    }
    long long get(const int tid) {
        return data[tid*PREFETCH_SIZE_WORDS];
    }
    long long getTotal() {
        long long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += get(tid);
        }
        return result;
    }
    void clear() {
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            data[tid*PREFETCH_SIZE_WORDS] = 0;
        }
    }
    debugCounter(const int numProcesses) : NUM_PROCESSES(numProcesses) {
        data = (new long long[numProcesses*PREFETCH_SIZE_WORDS + PREFETCH_SIZE_WORDS]) + PREFETCH_SIZE_WORDS; /* HACKY OVER-ALLOCATION AND POINTER SHIFT TO ADD PADDING AT THE BEGINNING WITH MINIMAL ARITHEMTIC OPS */
        clear();
    }
    ~debugCounter() {
        delete[] (data - PREFETCH_SIZE_WORDS); /* HACKY OVER-ALLOCATION AND POINTER SHIFT TO ADD PADDING AT THE BEGINNING WITH MINIMAL ARITHEMTIC OPS */
    }
};

#endif	/* DEBUGCOUNTER_H */

