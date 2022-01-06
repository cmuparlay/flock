/*
 * File:   multi_counter.h
 * Author: tbrown
 *
 * Created on August 15, 2018, 5:55 PM
 */

#ifndef MULTI_COUNTER_H
#define MULTI_COUNTER_H

#include "plaf.h"
#include <cstdlib>

struct SingleCounter {
    union {
        PAD;
        volatile size_t v;
    };
};

class MultiCounter {
private:
    PAD;
    SingleCounter * const counters;
    const int numCounters;
    PAD;
public:
    MultiCounter(const int numThreads, const int sizeMultiple)
            : counters(new SingleCounter[std::max(2, sizeMultiple*numThreads)+1]) // allocate one extra entry (don't use first entry---to effectively add padding at the start of the array)
            , numCounters(std::max(2, sizeMultiple*numThreads)) {
//        GSTATS_ADD(tid, num_multi_counter_array_created, 1);
//        counters = counters + 1;                                                // shift by +1 (don't use first entry---to effectively add padding at the start of the array)
        for (int i=0;i<numCounters+1;++i) {
            counters[i].v = 0;
        }
    }
    ~MultiCounter() {
        //printf("called destructor ~MultiCounter\n");
//        GSTATS_ADD(tid, num_multi_counter_array_reclaimed, 1);
        delete[] counters;
//        delete[] (counters - 1);                                                // shift by -1 (don't use first entry---to effectively add padding at the start of the array)
    }
    inline size_t inc(const int tid, Random64 * rng, const size_t amt = 1) {
        const int i = rng->next(numCounters);
        int j;
        do {
            j = rng->next(numCounters);
        } while (i == j);
        size_t vi = counters[1+i].v;
        size_t vj = counters[1+j].v;
        return FAA((vi < vj ? &counters[1+i].v : &counters[1+j].v), amt) + amt;
    }
    inline size_t readFast(const int tid, Random64 * rng) {
        const int i = rng->next(numCounters);
        return numCounters * counters[1+i].v;
    }
    size_t readAccurate() {
        size_t sum = 0;
        for (int i=0;i<numCounters;++i) {
            sum += counters[1+i].v;
        }
        return sum;
    }
};

#endif /* MULTI_COUNTER_H */

