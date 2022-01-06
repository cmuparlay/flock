/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef DEBUG_INFO_H
#define	DEBUG_INFO_H

#include "plaf.h"

struct _memrecl_counters {
    PAD;
    long allocated;
    long deallocated;
    long fromPool;
    long toPool; // how many objects have been added to this pool
    long given; // how many blocks have been moved from this pool to a shared pool
    long taken; // how many blocks have been moved from a shared pool to this pool
    long retired; // how many objects have been retired
    PAD;
};

class debugInfo {
private:
    PAD;
    const int NUM_PROCESSES;
    _memrecl_counters c[MAX_THREADS_POW2];
    PAD;
public:
    void clear() {
        assert(NUM_PROCESSES > 0 && NUM_PROCESSES <= MAX_THREADS_POW2);
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            c[tid].allocated = 0;
            c[tid].deallocated = 0;
            c[tid].fromPool = 0;
            c[tid].toPool = 0;
            c[tid].given = 0;
            c[tid].taken = 0;
            c[tid].retired = 0;
        }
    }
    void addAllocated(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].allocated += val;
    }
    void addDeallocated(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].deallocated += val;
    }
    void addFromPool(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].fromPool += val;
    }
    void addToPool(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].toPool += val;
    }
    void addGiven(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].given += val;
    }
    void addTaken(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].taken += val;
    }
    void addRetired(const int tid, const int val) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        c[tid].retired += val;
    }
    long getAllocated(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].allocated;
    }
    long getDeallocated(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].deallocated;
    }
    long getFromPool(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].fromPool;
    }
    long getToPool(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].toPool;
    }
    long getGiven(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].given;
    }
    long getTaken(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].taken;
    }
    long getRetired(const int tid) {
        assert(tid >= 0 && tid < MAX_THREADS_POW2);
        return c[tid].retired;
    }
    long getTotalAllocated() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getAllocated(tid);
        }
        return result;
    }
    long getTotalDeallocated() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getDeallocated(tid);
        }
        return result;
    }
    long getTotalFromPool() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getFromPool(tid);
        }
        return result;
    }
    long getTotalToPool() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getToPool(tid);
        }
        return result;
    }
    long getTotalGiven() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getGiven(tid);
        }
        return result;
    }
    long getTotalTaken() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getTaken(tid);
        }
        return result;
    }
    long getTotalRetired() {
        long result = 0;
        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            result += getRetired(tid);
        }
        return result;
    }
    debugInfo(int numProcesses) : NUM_PROCESSES(numProcesses) {
//        c = new _memrecl_counters[numProcesses];
        clear();
    }
    ~debugInfo() {
//        delete[] c;
    }
};

#endif	/* DEBUG_INFO_H */

