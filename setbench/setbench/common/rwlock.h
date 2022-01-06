/* 
 * File:   rwlock.h
 * Author: trbot
 *
 * Created on June 29, 2017, 8:25 PM
 */

#ifndef RWLOCK_H
#define RWLOCK_H

#ifdef RWLOCK_PTHREADS
#elif defined RWLOCK_FAVOR_WRITERS
#elif defined RWLOCK_FAVOR_READERS
#else
//    #warning "No RWLOCK implementation specified... using default: favour READERS. See rwlock.h for options. Note that this setting only affects algorithms that use the lock-based range query provider in common/rq/rq_rwlock.h."
    #define RWLOCK_FAVOR_READERS
//    #error Must specify RWLOCK implementation; see rwlock.h
#endif

#ifdef RWLOCK_PTHREADS

class RWLock {
private:
    pthread_rwlock_t lock;
    
public:
    RWLock() {
        if (pthread_rwlock_init(&lock, NULL)) setbench_error("could not init rwlock");
    }
    ~RWLock() {
        if (pthread_rwlock_destroy(&lock)) setbench_error("could not destroy rwlock");
    }
    inline void readLock() {
        if (pthread_rwlock_rdlock(&lock)) setbench_error("could not read-lock rwlock");
    }
    inline void readUnlock() {
        if (pthread_rwlock_unlock(&lock)) setbench_error("could not read-unlock rwlock");
    }
    inline void writeLock() {
        if (pthread_rwlock_wrlock(&lock)) setbench_error("could not write-lock rwlock");
    }
    inline void writeUnlock() {
        if (pthread_rwlock_unlock(&lock)) setbench_error("could not write-unlock rwlock");
    }
    inline bool isWriteLocked() {
        std::cout<<"ERROR: isWriteLocked() is not implemented"<<std::endl;
        exit(-1);
    }
    inline bool isReadLocked() {
        std::cout<<"ERROR: isReadLocked() is not implemented"<<std::endl;
        exit(-1);
    }
    inline bool isLocked() {
        std::cout<<"ERROR: isReadLocked() is not implemented"<<std::endl;
        exit(-1);
    }
};

#elif defined RWLOCK_FAVOR_WRITERS

class RWLock {
private:
    volatile long long lock; // two bit fields: [ number of readers ] [ writer bit ]
    
public:
    RWLock() {
        lock = 0;
    }
    inline bool isWriteLocked() {
        return lock & 1;
    }
    inline bool isReadLocked() {
        return lock & ~1;
    }
    inline bool isLocked() {
        return lock;
    }
    inline void readLock() {
        while (1) {
            while (isLocked()) {}
            if ((__sync_add_and_fetch(&lock, 2) & 1) == 0) return; // when we tentatively read-locked, there was no writer
            __sync_add_and_fetch(&lock, -2); // release our tentative read-lock
        }
    }
    inline void readUnlock() {
        __sync_add_and_fetch(&lock, -2);
    }
    inline void writeLock() {
        while (1) {
            long long v = lock;
            if (__sync_bool_compare_and_swap(&lock, v & ~1, v | 1)) {
                while (v & ~1) { // while there are still readers
                    v = lock;
                }
                return;
            }
        }
    }
    inline void writeUnlock() {
        __sync_add_and_fetch(&lock, -1);
    }
};

#elif defined RWLOCK_FAVOR_READERS

class RWLock {
private:
    volatile size_t lock; // two bit fields: [ number of readers ] [ writer bit ]
    
public:
    RWLock() {
        lock = 0;
    }
    inline void init() {
        lock = 0;
    }
    inline bool isWriteLocked() {
        return lock & 1;
    }
    inline bool isReadLocked() {
        return lock & ~3;
    }
    inline bool isUpgrading() {
        return lock & 2;
    }
    inline bool isLocked() {
        return lock;
    }
    inline void readLock() {
        __sync_add_and_fetch(&lock, 4);
        while (isWriteLocked());
        return;
    }
    inline void readUnlock() {
        __sync_add_and_fetch(&lock, -4);
    }
    // can call this only if you already read-locked.
    // upgrade the read lock to a write lock by first taking an upgrade bit (2)
    // and then (repeatedly) casing from an upgrader & zero readers (=2) to writer & zero readers (=1)
    // (if someone else takes the upgrade bit, we must give up and release our reader locks to prevent deadlock)
    inline bool upgradeLock() {
        while (1) {
            auto expval = lock;
            if (expval & 2) return false;
            auto seenval = __sync_val_compare_and_swap(&lock, expval, (expval - 4) | 2 /* subtract our reader count and covert to upgrader */);
            if (seenval == expval) { // cas success
                // cas to writer
                while (1) {
                    while (lock & ~2 /* locked by someone else */) {}
                    if (__sync_bool_compare_and_swap(&lock, 2, 1)) {
                        return true;
                    }
                }
            }
        }
    }
//    inline void upgradeAbort() { // probably works, but is not needed
//        while (1) {
//            auto expval = lock;
//            if (__sync_bool_compare_and_swap(&lock, expval, expval & ~2)) {
//                return;
//            }
//        }
//    }
    inline void writeLock() {
        while (1) {
            while (isLocked()) {}
            if (__sync_bool_compare_and_swap(&lock, 0, 1)) {
                return;
            }
        }
    }
    inline void writeUnlock() {
        __sync_add_and_fetch(&lock, -1);
    }
};

#endif

#endif /* RWLOCK_H */

