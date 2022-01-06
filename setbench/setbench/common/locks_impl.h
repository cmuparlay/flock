/* 
 * File:   locks_impl.h
 * Author: trbot
 *
 * Created on June 26, 2017, 7:51 PM
 */

#ifndef LOCKS_IMPL_H
#define LOCKS_IMPL_H

static void acquireLock(volatile int *lock) {
    while (1) {
        if (*lock) {
            __asm__ __volatile__("pause;");
            continue;
        }
        if (__sync_bool_compare_and_swap(lock, false, true)) {
            return;
        }
    }
}

static void releaseLock(volatile int *lock) {
    *lock = false;
}

static bool readLock(volatile int *lock) {
    return *lock;
}

#endif /* LOCKS_IMPL_H */

