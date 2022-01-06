/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 * 
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef ALLOC_PARLAY_H
#define	ALLOC_PARLAY_H

#include "plaf.h"
#include "pool_interface.h"
#include <cstdlib>
#include <cassert>
#include <iostream>
#include <parlay/alloc.h>

//__thread long long currentAllocatedBytes = 0;
//__thread long long maxAllocatedBytes = 0;

template<typename T = void>
class allocator_parlay : public allocator_interface<T> {
    // parlay::type_allocator<T> allocator;
    PAD; // post padding for allocator_interface
public:
    template<typename _Tp1>
    struct rebind {
        typedef allocator_parlay<_Tp1> other;
    };
    
    // reserve space for ONE object of type T
    T* allocate(const int tid) {
        T* newv = parlay::type_allocator<T>::alloc();
        new (newv) T();
        return newv;
    }
    void deallocate(const int tid, T * const p) {
#if !defined NO_FREE
        p->~T();
        parlay::type_allocator<T>::free(p);
#endif
    }
    void deallocateAndClear(const int tid, blockbag<T> * const bag) {
#ifdef NO_FREE
        bag->clearWithoutFreeingElements();
#else
        while (!bag->isEmpty()) {
            T* ptr = bag->remove();
            deallocate(tid, ptr);
        }
#endif
    }
    
    void debugPrintStatus(const int tid) {
//        std::cout<</*"thread "<<tid<<" "<<*/"allocated "<<this->debug->getAllocated(tid)<<" objects of size "<<(sizeof(T));
//        std::cout<<" ";
////        this->pool->debugPrintStatus(tid);
//        std::cout<<std::endl;
    }
    
    void initThread(const int tid) {}
    void deinitThread(const int tid) {}
    
    allocator_parlay(const int numProcesses, debugInfo * const _debug)
            : allocator_interface<T>(numProcesses, _debug) {
        VERBOSE DEBUG std::cout<<"constructor allocator_parlay"<<std::endl;
    }
    ~allocator_parlay() {
        VERBOSE DEBUG std::cout<<"destructor allocator_parlay"<<std::endl;
    }
};

#endif	/* ALLOC_NEW_H */

