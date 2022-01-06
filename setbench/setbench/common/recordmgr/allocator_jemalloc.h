// note: this does not work, because jemalloc's current release relies on a static TLS block that we can't allocate if we load the library this way... also, loading allocators this way is probably dangerous without ensuring that all data structures carefully use allocate() instead of new. worse, using allocate() is infeasible if you have nodes with variables sizes, as in the istree... the record manager probably needs a redesign to be based on object sizes, rather than types. of course, the size would have to be encoded somewhere...

///**
// * Wrapper for jemalloc
// * Copyright (C) 2019 Trevor Brown
// */
//
//#ifndef ALLOC_JEMALLOC_H
//#define	ALLOC_JEMALLOC_H
//
//#include "plaf.h"
//#include "pool_interface.h"
//#include <cstdlib>
//#include <cassert>
//#include <iostream>
//#include <dlfcn.h>
//#include <pthread.h>
//
//template<typename T = void>
//class allocator_jemalloc : public allocator_interface<T> {
//private:
////    PAD; // not needed after superclass layout
//    void* (*allocfn)(size_t size);
//    void (*freefn)(void *ptr);
//    PAD;
//    
//public:
//    template<typename _Tp1>
//    struct rebind {
//        typedef allocator_jemalloc<_Tp1> other;
//    };
//    
//    // reserve space for ONE object of type T
//    T* allocate(const int tid) {
//        // allocate a new object
//        MEMORY_STATS {
//            this->debug->addAllocated(tid, 1);
//            VERBOSE {
//                if ((this->debug->getAllocated(tid) % 2000) == 0) {
//                    debugPrintStatus(tid);
//                }
//            }
//        }
//        return (T*) allocfn(sizeof(T));
//    }
//    void deallocate(const int tid, T * const p) {
//        MEMORY_STATS this->debug->addDeallocated(tid, 1);
//#if !defined NO_FREE
//        p->~T(); // explicitly call destructor, since we lose automatic destructor calls when we bypass new/delete([])
//        freefn(p);
//#endif
//    }
//    void deallocateAndClear(const int tid, blockbag<T> * const bag) {
//#if defined NO_FREE
//        bag->clearWithoutFreeingElements();
//#else
//        while (!bag->isEmpty()) {
//            T* ptr = bag->remove();
//            deallocate(tid, ptr);
//        }
//#endif
//    }
//    
//    void debugPrintStatus(const int tid) {}
//    
//    void initThread(const int tid) {}
//    
//    static void* dummy_thr(void *p) { return 0; }
//    
//    allocator_jemalloc(const int numProcesses, debugInfo * const _debug)
//            : allocator_interface<T>(numProcesses, _debug) {
//        VERBOSE DEBUG std::cout<<"constructor allocator_jemalloc"<<std::endl;
//        
//        const char * lib = "../lib/libjemalloc.so";
//	void *h = dlopen(lib, RTLD_NOW | RTLD_LOCAL);
//	if (!h) {
//		fprintf(stderr, "unable to load '%s': %s\n", lib, dlerror());
//		exit(1);
//	}
//
//	// If the allocator exports pthread_create(), we assume it does so to detect
//	// multi-threading (through interposition on pthread_create()) and so call
//	// this function (since it might not be called otherwise, if the standard
//	// allocator does a similar trick).
//	int (*pthread_create)(pthread_t *, const pthread_attr_t *, void *(*) (void *), void *);
//	pthread_create = (__typeof(pthread_create)) dlsym(h, "pthread_create");
//	if (pthread_create) {
//		pthread_t thr;
//		pthread_create(&thr, NULL, dummy_thr, NULL);
//		pthread_join(thr, NULL);
//	}
//	allocfn = (__typeof(allocfn)) dlsym(h, "malloc");
//	if (!allocfn) {
//		fprintf(stderr, "unable to resolve malloc\n");
//		exit(1);
//	}
//	freefn = (__typeof(freefn)) dlsym(h, "free");
//	if (!freefn) {
//		fprintf(stderr, "unable to resolve free\n");
//		exit(1);
//	}
//        std::cout<<"[[loaded libjemalloc.so]]"<<std::endl;
//    }
//    ~allocator_jemalloc() {
//        VERBOSE DEBUG std::cout<<"destructor allocator_jemalloc"<<std::endl;
//    }
//};
//
//#endif	/* ALLOC_JEMALLOC_H */
//
