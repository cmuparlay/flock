/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECORD_MANAGER_SINGLE_TYPE_H
#define	RECORD_MANAGER_SINGLE_TYPE_H

#include <pthread.h>
#include <errno.h>
#include <cstring>
#include <iostream>
#include <typeinfo>

#include "plaf.h"
#include "debug_info.h"
#include "globals.h"

#include "recovery_manager.h"

// include DEFAULT reclaimer, allocator, pool
#include "reclaimer_debra.h"
#include "allocator_new.h"
#include "pool_none.h"

// others must be included by the user

// #include "allocator_interface.h"
// #include "allocator_bump.h"
// #include "allocator_new.h"
// //#include "allocator_new_segregated.h"
// #include "allocator_once.h"

// #include "pool_interface.h"
// #include "pool_none.h"
// #include "pool_perthread_and_shared.h"
// #ifdef USE_LIBNUMA
// #include "pool_numa.h"
// #endif

// #include "reclaimer_interface.h"
// #include "reclaimer_none.h"
// #include "reclaimer_ebr_tree.h"
// #include "reclaimer_ebr_token.h"
// //#include "reclaimer_ebr_tree_q.h"
// //#include "reclaimer_numa_ebr_tree_q.h"
// #include "reclaimer_debra.h"
// #include "reclaimer_debracap.h"
// #include "reclaimer_debraplus.h"
// #include "reclaimer_hazardptr.h"
// #ifdef USE_RECLAIMER_RCU
// #include "reclaimer_rcu.h"
// #endif

// maybe Record should be a size
template <typename Record, class Reclaim, class Alloc, class Pool>
class record_manager_single_type {
protected:
    typedef Record* record_pointer;

    typedef typename Alloc::template    rebind<Record>::other              classAlloc;
    typedef typename Pool::template     rebind2<Record, classAlloc>::other classPool;
    typedef typename Reclaim::template  rebind2<Record, classPool>::other  classReclaim;

public:
    PAD;
    classAlloc      *alloc;
    classPool       *pool;
    classReclaim    *reclaim;

    const int NUM_PROCESSES;
    debugInfo debugInfoRecord;
    RecoveryMgr<void *> * const recoveryMgr;
    PAD;

    record_manager_single_type(const int numProcesses, RecoveryMgr<void *> * const _recoveryMgr)
            : NUM_PROCESSES(numProcesses), debugInfoRecord(debugInfo(numProcesses)), recoveryMgr(_recoveryMgr) {
        VERBOSE DEBUG COUTATOMIC("constructor record_manager_single_type"<<std::endl);
        alloc = new classAlloc(numProcesses, &debugInfoRecord);
        pool = new classPool(numProcesses, alloc, &debugInfoRecord);
        reclaim = new classReclaim(numProcesses, pool, &debugInfoRecord, recoveryMgr);
    }
    ~record_manager_single_type() {
        VERBOSE DEBUG COUTATOMIC("destructor record_manager_single_type"<<std::endl);
        delete reclaim;
        delete pool;
        delete alloc;
    }

    void initThread(const int tid) {
        alloc->initThread(tid);
        pool->initThread(tid);
        reclaim->initThread(tid);
//        endOp(tid);
    }

    void deinitThread(const int tid) {
        reclaim->deinitThread(tid);
        pool->deinitThread(tid);
        alloc->deinitThread(tid);
    }

    inline void clearCounters() {
        debugInfoRecord.clear();
    }

    inline static bool shouldHelp() { // FOR DEBUGGING PURPOSES
        return Reclaim::shouldHelp();
    }
    inline bool isProtected(const int tid, record_pointer obj) {
        return reclaim->isProtected(tid, obj);
    }
    // for hazard pointers (and reference counting)
    inline bool protect(const int tid, record_pointer obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool hintMemoryBarrier = true) {
        return reclaim->protect(tid, obj, notRetiredCallback, callbackArg, hintMemoryBarrier);
    }
    inline void unprotect(const int tid, record_pointer obj) {
        reclaim->unprotect(tid, obj);
    }
    // warning: qProtect must be reentrant and lock-free (=== async-signal-safe)
    inline bool qProtect(const int tid, record_pointer obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool hintMemoryBarrier = true) {
        return reclaim->qProtect(tid, obj, notRetiredCallback, callbackArg, hintMemoryBarrier);
    }
    inline void qUnprotectAll(const int tid) {
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        reclaim->qUnprotectAll(tid);
    }
    inline bool isQProtected(const int tid, record_pointer obj) {
        return reclaim->isQProtected(tid, obj);
    }

    inline static bool supportsCrashRecovery() {
        return Reclaim::supportsCrashRecovery();
    }
    inline static bool quiescenceIsPerRecordType() {
        return Reclaim::quiescenceIsPerRecordType();
    }
    inline bool isQuiescent(const int tid) {
        return reclaim->isQuiescent(tid);
    }

    // for epoch based reclamation
    inline void endOp(const int tid) {
//        VERBOSE DEBUG2 COUTATOMIC("record_manager_single_type::endOp(tid="<<tid<<")"<<std::endl);
        reclaim->endOp(tid);
    }
    template <typename First, typename... Rest>
    inline void startOp(const int tid, void * const * const reclaimers, const int numReclaimers, const bool readOnly = false) {
//        assert(isQuiescent(tid));
        reclaim->template startOp<First, Rest...>(tid, reclaimers, numReclaimers, readOnly);
    }

    template <typename First, typename... Rest>
    inline void debugGCSingleThreaded(void * const * const reclaimers, const int numReclaimers) {
        reclaim->template debugGCSingleThreaded<First, Rest...>(reclaimers, numReclaimers);
    }

    // for all schemes except reference counting
    inline void retire(const int tid, record_pointer p) {
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        reclaim->retire(tid, p);
    }

    // for all schemes
    inline record_pointer allocate(const int tid) {
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        return pool->get(tid);
    }
    inline void deallocate(const int tid, record_pointer p) {
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        pool->add(tid, p);
    }

    void printStatus(void) {
        long long allocated = debugInfoRecord.getTotalAllocated();
        long long allocatedBytes = allocated * sizeof(Record);
        long long deallocated = debugInfoRecord.getTotalDeallocated();
        long long getFromPool = debugInfoRecord.getTotalFromPool(); // - allocated;

//        COUTATOMIC("recmgr status for objects of size "<<sizeof(Record)<<" and type "<<typeid(Record).name()<<std::endl);
//        COUTATOMIC("allocated   : "<<allocated<<" objects totaling "<<allocatedBytes<<" bytes ("<<(allocatedBytes/1000000.)<<"MB)"<<std::endl);
//        COUTATOMIC("recycled    : "<<recycled<<std::endl);
//        COUTATOMIC("deallocated : "<<deallocated<<" objects"<<std::endl);
//        COUTATOMIC("pool        : "<<pool->getSizeString()<<std::endl);
//        COUTATOMIC("reclaim     : "<<reclaim->getSizeString()<<std::endl);
//        COUTATOMIC("unreclaimed : "<<(allocated - deallocated - atoi(reclaim->getSizeString().c_str()))<<std::endl);
//        COUTATOMIC(std::endl);

        COUTATOMIC(typeid(Record).name()<<"_object_size="<<sizeof(Record)<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_allocated_count="<<allocated<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_allocated_size="<<(allocatedBytes/1000000.)<<"MB"<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_get_from_pool="<<getFromPool<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_deallocated="<<deallocated<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_limbo_count="<<reclaim->getSizeString()<<std::endl);
        COUTATOMIC(typeid(Record).name()<<"_limbo_details="<<reclaim->getDetailsString()<<std::endl);
        //COUTATOMIC(typeid(Record).name()<<"_pool_count="<<pool->getSizeString()<<std::endl);
        COUTATOMIC(std::endl);

        for (int tid=0;tid<NUM_PROCESSES;++tid) {
            reclaim->debugPrintStatus(tid);
        }
        COUTATOMIC(std::endl);

//        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
//            COUTATOMIC("thread "<<tid<<" ");
//            alloc->debugPrintStatus(tid);
//
//            COUTATOMIC("    ");
//            //COUTATOMIC("allocated "<<debugInfoRecord.getAllocated(tid)<<" Nodes");
//            //COUTATOMIC("allocated "<<(debugInfoRecord.getAllocated(tid) / 1000)<<"k Nodes");
//            //COUTATOMIC(" ");
//            reclaim->debugPrintStatus(tid);
//            COUTATOMIC(" ");
//            pool->debugPrintStatus(tid);
//            COUTATOMIC(" ");
//            COUTATOMIC("(given="<<debugInfoRecord.getGiven(tid)<<" taken="<<debugInfoRecord.getTaken(tid)<<") toPool="<<debugInfoRecord.getToPool(tid)<<" fromPool="<<debugInfoRecord.getFromPool(tid));
//            COUTATOMIC(std::endl);
//        }
    }
};

#endif