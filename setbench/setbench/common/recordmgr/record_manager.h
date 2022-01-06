/**
 * C++ record manager implementation (PODC 2015) by Trevor Brown.
 *
 * Copyright (C) 2015 Trevor Brown
 *
 */

#ifndef RECORD_MANAGER_H
#define	RECORD_MANAGER_H

#include <atomic>
#include "globals.h"
#include "errors.h"
#include "record_manager_single_type.h"

#include <iostream>
#include <exception>
#include <stdexcept>
#include <typeinfo>

inline CallbackReturn callbackReturnTrue(CallbackArg arg) {
    return true;
}

// compile time check for duplicate template parameters
// compare first with rest to find any duplicates
template <typename T> void check_duplicates(void) {}
template <typename T, typename First, typename... Rest>
void check_duplicates(void) {
    if (typeid(T) == typeid(First)) {
        throw std::logic_error("duplicate template arguments provided to RecordManagerSet");
    }
    check_duplicates<T, Rest...>();
}

// base case: empty template
// this is a compile time check for invalid arguments
template <class Reclaim, class Alloc, class Pool, typename... Rest>
class RecordManagerSet {
    PAD;
public:
    RecordManagerSet(const int numProcesses, RecoveryMgr<void *> * const _recoveryMgr) {}
    template <typename T>
    record_manager_single_type<T, Reclaim, Alloc, Pool> * get(T * const recordType) {
        throw std::logic_error("invalid type passed to RecordManagerSet::get()");
        return NULL;
    }
    void clearCounters(void) {}
    void registerThread(const int tid) {}
    void unregisterThread(const int tid) {}
    void printStatus() {}
    inline void qUnprotectAll(const int tid) {}
    inline void getReclaimers(const int tid, void ** const reclaimers, int index) {}
    inline void endOp(const int tid) {}
    inline void leaveQuiescentStateForEach(const int tid, const bool readOnly = false) {}
    inline void startOp(const int tid, const bool callForEach, const bool readOnly = false) {}
    inline void debugGCSingleThreaded() {
        printf("DEBUG: record_manager::debugGCSingleThreaded()\n");
    }
};

// "recursive" case
template <class Reclaim, class Alloc, class Pool, typename First, typename... Rest>
class RecordManagerSet<Reclaim, Alloc, Pool, First, Rest...> : RecordManagerSet<Reclaim, Alloc, Pool, Rest...> {
    PAD;
    record_manager_single_type<First, Reclaim, Alloc, Pool> * const mgr;
	PAD;
public:
    RecordManagerSet(const int numProcesses, RecoveryMgr<void *> * const _recoveryMgr)
        : RecordManagerSet<Reclaim, Alloc, Pool, Rest...>(numProcesses, _recoveryMgr)
        , mgr(new record_manager_single_type<First, Reclaim, Alloc, Pool>(numProcesses, _recoveryMgr))
        {
        //cout<<"RecordManagerSet with First="<<typeid(First).name()<<" and sizeof...(Rest)="<<sizeof...(Rest)<<std::endl;
        check_duplicates<First, Rest...>(); // check if first is in {rest...}
    }
    ~RecordManagerSet() {
        std::cout<<"recordmanager set destructor started for object type "<<typeid(First).name()<<std::endl;
        delete mgr;
        std::cout<<"recordmanager set destructor finished for object type "<<typeid(First).name()<<std::endl;
        // note: should automatically call the parent class' destructor afterwards
    }
    // note: the compiled code for get() should be a single read and return statement
    template<typename T>
    inline record_manager_single_type<T, Reclaim, Alloc, Pool> * get(T * const recordType) {
        if (typeid(First) == typeid(T)) {
            //cout<<"MATCH: typeid(First)="<<typeid(First).name()<<" typeid(T)="<<typeid(T).name()<<std::endl;
            return (record_manager_single_type<T, Reclaim, Alloc, Pool> *) mgr;
        } else {
            //cout<<"NO MATCH: typeid(First)="<<typeid(First).name()<<" typeid(T)="<<typeid(T).name()<<std::endl;
            return ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->get(recordType);
        }
    }
    // note: recursion should be compiled out
    void clearCounters(void) {
        mgr->clearCounters();
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->clearCounters();
    }
    void registerThread(const int tid) {
        mgr->initThread(tid);
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->registerThread(tid);
    }
    void unregisterThread(const int tid) {
        mgr->deinitThread(tid);
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->unregisterThread(tid);
    }
    void printStatus() {
        mgr->printStatus();
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->printStatus();
    }
    inline void qUnprotectAll(const int tid) {
        mgr->qUnprotectAll(tid);
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->qUnprotectAll(tid);
    }
    inline void getReclaimers(const int tid, void ** const reclaimers, int index) {
        reclaimers[index] = mgr->reclaim;
        ((RecordManagerSet <Reclaim, Alloc, Pool, Rest...> *) this)->getReclaimers(tid, reclaimers, 1+index);
    }
    inline void endOp(const int tid) {
        mgr->endOp(tid);
        ((RecordManagerSet<Reclaim, Alloc, Pool, Rest...> *) this)->endOp(tid);
    }
    inline void leaveQuiescentStateForEach(const int tid, const bool readOnly = false) {
        mgr->template startOp<First, Rest...>(tid, NULL, 0, readOnly);
        ((RecordManagerSet <Reclaim, Alloc, Pool, Rest...> *) this)->leaveQuiescentStateForEach(tid, readOnly);
    }
    inline void startOp(const int tid, const bool callForEach, const bool readOnly = false) {
        if (callForEach) {
            leaveQuiescentStateForEach(tid, readOnly);
        } else {
            void * reclaimers[1+sizeof...(Rest)];
            getReclaimers(tid, reclaimers, 0);
            get((First *) NULL)->template startOp<First, Rest...>(tid, reclaimers, 1+sizeof...(Rest), readOnly);
            __sync_synchronize(); // memory barrier needed (only) for epoch based schemes at the moment...
        }
    }
    inline void debugGCSingleThreaded() {
        printf("DEBUG: record_manager::debugGCSingleThreaded() 1+sizeof...(Rest)=%lu\n", 1+sizeof...(Rest));
        void * reclaimers[1+sizeof...(Rest)];
        getReclaimers(0, reclaimers, 0);
        get((First *) NULL)->template debugGCSingleThreaded<First, Rest...>(reclaimers, 1+sizeof...(Rest));
        __sync_synchronize(); // memory barrier needed (only) for epoch based schemes at the moment...
    }
};

template <class Reclaim, class Alloc, class Pool, typename First, typename... Rest>
class RecordManagerSetPostPadded : public RecordManagerSet<Reclaim, Alloc, Pool, First, Rest...> {
    PAD;
public:
    RecordManagerSetPostPadded(const int numProcesses, RecoveryMgr<void *> * const _recoveryMgr)
        : RecordManagerSet<Reclaim, Alloc, Pool, First, Rest...>(numProcesses, _recoveryMgr)
    {}
};

class padded_bool {
public:
    union {
        bool v;
        char padding[128];
    };
    padded_bool() {
        v = 0;
    }
};

template <class Reclaim, class Alloc, class Pool, typename RecordTypesFirst, typename... RecordTypesRest>
class record_manager {
protected:
    typedef record_manager<Reclaim,Alloc,Pool,RecordTypesFirst,RecordTypesRest...> SelfType;
    PAD;
    RecordManagerSetPostPadded<Reclaim,Alloc,Pool,RecordTypesFirst,RecordTypesRest...> * rmset;

   PAD;
   padded_bool init[MAX_THREADS_POW2];

public:
//    PAD;
    const int NUM_PROCESSES;
    RecoveryMgr<SelfType> * const recoveryMgr;
    PAD;

    record_manager(const int numProcesses, const int _neutralizeSignal = -1 /* unused except in conjunction with special DEBRA+ memory reclamation */)
            : NUM_PROCESSES(numProcesses)
            , recoveryMgr(new RecoveryMgr<SelfType>(numProcesses, _neutralizeSignal, this))
    {
        rmset = new RecordManagerSetPostPadded<Reclaim, Alloc, Pool, RecordTypesFirst, RecordTypesRest...>(numProcesses, (RecoveryMgr<void *> *) recoveryMgr);
    }
    ~record_manager() {
            delete recoveryMgr;
            delete rmset;
    }
    void initThread(const int tid) {
        if (init[tid].v) return; else init[tid].v = !init[tid].v;

        rmset->registerThread(tid);
        recoveryMgr->initThread(tid);
//        endOp(tid);
    }
    void deinitThread(const int tid) {
        if (!init[tid].v) return; else init[tid].v = !init[tid].v;

        recoveryMgr->deinitThread(tid);
        rmset->unregisterThread(tid);
    }
    void clearCounters() {
        rmset->clearCounters();
    }
    void printStatus(void) {
        rmset->printStatus();
    }
    template <typename T>
    debugInfo * getDebugInfo(T * const recordType) {
        return &rmset->get((T *) NULL)->debugInfoRecord;
    }
    template <typename T>
    inline record_manager_single_type<T, Reclaim, Alloc, Pool> * get(T * const recordType) {
        return rmset->get((T *) NULL);
    }

    // for hazard pointers

    template <typename T>
    inline bool isProtected(const int tid, T * const obj) {
        return rmset->get((T *) NULL)->isProtected(tid, obj);
    }

    template <typename T>
    inline bool protect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool hintMemoryBarrier = true) {
        return rmset->get((T *) NULL)->protect(tid, obj, notRetiredCallback, callbackArg, hintMemoryBarrier);
    }

    template <typename T>
    inline void unprotect(const int tid, T * const obj) {
        rmset->get((T *) NULL)->unprotect(tid, obj);
    }

    // for DEBRA+

    // warning: qProtect must be reentrant and lock-free (i.e., async-signal-safe)
    template <typename T>
    inline bool qProtect(const int tid, T * const obj, CallbackType notRetiredCallback, CallbackArg callbackArg, bool hintMemoryBarrier = true) {
        return rmset->get((T *) NULL)->qProtect(tid, obj, notRetiredCallback, callbackArg, hintMemoryBarrier);
    }

    template <typename T>
    inline bool isQProtected(const int tid, T * const obj) {
        return rmset->get((T *) NULL)->isQProtected(tid, obj);
    }

    inline void qUnprotectAll(const int tid) {
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        rmset->qUnprotectAll(tid);
    }

    // for epoch based reclamation
    inline bool isQuiescent(const int tid) {
        return rmset->get((RecordTypesFirst *) NULL)->isQuiescent(tid); // warning: if quiescence information is logically shared between all types, with the actual data being associated only with the first type (as it is here), then isQuiescent will return inconsistent results if called in functions that recurse on the template argument list in this class.
    }
    inline void endOp(const int tid) {
        assert(init[tid].v && "must call record_manager initThread before endOp");
//        VERBOSE DEBUG2 COUTATOMIC("record_manager_single_type::endOp(tid="<<tid<<")"<<std::endl);
        if (Reclaim::quiescenceIsPerRecordType()) {
//            std::cout<<"setting quiescent state for all record types\n";
            rmset->endOp(tid);
        } else {
            // only call endOp for one object type
//            std::cout<<"setting quiescent state for just one record type: "<<typeid(RecordTypesFirst).name()<<"\n";
            rmset->get((RecordTypesFirst *) NULL)->endOp(tid);
        }
    }
    inline void startOp(const int tid, const bool readOnly = false) {
        assert(init[tid].v && "must call record_manager initThread before startOp");
//        assert(isQuiescent(tid));
//        VERBOSE DEBUG2 COUTATOMIC("record_manager_single_type::startOp(tid="<<tid<<")"<<std::endl);
        // for some types of reclaimers, different types of records retired in the same
        // epoch can be reclaimed together (by aggregating their epochs), so we don't actually need
        // separate calls to startOp for each object type.
        // if appropriate, we make a single call to startOp,
        // and it takes care of all record types managed by this record manager.
        //cout<<"quiescenceIsPerRecordType = "<<Reclaim::quiescenceIsPerRecordType()<<std::endl;
        rmset->startOp(tid, Reclaim::quiescenceIsPerRecordType(), readOnly);
    }

    // for all schemes
    template <typename T>
    inline void retire(const int tid, T * const p) {
        assert(init[tid].v && "must call record_manager initThread before retire");
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        rmset->get((T *) NULL)->retire(tid, p);
    }

    template <typename T>
    inline T * allocate(const int tid) {
        assert(init[tid].v && "must call record_manager initThread before allocate");
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
//        GSTATS_ADD_IX(tid, num_prop_epoch_allocations, 1, GSTATS_GET(tid, thread_announced_epoch));
        return rmset->get((T *) NULL)->allocate(tid);
    }

    // optional function which can be used if it is safe to call free()
    template <typename T>
    inline void deallocate(const int tid, T * const p) {
        assert(init[tid].v && "must call record_manager initThread before deallocate");
        assert(!Reclaim::supportsCrashRecovery() || isQuiescent(tid));
        rmset->get((T *) NULL)->deallocate(tid, p);
    }

    inline static bool shouldHelp() { // FOR DEBUGGING PURPOSES
        return Reclaim::shouldHelp();
    }
    inline static bool supportsCrashRecovery() {
        return Reclaim::supportsCrashRecovery();
    }

    class MemoryReclamationGuard {
        const int tid;
        record_manager<Reclaim, Alloc, Pool, RecordTypesFirst, RecordTypesRest...> * recmgr;
    public:
        MemoryReclamationGuard(const int _tid, record_manager<Reclaim, Alloc, Pool, RecordTypesFirst, RecordTypesRest...> * _recmgr, const bool readOnly = false)
        : tid(_tid), recmgr(_recmgr) {
            recmgr->startOp(tid, readOnly);
        }
        ~MemoryReclamationGuard() {
            recmgr->endOp(tid);
        }
        void end() {
            recmgr->endOp(tid);
        }
    };

    inline MemoryReclamationGuard getGuard(const int tid, const bool readOnly = false) {
        SOFTWARE_BARRIER;
        return MemoryReclamationGuard(tid, this, readOnly);
    }

    void debugGCSingleThreaded() {
        rmset->debugGCSingleThreaded();
    }
};

#endif
