/* 
 * File:   dcss_impl.h
 * Author: Maya Arbel-Raviv and Trevor Brown
 *
 * Created on May 1, 2017, 10:52 AM
 */

#ifndef DCSS_IMPL_H
#define DCSS_IMPL_H

#include "dcss.h"
#include <cassert>
#include <stdint.h>
#include <sstream>

#define BOOL_CAS __sync_bool_compare_and_swap
#define VAL_CAS __sync_val_compare_and_swap

#define DCSS_TAGBIT 0x1

inline static
bool isDcss(casword_t val) {
    return (val & DCSS_TAGBIT);
}

template <typename Unused>
dcssresult_t dcssProvider<Unused>::dcssHelp(const int tid, dcsstagptr_t tagptr, dcssptr_t snapshot, bool helpingOther) {
    // figure out what the state should be
    casword_t state = DCSS_STATE_FAILED;

    SOFTWARE_BARRIER;
    casword_t val1 = *(snapshot->addr1);
    SOFTWARE_BARRIER;
    
    //DELAY_UP_TO(1000);
    if (val1 == snapshot->old1) { // linearize here(?)
        state = DCSS_STATE_SUCCEEDED;
    }
    
    // try to cas the state to the appropriate value
    dcssptr_t ptr = TAGPTR_UNPACK_PTR(dcssDescriptors, tagptr);
    casword_t retval;
    bool failedBit;
    MUTABLES_VAL_CAS_FIELD(failedBit, retval, ptr->mutables, snapshot->mutables, DCSS_STATE_UNDECIDED, state, DCSS_MUTABLES_MASK_STATE, DCSS_MUTABLES_OFFSET_STATE); 
    if (failedBit) return {DCSS_IGNORED_RETVAL,0};                             // failed to access the descriptor: we must be helping another process complete its operation, so we will NOT use this return value!
    
    // TODO: do we do the announcement here? what will be announced exactly? do we let the user provide a pointer/value to announce as an argument to dcss? do we need to provide an operation to retrieve the current announcement for a given process?
    
    // finish the operation based on the descriptor's state
    if ((retval == DCSS_STATE_UNDECIDED && state == DCSS_STATE_SUCCEEDED)     // if we changed the state to succeeded OR
      || retval == DCSS_STATE_SUCCEEDED) {                                     // if someone else changed the state to succeeded
//        if (state == DCSS_STATE_FAILED) DELAY_UP_TO(1000);
        assert(helpingOther || ((snapshot->mutables & DCSS_MUTABLES_MASK_STATE) >> DCSS_MUTABLES_OFFSET_STATE) == DCSS_STATE_SUCCEEDED);
        BOOL_CAS(snapshot->addr2, (casword_t) tagptr, snapshot->new2); 
        return {DCSS_SUCCESS,0};
    } else {                                                                    // either we or someone else changed the state to failed
        assert((retval == DCSS_STATE_UNDECIDED && state == DCSS_STATE_FAILED)
                || retval == DCSS_STATE_FAILED);
        assert(helpingOther || ((snapshot->mutables & DCSS_MUTABLES_MASK_STATE) >> DCSS_MUTABLES_OFFSET_STATE) == DCSS_STATE_FAILED);
        BOOL_CAS(snapshot->addr2, (casword_t) tagptr, snapshot->old2);
//        if (state == DCSS_STATE_FAILED) DELAY_UP_TO(1000);
        return {DCSS_FAILED_ADDR1,val1};
    }
}

template <typename Unused>
void dcssProvider<Unused>::dcssHelpOther(const int tid, dcsstagptr_t tagptr) {
    const int otherTid = TAGPTR_UNPACK_TID(tagptr);
#ifndef NDEBUG
    if (!(otherTid >= 0 && otherTid < NUM_PROCESSES)) {
        std::cout<<"otherTid="<<otherTid<<" NUM_PROCESSES="<<NUM_PROCESSES<<std::endl;
    }
#endif
    assert(otherTid >= 0 && otherTid < NUM_PROCESSES);
    dcssdesc_t newSnapshot;
    const int sz = dcssdesc_t::size;
    assert((((tagptr & MASK_SEQ) >> OFFSET_SEQ) & 1) == 1);
    if (DESC_SNAPSHOT(dcssdesc_t, dcssDescriptors, &newSnapshot, tagptr, sz)) {
        dcssHelp(tid, tagptr, &newSnapshot, true);
    } else {
        //TRACE COUTATOMICTID("helpOther unable to get snapshot of "<<tagptrToString(tagptr)<<std::endl);
    }
}

template <typename Unused>
inline
tagptr_t dcssProvider<Unused>::getDescriptorTagptr(const int otherTid) {
    dcssptr_t ptr = &dcssDescriptors[otherTid];
    tagptr_t tagptr = TAGPTR_NEW(otherTid, ptr->mutables, DCSS_TAGBIT);
    if ((UNPACK_SEQ(tagptr) & 1) == 0) {
        // descriptor is being initialized! essentially,
        // we can think of there being NO ongoing operation,
        // so we can imagine we return NULL = no descriptor.
        return (tagptr_t) NULL;
    }
    return tagptr;
}

template <typename Unused>
inline
dcssptr_t dcssProvider<Unused>::getDescriptorPtr(tagptr_t tagptr) {
    return TAGPTR_UNPACK_PTR(dcssDescriptors, tagptr);
}

template <typename Unused>
inline
bool dcssProvider<Unused>::getDescriptorSnapshot(tagptr_t tagptr, dcssptr_t const dest) {
    if (tagptr == (tagptr_t) NULL) return false;
    return DESC_SNAPSHOT(dcssdesc_t, dcssDescriptors, dest, tagptr, dcssdesc_t::size);
}

template <typename Unused>
inline
void dcssProvider<Unused>::helpProcess(const int tid, const int otherTid) {
    tagptr_t tagptr = getDescriptorTagptr(otherTid);
    if (tagptr != (tagptr_t) NULL) dcssHelpOther(tid, tagptr);
}

template <typename Unused>
dcssresult_t dcssProvider<Unused>::dcssVal(const int tid, casword_t * addr1, casword_t old1, casword_t * addr2, casword_t old2, casword_t new2) {
    return dcssPtr(tid, addr1, old1, addr2, old2 << DCSS_LEFTSHIFT , new2 << DCSS_LEFTSHIFT);
}

template <typename Unused>
dcssresult_t dcssProvider<Unused>::dcssPtr(const int tid, casword_t * addr1, casword_t old1, casword_t * addr2, casword_t old2, casword_t new2) {
    // create dcss descriptor
    dcssptr_t ptr = DESC_NEW(dcssDescriptors, DCSS_MUTABLES_NEW, tid);
    assert((((dcssDescriptors[tid].mutables & MASK_SEQ) >> OFFSET_SEQ) & 1) == 0);
    ptr->addr1 = addr1;
    ptr->old1 = old1;
    ptr->addr2 = addr2;
    ptr->old2 = old2;
    ptr->new2 = new2;
    
    DESC_INITIALIZED(dcssDescriptors, tid);
    
    // create tagptr
    assert((((dcssDescriptors[tid].mutables & MASK_SEQ) >> OFFSET_SEQ) & 1) == 1);
    tagptr_t tagptr = TAGPTR_NEW(tid, ptr->mutables, DCSS_TAGBIT);
    
    // perform the dcss operation described by our descriptor
    casword_t r;
    do {
        assert(!isDcss(ptr->old2));
        assert(isDcss(tagptr));
        r = VAL_CAS(ptr->addr2, ptr->old2, (casword_t) tagptr);
        if (isDcss(r)) {
#ifdef USE_DEBUGCOUNTERS
            this->dcssHelpCounter->inc(tid);
#endif
            dcssHelpOther(tid, (dcsstagptr_t) r);
        }
    } while (isDcss(r));
    if (r == ptr->old2){
//        DELAY_UP_TO(1000);
        return dcssHelp(tid, tagptr, ptr, false); // finish our own operation      
    } 
    return {DCSS_FAILED_ADDR2,r};//DCSS_FAILED_ADDR2;
}

template <typename Unused>
inline
casword_t dcssProvider<Unused>::dcssRead(const int tid, casword_t volatile * addr) {
    casword_t r;
    while (1) {
        r = *addr;
        if (unlikely(isDcss(r))) {
//            std::cout<<"found supposed dcss descriptor @ "<<(size_t) r<<std::endl;
#ifdef USE_DEBUGCOUNTERS
            this->dcssHelpCounter->inc(tid);
#endif
            dcssHelpOther(tid, (dcsstagptr_t) r);
        } else {
            return r;
        }
    }
}

template <typename Unused>
dcssProvider<Unused>::dcssProvider(const int numProcesses) : NUM_PROCESSES(numProcesses) {
#ifdef USE_DEBUGCOUNTERS
    dcssHelpCounter = new debugCounter(NUM_PROCESSES);
#endif
    DESC_INIT_ALL(dcssDescriptors, DCSS_MUTABLES_NEW, NUM_PROCESSES);
    for (int tid=0;tid<numProcesses;++tid) {
        dcssDescriptors[tid].addr1 = 0;
        dcssDescriptors[tid].addr2 = 0;
        dcssDescriptors[tid].new2 = 0;
        dcssDescriptors[tid].old1 = 0;
        dcssDescriptors[tid].old2 = 0;
    }
}

template <typename Unused>
dcssProvider<Unused>::~dcssProvider() {
#ifdef USE_DEBUGCOUNTERS
    delete dcssHelpCounter;
#endif
}

template <typename Unused>
inline
casword_t dcssProvider<Unused>::readPtr(const int tid, casword_t volatile * addr) {
    casword_t r;
    r = dcssRead(tid, addr);
    return r;
}

template <typename Unused>
casword_t dcssProvider<Unused>::readVal(const int tid, casword_t volatile * addr) {
    return ((casword_t) readPtr(tid, addr))>>DCSS_LEFTSHIFT;
}

template <typename Unused>
void dcssProvider<Unused>::writePtr(casword_t volatile * addr, casword_t ptr) {
    //assert((*addr & DCSS_TAGBIT) == 0);
    assert((ptr & DCSS_TAGBIT) == 0);
    *addr = ptr;
}

template <typename Unused>
void dcssProvider<Unused>::writeVal(casword_t volatile * addr, casword_t val) {
    writePtr(addr, val<<DCSS_LEFTSHIFT);
}

template <typename Unused>
void dcssProvider<Unused>::initThread(const int tid) {}

template <typename Unused>
void dcssProvider<Unused>::deinitThread(const int tid) {}

template <typename Unused>
void dcssProvider<Unused>::debugPrint() {
#ifdef USE_DEBUGCOUNTERS
    std::cout<<"dcss helping : "<<this->dcssHelpCounter->getTotal()<<std::endl;
#endif
}

#endif /* DCSS_IMPL_H */
