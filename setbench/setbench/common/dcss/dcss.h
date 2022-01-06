/*
 * File:   dcss.h
 * Author: Maya Arbel-Raviv and Trevor Brown
 *
 * Created on May 1, 2017, 10:42 AM
 */

#ifndef DCSS_H
#define DCSS_H

#include <cstdarg>
#include <csignal>
#include <string.h>
#include "plaf.h"
#include "descriptors.h"

#ifndef likely
    #define likely(x)       __builtin_expect((x),1)
#endif
#ifndef unlikely
    #define unlikely(x)     __builtin_expect((x),0)
#endif

#define dcsstagptr_t uintptr_t
#define dcssptr_t dcssdesc_t *
#ifndef casword_t
#   define casword_t intptr_t
#endif

#define DCSS_STATE_UNDECIDED 0
#define DCSS_STATE_SUCCEEDED 4
#define DCSS_STATE_FAILED 8

#define DCSS_LEFTSHIFT 1

#define DCSS_IGNORED_RETVAL -1
#define DCSS_SUCCESS 0
#define DCSS_FAILED_ADDR1 1
#define DCSS_FAILED_ADDR2 2

#define MAX_PAYLOAD_PTRS 6

struct dcssresult_t {
    int status;
    casword_t failed_val;
};

class dcssdesc_t {
public:
    volatile mutables_t mutables;
    casword_t volatile * volatile addr1;
    casword_t volatile old1;
    casword_t volatile * volatile addr2;
    casword_t volatile old2;
    casword_t volatile new2;
    const static int size = sizeof(mutables)+sizeof(addr1)+sizeof(old1)+sizeof(addr2)+sizeof(old2)+sizeof(new2);
    char padding[PREFETCH_SIZE_BYTES+(((64<<10)-size%64)%64)]; // add padding to prevent false sharing
} __attribute__ ((aligned(64)));

template <typename Unused>
class dcssProvider {
    /**
     * Data definitions
     */
private:
    // descriptor reduction algorithm
    #define DCSS_MUTABLES_OFFSET_STATE 0
    #define DCSS_MUTABLES_MASK_STATE 0xf
    #define DCSS_MUTABLES_NEW(mutables) \
        ((((mutables)&MASK_SEQ)+(1<<OFFSET_SEQ)) \
        | (DCSS_STATE_UNDECIDED<<DCSS_MUTABLES_OFFSET_STATE))
    #include "descriptors_impl2.h"
    PAD;
    dcssdesc_t dcssDescriptors[LAST_TID+1] __attribute__ ((aligned(64)));
    PAD;

public:
#ifdef USE_DEBUGCOUNTERS
    debugCounter * dcssHelpCounter;
    PAD;
#endif
    const int NUM_PROCESSES;
    PAD;

    /**
     * Function declarations
     */
    dcssProvider(const int numProcesses);
    ~dcssProvider();
    void initThread(const int tid);
    void deinitThread(const int tid);
    void writePtr(casword_t volatile * addr, casword_t val);        // use for addresses that might have been modified by DCSS (ONLY GOOD FOR INITIALIZING, CANNOT DEAL WITH CONCURRENT DCSS OPERATIONS.)
    void writeVal(casword_t volatile * addr, casword_t val);        // use for addresses that might have been modified by DCSS (ONLY GOOD FOR INITIALIZING, CANNOT DEAL WITH CONCURRENT DCSS OPERATIONS.)
    casword_t readPtr(const int tid, casword_t volatile * addr);    // use for addresses that might have been modified by DCSS
    casword_t readVal(const int tid, casword_t volatile * addr);    // use for addresses that might have been modified by DCSS
    inline dcssresult_t dcssPtr(const int tid, casword_t * addr1, casword_t old1, casword_t * addr2, casword_t old2, casword_t new2); // use when addr2 is a pointer, or another type that does not use its least significant bit
    inline dcssresult_t dcssVal(const int tid, casword_t * addr1, casword_t old1, casword_t * addr2, casword_t old2, casword_t new2); // use when addr2 uses its least significant bit, but does not use its most significant but
    void debugPrint();

    tagptr_t getDescriptorTagptr(const int otherTid);
    dcssptr_t getDescriptorPtr(tagptr_t tagptr);
    bool getDescriptorSnapshot(tagptr_t tagptr, dcssptr_t const dest);
    void helpProcess(const int tid, const int otherTid);
private:
    casword_t dcssRead(const int tid, casword_t volatile * addr);
    inline dcssresult_t dcssHelp(const int tid, dcsstagptr_t tagptr, dcssptr_t snapshot, bool helpingOther);
    void dcssHelpOther(const int tid, dcsstagptr_t tagptr);
};

#endif /* DCSS_H */

