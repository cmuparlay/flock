#pragma once

#include <cassert>
#include <cstring>
#include <immintrin.h>
#include <sstream>
#include <stdint.h>

#include "user_gstats_handler.h"

using namespace std;

/**
 * Note: this algorithm supports a limited number of threads (print LAST_TID to see how many).
 * It should be several thousand, at least.
 * The alg can be tweaked to support more.
 */

#define BOOL_CAS __sync_bool_compare_and_swap
#define VAL_CAS __sync_val_compare_and_swap

enum abort_codes {
      ABORT_RESERVED_ZERO
    , ABORT_RETURN_FALSE
    , ABORT_DESCRIPTOR
    , ABORT_WRSET_TOO_LARGE
};

enum kcas_tx_start_result {
      RETURN_FALSE
    , HTM_STARTED
    , FALLBACK
};

/**
 *
 * Descriptor reuse macros
 *
 */

/**
 * seqbits_t corresponds to the seqBits field of the descriptor.
 * it contains the mutable fields of the descriptor and a sequence number.
 * the width, offset and mask for the sequence number is defined below.
 * this sequence number width, offset and mask are also shared by tagptr_t.
 *
 * in particular, for any tagptr_t x and seqbits_t y, the sequence numbers
 * in x and y are equal iff x&MASK_SEQ == y&MASK_SEQ (despite differing types).
 *
 * tagptr_t consists of a triple <seq, tid, testbit>.
 * these three fields are defined by the TAGPTR_ macros below.
 */

typedef intptr_t tagptr_t;
typedef intptr_t seqbits_t;
#include <thread>

#ifndef WIDTH_SEQ
#define WIDTH_SEQ 48
#endif
#define OFFSET_SEQ 14
#define MASK_SEQ ((uintptr_t)((1LL << WIDTH_SEQ) - 1) << OFFSET_SEQ) /* cast to avoid signed bit shifting */
#define UNPACK_SEQ(tagptrOrSeqbits) (((uintptr_t)(tagptrOrSeqbits)) >> OFFSET_SEQ)

#define TAGPTR_OFFSET_USER 0
#define TAGPTR_OFFSET_TID 3
#define TAGPTR_MASK_USER ((1 << TAGPTR_OFFSET_TID) - 1) /* assumes TID is next field after USER */
#define TAGPTR_MASK_TID (((1 << OFFSET_SEQ) - 1) & (~(TAGPTR_MASK_USER)))
#define TAGPTR_UNPACK_TID(tagptr) ((int)((((tagptr_t)(tagptr)) & TAGPTR_MASK_TID) >> TAGPTR_OFFSET_TID))
#define TAGPTR_UNPACK_PTR(descArray, tagptr) (&(descArray)[TAGPTR_UNPACK_TID((tagptr))])
#define TAGPTR_NEW(tid, seqBits, userBits) ((tagptr_t)(((UNPACK_SEQ(seqBits)) << OFFSET_SEQ) | ((tid) << TAGPTR_OFFSET_TID) | (tagptr_t)(userBits) << TAGPTR_OFFSET_USER))
// assert: there is no thread with tid DUMMY_TID that ever calls TAGPTR_NEW
#define LAST_TID (TAGPTR_MASK_TID >> TAGPTR_OFFSET_TID)
#define TAGPTR_STATIC_DESC(id) ((tagptr_t)TAGPTR_NEW(LAST_TID - 1 - id, 0))
#define TAGPTR_DUMMY_DESC(id) ((tagptr_t)TAGPTR_NEW(LAST_TID, id << OFFSET_SEQ))

#define comma ,

#define SEQBITS_UNPACK_FIELD(seqBits, mask, offset) \
    ((((seqbits_t)(seqBits)) & (mask)) >> (offset))
// TODO: make more efficient version "SEQBITS_CAS_BIT"
// TODO: change sequence # unpacking to masking for quick comparison
// note: if there is only one subfield besides seq#, then the third if-block is redundant, and you should just return false if the cas fails, since the only way the cas fails and the field being cas'd contains still old is if the sequence number has changed.
#define SEQBITS_CAS_FIELD(successBit, fldSeqBits, snapSeqBits, oldval, val, mask, offset)             \
    {                                                                                                 \
        seqbits_t __v = (fldSeqBits);                                                                 \
        while (1) {                                                                                   \
            if (UNPACK_SEQ(__v) != UNPACK_SEQ((snapSeqBits))) {                                       \
                (successBit) = false;                                                                 \
                break;                                                                                \
            }                                                                                         \
            if ((successBit) = __sync_bool_compare_and_swap(&(fldSeqBits),                            \
                                                            (__v & ~(mask)) | (oldval),               \
                                                            (__v & ~(mask)) | ((val) << (offset)))) { \
                break;                                                                                \
            }                                                                                         \
            __v = (fldSeqBits);                                                                       \
            if (SEQBITS_UNPACK_FIELD(__v, (mask), (offset)) != (oldval)) {                            \
                (successBit) = false;                                                                 \
                break;                                                                                \
            }                                                                                         \
        }                                                                                             \
    }
// TODO: change sequence # unpacking to masking for quick comparison
// note: SEQBITS_FAA_FIELD would be very similar to SEQBITS_CAS_FIELD; i think one would simply delete the last if block and change the new val from (val)<<offset to (val&mask)+1.
#define SEQBITS_WRITE_FIELD(fldSeqBits, snapSeqBits, val, mask, offset)                                                                                                                                            \
    {                                                                                                                                                                                                              \
        seqbits_t __v = (fldSeqBits);                                                                                                                                                                              \
        while (UNPACK_SEQ(__v) == UNPACK_SEQ((snapSeqBits)) && SEQBITS_UNPACK_FIELD(__v, (mask), (offset)) != (val) && !__sync_bool_compare_and_swap(&(fldSeqBits), __v, (__v & ~(mask)) | ((val) << (offset)))) { \
            __v = (fldSeqBits);                                                                                                                                                                                    \
        }                                                                                                                                                                                                          \
    }
#define SEQBITS_WRITE_BIT(fldSeqBits, snapSeqBits, mask)                                                                                               \
    {                                                                                                                                                  \
        seqbits_t __v = (fldSeqBits);                                                                                                                  \
        while (UNPACK_SEQ(__v) == UNPACK_SEQ((snapSeqBits)) && !(__v & (mask)) && !__sync_bool_compare_and_swap(&(fldSeqBits), __v, (__v | (mask)))) { \
            __v = (fldSeqBits);                                                                                                                        \
        }                                                                                                                                              \
    }

// WARNING: uses a GCC extension "({ })". to get rid of this, use an inline function.
#define DESC_SNAPSHOT(descType, descArray, descDest, tagptr, sz) ({                                                                                         \
    descType *__src = TAGPTR_UNPACK_PTR((descArray), (tagptr));                                                                                             \
    memcpy((descDest), __src, (sz));                                                                                                                        \
    __asm__ __volatile__("" ::                                                                                                                              \
                             : "memory"); /* prevent compiler from reordering read of __src->seqBits before (at least the reading portion of) the memcpy */ \
    (UNPACK_SEQ(__src->seqBits) == UNPACK_SEQ((tagptr)));                                                                                                   \
})
#define DESC_READ_FIELD(successBit, fldSeqBits, tagptr, mask, offset) ({ \
    seqbits_t __seqBits = (fldSeqBits);                                  \
    successBit = (__seqBits & MASK_SEQ) == ((tagptr)&MASK_SEQ);          \
    SEQBITS_UNPACK_FIELD(__seqBits, (mask), (offset));                   \
})
#define DESC_NEW(descArray, macro_seqBitsNew, tid)                                        \
    &(descArray)[(tid)];                                                                  \
    { /* note: only the process invoking this following macro can change the sequence# */ \
        seqbits_t __v = (descArray)[(tid)].seqBits;                                       \
        (descArray)[(tid)].seqBits = macro_seqBitsNew(__v);                               \
        /*__sync_synchronize();*/                                                         \
    }
#define DESC_INITIALIZED(descArray, tid) \
    (descArray)[(tid)].seqBits += (1 << OFFSET_SEQ);

#define DESC_INIT_ALL(descArray, macro_seqBitsNew)        \
    {                                                     \
        for (int i = 0; i < (LAST_TID - 1); ++i) {        \
            (descArray)[i].seqBits = macro_seqBitsNew(0); \
        }                                                 \
    }

/**
 *
 * KCAS implementation
 *
 */

#define kcastagptr_t uintptr_t
#define rdcsstagptr_t uintptr_t
#define rdcssptr_t rdcssdesc_t *
#define kcasptr_t kcasdesc_t<MAX_KCAS> *
#define RDCSS_TAGBIT 0x1
#define KCAS_TAGBIT 0x2

#define KCAS_STATE_UNDECIDED 0
#define KCAS_STATE_SUCCEEDED 4
#define KCAS_STATE_FAILED 8

#define KCAS_LEFTSHIFT 2
#define HTM_READ_DESCRIPTOR 20
#define HTM_BAD_OLD_VAL 30

#define MAX_SHORT_RETRIES 5
#define MAX_FULL_RETRIES 1


#ifdef MAX_THREADS_POW2
    #define KCAS_MAX_THREADS MAX_THREADS_POW2
#else
    #define KCAS_MAX_THREADS 500
#endif

PAD;
void *volatile thread_ids[KCAS_MAX_THREADS] = {};
PAD;

class TIDGenerator {
  public:
    int myslot = -1;

    TIDGenerator() {
        int i;
        while (true) {
            i = 0;
            while (thread_ids[i]) {
                ++i;
            }

            assert(i < KCAS_MAX_THREADS);
            if (__sync_bool_compare_and_swap(&thread_ids[i], 0, this)) {
                myslot = i;
                break;
            }
        }
    }

    ~TIDGenerator() {
        thread_ids[myslot] = 0;
    }

    operator int() {
        return myslot;
    }

    int getId() {
        return myslot;
    }

    void explicitRelease() {
        thread_ids[myslot] = 0;
    }
};

thread_local TIDGenerator kcas_tid;

class htm_full_attempter {
  private:
    struct wrset_entry {
        casword_t * addr;
        casword_t newval;
    };
    static const int MAX_ENTRIES = 32;
    wrset_entry entries[MAX_ENTRIES];
    int sz;
    int attempts;
    bool active;

  public:
    htm_full_attempter() {
        active = 0;
    }
    inline kcas_tx_start_result tryStart() {
        sz = 0;
        active = true;
        for (attempts = 0; attempts < MAX_FULL_RETRIES; ++attempts) {
            int status = _xbegin();
            if (status == _XBEGIN_STARTED) {
                return kcas_tx_start_result::HTM_STARTED;
            } else {
                GSTATS_ADD(kcas_tid.getId(), fasthtm_abort, 1);
                int user_code = _XABORT_CODE(status);
                switch (user_code) {
                    case ABORT_RETURN_FALSE: {
                        active = false;
                        return kcas_tx_start_result::RETURN_FALSE;
                    } break;
                    case ABORT_WRSET_TOO_LARGE: {
                        setbench_error("exceeded MAX_ENTRIES");
                    } break;
                }
            }
        }
        active = false;
        return kcas_tx_start_result::FALLBACK;
    }
    inline void add(casword_t * addr, casword_t newval) {
        entries[sz++] = {addr, newval};
        // if (sz > MAX_ENTRIES) {
        //     _xabort(ABORT_WRSET_TOO_LARGE);
        // }
    }
    inline void commit() {
        for (int i=0;i<sz;++i) {
            *entries[i].addr = entries[i].newval;
        }
        _xend();
        active = false;
        GSTATS_ADD(kcas_tid.getId(), fasthtm_commit, 1);
    }
    inline bool isActive() {
        return active;
    }
};

PAD;
thread_local htm_full_attempter htm_full;
PAD;

struct rdcssdesc_t {
    volatile seqbits_t seqBits;
    casword_t volatile *addr1;
    casword_t old1;
    casword_t volatile *addr2;
    casword_t old2;
    casword_t new2;
    const static int size = sizeof(seqBits) + sizeof(addr1) + sizeof(old1) + sizeof(addr2) + sizeof(old2) + sizeof(new2);
    volatile char padding[128 + ((64 - size % 64) % 64)]; // add padding to prevent false sharing
};

struct kcasentry_t { // just part of kcasdesc_t, not a standalone descriptor
    casword_t volatile *addr;
    casword_t oldval;
    casword_t newval;
};

template <int MAX_K>
class kcasdesc_t {
  public:
    volatile seqbits_t seqBits;
    casword_t numEntries;
    kcasentry_t entries[MAX_K];
    const static int size = sizeof(seqBits) + sizeof(numEntries) + sizeof(entries);
    volatile char padding[128 + ((64 - size % 64) % 64)]; // add padding to prevent false sharing

    inline void addValAddr(casword_t volatile *addr, casword_t oldval, casword_t newval) {
        entries[numEntries].addr = addr;
        entries[numEntries].oldval = oldval << KCAS_LEFTSHIFT;
        entries[numEntries].newval = newval << KCAS_LEFTSHIFT;
        ++numEntries;
        assert(numEntries <= MAX_K);
    }

    inline void addPtrAddr(casword_t volatile *addr, casword_t oldval, casword_t newval) {
        entries[numEntries].addr = addr;
        entries[numEntries].oldval = oldval;
        entries[numEntries].newval = newval;
        ++numEntries;
        assert(numEntries <= MAX_K);
    }
};

inline static bool isRdcss(casword_t val) {
    return (val & RDCSS_TAGBIT);
}

inline static bool isKcas(casword_t val) {
    return (val & KCAS_TAGBIT);
}

inline static bool isAnyDescriptor(casword_t val) {
    return (val & (RDCSS_TAGBIT | KCAS_TAGBIT));
}

template <int MAX_K>
class KCASHTM_FULL {
  public:
    /**
     * Data definitions
     */
  private:
    // descriptor reduction algorithm
#define KCAS_SEQBITS_OFFSET_STATE 0
#define KCAS_SEQBITS_MASK_STATE 0xf
#define KCAS_SEQBITS_NEW(seqBits) \
    ((((seqBits)&MASK_SEQ) + (1 << OFFSET_SEQ)) | (KCAS_STATE_UNDECIDED << KCAS_SEQBITS_OFFSET_STATE))
#define RDCSS_SEQBITS_NEW(seqBits) \
    (((seqBits)&MASK_SEQ) + (1 << OFFSET_SEQ))
    volatile char __padding_desc[128];
    kcasdesc_t<MAX_K> kcasDescriptors[LAST_TID + 1] __attribute__((aligned(64)));
    rdcssdesc_t rdcssDescriptors[LAST_TID + 1] __attribute__((aligned(64)));
    volatile char __padding_desc3[128];

    /**
     * Function declarations
     */
  public:
    KCASHTM_FULL();
    void writeInitPtr(casword_t volatile *addr, casword_t const newval);
    void writeInitVal(casword_t volatile *addr, casword_t const newval);
    casword_t readPtr(casword_t volatile *addr);
    casword_t readVal(casword_t volatile *addr);
    bool execute();

    kcasptr_t getDescriptor();
    bool start();
    casword_t rdcssRead(casword_t volatile *addr);
    void helpOther(kcastagptr_t tagptr);
    void deinitThread();
    template <typename T>
    void add(casword<T> *caswordptr, T oldVal, T newVal);
    template <typename T, typename... Args>
    void add(casword<T> *caswordptr, T oldVal, T newVal, Args... args);

  private:
    casword_t rdcss(rdcssptr_t ptr, rdcsstagptr_t tagptr);
    bool help(kcastagptr_t tagptr, kcasptr_t ptr, bool helpingOther);
    void rdcssHelp(rdcsstagptr_t tagptr, rdcssptr_t snapshot, bool helpingOther);
    void rdcssHelpOther(rdcsstagptr_t tagptr);
};

template <int MAX_K>
void KCASHTM_FULL<MAX_K>::rdcssHelp(rdcsstagptr_t tagptr, rdcssptr_t snapshot, bool helpingOther) {
    bool readSuccess;
    casword_t v = DESC_READ_FIELD(readSuccess, *snapshot->addr1, snapshot->old1, KCAS_SEQBITS_MASK_STATE, KCAS_SEQBITS_OFFSET_STATE);
    if (!readSuccess)
        v = KCAS_STATE_SUCCEEDED; // return;

    if (v == KCAS_STATE_UNDECIDED) {
        BOOL_CAS(snapshot->addr2, (casword_t)tagptr, snapshot->new2);
    } else {
        // the "fuck it i'm done" action (the same action you'd take if the kcas descriptor hung around indefinitely)
        BOOL_CAS(snapshot->addr2, (casword_t)tagptr, snapshot->old2);
    }
}

template <int MAX_K>
void KCASHTM_FULL<MAX_K>::rdcssHelpOther(rdcsstagptr_t tagptr) {
    rdcssdesc_t newSnapshot;
    const int sz = rdcssdesc_t::size;
    if (DESC_SNAPSHOT(rdcssdesc_t, rdcssDescriptors, &newSnapshot, tagptr, sz)) {
        rdcssHelp(tagptr, &newSnapshot, true);
    }
}

template <int MAX_K>
casword_t KCASHTM_FULL<MAX_K>::rdcss(rdcssptr_t ptr, rdcsstagptr_t tagptr) {
    casword_t r;
    do {
        r = VAL_CAS(ptr->addr2, ptr->old2, (casword_t)tagptr);
        if (unlikely(isRdcss(r))) {
            rdcssHelpOther((rdcsstagptr_t)r);
        } else
            break;
    } while (true);
    if (r == ptr->old2)
        rdcssHelp(tagptr, ptr, false); // finish our own operation
    return r;
}

template <int MAX_K>
casword_t KCASHTM_FULL<MAX_K>::rdcssRead(casword_t volatile *addr) {
    casword_t r;
    do {
        r = *addr;
        if (unlikely(isRdcss(r))) {
            rdcssHelpOther((rdcsstagptr_t)r);
        } else
            break;
    } while (true);
    return r;
}

template <int MAX_K>
KCASHTM_FULL<MAX_K>::KCASHTM_FULL() {
    DESC_INIT_ALL(kcasDescriptors, KCAS_SEQBITS_NEW);
    DESC_INIT_ALL(rdcssDescriptors, RDCSS_SEQBITS_NEW);
}

template <int MAX_K>
void KCASHTM_FULL<MAX_K>::helpOther(kcastagptr_t tagptr) {
    kcasdesc_t<MAX_K> newSnapshot;
    const int sz = kcasdesc_t<MAX_K>::size;
    //cout<<"size of kcas descriptor is "<<sizeof(kcasdesc_t<MAX_K>)<<" and sz="<<sz<<endl;
    if (DESC_SNAPSHOT(kcasdesc_t<MAX_K>, kcasDescriptors, &newSnapshot, tagptr, sz)) {
        help(tagptr, &newSnapshot, true);
    }
}

template <int MAX_K>
bool KCASHTM_FULL<MAX_K>::help(kcastagptr_t tagptr, kcasptr_t snapshot, bool helpingOther) {
    // phase 1: "locking" addresses for this kcas
    int newstate;

    // read state field
    kcasptr_t ptr = TAGPTR_UNPACK_PTR(kcasDescriptors, tagptr);
    bool successBit;
    int state = DESC_READ_FIELD(successBit, ptr->seqBits, tagptr, KCAS_SEQBITS_MASK_STATE, KCAS_SEQBITS_OFFSET_STATE);
    if (!successBit) {
        assert(helpingOther);
        return false;
    }

    if (state == KCAS_STATE_UNDECIDED) {
        newstate = KCAS_STATE_SUCCEEDED;
        for (int i = helpingOther; i < snapshot->numEntries; i++) {
        retry_entry:
            // prepare rdcss descriptor and run rdcss
            rdcssdesc_t *rdcssptr = DESC_NEW(rdcssDescriptors, RDCSS_SEQBITS_NEW, kcas_tid.getId());
            rdcssptr->addr1 = (casword_t *)&ptr->seqBits;
            rdcssptr->old1 = tagptr; // pass the sequence number (as part of tagptr)
            rdcssptr->old2 = snapshot->entries[i].oldval;
            rdcssptr->addr2 = snapshot->entries[i].addr; // p stopped here (step 2)
            rdcssptr->new2 = (casword_t)tagptr;
            DESC_INITIALIZED(rdcssDescriptors, kcas_tid.getId());

            casword_t val;
            val = rdcss(rdcssptr, TAGPTR_NEW(kcas_tid.getId(), rdcssptr->seqBits, RDCSS_TAGBIT));

            // check for failure of rdcss and handle it
            if (isKcas(val)) {
                // if rdcss failed because of a /different/ kcas, we help it
                if (val != (casword_t)tagptr) {
                    helpOther((kcastagptr_t)val);
                    goto retry_entry;
                }
            } else {
                if (val != snapshot->entries[i].oldval) {
                    newstate = KCAS_STATE_FAILED;
                    break;
                }
            }
        }
        SEQBITS_CAS_FIELD(successBit, ptr->seqBits, snapshot->seqBits, KCAS_STATE_UNDECIDED, newstate, KCAS_SEQBITS_MASK_STATE, KCAS_SEQBITS_OFFSET_STATE);
    }
    // phase 2 (all addresses are now "locked" for this kcas)
    state = DESC_READ_FIELD(successBit, ptr->seqBits, tagptr, KCAS_SEQBITS_MASK_STATE, KCAS_SEQBITS_OFFSET_STATE);
    if (!successBit)
        return false;

    bool succeeded = (state == KCAS_STATE_SUCCEEDED);

    for (int i = 0; i < snapshot->numEntries; i++) {
        casword_t newval = succeeded ? snapshot->entries[i].newval : snapshot->entries[i].oldval;
        BOOL_CAS(snapshot->entries[i].addr, (casword_t)tagptr, newval);
    }

    return succeeded;
}

// TODO: replace crappy bubblesort with something fast for large MAX_K (maybe even use insertion sort for small MAX_K)
template <int MAX_K>
static void kcasdesc_sort(kcasptr_t ptr) {
    kcasentry_t temp;
    bool swapped = false;
    for (int i = 0; i < ptr->numEntries; i++) {
        for (int j = 0; j < ptr->numEntries - i - 1; j++) {
            if (ptr->entries[j].addr > ptr->entries[j + 1].addr) {
                temp = ptr->entries[j];
                ptr->entries[j] = ptr->entries[j + 1];
                ptr->entries[j + 1] = temp;
                swapped = true;
            }
        }
        if (!swapped) break;
    }
}

template <int MAX_K>
bool KCASHTM_FULL<MAX_K>::execute() {
    assert(kcas_tid.getId() != -1);

    if (htm_full.isActive()) {
        htm_full.commit();
        return true;
    }

    auto desc = &kcasDescriptors[kcas_tid.getId()];
    // sort entries in the kcas descriptor to guarantee progress

    DESC_INITIALIZED(kcasDescriptors, kcas_tid.getId());
    kcastagptr_t tagptr = TAGPTR_NEW(kcas_tid.getId(), desc->seqBits, KCAS_TAGBIT);

    for (int i = 0; i < MAX_SHORT_RETRIES; i++) {
        int status;
        if ((status = _xbegin()) == _XBEGIN_STARTED) {
            for (int j = 0; j < desc->numEntries; j++) {
                casword_t val = *desc->entries[j].addr;
                if (val != desc->entries[j].oldval) {
                    if (isKcas(val))
                        _xabort(HTM_READ_DESCRIPTOR);
                    _xabort(HTM_BAD_OLD_VAL);
                }
            }
            for (int j = 0; j < desc->numEntries; j++) {
                *desc->entries[j].addr = desc->entries[j].newval;
            }
            _xend();
            GSTATS_ADD(kcas_tid.getId(), htmpostfix_commit, 1);
            return true;
        } else {
            GSTATS_ADD(kcas_tid.getId(), htmpostfix_abort, 1);
            if (_XABORT_EXPLICIT & status) {
                if (_XABORT_CODE(status) == HTM_READ_DESCRIPTOR) {
                    break;
                } else if (_XABORT_CODE(status) == HTM_BAD_OLD_VAL) {
                    return false;
                }
            }
        }
    }

    kcasdesc_sort<MAX_K>(desc);
    return help(tagptr, desc, false);
}

template <int MAX_K>
inline casword_t KCASHTM_FULL<MAX_K>::readPtr(casword_t volatile *addr) {
    if (htm_full.isActive()) {
        casword_t r = *addr;
        if (isAnyDescriptor(r)) _xabort(ABORT_DESCRIPTOR);
        return r;
    } else {
        casword_t r;
        do {
            r = rdcssRead(addr);
            if (unlikely(isKcas(r))) {
                helpOther((kcastagptr_t)r);
            } else break;
        } while (true);
        return r;
    }
}

template <int MAX_K>
inline casword_t KCASHTM_FULL<MAX_K>::readVal(casword_t volatile *addr) {
    return ((casword_t)readPtr(addr)) >> KCAS_LEFTSHIFT;
}

template <int MAX_K>
inline void KCASHTM_FULL<MAX_K>::writeInitPtr(casword_t volatile *addr, casword_t const newval) {
    *addr = newval;
}

template <int MAX_K>
inline void KCASHTM_FULL<MAX_K>::writeInitVal(casword_t volatile *addr, casword_t const newval) {
    writeInitPtr(addr, newval << KCAS_LEFTSHIFT);
}

template <int MAX_K>
bool KCASHTM_FULL<MAX_K>::start() {
    kcas_tx_start_result ret = htm_full.tryStart();
    switch (ret) {
        case RETURN_FALSE: return false;
        case HTM_STARTED: return true;
        case FALLBACK: break;
    }

    // allocate a new kcas descriptor
    kcasptr_t ptr = DESC_NEW(kcasDescriptors, KCAS_SEQBITS_NEW, kcas_tid.getId());
    ptr->numEntries = 0;
    return true;
}

template <int MAX_K>
inline kcasptr_t KCASHTM_FULL<MAX_K>::getDescriptor() {
    return &kcasDescriptors[kcas_tid.getId()];
}

template <int MAX_K>
void KCASHTM_FULL<MAX_K>::deinitThread() {
    kcas_tid.explicitRelease();
}

template <int MAX_K>
template <typename T>
inline void KCASHTM_FULL<MAX_K>::add(casword<T> *caswordptr, T oldVal, T newVal) {
    if (htm_full.isActive()) {
        T val = caswordptr->getValue();
        // note: getValue() CANNOT return a descriptor... it will ABORT in that case! so no need to check for one.
        if (val != oldVal) { // assert val is not a descriptor
            _xabort(ABORT_RETURN_FALSE); // here i can prove my kcas should return false.
            // htm_full.abort(ABORT_DESCRIPTOR);
        }

        if constexpr (std::is_pointer<T>::value) {
            htm_full.add((casword_t *) &caswordptr->bits, (casword_t) newVal);
        } else {
            htm_full.add((casword_t *) &caswordptr->bits, (casword_t) (newVal << KCAS_LEFTSHIFT));
        }
    } else {
        caswordptr->addToDescriptor(oldVal, newVal);
    }
}

template <int MAX_K>
template <typename T, typename... Args>
void KCASHTM_FULL<MAX_K>::add(casword<T> *caswordptr, T oldVal, T newVal, Args... args) {
    if (htm_full.isActive()) {
        T val = caswordptr->getValue();
        // note: getValue() CANNOT return a descriptor... it will ABORT in that case! so no need to check for one.
        if (val != oldVal) { // assert val is not a descriptor
            _xabort(ABORT_RETURN_FALSE); // here i can prove my kcas should return false.
            // htm_full.abort(ABORT_DESCRIPTOR);
        }

        if constexpr (std::is_pointer<T>::value) {
            htm_full.add((casword_t *) &caswordptr->bits, (casword_t) newVal);
        } else {
            htm_full.add((casword_t *) &caswordptr->bits, (casword_t) (newVal << KCAS_LEFTSHIFT));
        }
    } else {
        caswordptr->addToDescriptor(oldVal, newVal);
    }
    add(args...);
}
