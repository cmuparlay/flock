#pragma once 

#include "record_manager.h"
#include "global.h"
#include "error.h"
#include "helper.h"         // for itemid_t declaration

class table_t;

#define KEY_TYPE idx_key_t
#define VALUE_TYPE itemid_t *
#define __NO_KEY -1
#define __NO_VALUE NULL
#define _TABSZ (1<< 20)
typedef uintptr_t vwlock;  /* (Version,LOCKBIT) */
#define TABMSK (_TABSZ-1)
#define UNS(a) ((uintptr_t)(a))
#define PSSHIFT ((sizeof(void*) == 4) ? 2 : 3)
//#define COLOR (128)
//#define PSLOCK(a) (LockTab + (((UNS(a)+COLOR) >> PSSHIFT) & TABMSK))

#define BIG_CONSTANT(x) (x##LLU)
#define PSLOCK(v) (LockTab + (UNS(hash_murmur3((v))) & TABMSK))
static inline uint64_t hash_murmur3(KEY_TYPE v) {
    // assert: _TABSZ is a power of 2 and KEY_TYPE is 64-bits
    v ^= v >> 33;
    v *= BIG_CONSTANT(0xff51afd7ed558ccd);
    v ^= v >> 33;
    v *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    v ^= v >> 33;
//    assert(0 <= (v & (_TABSZ-1)) && (v & (_TABSZ-1)) < INT32_MAX);
    return v; //& (_TABSZ-1);
}

class index_base {
protected:
    PAD;
    char initializedThreads[MAX_THREADS_POW2*PREFETCH_SIZE_BYTES];
    unsigned long long numInserts[MAX_THREADS_POW2*PREFETCH_SIZE_WORDS];
    unsigned long long numReads[MAX_THREADS_POW2*PREFETCH_SIZE_WORDS];
//    PAD; // not needed because of post padding on numReads
//    #define INCREMENT_NUM_INSERTS(tid) { if ((++numInserts[(tid)*PREFETCH_SIZE_WORDS] % 100000) == 0) cout<<"tid="<<tid<<": numInserts="<<numInserts[(tid)*PREFETCH_SIZE_WORDS]<<std::endl; }
//    #define INCREMENT_NUM_READS(tid) { if ((++numReads[(tid)*PREFETCH_SIZE_WORDS] % 100000) == 0) cout<<"tid="<<tid<<": numReads="<<numReads[(tid)*PREFETCH_SIZE_WORDS]<<std::endl; }
    #define INCREMENT_NUM_INSERTS(tid) 
    #define INCREMENT_NUM_READS(tid) 
    #define INCREMENT_NUM_RQS(tid) 
    
    PAD;
    volatile vwlock LockTab[_TABSZ];
    PAD;
    
    #define PRINT_BUCKETS 30
    long long lockHistogram[PRINT_BUCKETS];
    PAD; // needed because locktab might falsely share with some following fields (including index_id, which is read on every stat tracking increment
    uint64_t debug_init_is_done;
    PAD;
public:
    string index_name;
    int index_id;
    
    index_base() {
//        std::cout<<"start index_base()"<<std::endl;
        assert(sizeof(LockTab) == sizeof(vwlock)*_TABSZ);
        memset((void *) LockTab, 0, sizeof(LockTab));
        //std::cout<<"    sizeof(LockTab)="<<sizeof(LockTab)<<std::endl;
        memset(numInserts, 0, MAX_THREADS_POW2*PREFETCH_SIZE_WORDS*sizeof(unsigned long long));
        memset(numReads, 0, MAX_THREADS_POW2*PREFETCH_SIZE_WORDS*sizeof(unsigned long long));
        debug_init_is_done = 0xCAFEBABE;
//        std::cout<<"  end index_base()"<<std::endl;
    }

//    virtual bool index_exist(KEY_TYPE key) = 0; // check if the key exist.

    // NOTE: IF KEY EXISTS, THEN WE NEED TO INSERT ITEM INTO A LINKED LIST
    //       OF ITEMS FOUND AT THE NODE CONTAINING KEY.
    //       This requirement comes from the fact that DBx1000 uses
    //       this linked list to do a limited form of range queries.
    //       To maintain atomicity when modifying this list,
    //       we lock the target key, then perform the insertion.
    //       If there was already a value (a preexisting linked list),
    //       then the insertion will replace that value and return it,
    //       and we will append that list to our newly inserted list.
    virtual RC index_insert(KEY_TYPE key,
                            VALUE_TYPE item,
                            int part_id = -1) = 0;
    virtual RC index_read(KEY_TYPE key,
                  VALUE_TYPE * item,
                  int part_id = -1, int thd_id = 0) = 0;
    virtual RC index_read(KEY_TYPE key, VALUE_TYPE * item, int part_id = -1) {
        return index_read(key, item, part_id, 0);
    }
    virtual RC index_read(KEY_TYPE key, VALUE_TYPE * item) {
        return index_read(key, item, -1, 0);
    }
    
    // TODO implement index_remove

    virtual RC index_remove(KEY_TYPE key) {
        return RCOK;
    }
    
    virtual void print_stats(){}
    virtual size_t getNodeSize(){return 0;}
    virtual size_t getDescriptorSize(){return 0;}
private:
    inline void vwlock_acquire(volatile vwlock * lock) {
        assert(debug_init_is_done == 0xCAFEBABE);
        assert(((uint64_t) lock) >= ((uint64_t) LockTab));
        assert(((uint64_t) lock) <= ((uint64_t) LockTab)+sizeof(LockTab) - sizeof(vwlock));
        while (1) {
            vwlock val = *lock;
            if (val & 1) continue; // wait while lock is held
            if (__sync_bool_compare_and_swap(lock, val, val+1)) break;
        }
    }
    inline void vwlock_release(volatile vwlock * lock) {
        SOFTWARE_BARRIER;
        ++(*lock);
    }
    
public:
    inline void lock_key(KEY_TYPE key) {
        vwlock_acquire(PSLOCK(key));
    }
    inline void unlock_key(KEY_TYPE key) {
        vwlock_release(PSLOCK(key));
    }
    inline vwlock read_lock_state(KEY_TYPE key) {
        return *PSLOCK(key);
    }
    inline bool state_is_locked(vwlock s) {
        return s&1;
    }
    inline size_t state_get_version(vwlock s) {
        return s>>1;
    }

//private:
//    inline void vwlock_acquire(volatile vwlock * lock);
//    inline void vwlock_release(volatile vwlock * lock);
//
//public:
//    inline void lock_key(KEY_TYPE key);
//    inline void unlock_key(KEY_TYPE key);
    
    void printLockCounts() {
        memset(lockHistogram, 0, sizeof(long long)*PRINT_BUCKETS);
        const long long bucketSize = _TABSZ/PRINT_BUCKETS;
        for (int i=0;i<_TABSZ;++i) {
            lockHistogram[i/bucketSize] += (LockTab[i]>>1);
        }
        printf("LOCK COUNTS:\n");
        for (int i=0;i<PRINT_BUCKETS;++i) {
            printf("%5d: %lld\n", i, lockHistogram[i]);
        }
    }
    
protected:

    // the index in on "table". The key is the merged key of "fields"
    table_t * table;
};
