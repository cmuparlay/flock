/* 
 * File:   hashlist.h
 * Author: trbot
 * 
 * Note: the list code is based loosely on the list for TL2.
 *
 * Created on May 10, 2017, 4:44 PM
 */

#ifndef HASHLIST_H
#define HASHLIST_H

#if !defined USE_STL_HASHLIST && !defined USE_SIMPLIFIED_HASHLIST
    #define USE_SIMPLIFIED_HASHLIST
#endif

#ifdef USE_STL_HASHLIST
    #include <unordered_set>

    template <typename T>
    class HashList {
    private:
        unordered_set<T> * hashlist;

    public:
        HashList() {
            hashlist = new unordered_set<T>;
        }
        ~HashList() {
            delete hashlist;
        }

        void init(long initCapacityPow2) {
            hashlist->reserve(initCapacityPow2);
        }

        void destroy() {}

        inline void clear() {
            hashlist->clear();
        }

        inline bool contains(const T& element) {
            return hashlist->find(element) != hashlist->end();
        }

        inline void insert(const T& element) {
            hashlist->insert(element);
        }
    };

#elif defined USE_SIMPLIFIED_HASHLIST

    #include "plaf.h"
    #ifndef BIG_CONSTANT
        #define BIG_CONSTANT(x) (x##LLU)
    #endif
    #ifndef ERROR
        #include <iostream>

        #define aout(x) { std::cout<<x<<std::endl; }
        #define ERROR(s) { aout("ERROR: "<<s); exit(-1); }
    #endif

    /* node in a list AND in a hash table */
    template <typename T>
    class HLNode {
    public:
        HLNode<T> * next;
        T element;
        int hashTableIndex;
        
        HLNode(HLNode<T> * next, T element)
        : next(next), element(element), hashTableIndex(-1) {}
    };

    template <typename T>
    class HashTable {
    private:
        HLNode<T> ** data;
        long long size;     // number of elements in the hash table
        long long capacity; // capacity of the hash table

        inline int32_t hash(const T& element) {
            if (sizeof(element) == 8) {
                unsigned long long p = (unsigned long long) element;
                p ^= p >> 33;
                p *= BIG_CONSTANT(0xff51afd7ed558ccd);
                p ^= p >> 33;
                p *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
                p ^= p >> 33;
                assert(__builtin_popcount(capacity) == 1); // capacity is a power of 2
                p = p & (capacity - 1);
                assert(p < INT32_MAX);
                return p;
            } else if (sizeof(element) == 4) {
                unsigned int p = (unsigned int) element;
                p ^= p >> 16;
                p *= 0x85ebca6b;
                p ^= p >> 13;
                p *= 0xc2b2ae35;
                p ^= p >> 16;
                assert(__builtin_popcount(capacity) == 1); // capacity is a power of 2
                p = p & (capacity - 1);
                assert(p < INT32_MAX);
                return p;
            } else {
                ERROR("no hash function defined for element of size "<<sizeof(element));
            }
        }
        
        inline int32_t findIx(const T& element) {
            assert(__builtin_popcount(capacity) == 1);  // capacity is a power of 2
            int32_t ix = hash(element);
            int i=0;                                    // quadratic probing
            while (data[ix]) {
                ++i;                                    // quadratic probing
                if (data[ix]->element == element) return ix;
                ix = (ix + i*i) & (capacity - 1);       // quadratic probing
            }
            return ix;
        }

    private:
        inline void tryExpand(HLNode<T>* head) {
            if (size * 2 < capacity) return;
            
            const long long oldCapacity = capacity;
            capacity = oldCapacity*2;
            //cout<<"expanding table from "<<oldCapacity<<" to "<<capacity<<" because size="<<size<<std::endl;
            delete[] data;
            data = new HLNode<T> * [capacity];
            memset(data, 0, sizeof(HLNode<T> *) * capacity);
            size = 0; // since we are reinserting everything...

            for (HLNode<T> * curr = head; curr; curr = curr->next) {
                insertNode(curr, head);
            }
        }
        
    public:
        void init(const long long initialCapacityPow2) {
            assert(__builtin_popcount(initialCapacityPow2) == 1); // initialCapacity is a power of 2
            size = 0;
            capacity = initialCapacityPow2;
            data = new HLNode<T> * [capacity];
            for (int i=0;i<capacity;++i) data[i] = NULL;
//            memset(data, 0, sizeof(HLNode<T> *) * capacity);
        }
        
        void destroy() {
            delete[] data;
        }
        
        inline HLNode<T> * find(const T& element) {
            int32_t ix = findIx(element);
            return data[ix];
        }
        
        // assumes: node is not in the hash table
        inline void insertNode(HLNode<T> * node, HLNode<T> * head) {
            assert(node);
            assert(find(node->element) == NULL);
            tryExpand(head);
            assert(__builtin_popcount(capacity) == 1); // capacity is a power of 2
            int32_t ix = findIx(node->element);
            assert(ix >= 0 && ix < capacity);
            assert(data[ix] == NULL);
            data[ix] = node;
            node->hashTableIndex = ix;
            assert(data[node->hashTableIndex] == node);
            ++size;
        }
        
        inline void clear(HLNode<T>* head) {
            //cout<<" clearing hashtable size="<<size<<" ...";
            int cnt = 0;

            if (size * 4 < capacity) {
                // clear hash table by using the list
                for (HLNode<T> * curr = head; curr; curr = curr->next) {
                    const int ix = curr->hashTableIndex;
                    assert(ix >= 0 && ix < capacity);
                    assert(data[ix] == curr);
                    data[ix] = NULL;
                    cnt++;
                }
                size = 0;
            } else {
                // clear entire hash table
                for (int i=0;i<capacity;++i) data[i] = NULL;
                size = 0;
                cnt = capacity;
            }
            
            //cout<<" cells set to NULL="<<cnt<<std::endl;
        }
    };
    
    template <typename T>
    class HashList {
    private:
        volatile char padding0[PREFETCH_SIZE_BYTES];
        HashTable<T> ht;
        HLNode<T> * head;
        HLNode<T> * tail;
        HLNode<T> * freeNodes;
        long long size;
        volatile char padding1[PREFETCH_SIZE_BYTES];
        
        inline HLNode<T> * allocateNode(const T& element) {
            if (freeNodes) {
                HLNode<T> * temp = freeNodes;
                freeNodes = freeNodes->next;
                temp->element = element;
                temp->next = head;
                return temp;
            }
            return new HLNode<T>(head, element);
        }
        
    public:
        void init(const long long initialCapacityPow2) {
            head = NULL;
            tail = NULL;
            freeNodes = NULL;
            size = 0;
            ht.init(initialCapacityPow2);
        }
        
        inline void clear() {
            //cout<<"clearing list size="<<size<<" ...";
            
            // clear hash table
            ht.clear(head);
            
            // move list nodes into freeNodes
            if (tail) {
                tail->next = freeNodes;
                freeNodes = head;
            }
            head = NULL;
            tail = NULL;
            size = 0;
        }

        void destroy() {
            clear();
            ht.destroy();
            
            // delete freeNodes
            HLNode<T> * curr = freeNodes;
            while (curr) {
                HLNode<T> * temp = curr;
                curr = curr->next;
                delete temp;
            }
        }
        
        inline bool contains(const T& element) {
            return ht.find(element);
        }
        
        inline void insert(const T& element) {
            // first check if element is in the hash table
            HLNode<T> * node = ht.find(element);
            if (node == NULL) {
                // element is not in the hash table,
                // so we need to insert it into both the list and hash table
                node = allocateNode(element);   // allocate node AND set its next pointer (to head) and element (to element)
                ht.insertNode(node, head);      // insert it into the hash table
                if (!head) {
                    tail = node;
                }
                head = node;                    // insert it into the list
                // note: list insertion should happen after hash table insertion, because, if the table is expanded and rehashed, we want it only to rehash keys starting from the old head, THEN add the new key.
                ++size;
            }
        }
        
        inline long long getSize() { return size; }
    };

#else

    #define HASHTABLE_CLEAR_FROM_LIST
    #define USE_FULL_HASHTABLE

    #include <cassert>
    #include <iostream>

    #define aout(x) { \
        std::cout<<x<<std::endl; \
    }
    #ifndef DEBUG3
    #define DEBUG3 if(0)
    #endif
    #define ERROR(s) { aout("ERROR: "<<s); exit(-1); }
    #define __INLINE__ inline
    #define VALIDATE if(0)
    #define VALIDATE_INV(arg) VALIDATE (arg)->validateInvariants();
    #define renamePointer(arg) (arg)
    #define BIG_CONSTANT(x) (x##LLU)
    #ifndef CACHE_LINE_SIZE
    #   define CACHE_LINE_SIZE 64
    #endif
    #define debug(x) #x<<"="<<(x)

    /* list element (serves as an entry in a list and in a hash table) */
    template <typename T>
    class HLNode {
    public:
        HLNode<T>* Next;
        HLNode<T>* Prev;
        T element;
        long Ordinal;
        HLNode<T>** hashTableEntry;

        HLNode() {}
        HLNode(HLNode<T>* _Next, HLNode<T>* _Prev, long _Ordinal)
            : Next(_Next), Prev(_Prev), Ordinal(_Ordinal), hashTableEntry(0)
        {}

        void validateInvariants() {}
    };

    template <typename T>
    class HashTable {
    public:
        HLNode<T>** data;
        long sz; // number of elements in the hash table
        long cap; // capacity of the hash table
    private:

        void validateInvariants() {
            // hash table capacity is a power of 2
            long htc = cap;
            while (htc > 0) {
                if ((htc & 1) && (htc != 1)) {
                    ERROR(debug(cap) << " is not a power of 2");
                }
                htc /= 2;
            }
            // htabcap >= 2*htabsz
            if (requiresExpansion()) {
                ERROR("hash table capacity too small: " << debug(cap) << " " << debug(sz));
            }
    #ifdef LONG_VALIDATION
            // htabsz = size of hash table
            long _htabsz = 0;
            for (int i = 0; i < cap; ++i) {
                if (data[i]) {
                    ++_htabsz; // # non-null entries of htab
                }
            }
            if (sz != _htabsz) {
                ERROR("hash table size incorrect: " << debug(sz) << " " << debug(_htabsz));
            }
    #endif
        }

    public:

        __INLINE__ void init(const long _sz) {
            // assert: _sz is a power of 2!
            DEBUG3 aout("hash table " << renamePointer(this) << " init");
            sz = 0;
            cap = 2 * _sz;
            data = (HLNode<T>**) malloc(sizeof (HLNode<T>*) * cap);
            memset(data, 0, sizeof (HLNode<T>*) * cap);
            VALIDATE_INV(this);
        }

        __INLINE__ void destroy() {
            DEBUG3 aout("hash table " << renamePointer(this) << " destroy");
            free(data);
        }

        __INLINE__ int32_t hash(const T& element) {
            // assert: htabcap is a power of 2
            if (sizeof(element) == 8) {
                unsigned long long p = (unsigned long long) element;
                p ^= p >> 33;
                p *= BIG_CONSTANT(0xff51afd7ed558ccd);
                p ^= p >> 33;
                p *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
                p ^= p >> 33;
                assert(0 <= (p & (cap - 1)) && (p & (cap - 1)) < INT32_MAX);
                return p & (cap - 1);
            } else if (sizeof(element) == 4) {
                unsigned int p = (unsigned int) element;
                p ^= p >> 16;
                p *= 0x85ebca6b;
                p ^= p >> 13;
                p *= 0xc2b2ae35;
                p ^= p >> 16;
                assert(0 <= (p & (cap - 1)) && (p & (cap - 1)) < INT32_MAX);
                return p & (cap - 1);
            } else {
                ERROR("no hash function defined for element of size "<<sizeof(element));
            }
        }

        __INLINE__ int32_t findIx(const T& element) {
    //        int k = 0;
            int32_t ix = hash(element);
            while (data[ix]) {
                if (data[ix]->element == element) {
                    return ix;
                }
                ix = (ix + 1) & (cap - 1);
    //            ++k; if (k > 100*cap) { exit(-1); } // TODO: REMOVE THIS DEBUG CODE: catch infinite loops
            }
            return -1;
        }

        __INLINE__ HLNode<T>* find(const T& element) {
            int32_t ix = findIx(element);
            if (ix < 0) return NULL;
            return data[ix];
        }

        // assumes there is space for e, and e is not in the hash table
        __INLINE__ void insertFresh(HLNode<T>* e) {
            DEBUG3 aout("hash table " << renamePointer(this) << " insertFresh(" << debug(e) << ")");
            VALIDATE_INV(this);
            int32_t ix = hash(e->element);
            while (data[ix]) { // assumes hash table does NOT contain e
                ix = (ix + 1) & (cap - 1);
            }
            data[ix] = e;
    #ifdef HASHTABLE_CLEAR_FROM_LIST
            e->hashTableEntry = &data[ix];
    #endif
            ++sz;
            VALIDATE_INV(this);
        }

        __INLINE__ int requiresExpansion() {
            return 2 * sz > cap;
        }

    private:
        // expand table by a factor of 2

        __INLINE__ void expandAndClear() {
            HLNode<T>** olddata = data;
            init(cap); // note: cap will be doubled by init
            free(olddata);
        }

    public:

        __INLINE__ void expandAndRehashFromList(HLNode<T>* head, HLNode<T>* stop) {
            DEBUG3 aout("hash table " << renamePointer(this) << " expandAndRehashFromList");
            VALIDATE_INV(this);
            expandAndClear();
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                insertFresh(e);
            }
            VALIDATE_INV(this);
        }

        __INLINE__ void clear(HLNode<T>* head, HLNode<T>* stop) {
    #ifdef HASHTABLE_CLEAR_FROM_LIST
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                //assert(*e->hashTableEntry);
                //assert(*e->hashTableEntry == e);
                *e->hashTableEntry = NULL;
            }
    #else
            memset(data, 0, sizeof (HLNode<T>*) * cap);
    #endif
        }

        void validateContainsAllAndSameSize(HLNode<T>* head, HLNode<T>* stop, const int listsz) {
            // each element of list appears in hash table
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                if (find(e->element) != e) {
                    ERROR("element " << debug(*e) << " of list was not in hash table");
                }
            }
            if (listsz != sz) {
                ERROR("list and hash table sizes differ: " << debug(listsz) << " " << debug(sz));
            }
        }
    };

    template <typename T>
    class HashList {
    public:
        // linked list (for iteration)
        HLNode<T>* head;
        HLNode<T>* put;    /* Insert position - cursor */
        HLNode<T>* tail;   /* CCM: Pointer to last valid entry */
        HLNode<T>* end;    /* CCM: Pointer to last entry */
        long ovf;       /* Overflow - request to grow */
        long initcap;
        long currsz;

        HashTable<T> tab;

    private:
        __INLINE__ HLNode<T>* extendList() {
            VALIDATE_INV(this);
            // Append at the tail. We want the front of the list,
            // which sees the most traffic, to remain contiguous.
            ovf++;
            HLNode<T>* e = (HLNode<T>*) malloc(sizeof(*e));
            assert(e);
            tail->Next = e;
            *e = HLNode<T>(NULL, tail, tail->Ordinal+1);
            end = e;
            VALIDATE_INV(this);
            return e;
        }

        void validateInvariants() {
            // currsz == size of list
            long _currsz = 0;
            HLNode<T>* stop = put;
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                VALIDATE_INV(e);
                ++_currsz;
            }
            if (currsz != _currsz) {
                ERROR("list size incorrect: "<<debug(currsz)<<" "<<debug(_currsz));
            }

            // capacity is correct and next fields are not too far
            long _currcap = 0;
            for (HLNode<T>* e = head; e; e = e->Next) {
                VALIDATE_INV(e);
                if (e->Next > head+initcap && ovf == 0) {
                    ERROR("list has AVPair with a next field that jumps off the end of the AVPair array, but ovf is 0;");// "<<debug(*e));
                }
                if (e->Next && e->Next != e+1) {
                    ERROR("list has incorrect distance between AVPairs; "<<debug(e)<<" "<<debug(e->Next));
                }
                ++_currcap;
            }
            if (_currcap != initcap) {
                ERROR("list capacity incorrect: "<<debug(initcap)<<" "<<debug(_currcap));
            }
        }
    public:
        void init(long initCapacityPow2) {
            DEBUG3 aout("list "<<renamePointer(this)<<" init");
            // assert: _sz is a power of 2

            // Allocate the primary list as a large chunk so we can guarantee ascending &
            // adjacent addresses through the list. This improves D$ and DTLB behavior.
            head = (HLNode<T>*) malloc((sizeof (HLNode<T>) * initCapacityPow2) + CACHE_LINE_SIZE);
            assert(head);
            memset(head, 0, sizeof(HLNode<T>) * initCapacityPow2);
            HLNode<T>* curr = head;
            put = head;
            end = NULL;
            tail = NULL;
            for (int i = 0; i < initCapacityPow2; i++) {
                HLNode<T>* e = curr++;
                *e = HLNode<T>(curr, tail, i); // note: curr is invalid in the last iteration
                tail = e;
            }
            tail->Next = NULL; // fix invalid next pointer from last iteration
            initcap = initCapacityPow2;
            ovf = 0;
            currsz = 0;
            VALIDATE_INV(this);
    #ifdef USE_FULL_HASHTABLE
            tab.init(initCapacityPow2);
    #elif defined(USE_BLOOM_FILTER)
            tab.init();
    #endif
        }

        void destroy() {
            DEBUG3 aout("list "<<renamePointer(this)<<" destroy");
            /* Free appended overflow entries first */
            HLNode<T>* e = end;
            if (e != NULL) {
                while (e->Ordinal >= initcap) {
                    HLNode<T>* tmp = e;
                    e = e->Prev;
                    free(tmp);
                }
            }
            /* Free contiguous beginning */
            free(head);
    #if defined(USE_FULL_HASHTABLE) || defined(USE_BLOOM_FILTER)
            tab.destroy();
    #endif
        }

        __INLINE__ void clear() {
            DEBUG3 aout("list "<<renamePointer(this)<<" clear");
            VALIDATE_INV(this);
    #ifdef USE_FULL_HASHTABLE
            tab.clear(head, put);
    #elif defined(USE_BLOOM_FILTER)
            tab.init();
    #endif
            // free any allocated overflow nodes
            HLNode<T>* e = end;
            if (e != NULL) {
                while (e->Ordinal >= initcap) {
                    HLNode<T>* tmp = e;
                    e = e->Prev;
                    free(tmp);
                }
            }

            // "forget" data in the array
            put = head;
            tail = NULL;
            currsz = 0;
            VALIDATE_INV(this);
        }

        __INLINE__ HLNode<T>* find(const T& element) {
    #ifdef USE_FULL_HASHTABLE
            return tab.find(element);
    #elif defined(USE_BLOOM_FILTER)
            if (!tab.contains(element)) return NULL;
    #endif
            HLNode<T>* stop = put;
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                if (e->element == element) {
                    return e;
                }
            }
            return NULL;
        }

        __INLINE__ bool contains(const T& element) {
            return find(element) != NULL;
        }

    private:
        __INLINE__ HLNode<T>* append(const T& element) {
            HLNode<T>* e = put;
            if (e == NULL) e = extendList();
            tail = e;
            put = e->Next;
            e->element = element;
    //        e->hashTableEntry = NULL;
            VALIDATE ++currsz;
            return e;
        }

    public:
        __INLINE__ void insert(const T& element) {
            //DEBUG3 aout("list "<<renamePointer(this)<<" insert("<<debug(renamePointer((const void*) element))<<")");
            HLNode<T>* e = find(element);
            if (e) {
                // do nothing
            } else {
                e = append(element);
    #ifdef USE_FULL_HASHTABLE
                // insert in hash table
                tab.insertFresh(e);
                if (tab.requiresExpansion()) tab.expandAndRehashFromList(head, put);
    #elif defined(USE_BLOOM_FILTER)
                tab.insertFresh(element);
    #endif
            }
        }

        void copyToArray(T * const dest) {
            int k = 0;
            HLNode<T>* stop = put;
            for (HLNode<T>* e = head; e != stop; e = e->Next) {
                dest[k++] = e->element;
            }
        }

        void validateContainsAllAndSameSize(HashTable<T>* tab) {
    #ifdef USE_FULL_HASHTABLE
            if (currsz != tab->sz) {
                ERROR("hash table "<<debug(tab->sz)<<" has different size from list "<<debug(currsz));
            }
            HLNode<T>* stop = put;
            // each element of hash table appears in list
            for (int i=0;i<tab->cap;++i) {
                HLNode<T>* elt = tab->data[i];
                if (elt) {
                    // element in hash table; is it in the list?
                    bool found = false;
                    for (HLNode<T>* e = head; e != stop; e = e->Next) {
                        if (e == elt) {
                            found = true;
                        }
                    }
                    if (!found) {
                        ERROR("element "<<debug(*elt)<<" of hash table was not in list");
                    }
                }
            }
    #endif
        }
    };

#endif // #if !defined USE_STL_HASHLIST

#endif /* HASHLIST_H */

