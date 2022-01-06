//
// Created by demon1999 on 15.11.19.
//

#include "locks_impl.h"
#include <bits/stdc++.h>
using namespace std;
#ifndef LOCK_BASED_FLEX_LIST_FLEX_LIST_H
#define LOCK_BASED_FLEX_LIST_FLEX_LIST_H
const int MAX_LEVEL = 44;
const int MAX_THREADS = 1000;
const int PADDING_SIZE = 64;

template<typename K, typename V>
struct Node {
    volatile K key;
    volatile char pad[PADDING_SIZE];
    volatile V value;
    volatile char pad2[PADDING_SIZE];
    volatile int zeroLevel;
    volatile char pad3[PADDING_SIZE];
    volatile int lock;
    volatile char pad4[PADDING_SIZE];
    volatile int topLevel;
    volatile char pad5[PADDING_SIZE];
    volatile int selfhits;
    volatile int contains;
    Node<K, V>* volatile next[MAX_LEVEL + 1];
    volatile char pad6[PADDING_SIZE];
    volatile int hits[MAX_LEVEL + 1];
};

struct MyLength {
    long long value;
    volatile char pad[PADDING_SIZE];
    MyLength() {
        value = 0;
    }
};

struct MyCouner {
    long long value;
    volatile char pad[PADDING_SIZE];
    MyCouner() {
        value = 0;
    }
};

template<typename K, typename V, class RecordManager>
class FlexList {
private:
    volatile char pad_5[PADDING_SIZE];
    int UPDATE_NUM = 1;
    int kekerok = -1;
    volatile char pad_4[PADDING_SIZE];
    MyLength sumLengths[MAX_THREADS + 1];
    volatile char pad_3[PADDING_SIZE];
    volatile int zeroLevel = MAX_LEVEL - 1;
    volatile char pad_2[PADDING_SIZE];
    MyCouner update_counter[MAX_THREADS + 1];
    volatile char pad_1[PADDING_SIZE];
    Node<K, V>* head;
    volatile char pad0[PADDING_SIZE];
    Node<K, V>* tail;
    volatile char pad1[PADDING_SIZE];
    RecordManager * const recordManager;
    volatile char pad2[PADDING_SIZE];
    V noValue;
    volatile char pad3[PADDING_SIZE];
    int init[MAX_THREADS] = {0,};
    volatile char pad4[PADDING_SIZE];
    volatile long long sum_lengths = 0;
    volatile char pad5[PADDING_SIZE];
    long long accessCounter = 0;
public:
    FlexList(const int numThreads, const V noValue, const K minKey, const K maxKey);

    ~FlexList();

    void freeAll();

    Node<K, V>* createNode(const int tid, int h, const K& key, const V& value);

    V qfind(const int tid, const K &key);

    bool contains(const int tid, const K &key);

    V insertIfAbsent(const int tid, const K &key, const V &value);

    V erase(const int tid, const K &key);

    bool validate();

    void setCops(int cops);

    void printDebuggingDetails();

    void initThread(const int tid);

    void deinitThread(const int tid);

    long long getPathsLength(const int tid);
    int getHeight();

    std::vector<pair<int, int> > getPairsKeyHeight();

    std::vector<pair<int, int> > getPairsKeyContains();
    
    Node<K, V>* getRoot();

private:
    bool find(const int tid, const K& key, Node<K, V>*& pred, Node<K, V>*& succ);
    void update(const int tid, const K& key);
    void insert(const int tid, const int h, const K& key, const V& value, Node<K, V>* pred, Node<K, V>* predpred, int curAccess);
    V updateWithInsert(const int tid, const K& key, const V &value);
    void updateZeroLevel(Node<K, V> *curr);//This function supposes that curr is locked now
    int getHits(Node<K, V> *curr, int h);
};




template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::updateZeroLevel(Node<K, V> *curr) {
    // This function should be used only if we have lock on curr
    int currZeroLevel = zeroLevel;
    if ((curr->zeroLevel) > currZeroLevel) {
        //assert(cur->zeroLevel > 0);
        curr->hits[(curr->zeroLevel) - 1] = 0;
        curr->next[(curr->zeroLevel) - 1] = curr->next[(curr->zeroLevel)];
        (curr->zeroLevel)--;
    }
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::update(const int tid, const K& key) {
    acquireLock(&(head->lock));
    int curAccess = __sync_add_and_fetch(&accessCounter, 1);
    head->hits[MAX_LEVEL]++;
    Node<K, V>* pred = head;
    for (int h = MAX_LEVEL - 1; h >= zeroLevel; h--) {
        while (pred->zeroLevel > h) {
            updateZeroLevel(pred);
        }

        Node<K, V>* predpred = pred;
        Node<K, V>* cur = pred->next[h];

        if (cur->zeroLevel > h) {
            acquireLock(&(cur->lock));
            while (cur->zeroLevel > h) {
                updateZeroLevel(cur);
            }
            releaseLock(&(cur->lock));
        }
        __sync_fetch_and_add(&sum_lengths, 1);
        if ((cur->key) > key) {
            (pred->hits[h])++;
            continue;
        }
        bool foundKey = false;                
        while ((cur->key) <= key) {
            if (cur->zeroLevel > h) {
                acquireLock(&(cur->lock));
                while (cur->zeroLevel > h) {
                    updateZeroLevel(cur);
                }
                releaseLock(&(cur->lock));
            }
            __sync_fetch_and_add(&sum_lengths, 1);
            bool acquired = false;
            if((cur->next[h]->key) > key) {
                acquireLock(&(cur->lock));
                if ((cur->next[h]->key) <= key) {
                    releaseLock(&(cur->lock));
                } else {
                    acquired = true;
                    if (cur->key == key) {
                        cur->selfhits++;
                        foundKey = true;
                    } else
                        cur->hits[h]++;
                }
            }
            if (h + 1 < MAX_LEVEL && h < (predpred->topLevel) && (predpred->next[h]) == cur && ((predpred->hits[h + 1]) - (predpred->hits[h])) > (curAccess / (1 << (MAX_LEVEL - 1 - h - 1)))) {
                if (acquired == false) {
                    acquireLock(&(cur->lock));
                }
                int curh = (cur->topLevel);
                while (curh + 1 < MAX_LEVEL && curh < (predpred->topLevel) &&
                       ((predpred->hits[curh + 1]) - (predpred->hits[curh])) > (curAccess / (1 << (MAX_LEVEL - 1 - curh - 1)))) {
                    (cur->topLevel++);
                    curh++;
                    cur->hits[curh] = ((predpred->hits[curh]) - (predpred->hits[curh - 1]) - (cur->selfhits));
                    cur->next[curh] = predpred->next[curh];
                    predpred->next[curh] = cur;
                    predpred->hits[(curh)] = (predpred->hits[(curh) - 1]);
                }
                releaseLock(&(predpred->lock));
                predpred = cur;
                pred = cur;
                cur = cur->next[h];
                continue;
            } else if ((cur->topLevel) == h && (cur->next[h]->key) <= key && getHits(cur, h) + getHits(pred, h) <= (curAccess / (1 << (MAX_LEVEL - 1 - h)))) {
                int curZeroLevel = zeroLevel;
                if (pred != predpred) {
                    acquireLock(&(pred->lock));
                }
                acquireLock(&(cur->lock));
                if ((cur->topLevel) != h || getHits(cur, h) + getHits(pred, h) > (curAccess / (1 << (MAX_LEVEL - 1 - h))) ||
                    (cur->next[h]->key) > key || (pred->next[h]) != cur) {
                    if (pred != predpred) {
                        releaseLock(&(pred->lock));
                    }
                    releaseLock(&(cur->lock));
                    cur = pred->next[h];
                    continue;
                } else {
                    if (h == curZeroLevel) {
                        __sync_bool_compare_and_swap(&zeroLevel, curZeroLevel, curZeroLevel - 1);
                    }
                    //assert((cur->topLevel) == h);
                    if ((cur->zeroLevel) > h - 1) {
                        updateZeroLevel(cur);
                    }
                    if ((pred->zeroLevel) > h - 1) {
                        updateZeroLevel(pred);
                    }
                    int cur_hits = getHits(cur, h);
                    pred->hits[h] += cur_hits;
                    cur->hits[h] = 0;
                    pred->next[h] = cur->next[h];
                    cur->next[h] = NULL;
                    if (pred != predpred) {
                        releaseLock(&(pred->lock));
                    }
                    cur->topLevel--;
                    releaseLock(&(cur->lock));
                    cur = pred->next[h];
                    continue;
                }
            }
            pred = cur;
            cur = cur->next[h];
        }
        if (predpred != pred)
            releaseLock(&(predpred->lock));
        if (foundKey) {
            releaseLock(&(pred->lock));
            return;
        }
    }
    releaseLock(&(pred->lock));
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::insert(const int tid, const int h, const K & key, const V & value, Node<K, V> *pred, Node<K, V> *predpred, int curAccess) {
   // if (key == 5337) {
   //    std::cout << "Hay guy!\n";
   //    // kekerok = h;
   // }
   Node<K, V>* cur = createNode(tid, h, key, value);
   cur->next[h] = pred->next[h];
   acquireLock(&(cur->lock));
   // if (key == 5337) {
   //      std::cout << "zeroLevel\n";
   //    kekerok = pred->zeroLevel;
   // }
   pred->next[h] = cur;
   cur->selfhits++;
   if (h + 1 < MAX_LEVEL && h < (predpred->topLevel) && (predpred->next[h]) == cur &&
      ((predpred->hits[h + 1]) - (predpred->hits[h])) > (curAccess / (1 << (MAX_LEVEL - 1 - h - 1)))) {
      int curh = (cur->topLevel);
      while (curh + 1 < MAX_LEVEL && curh < (predpred->topLevel) &&
             ((predpred->hits[curh + 1]) - (predpred->hits[curh])) > (curAccess / (1 << (MAX_LEVEL - 1 - curh - 1)))) {
         (cur->topLevel++);
         curh++;
         cur->hits[curh] = ((predpred->hits[curh]) - (predpred->hits[curh - 1]) - (cur->selfhits));
         cur->next[curh] = predpred->next[curh];
         predpred->next[curh] = cur;
         predpred->hits[(curh)] = (predpred->hits[(curh) - 1]);
      }
      if (predpred != pred)
         assert(0);
      //releaseLock(&(predpred->lock));

   } else
   if (predpred != pred) {
      releaseLock(&(predpred->lock));
   }
   releaseLock(&(cur->lock));
}

template <typename K, typename V, class RecordManager>
V FlexList<K, V, RecordManager>::updateWithInsert(const int tid, const K & key, const V & value) {
    acquireLock(&(head->lock));
    int curAccess = __sync_add_and_fetch(&accessCounter, 1);
    head->hits[MAX_LEVEL]++;
    Node<K, V>* pred = head;
    for (int h = MAX_LEVEL - 1; h >= zeroLevel; h--) {
        //std::cout << h << " level\n";
        while (pred->zeroLevel > h) {
            updateZeroLevel(pred);
        }

        Node<K, V>* predpred = pred;
        Node<K, V>* cur = pred->next[h];

        if (cur->zeroLevel > h) {
            acquireLock(&(cur->lock));
            while (cur->zeroLevel > h) {
                updateZeroLevel(cur);
            }
            releaseLock(&(cur->lock));
        }
        __sync_fetch_and_add(&sum_lengths, 1);
        int curZeroLevel = zeroLevel;
        if ((cur->key) > key) {
           if (h == curZeroLevel) {
              insert(tid, h, key, value, pred, predpred, curAccess);
              break;
           }
           (pred->hits[h])++;
           continue;
        }
        bool foundKey = false;
        while ((cur->key) <= key) {
            if (cur->zeroLevel > h) {
                acquireLock(&(cur->lock));
                while (cur->zeroLevel > h) {
                    updateZeroLevel(cur);
                }
                releaseLock(&(cur->lock));
            }
            __sync_fetch_and_add(&sum_lengths, 1);
            bool acquired = false;
            if((cur->next[h]->key) > key) {
                acquireLock(&(cur->lock));
                if ((cur->next[h]->key) <= key) {
                    releaseLock(&(cur->lock));
                } else {
                    acquired = true;
                    if (cur->key == key) {
                        cur->selfhits++;
                        foundKey = true;
                    } else
                    {
                       if (h != curZeroLevel)
                          cur->hits[h]++;
                    }
                }
            }
            if (h + 1 < MAX_LEVEL && h < (predpred->topLevel) && (predpred->next[h]) == cur && ((predpred->hits[h + 1]) - (predpred->hits[h])) > (curAccess / (1 << (MAX_LEVEL - 1 - h - 1)))) {
                if (acquired == false) {
                    acquireLock(&(cur->lock));
                }
                int curh = (cur->topLevel);
                while (curh + 1 < MAX_LEVEL && curh < (predpred->topLevel) &&
                       ((predpred->hits[curh + 1]) - (predpred->hits[curh])) > (curAccess / (1 << (MAX_LEVEL - 1 - curh - 1)))) {
                    (cur->topLevel++);
                    curh++;
                    cur->hits[curh] = ((predpred->hits[curh]) - (predpred->hits[curh - 1]) - (cur->selfhits));
                    cur->next[curh] = predpred->next[curh];
                    predpred->next[curh] = cur;
                    predpred->hits[(curh)] = (predpred->hits[(curh) - 1]);
                }
                releaseLock(&(predpred->lock));
                predpred = cur;
                pred = cur;
                cur = cur->next[h];
                continue;
            } else if ((cur->topLevel) == h && (cur->next[h]->key) <= key && getHits(cur, h) + getHits(pred, h) <= (curAccess / (1 << (MAX_LEVEL - 1 - h)))) {
                int curZeroLevel = zeroLevel;
                if (pred != predpred) {
                    acquireLock(&(pred->lock));
                }
                acquireLock(&(cur->lock));
                if ((cur->topLevel) != h || getHits(cur, h) + getHits(pred, h) > (curAccess / (1 << (MAX_LEVEL - 1 - h))) ||
                    (cur->next[h]->key) > key || (pred->next[h]) != cur) {
                    if (pred != predpred) {
                        releaseLock(&(pred->lock));
                    }
                    releaseLock(&(cur->lock));
                    cur = pred->next[h];
                    continue;
                } else {
                    if (h == curZeroLevel) {
                        __sync_bool_compare_and_swap(&zeroLevel, curZeroLevel, curZeroLevel - 1);
                    }
                    //assert((cur->topLevel) == h);
                    if ((cur->zeroLevel) > h - 1) {
                        updateZeroLevel(cur);
                    }
                    if ((pred->zeroLevel) > h - 1) {
                        updateZeroLevel(pred);
                    }
                    int cur_hits = getHits(cur, h);
                    pred->hits[h] += cur_hits;
                    cur->hits[h] = 0;
                    pred->next[h] = cur->next[h];
                    cur->next[h] = NULL;
                    if (pred != predpred) {
                        releaseLock(&(pred->lock));
                    }
                    cur->topLevel--;
                    releaseLock(&(cur->lock));
                    cur = pred->next[h];
                    continue;
                }
            }
            pred = cur;
            cur = cur->next[h];
        }
        if (h == curZeroLevel && zeroLevel == curZeroLevel && !foundKey) {
           insert(tid, h, key, value, pred, predpred, curAccess);
           break;
        }
        if (predpred != pred)
            releaseLock(&(predpred->lock));
        if (foundKey) {
            V v;
            do {
                v = pred->value;
                if (v != noValue) {
                    releaseLock(&(pred->lock));
                    return v;
                }
            } while (!__sync_bool_compare_and_swap(&(pred->value), noValue, value));
            releaseLock(&(pred->lock));
            //std::cout << "endinsert\n";
            return noValue;
        }
        if (h == curZeroLevel) {
            pred->hits[h]++;
        }
    }
    releaseLock(&(pred->lock));
    //std::cout << "endinsert\n";
    return noValue;
}

template <typename K, typename V, class RecordManager>
int FlexList<K, V, RecordManager>::getHits(Node<K, V> *curr, int h) {
    if (curr->zeroLevel > h)
        return curr->selfhits;
    return curr->selfhits + curr->hits[h]; //think how to make this place better
}


template <typename K, typename V, class RecordManager>
Node<K, V>* FlexList<K, V, RecordManager>::createNode(const int tid, int h, const K& key, const V& value) {
    Node<K, V>* node = recordManager->template allocate<Node<K,V>>(tid);//new Node<K, V>();//
    node->topLevel = h;
    node->key = key;
    node->value = value;
    node->lock = 0;
    node->zeroLevel = h;
    node->selfhits = 0;
    node->contains = 0;

    for (int i = 0; i <= MAX_LEVEL; i++) {
        node->hits[i] = 0;
        node->next[i] = NULL;
    }
    return node;
}

template <typename K, typename V, class RecordManager>
FlexList<K, V, RecordManager>::FlexList(const int numThreads, const V noValue, const K minKey, const K maxKey) :
    recordManager(new RecordManager(numThreads)) {
    const int tid = 0;
    initThread(tid);
    UPDATE_NUM = 100 * numThreads;
    recordManager->endOp(tid);
    head = createNode(tid, zeroLevel, minKey, 0);
    tail = createNode(tid, zeroLevel, maxKey, 0);
    head->topLevel = MAX_LEVEL;
    tail->topLevel = MAX_LEVEL;
    this->noValue = noValue;
    for (unsigned int i = 0; i <= numThreads; i++) {
        sumLengths[i] = MyLength();
        update_counter[i] = MyCouner();
    }
    for (int i = zeroLevel; i <= MAX_LEVEL; i++) {
        head->next[i] = tail;
        tail->next[i] = NULL;
    }
}

template <typename K, typename V, class RecordManager>
bool FlexList<K, V, RecordManager>::contains(const int tid, const K &key) {
//   std::cout << "Contains: " << key << std::endl;
//    update(key);
//    return true;
    Node<K, V>* pred;
    Node<K, V>* succ;
    if (find(tid, key, pred, succ)) {
        update_counter[tid].value++;
        if (update_counter[tid].value == UPDATE_NUM) {
            update(tid, key);
            update_counter[tid].value = 0;
        }
        return succ->value != noValue;
    }
    return false;
}

template <typename K, typename V, class RecordManager>
V FlexList<K, V, RecordManager>::qfind(const int tid, const K &key) {
//    std::cout << "Find: " << key << std::endl;
    Node<K, V>* pred;
    Node<K, V>* succ;
    if (find(tid, key, pred, succ)) {
        V v = (succ->value);
        update_counter[tid].value++;
        if (update_counter[tid].value == UPDATE_NUM) {
            update(tid, key);
            update_counter[tid].value = 0;
        }
        return v;
    }
    return noValue;
}


template <typename K, typename V, class RecordManager>
V FlexList<K, V, RecordManager>::insertIfAbsent(const int tid, const K &key, const V &value) {
//    std::cout << "Insert: " << key << std::endl;
    //std::cout << tid << "insert\n";
//    long long save = sumLengths[tid].value;

    Node<K, V>* pred;
    Node<K, V>* succ;
    if (find(tid, key, pred, succ)) {
        update_counter[tid].value++;
        if (update_counter[tid].value == UPDATE_NUM) {
            update(tid, key);
            update_counter[tid].value = 0;
        }

        V v;
        do {
            v = succ->value;
            if (v != noValue) return v;
        } while (!__sync_bool_compare_and_swap(&(succ->value), noValue, value));
//        sumLengths[tid].value = save;
        return noValue;
    }
//    sumLengths[tid].value = save;
    return updateWithInsert(tid, key, value);
}

template <typename K, typename V, class RecordManager>
V FlexList<K, V, RecordManager>::erase(const int tid, const K &key) {
//    long long save = sumLengths[tid].value;
    Node<K, V>* pred;
    Node<K, V>* succ;
    if (find(tid, key, pred, succ)) {
        update_counter[tid].value++;
        if (update_counter[tid].value == UPDATE_NUM) {
            update(tid, key);
            update_counter[tid].value = 0;
        }

        V v;
        do {
            v = succ->value;
            if (v == noValue) return noValue;
        } while (!__sync_bool_compare_and_swap(&(succ->value), v, noValue));
//        sumLengths[tid].value = save;
        return v;
    }
//    sumLengths[tid].value = save;
    return noValue;
}

template <typename K, typename V, class RecordManager>
bool FlexList<K, V, RecordManager>::validate() {
   for (int h = zeroLevel; h < MAX_LEVEL - 1; h++) {
       Node<K, V>* cur = head;
       long long sum = 0, val = 0;
       while (cur != tail) {
           if (cur->topLevel > h) {
               if (sum != val) {
                   std::cout << accessCounter << "\n";
                   printDebuggingDetails();
                   std::cout << "Bad sum of hits!\n";
                   std::cout << "tL: " << (cur->topLevel) << " lv: " << h << ", zL " << zeroLevel << ", " << (cur->key) << "\n";
                   return false;
               }
               val = getHits(cur, h + 1);
               sum = 0;
           } else if (val - sum > (accessCounter / (1 << (MAX_LEVEL - 1 - h - 1)))) {
               printDebuggingDetails();
               std::cout << "Wrong ascent condition!\n";
               std::cout << "key" << (cur->key) << " " << val - sum << " " << "level" << h << " " << zeroLevel << " " << accessCounter << " " << (1 << (MAX_LEVEL - 1 - h - 1)) << std::endl;
               return false;
           }
           if ((cur->topLevel) < h) {
               printDebuggingDetails();
               std::cout << "Wrong level of element!\n";
               return false;
           }
           if ((cur->zeroLevel) > (cur->topLevel)) {
               printDebuggingDetails();
               std::cout << "Wrong level of element!\n";
               return false;
           }
           sum += getHits(cur, h);
           Node<K, V>* cnext = NULL;
           if ((cur->zeroLevel) > h) {
               if ((cur->next[cur->zeroLevel]) == NULL) {
                   printDebuggingDetails();
                   return false;
               }
               cnext = cur->next[cur->zeroLevel];
           } else {
               if ((cur->next[h]) == NULL) {
                   printDebuggingDetails();
                   return false;
               }
               cnext = (cur->next[h]);
           }
           if ((cur->key) >= (cnext->key)) {
               printDebuggingDetails();
               return false;
           }
           cur = cnext;
       }
       if (sum != val) {
           printDebuggingDetails();
           std::cout << "Bad sum on level! " << h << "\n";
           return false;
       }
   }
   std::cout << "You are great!\n";
    return true;
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::initThread(const int tid) {
    if (init[tid]) return;
    else init[tid] = !init[tid];
    recordManager->initThread(tid);
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::setCops(int cops) {
    UPDATE_NUM = cops;
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::printDebuggingDetails() {
    std::cout << "kekerok " << kekerok << "\n";
    for (int h = zeroLevel; h <= MAX_LEVEL; h++) {
        Node<K, V>* cur = head;
        std::cout << h << ": ";
        while (cur != tail) {
            std::cout << (cur->key) << " ";
            //std::cout << "(" << (cur->key) << ", " << getHits(cur, h) << ") ";
            //std::cout << "tL: " << (cur->topLevel) << " lv: " << h << ", zL: " << zeroLevel << ") ";
            Node<K, V>* cnext = NULL;
            if ((cur->zeroLevel) > h) {
                cnext = cur->next[cur->zeroLevel];
            } else
                cnext = (cur->next[h]);
            if ((cur->key) >= (cnext->key))
                exit(0);
            cur = cnext;
        }
        std::cout << "\n";
    }

    for (int h = zeroLevel; h <= MAX_LEVEL; h++) {
        Node<K, V>* cur = head;
        std::cout << h << ": ";
        while (cur != tail) {
            //std::cout << (cur->key) << " ";
            std::cout << getHits(cur, h) << " ";
            //std::cout << "tL: " << (cur->topLevel) << " lv: " << h << ", zL: " << zeroLevel << ") ";
            Node<K, V>* cnext = NULL;
            if ((cur->zeroLevel) > h) {
                cnext = cur->next[cur->zeroLevel];
            } else
                cnext = (cur->next[h]);
            if ((cur->key) >= (cnext->key))
                exit(0);
            cur = cnext;
        }
        std::cout << "\n";
    }
    return;
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::deinitThread(const int tid) {
    if (!init[tid]) return;
    else init[tid] = !init[tid];
    recordManager->deinitThread(tid);
}

template <typename K, typename V, class RecordManager>
Node<K, V>* FlexList<K, V, RecordManager>::getRoot() {
    return head->next[head->zeroLevel];
}

template <typename K, typename V, class RecordManager>
bool FlexList<K, V, RecordManager>::find(const int tid, const K& key, Node<K, V>*& pred, Node<K, V>*& succ) {
    //std::cout <<"find\n";
//    std::cout << key << " " << sumLengths[tid].value << std::endl;
    pred = head;
    succ = (head->next[MAX_LEVEL]);
    for (int level = MAX_LEVEL - 1; level >= zeroLevel; level--) {
        sumLengths[tid].value++;
        if (pred->zeroLevel > level) {
            acquireLock(&(pred->lock));
            while ((pred->zeroLevel) > level)
                updateZeroLevel(pred);
            releaseLock(&(pred->lock));
        }
        //assert((pred->zeroLevel) <= level);
        succ = (pred->next[level]);
        if (succ == NULL) {
            continue;
        }
        //assert(succ != NULL);

        if (succ->zeroLevel > level) {
            acquireLock(&(succ->lock));
            while ((succ->zeroLevel) > level)
                updateZeroLevel(succ);
            releaseLock(&(succ->lock));
        }

        while (key > succ->key) {
            sumLengths[tid].value++;
            pred = succ;
            //assert((pred->zeroLevel) <= level);
            succ = pred->next[level];
            if (succ == NULL) {
                break;
            }
            //assert(succ != NULL);
            if (succ->zeroLevel > level) {
                acquireLock(&(succ->lock));
                while ((succ->zeroLevel) > level)
                    updateZeroLevel(succ);
                releaseLock(&(succ->lock));
            }
        }
        if (succ != NULL && key == succ->key) {
            succ->contains++;
            return true;
        }
    }
    return false;
}

template <typename K, typename V, class RecordManager>
FlexList<K, V, RecordManager>::~FlexList() {
    recordManager->printStatus();
    freeAll();
    delete recordManager;
}

template <typename K, typename V, class RecordManager>
void FlexList<K, V, RecordManager>::freeAll() {
    Node<K,V>* curr = head;
    Node<K,V>* next;
    recordManager->initThread(0);
    do {
        next = curr->next[curr->zeroLevel];
        recordManager->deallocate(0, curr);
        curr = next;
    } while (curr != nullptr);
    recordManager->deinitThread(0);
}

template <typename K, typename V, class RecordManager>
long long FlexList<K, V, RecordManager>::getPathsLength(const int tid) {
    return sumLengths[tid].value;
}

template <typename K, typename V, class RecordManager>
int FlexList<K, V, RecordManager>::getHeight() {
    return MAX_LEVEL - 1 - zeroLevel;
}

template <typename K, typename V, class RecordManager>
std::vector<pair<int, int> > FlexList<K, V, RecordManager>::getPairsKeyHeight() {
    vector<pair<int, int> > key_height;
    for (int h = zeroLevel; h <= zeroLevel; h++) {
        Node<K, V>* cur = head;
        while (cur != tail) {
            Node<K, V>* cnext = NULL;
            if ((cur->zeroLevel) > h) {
                cnext = cur->next[cur->zeroLevel];
            } else
                cnext = (cur->next[h]);
            if (cur != head)
                key_height.push_back(make_pair((cur->key), (cur->topLevel) - zeroLevel + 1));
            cur = cnext;
        }
    }
    return key_height;
}

template <typename K, typename V, class RecordManager>
std::vector<pair<int, int> > FlexList<K, V, RecordManager>::getPairsKeyContains() {
    vector<pair<int, int> > key_contains;
    for (int h = zeroLevel; h <= zeroLevel; h++) {
        Node<K, V>* cur = head;
        while (cur != tail) {
            Node<K, V>* cnext = NULL;
            if ((cur->zeroLevel) > h) {
                cnext = cur->next[cur->zeroLevel];
            } else
                cnext = (cur->next[h]);
            if (cur != head)
                key_contains.push_back(make_pair((cur->key), (cur->contains)));
            cur = cnext;
        }
    }
    return key_contains;   
}
#endif //LOCK_BASED_FLEX_LIST_FLEX_LIST_H
