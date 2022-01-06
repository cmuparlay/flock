#pragma once

#include <cassert>


#define MAX_KCAS 41
#include "kcas.h"

#include <unordered_set>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <ctime>
#include <immintrin.h>

using namespace std;

#define MAX_THREADS 200
#define MAX_PATH_SIZE 64
#define PADDING_BYTES 128
#define MAX_TOWER_HEIGHT 20

#define IS_MARKED(word) (word & 0x1)


class PaddedRandom {
private:
    volatile char padding[PADDING_BYTES-sizeof(unsigned int)];
    unsigned int seed;
public:
    PaddedRandom(void) {
        this->seed = 0;
    }
    PaddedRandom(int seed) {
        this->seed = seed;
    }

    void setSeed(int seed) {
        this->seed = seed;
    }

    /** returns pseudorandom x satisfying 0 <= x < n. **/
    unsigned int nextNatural() {
        seed ^= seed << 6;
        seed ^= seed >> 21;
        seed ^= seed << 7;
        return seed;
    }
};


template<typename K, typename V>
struct Node {
    K key;
    V value;
    casword<uint64_t> vNumMark;
    int height;

    casword<Node<K,V> *> next[MAX_TOWER_HEIGHT];
};

enum RetCode: int {
    RETRY = 0,
    UNNECCESSARY = 0,
    FAILURE = -1,
    SUCCESS = 1,
    SUCCESS_WITH_HEIGHT_UPDATE = 2
};

template<class RecordManager, typename K, typename V>
class SkipListKCAS {
private:

    /*
     * ObservedNode acts as a Node-VersionNumber pair to track an "observed version number"
     * of a given node. We can then be sure that a version number does not change
     * after we have read it by comparing the current version number to this saved value
     * NOTE: This is a thread-private structure, no fields need to be volatile
     */
    struct ObservedNode {
        ObservedNode() {}
        Node<K, V> * node = NULL;
        casword_t oVNumMark = -1;
    };

    struct PathContainer {
        ObservedNode path[MAX_TOWER_HEIGHT];
        volatile char padding[PADDING_BYTES];
    };


    volatile char padding0[PADDING_BYTES];
    //Debugging, used to validate that no thread's parent can't be NULL, save for the root
    const int numThreads;
    const int minKey;
    const long long maxKey;
    volatile char padding4[PADDING_BYTES];
    Node<K, V> * head;
    volatile char padding5[PADDING_BYTES];
    RecordManager * const recmgr;
    volatile char padding7[PADDING_BYTES];
    PathContainer paths[MAX_THREADS];
    volatile char padding8[PADDING_BYTES];
    PaddedRandom rngs[MAX_THREADS];
    volatile char padding9[PADDING_BYTES];

public:

    SkipListKCAS(const int _numThreads, const int _minKey, const long long _maxKey);

    ~SkipListKCAS();

    bool contains(const int tid, const K &key);

    V insertIfAbsent(const int tid, const K &key, const V &value);

    V erase(const int tid, const K &key);

    bool validate();

    void printDebuggingDetails();

    void initThread(const int tid);

    void deinitThread(const int tid);

    Node<K, V> * getRoot();

    RecordManager * const debugGetRecMgr() {
        return recmgr;
    }

private:
    Node<K, V> * createNode(const int tid, int height, K key, V value);
    bool validatePath(const int tid, const int &size, const K &key, ObservedNode path[]);
    Node<K, V> * search(const int tid, const K &key);
    int getRandomLevel(const int tid);
};

template<class RecordManager, typename K, typename V>
int SkipListKCAS<RecordManager, K, V>::getRandomLevel(const int tid)
{
  int i, level = 1;
  for (i = 0; i < MAX_TOWER_HEIGHT - 1; i++)
    {
      if ((rngs[tid].nextNatural() % 100) < 50) level++;
      else break;
    }
  //printf("NEW NODE WITH LEVEL %d\n", level);

  return level;
}


template<class RecordManager, typename K, typename V>
Node<K, V> * SkipListKCAS<RecordManager, K, V>::createNode(const int tid, int height, K key, V value) {
    Node<K, V> * node = recmgr->template allocate<Node<K, V> >(tid);
    node->height = height;
    node->key = key;
    node->value = value;
    node->vNumMark.setInitVal(0);
    return node;
}

template<class RecordManager, typename K, typename V>
SkipListKCAS<RecordManager, K, V>::SkipListKCAS(const int _numThreads, const int _minKey, const long long _maxKey)
: numThreads(_numThreads), minKey(_minKey), maxKey(_maxKey), recmgr(new RecordManager(numThreads)) {
    head = createNode(tid, MAX_TOWER_HEIGHT, minKey - 1, 0);
    for(int i = 0; i < MAX_THREADS; i++){
        rngs[tid].setSeed(i + 1);
    }
}

template<class RecordManager, typename K, typename V>
SkipListKCAS<RecordManager, K, V>::~SkipListKCAS() {

}

template<class RecordManager, typename K, typename V>
Node<K, V> * SkipListKCAS<RecordManager, K, V>::getRoot() {
    return head->next[0];
}


template<class RecordManager, typename K, typename V>
void SkipListKCAS<RecordManager, K, V>::initThread(const int tid) {
    recmgr->initThread(tid);
}

template<class RecordManager, typename K, typename V>
void SkipListKCAS<RecordManager, K, V>::deinitThread(const int tid) {
    recmgr->deinitThread(tid);
}


template<class RecordManager, typename K, typename V>
inline bool SkipListKCAS<RecordManager, K, V>::contains(const int tid, const K &key) {
    return search(tid, key) != NULL;
}

template<class RecordManager, typename K, typename V>
Node<K, V> * SkipListKCAS<RecordManager, K, V>::search(const int tid, const K &key) {
    auto & path = paths[tid].path;

    while(true){
        bool retry = false;
        Node<K, V> * prev = NULL;
        Node<K, V> * currNode = head;
        Node<K, V> * found = NULL;

        uint64_t ver = head->vNumMark;
        for(int level = MAX_TOWER_HEIGHT - 1; level >= 0; level--){
            while(currNode != NULL && key > currNode->key ){
                prev = currNode;
                ver = prev->vNumMark;
                currNode = prev->next[level];
            }

            if(currNode != NULL && key == currNode->key){
                found = currNode;
            }

            if(IS_MARKED(ver)){
                retry = true;
                break;
            }

            path[level].node = prev;
            path[level].oVNumMark = ver;
            currNode = prev;
        }

        if(!retry) return found;
    }
}

template<class RecordManager, typename K, typename V>
inline bool SkipListKCAS<RecordManager, K, V>::validatePath(const int tid, const int &size, const K &key, ObservedNode path[]) {
    return false;
}

template<class RecordManager, typename K, typename V>
inline V SkipListKCAS<RecordManager, K, V>::insertIfAbsent(const int tid, const K &key, const V &value) {
    auto & path = paths[tid].path;

    while (true){
        Node<K, V> * node = search(tid, key);
        if(node == NULL){
            node = createNode(tid, getRandomLevel(tid), key, value);
            kcas::start();

            for(int level = node->height - 1; level >= 0; level--){
                Node<K, V> * next = path[level].node->next[level];
                node->next[level].setInitVal(next);
                kcas::add(&path[level].node->next[level], next, node,
                          &path[level].node->vNumMark, path[level].oVNumMark, path[level].oVNumMark + 2
                );
                //assert(path[level].node->vNumMark == path[level].oVNumMark);
                //assert(path[level].node->next[level] == next);
            }

            if(kcas::execute()){
                //printf("SUCCESS INSERT\n");
                return 0;
            }
            //printf("FAIL INSERT\n");

        }
        else {
            return node->value;
        }
    }
}

template<class RecordManager, typename K, typename V>
inline V SkipListKCAS<RecordManager, K, V>::erase(const int tid, const K &key) {
    auto & path = paths[tid].path;

    while (true){
        Node<K, V> * node = search(tid, key);
        if(node != NULL){
            uint64_t ver = node->vNumMark;
            if(IS_MARKED(ver)) continue;
            kcas::start();

            for(int level = node->height - 1; level >= 0; level--){
                Node<K, V> * next = node->next[level];
                kcas::add(&path[level].node->next[level], node, next,
                          &path[level].node->vNumMark, path[level].oVNumMark, path[level].oVNumMark + 2
                );
                //assert(path[level].node->vNumMark == path[level].oVNumMark);
                //assert(path[level].node->next[level] == node);
            }

            kcas::add(&node->vNumMark, ver, ver + 3);
            //assert(node->vNumMark == ver);

            if(kcas::execute()){
                //printf("SUCCESS ERASE\n");
                return node->value;
            }
            //printf("FAIL ERASE\n");
        }
        else {
            return 0;
        }
    }
}

template<class RecordManager, typename K, typename V>
void SkipListKCAS<RecordManager, K, V>::printDebuggingDetails() {

}
template<class RecordManager, typename K, typename V>
bool SkipListKCAS<RecordManager, K, V>::validate() {
    std::unordered_set<K> keys = {};

    Node<K, V> * currNode = head->next[0];
    int64_t total = 0;
    while(currNode != NULL){
        K key = currNode->key;
        assert(keys.count(key) == 0);
        keys.insert(key);

        total += currNode->key;
        currNode = currNode->next[0];

    }

//    printf("--------------------------------\n");
//    printf("CHECKSUM INTERNAL: %lld\n", total);
//    printf("--------------------------------\n");

    return true;
}