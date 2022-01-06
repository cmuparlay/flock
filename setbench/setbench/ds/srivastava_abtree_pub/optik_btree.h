#pragma once

#include <atomic>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <unordered_set>
#include <utility>
#include <tuple>

using namespace std;

#define PADDING_BYTES 128
#define MAX_PATH_SIZE 32
#ifndef MAX_THREADS_POW2
#define MAX_THREADS_POW2 512
#endif

#define RECLAIM_NODE(node) recmgr->retire(tid, (nodeptr) (node))
#define DEALLOCATE_NODE(node) recmgr->deallocate(tid, (nodeptr) (node))

#define arraycopy(src, srcStart, dest, destStart, len)         \
    for (int ___i = 0; ___i < (len); ++___i) {                 \
        (dest)[(destStart) + ___i] = (src)[(srcStart) + ___i]; \
    }

template <typename K>
struct kvpair {
    K key;
    void * val;
    kvpair() {}
};

template <typename K, class Compare>
int kv_compare(const void *_a, const void *_b) {
    const kvpair<K> *a = (const kvpair<K> *)_a;
    const kvpair<K> *b = (const kvpair<K> *)_b;
    static Compare cmp;
    return cmp(a->key, b->key) ? -1 : (cmp(b->key, a->key) ? 1 : 0);
}

template <typename K, typename V, int DEGREE>
class Node {
  public:
    bool leaf;
    bool marked;
    int size;
    K keys[DEGREE];
    Node<K,V,DEGREE>* ptrs[DEGREE]; // also doubles as a spot for VALUES

    atomic<bool> locked;
    atomic<int> writeVersion;
    // Putting these in an ElimRecord struct prevents them from being put in register
    // for some reason.
    int elimVer;
    K elimKey;
    V elimVal;

    bool weight;
    K searchKey; // key that can be used to find this node (even if its empty)

    class LockGuard {
        Node<K,V,DEGREE>* node;
        bool owned = false;
      public:
        // For the fixUnderfullNode sibling deadlock case it's cleanest to create the
        // LockGuards before we know what the nodes in them are.
        LockGuard() {}

        LockGuard(Node<K,V,DEGREE>* _node): node{_node} {}

        /* Try to acquire node->locked. If shortcircuit is true, tries to read a consistent
           shortcircuit key, value, and version (and shortcircuits if possible). Returns
           true and a garbage value if the lock was acquired, or false and the shortcircuit
           value on shortcircuit.
        */
        tuple<bool, V> acquire(const int tid, const K key = (K) 0, const bool shortcircuit = false, const int oldestWV = 0) {
            // Oldest version we are concurrent with. We can shortcircuit whenever
            // elimVer is at least this value since we would be concurrent with it.
            // const int oldestWV = node->writeVersion.load(memory_order_acquire);
            while (true) {
                while (shortcircuit) {
                    int writeVer1;
                    while ((writeVer1 = node->writeVersion.load(memory_order_acquire)) & 1) {}
                    // Try to snapshot sc during an unlocked writeVersion
                    SOFTWARE_BARRIER;
                    // ShortCircuit sc = node->sc;
                    int elimVer = node->elimVer;
                    K elimKey = node->elimKey;
                    V elimVal = node->elimVal;
                    SOFTWARE_BARRIER;
                    int writeVer2 = node->writeVersion.load(memory_order_acquire);

                    // If versions didn't match there was a concurrent update. Read versions again.
                    if (writeVer1 != writeVer2) {
                        // writeVer1 = writeVer2;
                        continue;
                    }

                    // If versions matched but we are not concurrent with the update or the key doesn't match,
                    // restart from the beginning.
                    if (oldestWV <= elimVer && elimKey == key && elimVer - oldestWV < 500) {
                        return make_tuple(false, elimVal);
                    }
                    break;
                }

                if (!node->locked.load(memory_order_relaxed)) {
                    // unlocked, try to acquire
                    bool f = false;
                    if (node->locked.compare_exchange_weak(f, true)) {
                        owned = true;
                        return make_tuple(true, (V) 0);
                    }
                }

            }
            assert(false);
        }

        // Set the shortcircuit fields
        inline void elim(const K key, const V val, const int writeVersion) {
            assert(owned);
            assert(key != 0);
            assert(val != 0);
            node->elimVer = writeVersion;
            node->elimKey = key;
            node->elimVal = val;
        }

        // NOT USED
        //
        // Acquire the lock if and only if the currect version is expectedVersion
        // bool acquireWithVersion(int expectedVersion) {
        //     assert(!(expectedVersion & 1));
        //     return node->lock.compare_exchange_strong(expectedVersion, expectedVersion + 1);
        // }

        inline void release() {
            assert(owned);
            owned = false;
            node->locked.store(false, memory_order_release);
        }

        ~LockGuard() {
            if (owned) {
                release();
            }
        }
    };
};
#define nodeptr Node<K,V,DEGREE>*
#define NodeLockGuard typename Node<K,V,DEGREE>::LockGuard


enum RetCode : int {
    RETRY = 0,
    UNNECCESSARY = 0,
    INCONSISTENT = 0,
    FAILURE = -1,
    SUCCESS = 1,
};

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
class ABTreeOptik {
  public:
    volatile char padding0[PADDING_BYTES];
    V const NO_VALUE;

  private:
    struct SearchInfo {
        nodeptr oNode;
        nodeptr oParent;
        nodeptr oGParent;
        int parentIndex = 0; // parent index in grandparent
        int nodeIndex = 0;   // node index in parent
        int keyIndex = 0;    // key index in node
        V val;
        int nodeVersion;
    };

    volatile char padding1[PADDING_BYTES];
    const int numThreads;
    const int a;
    const int b;
    K maxKey;
    volatile char padding2[PADDING_BYTES];
    nodeptr entry;
    volatile char padding3[PADDING_BYTES];
    RecordManager * const recmgr;
    volatile char padding4[PADDING_BYTES];
    Compare compare;
    volatile char padding5[PADDING_BYTES];

  public:
    ABTreeOptik(const int _numThreads, const K anyKey, const K _maxKey);
    ~ABTreeOptik();
    bool contains(const int tid, const K &key);
    V tryInsert(const int tid, const K &key, const V &value);
    V tryErase(const int tid, const K &key);
    V find(const int tid, const K &key);
    void printDebuggingDetails();
    nodeptr getRoot();
    void initThread(const int tid);
    void deinitThread(const int tid);
    bool validate();

  private:
    int getKeyCount(nodeptr node);
    int getChildIndex(nodeptr node, const K &key);
    tuple<RetCode, int, V, int> tryGetKeyIndexValueVersion(nodeptr node, const K &key);
    tuple<RetCode, int, V, int> getKeyIndexValueVersion(nodeptr node, const K &key);
    nodeptr createInternalNode(const int tid, bool weight, int size, K searchKey);
    nodeptr createExternalNode(const int tid, bool weight, int size, K searchKey);
    void freeSubtree(const int tid, nodeptr node);
    long validateSubtree(nodeptr node, std::unordered_set<K> &keys, ofstream &graph, ofstream &log, bool &errorFound);
    int erase(const int tid, SearchInfo &info, const K &key);
    int insert(const int tid, SearchInfo &info, const K &key, const V &value);
    V searchBasic(const int tid, const K &key);
    int trySearch(const int tid, SearchInfo &info, const K &key, nodeptr target = NULL);
    int fixUnderfullViolation(const int tid, nodeptr viol);
    int fixTagViolation(const int tid, nodeptr viol);
};

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::getKeyCount(nodeptr node) {
    return node->leaf ? node->size : node->size - 1;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::getChildIndex(nodeptr node, const K &key) {
    int nkeys = getKeyCount(node);
    int retval = 0;
    while (retval < nkeys && !compare(key, (const K &)node->keys[retval])) {
        ++retval;
    }
    return retval;
}

// Try to read a consistent version of a key/value pair in the node.
template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline tuple<RetCode, int, V, int> ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::tryGetKeyIndexValueVersion(nodeptr node, const K &key) {
    int writeVersion = node->writeVersion.load(memory_order_acquire);
    if (writeVersion & 1) {
        return make_tuple(RetCode::INCONSISTENT, 0, NO_VALUE, writeVersion);
    }

    int keyIndex = 0;
    while (keyIndex < DEGREE && node->keys[keyIndex] != key) {
        ++keyIndex;
    }
    V val = keyIndex < DEGREE ? (V) node->ptrs[keyIndex] : NO_VALUE;
    if (node->writeVersion.load(memory_order_acquire) != writeVersion) {
        return make_tuple(RetCode::INCONSISTENT, 0, NO_VALUE, writeVersion);
    }

    // Returns the key's index, the associated value, and the version at which the
    // read occured
    return make_tuple(val == NO_VALUE ? RetCode::FAILURE : RetCode::SUCCESS, keyIndex, val, writeVersion);
}

// Search a node for a key repeatedly until we successfully we read a consistent version
template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline tuple<RetCode, int, V, int> ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::getKeyIndexValueVersion(nodeptr node, const K &key) {
    int keyIndex;
    V val;
    int writeVersion;
    do {
        while ((writeVersion = node->writeVersion.load(memory_order_acquire)) & 1) {}
        keyIndex = 0;
        while (keyIndex < DEGREE && node->keys[keyIndex] != key) {
            ++keyIndex;
        }
        val = keyIndex < DEGREE ? (V) node->ptrs[keyIndex] : NO_VALUE;
    } while (node->writeVersion.load(memory_order_acquire) != writeVersion);
    // Returns the key's index, the associated value, and the version at which the
    // read occured
    return make_tuple(val == NO_VALUE ? RetCode::FAILURE : RetCode::SUCCESS, keyIndex, val, writeVersion);
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
nodeptr ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::createInternalNode(const int tid, bool weight, int size, K searchKey) {
    nodeptr node = recmgr->template allocate<Node<K,V,DEGREE>>(tid);
    node->leaf = false;
    node->weight = weight;
    node->marked = false;
    node->locked = false;
    node->writeVersion = 0;
    node->elimVer = 0;
    node->elimVal = 0;
    node->elimKey = 0;
    node->size = size;
    node->searchKey = searchKey;
    for (int i = 0; i < DEGREE; ++i) {
        node->keys[i] = (K) 0;
    }
    return node;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
nodeptr ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::createExternalNode(const int tid, bool weight, int size, K searchKey) {
    nodeptr node = createInternalNode(tid, weight, size, searchKey);
    node->leaf = true;
    return node;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::ABTreeOptik(const int _numThreads, const K anyKey, const K _maxKey) : NO_VALUE((V)0), numThreads(_numThreads),
                                                                                                                       a(max(DEGREE / 4, 2)), b(DEGREE), maxKey(_maxKey),
                                                                                                                       recmgr(new RecordManager(numThreads)) {
    assert(sizeof(V) == sizeof(nodeptr));
    assert(SUCCESS == RetCode::SUCCESS);
    assert(RETRY == RetCode::RETRY);

    compare = Compare();

    const int tid = 0;
    initThread(tid);

    // initial tree: entry is a sentinel node (with one pointer and no keys)
    //               that points to an empty node (no pointers and no keys)
    nodeptr _entryLeft = createExternalNode(tid, true, 0, anyKey);

    //sentinel node
    nodeptr _entry = createInternalNode(tid, true, 1, anyKey);
    _entry->ptrs[0] = _entryLeft;

    entry = _entry;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::~ABTreeOptik() {
    int tid = 0;
    initThread(tid);
    freeSubtree(0, entry);
    deinitThread(tid);
    delete recmgr;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline nodeptr ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::getRoot() {
    return entry;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
void ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::initThread(const int tid) {
    recmgr->initThread(tid);
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
void ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::deinitThread(const int tid) {
    recmgr->deinitThread(tid);
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
inline bool ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::contains(const int tid, const K &key) {
    auto guard = recmgr->getGuard(tid, true);
    bool retval = (searchBasic(tid, key) != NO_VALUE);
    return retval;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
V ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::find(const int tid, const K &key) {
    auto guard = recmgr->getGuard(tid, true);
    V retval = searchBasic(tid, key);
    return retval;
}

/* searchBasic(const int tid, const K &key)
 * Basic search, returns respective value associated with key, or NO_VALUE if nothing is found
 * does not return any path information like other searches (and is therefore slightly faster)
 * called by contains() and find()
 */
template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
V ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::searchBasic(const int tid, const K &key) {
    nodeptr node = entry->ptrs[0];
    while (!node->leaf) node = node->ptrs[getChildIndex(node, key)];
    V retval;
    tie(std::ignore, std::ignore, retval, std::ignore) = getKeyIndexValueVersion(node, key);
    return retval;
}

/* trySearch(const int tid, SearchInfo &info, const K &key)
 * Search for the node containing key. Tries to read a consistent key/value pair in the node, but does not retry
 * if the node is locked or becomes locked during the read. Always returns the node, parent, and grandparent through
 * the SearchInfo struct.
 */
template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::trySearch(const int tid, SearchInfo &info, const K &key, nodeptr target) {
    info.oGParent = NULL;
    info.oParent = entry;
    info.nodeIndex = 0;
    info.oNode = entry->ptrs[0];
    while (!info.oNode->leaf && (target ? info.oNode != target : true)) {
        info.oGParent = info.oParent;
        info.oParent = info.oNode;
        info.parentIndex = info.nodeIndex;
        info.nodeIndex = getChildIndex(info.oNode, key);
        info.oNode = info.oNode->ptrs[info.nodeIndex];
    }
    if (target) {
        if (info.oNode == target) return RetCode::SUCCESS;
        return RetCode::FAILURE;
    } else {
        RetCode retcode;
        // Faster than search, for publishing elimination
        tie(retcode, info.keyIndex, info.val, info.nodeVersion) = tryGetKeyIndexValueVersion(info.oNode, key);
        return retcode;
    }
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
V ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::tryInsert(const int tid, const K &key, const V &value) {
    SearchInfo info;
    while (true) {
        auto guard = recmgr->getGuard(tid);
        const int res = trySearch(tid, info, key);
        if (res == RetCode::SUCCESS) {
            // GSTATS_ADD(tid, num_searched_consistent, 1);
            return info.val;
        }
        const int insertRet = insert(tid, info, key, value);
        if (insertRet == RetCode::SUCCESS) {
            return NO_VALUE;
        }
        else if (insertRet == RetCode::FAILURE) {
            return info.val;
        }
        // else retry
    }
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::insert(const int tid, SearchInfo &info, const K &key, const V &value) {
    auto node = info.oNode;
    auto parent = info.oParent;

    assert(node->leaf);
    assert(!parent->leaf);

    NodeLockGuard leafLock(node);

    bool acquired;
    tie(acquired, info.val) = leafLock.acquire(tid, key, true, info.nodeVersion);
    if (!acquired) {
        // Shortcircuited
        // GSTATS_ADD(tid, num_shortcircuited, 1);
        assert(info.val != NO_VALUE);
        return RetCode::FAILURE;
    }

    if (node->marked) {
        return RetCode::RETRY;
    }

    // Maybe do only if version changed (if branch prediction isn't bad)
    for (int i = 0; i < DEGREE; ++i) {
        if (node->keys[i] == key) {
            info.val = (V) node->ptrs[i];
            // GSTATS_ADD(tid, num_did_operation, 1);
            return RetCode::FAILURE;
        }
    }
    // At this point, we are guaranteed key is not in node

    int currSize = node->size;
    if (currSize < b) {
        // we have the capacity to fit this new key
        // find empty slot
        for (int i = 0; i < DEGREE; ++i) {
            if (node->keys[i] == (K) 0) {
                const int oldVersion = node->writeVersion.load(memory_order_relaxed);
                node->writeVersion.store(oldVersion + 1, memory_order_relaxed);
                SOFTWARE_BARRIER;
                leafLock.elim(key, value, oldVersion + 1);
                node->keys[i] = key;
                node->ptrs[i] = (nodeptr) value;
                ++node->size;
                node->writeVersion.store(oldVersion + 2, memory_order_relaxed);
                // GSTATS_ADD(tid, num_did_operation, 1);
                return RetCode::SUCCESS;
            }
        }
        assert(false); // SHOULD NEVER HAPPEN
    } else {
        NodeLockGuard parentLock(parent);
        parentLock.acquire(tid);

        if (parent->marked) {
            return RetCode::RETRY;
        }

        // OVERFLOW
        // We do not have room for this key, we need to make new nodes so it fits
        // first, we create a std::pair of large arrays
        // containing too many keys and pointers to fit in a single node

        kvpair<K> tosort[DEGREE + 1];
        int k = 0;
        for (int i = 0; i < DEGREE; i++) {
            if (node->keys[i]) {
                tosort[k].key = node->keys[i];
                tosort[k].val = node->ptrs[i];
                ++k;
            }
        }
        tosort[k].key = key;
        tosort[k].val = (void*) value;
        ++k;
        qsort(tosort, k, sizeof(kvpair<K>), kv_compare<K, Compare>);

        // create new node(s):
        // since the new arrays are too big to fit in a single node,
        // we replace l by a new subtree containing three new nodes:
        // a parent, and two leaves;
        // the array contents are then split between the two new leaves

        const int leftSize = k / 2;
        auto left = createExternalNode(tid, true, leftSize, tosort[0].key);
        for (int i = 0; i < leftSize; i++) {
            left->keys[i] = tosort[i].key;
            left->ptrs[i] = (nodeptr)tosort[i].val;
        }

        const int rightSize = (DEGREE + 1) - leftSize;
        auto right = createExternalNode(tid, true, rightSize, tosort[leftSize].key);
        for (int i = 0; i < rightSize; i++) {
            right->keys[i] = tosort[i + leftSize].key;
            right->ptrs[i] = (nodeptr)tosort[i + leftSize].val;
        }

        // note: weight of new internal node n will be zero,
        //       unless it is the root; this is because we test
        //       p == entry, above; in doing this, we are actually
        //       performing Root-Zero at the same time as this Overflow
        //       if n will become the root
        auto replacementNode = createInternalNode(tid, parent == entry, 2, tosort[leftSize].key);
        replacementNode->keys[0] = tosort[leftSize].key;
        replacementNode->ptrs[0] = left;
        replacementNode->ptrs[1] = right;

        // If the parent is not marked, parent->ptrs[info.nodeIndex] is guaranteed to contain
        // node since any update to parent would have deleted node (and hence we would have
        // returned at the node->marked check)
        parent->ptrs[info.nodeIndex] = replacementNode;
        node->marked = true;
        leafLock.release();

        // Manully unlock so we can fix the tag
        parentLock.release();
        RECLAIM_NODE(node);
        fixTagViolation(tid, replacementNode);
        // GSTATS_ADD(tid, num_did_operation, 1);
        return RetCode::SUCCESS;
    }
    assert(false);
    return RetCode::RETRY;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
V ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::tryErase(const int tid, const K &key) {
    SearchInfo info;
    while (true) {
        auto guard = recmgr->getGuard(tid);
        const int res = trySearch(tid, info, key);
        if (res == RetCode::FAILURE) {
            return NO_VALUE;
        }

        const int eraseRet = erase(tid, info, key);
        if (eraseRet == RetCode::SUCCESS) {
            return info.val;
        }
        else if (eraseRet == RetCode::FAILURE) {
            return NO_VALUE;
        }
        // else retry
    }
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::erase(const int tid, SearchInfo &info, const K &key) {
    auto node = info.oNode;
    auto parent = info.oParent;
    auto gParent = info.oGParent;
    assert(node->leaf);
    assert(!parent->leaf);
    assert(gParent == NULL || !gParent->leaf);

    NodeLockGuard leafLock(node);
    bool acquired;
    tie(acquired, std::ignore) = leafLock.acquire(tid, key, true, info.nodeVersion);
    if (!acquired) {
        // Shortcircuited
        return RetCode::FAILURE;
    }

    if (node->marked) {
        return RetCode::RETRY;
    }

    // See branch prediction note in insert
    // Could use keyIndex here in best case
    int newSize = node->size - 1;
    for (int i = 0; i < DEGREE; ++i) {
        if (node->keys[i] == key) {
            info.val = (V) node->ptrs[i];
            const int oldVersion = node->writeVersion.load(memory_order_relaxed);
            node->writeVersion.store(oldVersion + 1, memory_order_relaxed);
            SOFTWARE_BARRIER;
            leafLock.elim(key, info.val, oldVersion + 1);
            node->keys[i] = (K) 0;
            node->size = newSize;
            node->writeVersion.store(oldVersion + 2, memory_order_relaxed);
            if (newSize == a - 1) {
                leafLock.release();
                fixUnderfullViolation(tid, node);
            }
            return RetCode::SUCCESS;
        }
    }

    return RetCode::FAILURE;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::fixTagViolation(const int tid, nodeptr viol) {
    while (true) {
        if (viol->weight) {
            return RetCode::UNNECCESSARY;
        }

        // assert: viol is internal (because leaves always have weight = 1)
        assert(!viol->leaf);
        // assert: viol is not entry or root (because both should always have weight = 1)
        assert(viol != entry && viol != entry->ptrs[0]);

        SearchInfo info;
        int res = trySearch(tid, info, viol->searchKey, viol);

        if (res != RetCode::SUCCESS) {
            return RetCode::UNNECCESSARY;
        }

        auto node = info.oNode;
        auto parent = info.oParent;
        auto gParent = info.oGParent;
        assert(!node->leaf);
        assert(!parent->leaf);
        assert(gParent);
        assert(!gParent->leaf);

        if (node != viol) {
            // viol was replaced by another update.
            // we hand over responsibility for viol to that update.
            return RetCode::UNNECCESSARY;
        }

        // we cannot apply this update if p has a weight violation
        // so, we check if this is the case, and, if so, try to fix it
        if (!parent->weight) {
            fixTagViolation(tid, parent);
            continue;
        }

        NodeLockGuard nodeLock(node);
        nodeLock.acquire(tid);
        if (node->marked) {
            continue;
        }

        NodeLockGuard parentLock(parent);
        parentLock.acquire(tid);
        if (parent->marked) {
            continue;
        }

        NodeLockGuard gParentLock(gParent);
        gParentLock.acquire(tid);
        if (gParent->marked) {
            continue;
        }

        const int psize = parent->size;
        const int nsize = viol->size;
        // We don't ever change the size of a tag node, so its size should always be 2
        assert(nsize == 2);
        const int c = psize + nsize;
        const int size = c - 1;

        if (size <= b) {
            /**
             * Absorb
             */

            // create new node(s)
            // the new arrays are small enough to fit in a single node,
            // so we replace p by a new internal node.
            nodeptr absorber = createInternalNode(tid, true, size, (K)0x0);
            arraycopy(parent->ptrs, 0, absorber->ptrs, 0, info.nodeIndex);
            arraycopy(node->ptrs, 0, absorber->ptrs, info.nodeIndex, nsize);
            arraycopy(parent->ptrs, info.nodeIndex + 1, absorber->ptrs, info.nodeIndex + nsize, psize - (info.nodeIndex + 1));

            arraycopy(parent->keys, 0, absorber->keys, 0, info.nodeIndex);
            arraycopy(node->keys, 0, absorber->keys, info.nodeIndex, getKeyCount(node));
            arraycopy(parent->keys, info.nodeIndex, absorber->keys, info.nodeIndex + getKeyCount(node), getKeyCount(parent) - info.nodeIndex);
            //hate this
            absorber->searchKey = absorber->keys[0]; // TODO: verify this is same as in llx/scx abtree

            gParent->ptrs[info.parentIndex] = absorber;
            node->marked = true;
            parent->marked = true;
            RECLAIM_NODE(node);
            RECLAIM_NODE(parent);

            return RetCode::SUCCESS;
            // DEALLOCATE_NODE(absorber);
        } else {
            /**
             * Split
             */

            // merge keys of p and l into one big array (and similarly for children)
            // (we essentially replace the pointer to l with the contents of l)
            K keys[2 * DEGREE];
            nodeptr ptrs[2 * DEGREE];
            arraycopy(parent->ptrs, 0, ptrs, 0, info.nodeIndex);
            arraycopy(node->ptrs, 0, ptrs, info.nodeIndex, nsize);
            arraycopy(parent->ptrs, info.nodeIndex + 1, ptrs, info.nodeIndex + nsize, psize - (info.nodeIndex + 1));
            arraycopy(parent->keys, 0, keys, 0, info.nodeIndex);
            arraycopy(node->keys, 0, keys, info.nodeIndex, getKeyCount(node));
            arraycopy(parent->keys, info.nodeIndex, keys, info.nodeIndex + getKeyCount(node), getKeyCount(parent) - info.nodeIndex);

            // the new arrays are too big to fit in a single node,
            // so we replace p by a new internal node and two new children.
            //
            // we take the big merged array and split it into two arrays,
            // which are used to create two new children u and v.
            // we then create a new internal node (whose weight will be zero
            // if it is not the root), with u and v as its children.

            // create new node(s)
            const int leftSize = size / 2;
            auto left = createInternalNode(tid, true, leftSize, keys[0]);
            arraycopy(keys, 0, left->keys, 0, leftSize - 1);
            arraycopy(ptrs, 0, left->ptrs, 0, leftSize);

            const int rightSize = size - leftSize;
            auto right = createInternalNode(tid, true, rightSize, keys[leftSize]);
            arraycopy(keys, leftSize, right->keys, 0, rightSize - 1);
            arraycopy(ptrs, leftSize, right->ptrs, 0, rightSize);

            // note: keys[leftSize - 1] should be the same as n->keys[0]
            auto newNode = createInternalNode(tid, gParent == entry, 2, keys[leftSize - 1]);
            newNode->keys[0] = keys[leftSize - 1];
            newNode->ptrs[0] = left;
            newNode->ptrs[1] = right;

            // note: weight of new internal node n will be zero,
            //       unless it is the root; this is because we test
            //       gp == entry, above; in doing this, we are actually
            //       performing Root-Zero at the same time as this Overflow
            //       if n will become the root

            gParent->ptrs[info.parentIndex] = newNode;
            node->marked = true;
            parent->marked = true;
            RECLAIM_NODE(node);
            RECLAIM_NODE(parent);

            nodeLock.release();
            parentLock.release();
            gParentLock.release();
            fixTagViolation(tid, newNode);
            // fixUnderfullViolation(tid, newNode);

            return RetCode::SUCCESS;
            // DEALLOCATE_NODE(left);
            // DEALLOCATE_NODE(right);
            // DEALLOCATE_NODE(newNode);
        }
    }
    assert(false);
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
int ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::fixUnderfullViolation(const int tid, nodeptr viol) {
    // we search for viol and try to fix any violation we find there
    // this entails performing AbsorbSibling or Distribute.

    nodeptr parent;
    nodeptr gParent;
    nodeptr node = NULL;
    nodeptr sibling = NULL;

    while (true) {
        // We do not need a lock for the viol == entry->ptrs[0] check since since we cannot
        // "be turned into" the root. The root is only created by the root absorb
        // operation below, so a node that is not the root will never become the root.
        if (viol->size >= a || viol == entry || viol == entry->ptrs[0]) {
            return RetCode::UNNECCESSARY; // no degree violation at viol
        }

        /**
         * search for viol
         */
        SearchInfo info;
        trySearch(tid, info, viol->searchKey, viol);
        node = info.oNode;
        parent = info.oParent;
        gParent = info.oGParent;

        // Technically this only matters if the parent has fewer than 2 pointers.
        // Maybe should change the check to that?
        if (parent->size < a && parent != entry && parent != entry->ptrs[0]) {
            fixUnderfullViolation(tid, parent);
            continue;
        }

        if (node != viol) {
            // viol was replaced by another update.
            // we hand over responsibility for viol to that update.
            return RetCode::UNNECCESSARY;
        }

        // assert: gp != NULL (because if AbsorbSibling or Distribute can be applied, then p is not the root)
        int siblingIndex = (info.nodeIndex > 0 ? info.nodeIndex - 1 : 1);
        // Don't need a lock on parent here because if the pointer to sibling changes to a different node
        // after this, sibling will be marked (Invariant: when a pointer switches away from a node, the node
        // is marked)
        sibling = parent->ptrs[siblingIndex];

        // Prevent deadlocks by acquiring left node first
        NodeLockGuard nodeLock;
        NodeLockGuard siblingLock;
        int leftIndex;
        int rightIndex;
        nodeptr left = NULL;
        nodeptr right = NULL;
        if (siblingIndex < info.nodeIndex) {
            left = sibling;
            right = node;
            leftIndex = siblingIndex;
            rightIndex = info.nodeIndex;
            siblingLock = NodeLockGuard(sibling);
            siblingLock.acquire(tid);
            nodeLock = NodeLockGuard(node);
            nodeLock.acquire(tid);
        }
        else {
            left = node;
            right = sibling;
            leftIndex = info.nodeIndex;
            rightIndex = siblingIndex;
            nodeLock = NodeLockGuard(node);
            nodeLock.acquire(tid);
            siblingLock = NodeLockGuard(sibling);
            siblingLock.acquire(tid);
        }
 
        // Repeat this check, this might have changed while we locked viol
        if (viol->size >= a) {
            return RetCode::UNNECCESSARY; // no degree violation at viol
        }

        if (node->marked || sibling->marked) {
            continue;
        }

        NodeLockGuard parentLock(parent);
        parentLock.acquire(tid);
        if (parent->marked) {
            continue;
        }

        NodeLockGuard gParentLock(gParent);
        gParentLock.acquire(tid);
        if (gParent->marked) {
            continue;
        }

        // we can only apply AbsorbSibling or Distribute if there are no
        // weight violations at parent, node, or sibling.
        // So, we first check for any weight violations and fix any that we see.
        if (!parent->weight || !node->weight || !sibling->weight) {
            nodeLock.release();
            siblingLock.release();
            parentLock.release();
            gParentLock.release();
            fixTagViolation(tid, parent);
            fixTagViolation(tid, node);
            fixTagViolation(tid, sibling);
            continue;
        }

        // assert: there are no weight violations at parent, node or sibling
        assert(parent->weight && node->weight && sibling->weight);
        // assert: l and s are either both leaves or both internal nodes
        //         (because there are no weight violations at these nodes)
        assert((node->leaf && sibling->leaf) || (!node->leaf && !sibling->leaf));

        // also note that p->size >= a >= 2

        int lsize = left->size;
        int rsize = right->size;
        int psize = parent->size;
        int size = lsize+rsize;
        // assert(left->weight && right->weight); // or version # has changed

        if (size < 2 * a) {
            /**
             * AbsorbSibling
             */

            nodeptr newNode = NULL;
            // create new node(s))
            int keyCounter = 0, ptrCounter = 0;
            if (left->leaf) {
                //duplicate code can be cleaned up, but it would make it far less readable...
                nodeptr newNodeExt = createExternalNode(tid, true, size, node->searchKey);
                for (int i = 0; i < DEGREE; i++) {
                    if (left->keys[i]) {
                        newNodeExt->keys[keyCounter++] = left->keys[i];
                        newNodeExt->ptrs[ptrCounter++] = left->ptrs[i];
                    }
                }
                assert(right->leaf);
                for (int i = 0; i < DEGREE; i++) {
                    if (right->keys[i]) {
                        newNodeExt->keys[keyCounter++] = right->keys[i];
                        newNodeExt->ptrs[ptrCounter++] = right->ptrs[i];
                    }
                }
                newNode = newNodeExt;
            } else {
                nodeptr newNodeInt = createInternalNode(tid, true, size, node->searchKey);
                for (int i = 0; i < getKeyCount(left); i++) {
                    newNodeInt->keys[keyCounter++] = left->keys[i];
                }
                newNodeInt->keys[keyCounter++] = parent->keys[leftIndex];
                for (int i = 0; i < lsize; i++) {
                    newNodeInt->ptrs[ptrCounter++] = left->ptrs[i];
                }
                assert(!right->leaf);
                for (int i = 0; i < getKeyCount(right); i++) {
                    newNodeInt->keys[keyCounter++] = right->keys[i];
                }
                for (int i = 0; i < rsize; i++) {
                    newNodeInt->ptrs[ptrCounter++] = right->ptrs[i];
                }
                newNode = newNodeInt;
            }

            // now, we atomically replace p and its children with the new nodes.
            // if appropriate, we perform RootAbsorb at the same time.
            if (gParent == entry && psize == 2) {
                assert(info.parentIndex == 0);
                gParent->ptrs[info.parentIndex] = newNode;
                node->marked = true;
                parent->marked = true;
                sibling->marked = true;
                RECLAIM_NODE(node);
                RECLAIM_NODE(parent);
                RECLAIM_NODE(sibling);

                nodeLock.release();
                siblingLock.release();
                parentLock.release();
                gParentLock.release();
                fixUnderfullViolation(tid, newNode);
                return RetCode::SUCCESS;
            } else {
                assert(gParent != entry || psize > 2);
                // create n from p by:
                // 1. skipping the key for leftindex and child pointer for ixToS
                // 2. replacing l with newl
                auto newParent = createInternalNode(tid, true, psize - 1, parent->searchKey);
                for (int i = 0; i < leftIndex; i++) {
                    newParent->keys[i] = parent->keys[i];
                }
                for (int i = 0; i < siblingIndex; i++) {
                    newParent->ptrs[i] = parent->ptrs[i];
                }
                for (int i = leftIndex + 1; i < getKeyCount(parent); i++) {
                    newParent->keys[i - 1] = parent->keys[i];
                }
                for (int i = info.nodeIndex + 1; i < psize; i++) {
                    newParent->ptrs[i - 1] = parent->ptrs[i];
                }

                // replace l with newl in n's pointers
                newParent->ptrs[info.nodeIndex - (info.nodeIndex > siblingIndex)] = newNode;

                gParent->ptrs[info.parentIndex] = newParent;
                node->marked = true;
                parent->marked = true;
                sibling->marked = true;
                RECLAIM_NODE(node);
                RECLAIM_NODE(parent);
                RECLAIM_NODE(sibling);

                nodeLock.release();
                siblingLock.release();
                parentLock.release();
                gParentLock.release();
                // Rewrite so we don't need this check?
                fixUnderfullViolation(tid, newNode);
                fixUnderfullViolation(tid, newParent);
                return RetCode::SUCCESS;
            }

        } else {
            /**
             * Distribute
             */

            int leftSize = size / 2;
            int rightSize = size - leftSize;

            nodeptr newLeft = NULL;
            nodeptr newRight = NULL;

            kvpair<K> tosort[2 * DEGREE];

            // combine the contents of l and s (and one key from p if l and s are internal)

            int keyCounter = 0;
            int valCounter = 0;
            if (left->leaf) {
                assert(right->leaf);
                for (int i = 0; i < DEGREE; i++) {
                    if (left->keys[i]) {
                        tosort[keyCounter++].key = left->keys[i];
                        tosort[valCounter++].val = left->ptrs[i];
                    }
                }
            } else {
                for (int i = 0; i < getKeyCount(left); i++) {
                    tosort[keyCounter++].key = left->keys[i];
                }
                for (int i = 0; i < lsize; i++) {
                    tosort[valCounter++].val = left->ptrs[i];
                }
            }

            if (!left->leaf) tosort[keyCounter++].key = parent->keys[leftIndex];

            if (right->leaf) {
                assert(left->leaf);
                for (int i = 0; i < DEGREE; i++) {
                    if (right->keys[i]) {
                        tosort[keyCounter++].key = right->keys[i];
                        tosort[valCounter++].val = right->ptrs[i];
                    }
                }
            } else {
                for (int i = 0; i < getKeyCount(right); i++) {
                    tosort[keyCounter++].key = right->keys[i];
                }
                for (int i = 0; i < rsize; i++) {
                    tosort[valCounter++].val = right->ptrs[i];
                }
            }

            if (left->leaf) qsort(tosort, keyCounter, sizeof(kvpair<K>), kv_compare<K, Compare>);

            keyCounter = 0;
            valCounter = 0;
            K pivot;

            if (left->leaf) {
                nodeptr newLeftExt = createExternalNode(tid, true, leftSize, (K)0x0);
                for (int i = 0; i < leftSize; i++) {
                    newLeftExt->keys[i] = tosort[keyCounter++].key;
                    newLeftExt->ptrs[i] = (nodeptr)tosort[valCounter++].val;
                }
                newLeft = newLeftExt;
                newLeft->searchKey = newLeftExt->keys[0];
                pivot = tosort[keyCounter].key;

            } else {
                nodeptr newLeftInt = createInternalNode(tid, true, leftSize, (K)0x0);
                for (int i = 0; i < leftSize - 1; i++) {
                    newLeftInt->keys[i] = tosort[keyCounter++].key;
                }
                for (int i = 0; i < leftSize; i++) {
                    newLeftInt->ptrs[i] = (nodeptr)tosort[valCounter++].val;
                }
                newLeft = newLeftInt;
                newLeft->searchKey = newLeftInt->keys[0];
                pivot = tosort[keyCounter++].key;
            }

            // reserve one key for the parent (to go between newleft and newright))

            if (right->leaf) {
                assert(left->leaf);
                auto newRightExt = createExternalNode(tid, true, rightSize, (K)0x0);
                for (int i = 0; i < rightSize - !left->leaf; i++) {
                    newRightExt->keys[i] = tosort[keyCounter++].key;
                }
                newRight = newRightExt;
                newRight->searchKey = newRightExt->keys[0]; // TODO: verify searchKey setting is same as llx/scx based version
                for (int i = 0; i < rightSize; i++) {
                    newRight->ptrs[i] = (nodeptr)tosort[valCounter++].val;
                }
            } else {
                nodeptr newRightInt = createInternalNode(tid, true, rightSize, (K)0x0);
                for (int i = 0; i < rightSize - !left->leaf; i++) {
                    newRightInt->keys[i] = tosort[keyCounter++].key;
                }
                newRight = newRightInt;
                newRight->searchKey = newRightInt->keys[0];
                for (int i = 0; i < rightSize; i++) {
                    newRight->ptrs[i] = (nodeptr)tosort[valCounter++].val;
                }
            }

            // in this case we replace the parent, despite not having to in the llx/scx version...
            // this is a holdover from kcas. experiments show this case almost never occurs, though, so perf impact is negligible.
            auto newParent = createInternalNode(tid, parent->weight, psize, parent->searchKey);
            arraycopy(parent->keys, 0, newParent->keys, 0, getKeyCount(parent));
            arraycopy(parent->ptrs, 0, newParent->ptrs, 0, psize);
            newParent->ptrs[leftIndex] = newLeft;
            newParent->ptrs[rightIndex] = newRight;
            newParent->keys[leftIndex] = pivot;

            gParent->ptrs[info.parentIndex] = newParent;
            node->marked = true;
            parent->marked = true;
            sibling->marked = true;
            RECLAIM_NODE(node);
            RECLAIM_NODE(parent);
            RECLAIM_NODE(sibling);

            return RetCode::SUCCESS;
// cleanup_retry2:
//             DEALLOCATE_NODE(newLeft);
//             DEALLOCATE_NODE(newRight);
//             DEALLOCATE_NODE(newParent);
        }
    }
    assert(false);
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
void ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::printDebuggingDetails() {
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
long ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::validateSubtree(nodeptr node, std::unordered_set<K> &keys, ofstream &graph, ofstream &log, bool &errorFound) {
    if (node == NULL) return 0;
    // graph << "\"" << node << "\"" << "[label=\"K: " << node->searchKey << " - W: "
    //         << node->weight << " - L: " << node->leaf << " - N: " << node << "\"];\n";
    graph << "\"" << node << "\"" << "[shape=record, label=\"S" << node->searchKey << " | W"
            << node->weight << " | L" << node->leaf; //<< "\"];\n";
    if (node->leaf) {
        for (int i = 0; i < DEGREE; i++) {
            K key = node->keys[i];
            graph << " | <k"<<i<<">";
            if (key) graph << key; else graph << "x";
        }
    } else {
        for (int i = 0; i < node->size-1; i++) {
            K key = node->keys[i];
            graph << " | <p"<<i<<">";
            graph << " | <k"<<i<<">";
            if (key) graph << key; else graph << "x";
        }
        graph << " | <p"<<(node->size-1)<<">";
    }
    graph << " \"];\n";

    if (!node->weight) {
        log << "Weight Violation! " << node->searchKey << "\n";
        errorFound = true;
    }

    if (node->leaf) {
        for (int i = 0; i < DEGREE; i++) {
            auto leaf = node;
            K key = leaf->keys[i];
            if (key) {
                // graph << "\"" << node << "\" -> \"" << key << "\";\n";
                if (key < 0 || key > maxKey) {
                    log << "Suspected pointer in leaf! " << node->searchKey << "\n";
                    errorFound = true;
                }
                if (keys.count(key) > 0) {
                    log << "DUPLICATE KEY! " << node->searchKey << "\n";
                    errorFound = true;
                }
                keys.insert(key);
            }
        }
    }

    if (!node->leaf) {
        for (int i = 0; i < node->size; i++) {
            graph << "\"" << node << "\":<p"<<i<<"> -> \"" << node->ptrs[i] << "\";\n";
            validateSubtree(node->ptrs[i], keys, graph, log, errorFound);
        }
    }

    return 1;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
bool ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::validate() {
    fflush(stdout);
    std::unordered_set<K> keys = {};
    bool errorFound = false;

    rename("graph.dot", "graph_before.dot");
    ofstream graph;
    graph.open("graph.dot");
    graph << "digraph G {\n";

    ofstream log;
    log.open("log.txt", std::ofstream::out);

    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    log << "Run at: " << std::put_time(&tm, "%d-%m-%Y %H-%M-%S") << "\n";

    long ret = validateSubtree(getRoot(), keys, graph, log, errorFound);
    graph << "}";
    graph.flush();

    graph.close();

    if (!errorFound) {
        log << "Validated Successfully!\n";
    }
    log.flush();

    log.close();
    fflush(stdout);
    return !errorFound;
    return true;
}

template <class RecordManager, typename K, typename V, int DEGREE, class Compare>
void ABTreeOptik<RecordManager, K, V, DEGREE, Compare>::freeSubtree(const int tid, nodeptr node) {
    if (!node->leaf) {
        for (int i = 0; i < node->size; i++) {
            freeSubtree(tid, node->ptrs[i]);
        }
    }
    DEALLOCATE_NODE(node);
}
