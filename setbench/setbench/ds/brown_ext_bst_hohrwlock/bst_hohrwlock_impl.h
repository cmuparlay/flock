#ifndef BST_HOHRWLOCK_H
#define	BST_HOHRWLOCK_H

#include "record_manager.h"
#include "locks_impl.h"

#include "rwlock.h"
#define RWLOCK_FAVOR_READERS

/* USE_LOCK_TABLE HAS PROGRESS PROBLEMS!!! if two nodes locked at the same time by the same operation hash to the same rwlock, an upgrade can self deadlock */
//#define USE_LOCK_TABLE

#if defined USE_LOCK_TABLE
    static inline uint64_t hash_murmur3(uint64_t v) {
        v ^= v>>33;
        v *= 0xff51afd7ed558ccdLLU;
        v ^= v>>33;
        v *= 0xc4ceb9fe1a85ec53LLU;
        v ^= v>>33;
        return v;
    }
    struct PaddedRWLock {
        union {
            RWLock lock;
            PAD;
        };
        PaddedRWLock() {
            lock.init();
        }
    };
    #define LOCK_TABLE_SIZE (1<<20)
    struct LockTable {
        PAD;
        PaddedRWLock paddedlocks[LOCK_TABLE_SIZE];
        // ending PAD is implied
    };
    LockTable __locktab;
#   define LOCK(x) __locktab.paddedlocks[hash_murmur3((uint64_t) (x)->key) & (LOCK_TABLE_SIZE-1)].lock
#else
#   define LOCK(x) x->lock
#endif

namespace bst_hohrwlock_ns {

    #define nodeptr Node<K,V> * volatile

    template <class K, class V>
    class Node {
    public:
#if !defined USE_LOCK_TABLE
        RWLock lock;
#endif
        V value;
        K key;
        nodeptr left;
        nodeptr right;
        Node() {}
    };

    template <class K, class V, class Compare, class RecManager>
    class bst_hohrwlock {
    private:
    PAD;
        RecManager * const recmgr;
    PAD;
        nodeptr root;        // actually const
        Compare cmp;
    PAD;
        inline nodeptr createNode(const int tid, const K& key, const V& value, nodeptr const left, nodeptr const right) {
            nodeptr newnode = recmgr->template allocate<Node<K,V> >(tid);
            if (newnode == NULL) {
                COUTATOMICTID("ERROR: could not allocate node"<<std::endl);
                exit(-1);
            }
        // #ifdef GSTATS_HANDLE_STATS
        //     GSTATS_APPEND(tid, node_allocated_addresses, ((long long) newnode)%(1<<12));
        // #endif
            newnode->key = key;
            newnode->value = value;
            newnode->left = left;
            newnode->right = right;
#if !defined USE_LOCK_TABLE
            newnode->lock.init();
#endif
            return newnode;
        }
        const V doInsert(const int tid, const K& key, const V& val, bool onlyIfAbsent);

        int init[MAX_THREADS_POW2] = {0,};
    PAD;

public:
        const K NO_KEY;
        const V NO_VALUE;
    PAD;

        /**
         * This function must be called once by each thread that will
         * invoke any functions on this class.
         *
         * It must be okay that we do this with the main thread and later with another thread!!!
         */
        void initThread(const int tid) {
            if (init[tid]) return; else init[tid] = !init[tid];
            recmgr->initThread(tid);
        }
        void deinitThread(const int tid) {
            if (!init[tid]) return; else init[tid] = !init[tid];
            recmgr->deinitThread(tid);
        }

        bst_hohrwlock(const K _NO_KEY,
                    const V _NO_VALUE,
                    const int numProcesses)
            :         NO_KEY(_NO_KEY)
                    , NO_VALUE(_NO_VALUE)
                    , recmgr(new RecManager(numProcesses))
        {

            VERBOSE DEBUG COUTATOMIC("constructor bst_hohrwlock"<<std::endl);
            cmp = Compare();

            const int tid = 0;
            initThread(tid);

            recmgr->endOp(tid); // enter an initial quiescent state.
            nodeptr rootleft = createNode(tid, NO_KEY, NO_VALUE, NULL, NULL);
            nodeptr _root = createNode(tid, NO_KEY, NO_VALUE, rootleft, NULL);
            root = _root;
        }


        void dfsDeallocateBottomUp(nodeptr const u, int *numNodes) {
            if (u == NULL) return;
            if (u->left != NULL) {
                dfsDeallocateBottomUp(u->left, numNodes);
                dfsDeallocateBottomUp(u->right, numNodes);
            }
            MEMORY_STATS ++(*numNodes);
            recmgr->deallocate(0 /* tid */, u);
        }
        ~bst_hohrwlock() {
            VERBOSE DEBUG COUTATOMIC("destructor bst_hohrwlock");
            // free every node and scx record currently in the data structure.
            // an easy DFS, freeing from the leaves up, handles all nodes.
            // cleaning up scx records is a little bit harder if they are in progress or aborted.
            // they have to be collected and freed only once, since they can be pointed to by many nodes.
            // so, we keep them in a set, then free each set element at the end.
            int numNodes = 0;
            dfsDeallocateBottomUp(root, &numNodes);
            VERBOSE DEBUG COUTATOMIC(" deallocated nodes "<<numNodes<<std::endl);
            recmgr->printStatus();
            delete recmgr;
        }

        const V insert(const int tid, const K& key, const V& val) { return doInsert(tid, key, val, false); }
        const V insertIfAbsent(const int tid, const K& key, const V& val) { return doInsert(tid, key, val, true); }
        const std::pair<V,bool> erase(const int tid, const K& key);
        const std::pair<V,bool> find(const int tid, const K& key);
        int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) { return 0; }
        bool contains(const int tid, const K& key) { return find(tid, key).second; }

        RecManager * debugGetRecMgr() { return recmgr; }
        nodeptr debug_getEntryPoint() { return root; }
    };

}

template<class K, class V, class Compare, class RecManager>
const std::pair<V,bool> bst_hohrwlock_ns::bst_hohrwlock<K,V,Compare,RecManager>::find(const int tid, const K& key) {
    auto guard = recmgr->getGuard(tid, true);

    LOCK(root).readLock();
    auto p = root->left;
    LOCK(p).readLock();
    LOCK(root).readUnlock();
    auto l = p->left;
    if (l == NULL) {
        LOCK(p).readUnlock();
        return std::pair<V,bool>(NO_VALUE, false); // no keys in data structure
    }
    while (1) {
        LOCK(l).readLock();
        LOCK(p).readUnlock();
        if (l->left == NULL) break;
        p = l;
        l = (p->key == NO_KEY || cmp(key, p->key)) ? p->left : p->right;
    }
    auto result = (key == l->key) ? std::pair<V,bool>(l->value, true) : std::pair<V,bool>(NO_VALUE, false);
    LOCK(l).readUnlock();
    return result;
}

template<class K, class V, class Compare, class RecManager>
const V bst_hohrwlock_ns::bst_hohrwlock<K,V,Compare,RecManager>::doInsert(const int tid, const K& key, const V& val, bool onlyIfAbsent) {
    auto guard = recmgr->getGuard(tid);
retry:
    auto p = root;
    LOCK(p).readLock();
    auto l = p->left;
    while (1) {
        LOCK(l).readLock();
        if (l->left == NULL) break;
        LOCK(p).readUnlock(); // want p to be locked after the loop, so release only if we didn't break
        p = l;
        l = (p->key == NO_KEY || cmp(key, p->key)) ? p->left : p->right;
    }
    // now, p and l are read locked

    // if we find the key in the tree already
    if (key == l->key) {
        auto result = l->value;
        if (onlyIfAbsent) {
            LOCK(p).readUnlock();
            LOCK(l).readUnlock();
            return result;
        }

        if (!LOCK(l).upgradeLock()) {
            LOCK(p).readUnlock();
            LOCK(l).readUnlock();
            goto retry;
        }
        // now l is write locked
        l->value = val;

        LOCK(p).readUnlock();
        LOCK(l).writeUnlock();
        return result;

    } else {
        if (!LOCK(p).upgradeLock()) {
            LOCK(p).readUnlock();
            LOCK(l).readUnlock();
            goto retry;
        }
        // now p is write locked

        auto newLeaf = createNode(tid, key, val, NULL, NULL);
        auto newParent = (l->key == NO_KEY || cmp(key, l->key))
            ? createNode(tid, l->key, l->value, newLeaf, l)
            : createNode(tid, key, val, l, newLeaf);

        (l == p->left ? p->left : p->right) = newParent;

        LOCK(p).writeUnlock();
        LOCK(l).readUnlock();
        return NO_VALUE;
    }
}

template<class K, class V, class Compare, class RecManager>
const std::pair<V,bool> bst_hohrwlock_ns::bst_hohrwlock<K,V,Compare,RecManager>::erase(const int tid, const K& key) {
    auto guard = recmgr->getGuard(tid);
retry:
    auto gp = root;
    LOCK(gp).readLock();
    auto p = gp->left;
    LOCK(p).readLock();
    auto l = p->left;
    if (l == NULL) { // tree is empty
        LOCK(gp).readUnlock();
        LOCK(p).readUnlock();
        return std::pair<V,bool>(NO_VALUE, false);
    }
    // now, gp and p are read locked, and l is non-null

    while (1) {
        LOCK(l).readLock();
        if (l->left == NULL) break;
        LOCK(gp).readUnlock(); // only unlock gp before overwriting the pointer (want gp locked when we exit this loop, since we change it)
        gp = p;
        p = l;
        l = (p->key == NO_KEY || cmp(key, p->key)) ? p->left : p->right;
    }
    // now, gp, p and l are read locked

    // if we fail to find the key in the tree
    if (key != l->key) {
        LOCK(gp).readUnlock();
        LOCK(p).readUnlock();
        LOCK(l).readUnlock();
        return std::pair<V,bool>(NO_VALUE, false);
    } else {
        if (!LOCK(gp).upgradeLock()) {
            LOCK(gp).readUnlock();
            LOCK(p).readUnlock();
            LOCK(l).readUnlock();
            goto retry;
        }
        // now gp is write locked (and p and l are read locked)

        auto result = l->value;
        auto s = (l == p->left ? p->right : p->left);
        (p == gp->left ? gp->left : gp->right) = s;

        recmgr->retire(tid, p);
        recmgr->retire(tid, l);
        LOCK(gp).writeUnlock();
        LOCK(p).readUnlock();
        LOCK(l).readUnlock();
        return std::pair<V,bool>(result, true);
    }
}

#endif
