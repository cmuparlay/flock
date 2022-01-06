#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cassert>
#include <climits>
#include <tuple>
#include <thread>
#include <utility>

using namespace std;
using namespace std::chrono;

#define RECLAIM_NODE(node) recmgr->retire(tid, (nodeptr) (node))
#define DEALLOCATE_NODE(node) recmgr->deallocate(tid, (nodeptr) (node))

#define SINGLE_ADJUSTER
#ifdef SINGLE_ADJUSTER
#define IS_ADJUSTER tid == 1 && duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() & 7
#else
#define IS_ADJUSTER true
#endif

enum Direction {
    LEFT,
    RIGHT
};

const int OVLBitsBeforeOverflow = 8;
const int64_t UnlinkedOVL = 1L;
const int64_t OVLGrowLockMask = 2L;
const int64_t OVLShrinkLockMask = 4L;
const int OVLGrowCountShift = 3;
const int64_t OVLGrowCountMask = ((1L << OVLBitsBeforeOverflow) - 1) << OVLGrowCountShift;
const int64_t OVLShrinkCountShift = OVLGrowCountShift + OVLBitsBeforeOverflow;


/** The number of spins before yielding. */
const int SpinCount = 100;

/** The number of yields before blocking. */
const int YieldCount = 100;

template <typename K, typename V>
struct Node {
    K key;
    volatile int64_t changeOVL;
    Node<K,V>* volatile left;
    Node<K,V>* volatile right;
    atomic<bool> locked;
    Node<K,V>* volatile parent;

    int ncnt;
    int rcnt;
    int lcnt;

    volatile V val;

    struct LockGuard {
        Node<K,V>* node;
        bool owned;

        LockGuard(Node<K,V>* _node): node{_node}, owned{false} {
            while (true) {
                while (node->locked.load(memory_order_acquire)) {}
                bool wasLocked = false;
                if (node->locked.compare_exchange_weak(wasLocked, true)) {
                    owned = true;
                    return;
                }
            }
        }

        ~LockGuard() {
            if (owned) {
                node->locked.store(false, memory_order_release);
            }
        }
    };

    Node<K,V>* child(Direction dir) {
        return dir == LEFT ? left : right;
    }

    Node<K,V>* childSibling(Direction dir) {
        return dir == LEFT ? right : left;
    }

    void setChild(Direction dir, Node<K,V>* node) {
        if (dir == LEFT) {
            left = node;
        } else {
            right = node;
        }
    }

    //////// per-node blocking
    bool isChanging(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask)) != 0;
    }
    bool isUnlinked(const int64_t ovl) {
        return ovl == UnlinkedOVL;
    }
    bool isShrinkingOrUnlinked(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | UnlinkedOVL)) != 0;
    }
    bool isChangingOrUnlinked(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask | UnlinkedOVL)) != 0;
    }

    bool hasShrunkOrUnlinked(const int64_t orig, const int64_t current) {
        return ((orig ^ current) & ~(OVLGrowLockMask | OVLGrowCountMask)) != 0;
    }
    bool hasChangedOrUnlinked(const int64_t orig, const int64_t current) {
        return orig != current;
    }

    int64_t beginGrow(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));
        return ovl | OVLGrowLockMask;
    }

    int64_t endGrow(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));

        // Overflows will just go into the shrink lock count, which is fine.
        return ovl + (1L << OVLGrowCountShift);
    }

    int64_t beginShrink(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));
        return ovl | OVLShrinkLockMask;
    }

    int64_t endShrink(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));

        // increment overflows directly
        return ovl + (1L << OVLShrinkCountShift);
    }

    void waitUntilChangeCompleted(const int64_t ovl) {
        if (!isChanging(ovl)) {
            return;
        }

        for (int tries = 0; tries < SpinCount; ++tries) {
            if (changeOVL != ovl) {
                return;
            }
        }

        for (int tries = 0; tries < YieldCount; ++tries) {
            std::this_thread::yield();
            if (changeOVL != ovl) {
                return;
            }
        }

        // // spin and yield failed, use the nuclear option
        Node<K,V>::LockGuard nodeLock(this);
        // we can't have gotten the lock unless the shrink was over
        assert(changeOVL != ovl);
    }
};
#define nodeptr Node<K,V>*
#define NodeLockGuard typename Node<K,V>::LockGuard

template <class RecordManager, typename K, typename V, class Compare>
class CBTree {
  public:
    V const NO_VALUE;
    const int numThreads;
    PAD;
    RecordManager * const recmgr;    
    PAD;
    Compare compare;
    PAD;
    nodeptr rootHolder;
    PAD;

    nodeptr createNode(const int tid, const K key, const V val, Node<K,V>* parent, const int64_t changeOVL, Node<K,V>* left, Node<K,V>* right) {
        nodeptr node = recmgr->template allocate<Node<K,V>>(tid);
        node->key = key;
        node->val = val;
        node->parent = parent;
        node->changeOVL = changeOVL;
        node->left = left;
        node->right = right;
        node->ncnt = 1;
        node->rcnt = 0;
        node->lcnt = 0;
        node->locked.store(false, memory_order_relaxed);
        return node;
    }

    void initThread(const int tid) {
        recmgr->initThread(tid);
    }

    void deinitThread(const int tid) {
        recmgr->deinitThread(tid);
    }

    CBTree(const int _numThreads, const K anyKey, const K _maxKey) : NO_VALUE((V)0), numThreads(_numThreads), recmgr(new RecordManager(numThreads)) {
        const int tid = 0;
        initThread(0);

        compare = Compare();
        rootHolder = createNode(tid, (K) 0, NO_VALUE, nullptr, 0, nullptr, nullptr);
        rootHolder->ncnt = INT_MAX;
    }

    void freeSubtree(const int tid, nodeptr node) {
        if (node->left) freeSubtree(0, node->left);
        if (node->right) freeSubtree(0, node->right);
        DEALLOCATE_NODE(node);
    }

    ~CBTree() {
        int tid = 0;
        initThread(tid);
        freeSubtree(tid, rootHolder);
        deinitThread(tid);
        delete recmgr;
    }

    bool isChanging(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask)) != 0;
    }
    bool isUnlinked(const int64_t ovl) {
        return ovl == UnlinkedOVL;
    }
    bool isShrinkingOrUnlinked(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | UnlinkedOVL)) != 0;
    }
    bool isChangingOrUnlinked(const int64_t ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask | UnlinkedOVL)) != 0;
    }

    bool hasShrunkOrUnlinked(const int64_t orig, const int64_t current) {
        return ((orig ^ current) & ~(OVLGrowLockMask | OVLGrowCountMask)) != 0;
    }
    bool hasChangedOrUnlinked(const int64_t orig, const int64_t current) {
        return orig != current;
    }

    int64_t beginGrow(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));
        return ovl | OVLGrowLockMask;
    }

    int64_t endGrow(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));

        // Overflows will just go into the shrink lock count, which is fine.
        return ovl + (1L << OVLGrowCountShift);
    }

    int64_t beginShrink(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));
        return ovl | OVLShrinkLockMask;
    }

    int64_t endShrink(const int64_t ovl) {
        assert(!isChangingOrUnlinked(ovl));

        // increment overflows directly
        return ovl + (1L << OVLShrinkCountShift);
    }

    int compareKey(const K& k1, const K& k2) {
        if (compare(k1, k2)) return -1;
        if (compare(k2, k1)) return 1;
        return 0;
    }

    nodeptr getRoot() {
        return rootHolder;
    }

    bool contains(const int tid, const K key) {
        return get(tid, key) == NO_VALUE;
    }

    //////// search
    /** Returns either a value, if present, or NO_VALUE, if absent. */
    V get(const int tid, const K key) {
        while (true) {
            auto guard = recmgr->getGuard(tid);

            nodeptr const right = rootHolder->right;
            if (right == nullptr) {
                return NO_VALUE;
            } else {
                const int rightCmp = compareKey(key, right->key);
                if (rightCmp == 0) {
                    return right->val;
                }

                const int64_t ovl = right->changeOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right->waitUntilChangeCompleted(ovl);
                    // RETRY
                } else if (right == rootHolder->right) {
                    // the reread of .right is the one protected by our read of ovl
                    bool succeeded;
                    V retval;
                    tie(succeeded, retval) = attemptGet(tid, key, right, (rightCmp < 0 ? LEFT : RIGHT), ovl, 1, false);
                    if (succeeded) {
                        return retval;
                    }
                    // else RETRY
                }
            }
        }
    }

    // Returns <true, val> if found, <true, NO_VALUE> if not found, and <false, _> if we need to retry from root.
    tuple<bool, V> attemptGet(const int tid, const K key, nodeptr const node, const Direction dirToChild, const int64_t nodeOVL, const int height, bool shouldRebalance) {
        const auto retryFromRoot = make_tuple(false, (V) 0);
        while (true) {
            nodeptr const child = node->child(dirToChild);

            if (child == nullptr) {
                if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                    return retryFromRoot;
                }

                // Node is not present.  Read of node->child occurred while
                // parent->child was valid, so we were not affected by any
                // shrinks.
                return make_tuple(true, NO_VALUE);
            } else {
            	// check along the path that the potential is decreasing.
                int childCmp = compareKey(key, child->key);

                if (IS_ADJUSTER && shouldRebalance) {
                    if (rebalance(node, child, childCmp)) {
                        return retryFromRoot;
                    }
                }
                
                if (childCmp == 0) {
                    if (IS_ADJUSTER) {
                        child->ncnt++;
                    }
                    return make_tuple(true, child->val);
                }
                
                // child is non-null
                const int64_t childOVL = child->changeOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child->waitUntilChangeCompleted(childOVL);

                    if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                        return retryFromRoot;
                    }
                    // else RETRY
                } else if (child != node->child(dirToChild)) {
                    // this .child is the one that is protected by childOVL
                    if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                        return retryFromRoot;
                    }
                    // else RETRY
                } else {
                    if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                        return retryFromRoot;
                    }
                    
                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    bool succeeded;
                    V retval;
                    tie(succeeded, retval) = attemptGet(tid, key, child, (childCmp < 0 ? LEFT : RIGHT), childOVL, height + 1, !shouldRebalance);
                    if (succeeded) {
                        if (retval != NO_VALUE) {
                            if (IS_ADJUSTER) {
                                if (dirToChild == LEFT) {
                                    node->lcnt++;
                                } else {
                                    node->rcnt++;
                                }
                            }
                        }
                        return make_tuple(true, retval);
                    }
                    // else RETRY
                }
            }
        }
    }

    V insert(const int tid, const K key, const V val) {
        while (true) {
            auto guard = recmgr->getGuard(tid);

            nodeptr right = rootHolder->right;
            if (right == nullptr) {
                // Tree is empty
                if (attemptInsertIntoEmpty(tid, key, val)) {
                    // nothing needs to be done, or we were successful, prev value is Absent
                    // nothing to remove in empty tree or inserted the first node.
                    return NO_VALUE;
                }
                // else RETRY
            } else {
                const int64_t ovl = right->changeOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right->waitUntilChangeCompleted(ovl);
                    // RETRY
                } else if (right == rootHolder->right) {
                    bool succeeded;
                    V retval;
                    // this is the protected .right
                    tie(succeeded, retval) = attemptInsert(tid, key, val, rootHolder, right, ovl, 1, false);

                    if (succeeded) {
                        return retval;
                    }
                    // else RETRY
                }
            }
        }
    }

    bool attemptInsertIntoEmpty(const int tid, const K key, const V val) {
        NodeLockGuard nodeLock(rootHolder);
        if (rootHolder->right == nullptr) {
            rootHolder->right = createNode(tid, key, val, rootHolder, 0, nullptr, nullptr);
            return true;
        } else {
            return false;
        }
    }

    tuple<bool, V> attemptInsert(const int tid, const K key, const V newValue, nodeptr const parent, nodeptr const node, const int64_t nodeOVL, const int height, bool shouldRebalance) {
        // As the search progresses there is an implicit min and max assumed for the
        // branch of the tree rooted at node. A left rotation of a node x results in
        // the range of keys in the right branch of x being reduced, so if we are at a
        // node and we wish to traverse to one of the branches we must make sure that
        // the node has not undergone a rotation since arriving from the parent.
        //
        // A rotation of node can't screw us up once we have traversed to node's
        // child, so we don't need to build a huge transaction, just a chain of
        // smaller read-only transactions.

        const auto retryFromRoot = make_tuple(false, (V) 0);
        assert(nodeOVL != UnlinkedOVL);

        const int cmp = compareKey(key, node->key);
        if (cmp == 0) {
            if (IS_ADJUSTER) {
                node->ncnt++;
            }
            bool succeeded;
            V retval;
            return attemptNodeUpdate(tid, newValue, parent, node);
        }

        const Direction dirToChild = cmp < 0 ? LEFT : RIGHT;

        while (true) {
            nodeptr const child = node->child(dirToChild);

            if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                return retryFromRoot;
            }

            if (child == nullptr) {
                // key is not present
                // Update will be an insert.
                NodeLockGuard nodeLock(node);
                // Validate that we haven't been affected by past
                // rotations.  We've got the lock on node, so no future
                // rotations can mess with us.
                if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                    return retryFromRoot;
                }

                if (node->child(dirToChild) != nullptr) {
                    // Lost a race with a concurrent insert.  No need
                    // to back up to the parent, but we must RETRY in
                    // the outer loop of this method.
                } else {
                    // We're valid.

                    // Create a new leaf
                    node->setChild(dirToChild, createNode(tid, key, newValue, node, 0L, nullptr, nullptr));
                    if (IS_ADJUSTER) {
                        if (dirToChild == LEFT) {
                            node->lcnt++;
                        } else {
                            node->rcnt++;
                        }
                    }

                    return make_tuple(true, NO_VALUE);
                }
            } else {
            	// check along the path that the potential is decreasing.
            	int childCmp = compareKey(key, child->key);
            	
                if (IS_ADJUSTER && shouldRebalance) {
                    if (rebalance(node, child, childCmp)) {
                        return retryFromRoot;
                    }
                }

                // non-null child
                const int64_t childOVL = child->changeOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child->waitUntilChangeCompleted(childOVL);
                    // RETRY
                } else if (child != node->child(dirToChild)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                } else {
                    // validate the read that our caller took to get to node
                    if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                        return retryFromRoot;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    bool succeeded;
                    V retval;
                    tie(succeeded, retval) = attemptInsert(tid, key, newValue, node, child, childOVL, height + 1, !shouldRebalance);
                    if (succeeded) {
                        if (IS_ADJUSTER) {
                            if (dirToChild == LEFT) {
                                node->lcnt++;
                            } else {
                                node->rcnt++;
                            }
                        }
                        return make_tuple(true, retval);
                    }
                    // else RETRY
                }
            }
        }
    }

    ///////////////////////// remove
    V remove(const int tid, K key) {
        while (true) {
            auto guard = recmgr->getGuard(tid);

            nodeptr const right = rootHolder->right;
            if (right == nullptr) {
                // key is not present
                // nothing needs to be done, or we were successful, prev
                // value is Absent
                // nothing to remove in empty tree or inserted the first
                // node.
                return NO_VALUE;
                // else RETRY
            } else {
                const int64_t ovl = right->changeOVL;
                if (isShrinkingOrUnlinked(ovl)) {
                    right->waitUntilChangeCompleted(ovl);
                    // RETRY
                } else if (right == rootHolder->right) {
                    bool succeeded;
                    V retval;
                    // this is the protected .right
                    tie(succeeded, retval) = attemptRemove(tid, key, rootHolder, right, ovl, 1);

                    if (succeeded) {
                        return retval;
                    }
                    // else RETRY
                }
            }
        }
    }

    /** If successful returns <true, previous_value> or <true, NO_VALUE> if not previously in the map.
     *  The caller should retry if this method returns <false, _>.
     */
    tuple<bool, V> attemptRemove(const int tid, const K key, nodeptr const parent, nodeptr const node, const int64_t nodeOVL, const int height) {
        // As the search progresses there is an implicit min and max assumed for the
        // branch of the tree rooted at node. A left rotation of a node x results in
        // the range of keys in the right branch of x being reduced, so if we are at a
        // node and we wish to traverse to one of the branches we must make sure that
        // the node has not undergone a rotation since arriving from the parent.
        //
        // A rotation of node can't screw us up once we have traversed to node's
        // child, so we don't need to build a huge transaction, just a chain of
        // smaller read-only transactions.

        const auto retryFromRoot = make_tuple(false, (V) 0);
        assert (nodeOVL != UnlinkedOVL);

        const int cmp = compareKey(key, node->key);
        if (cmp == 0) {
            bool succeeded;
            V retval;
            return attemptNodeUpdate(tid, NO_VALUE, parent, node);   
        }

        const Direction dirToChild = cmp < 0 ? LEFT : RIGHT;

        while (true) {
            nodeptr const child = node->child(dirToChild);

            if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                return retryFromRoot;
            }

            if (child == nullptr) {
                // key is not present
                // Removal is requested.  Read of node->child occurred
                // while parent->child was valid, so we were not affected
                // by any shrinks.
                return make_tuple(true, NO_VALUE);
            } else {
                // non-null child
                const int64_t childOVL = child->changeOVL;
                if (isShrinkingOrUnlinked(childOVL)) {
                    child->waitUntilChangeCompleted(childOVL);
                    // RETRY
                } else if (child != node->child(dirToChild)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                } else {
                    // validate the read that our caller took to get to node
                    if (hasShrunkOrUnlinked(nodeOVL, node->changeOVL)) {
                        return retryFromRoot;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    bool succeeded;
                    V retval;
                    tie(succeeded, retval) = attemptRemove(tid, key, node, child, childOVL, height + 1);
                    if (succeeded) {
                        return make_tuple(true, retval);
                    }
                    // else RETRY
                }
            }
        }
    }

    /** parent will only be used for unlink, update can proceed even if parent
     *  is stale.
     */
    tuple<bool, V> attemptNodeUpdate(const int tid, const V newValue, nodeptr const parent, nodeptr const node) {
        const auto retryFromRoot = make_tuple(false, (V) 0);
        if (newValue == NO_VALUE) {
            // removal
            if (node->val == NO_VALUE) {
                // This node is already removed, nothing to do.
                return make_tuple(true, NO_VALUE);
            }
        }

        if (newValue == NO_VALUE && (node->left == nullptr || node->right == nullptr)) {
            // potential unlink, get ready by locking the parent
            NodeLockGuard parentLock(parent);
            if (isUnlinked(parent->changeOVL) || node->parent != parent) {
                return retryFromRoot;
            }

            NodeLockGuard nodeLock(node);
            const V prev = node->val;
            if (prev == NO_VALUE) {
                // nothing to do
                return make_tuple(true, NO_VALUE);
            }
            if (!attemptUnlink_nl(parent, node)) {
                return retryFromRoot;
            }
            return make_tuple(true, prev);
        } else {
            // potential update (including remove-without-unlink)
            NodeLockGuard nodeLock(node);
            // regular version changes don't bother us
            if (isUnlinked(node->changeOVL)) {
                return retryFromRoot;
            }

            // retry if we now detect that unlink is possible
            if (newValue == NO_VALUE && (node->left == nullptr || node->right == nullptr)) {
                return retryFromRoot;
            }

            // update in-place
            V prev = node->val;
            if (newValue == NO_VALUE) { // remove
                node->val = NO_VALUE;
            } else { // insert
                if (prev == NO_VALUE) {
                    node->val = newValue;
                }
            }
            return make_tuple(true, prev);
        }
    }

    /** Does not adjust the size or any heights. */
    bool attemptUnlink_nl(nodeptr const parent, nodeptr const node) {
        // assert (Thread.holdsLock(parent));
        // assert (Thread.holdsLock(node));
        assert (!isUnlinked(parent->changeOVL));

        nodeptr const parentL = parent->left;
        nodeptr const parentR = parent->right;
        if (parentL != node && parentR != node) {
            // node is no longer a child of parent
            return false;
        }

        assert (!isUnlinked(node->changeOVL));
        assert (parent == node->parent);

        nodeptr const left = node->left;
        nodeptr const right = node->right;
        if (left != nullptr && right != nullptr) {
            // splicing is no longer possible
            return false;
        }
        nodeptr const splice = left != nullptr ? left : right;

        if (parentL == node) {
            parent->left = splice;
        } else {
            parent->right = splice;
        }
        if (splice != nullptr) {
            splice->parent = parent;
        }

        node->changeOVL = UnlinkedOVL;
        node->val = NO_VALUE;

        RECLAIM_NODE(node);
        return true;
    }

    // return the next node to continue the search
    bool rebalance(nodeptr const parent, nodeptr const node, int nodeCmp) {
        // increment counter + rotate if needed.
        int ncnt = 0;  // got the previous counter value then add
        int pcnt = parent->ncnt + parent->rcnt + parent->lcnt;
        bool doZig = false;
        bool doZigZag = false;
        
        if (parent->left == node) {
			if (nodeCmp <= 0){
	    		if (5 * node->lcnt > 4 * pcnt){
	    			doZig = true;
	    		}
			} else {
	    		if (5 * node->rcnt > 4 * pcnt){
	    			doZigZag = true;
	           		ncnt = node->ncnt + node->lcnt;
	    		}
        	}
        } else {
            ncnt = node->ncnt + node->rcnt;
			if (nodeCmp >= 0){
	    		if (5 * node->rcnt > 4 * pcnt){
	    			doZig = true;
	    		}
			} else {
	    		if (5 * node->lcnt > 4 * pcnt){
	    			doZigZag = true;
	           		ncnt = node->ncnt + node->lcnt;
	    		}
        	}
        }

        if (doZig) {
            nodeptr const grand = parent->parent;
            NodeLockGuard grandLock(grand);
            if ((grand->left == parent) || (grand->right == parent)) {
                NodeLockGuard parentLock(parent);
                if (parent->left == node) {
                    NodeLockGuard nodeLock(node);
                    rotateRight(grand, parent, node, node->right);
                    parent->lcnt = node->rcnt;
                    node->rcnt += pcnt;
                    return true;
                } else if (parent->right == node) {
                    NodeLockGuard nodeLock(node);
                    rotateLeft(grand, parent, node, node->left);
                    parent->rcnt = node->lcnt;
                    node->lcnt += pcnt;
                    return true;
                } // else lost for another concurrent update
            } // else lost for another concurrent update
        } else if (doZigZag) {
            nodeptr const grand = parent->parent;
            NodeLockGuard grandLock(grand);
            if ((grand->left == parent) || (grand->right == parent)) {
                NodeLockGuard parentLock(parent);
                if (parent->left == node) {
                    NodeLockGuard nodeLock(node);
                    nodeptr const nR = node->right;
                    if (nR != nullptr) { // check that it wasn't deleted by concurrent thread
                        NodeLockGuard nRLock(nR);
                        rotateRightOverLeft(grand, parent, node, nR);
                        parent->lcnt = nR->rcnt;
                        node->rcnt = nR->lcnt;
                        nR->rcnt += pcnt;
                        nR->lcnt += ncnt;
                        return true;
                    }
                } else if (parent->right == node) {
                    NodeLockGuard nodeLock(node);
                    nodeptr const nL = node->left;
                    if (nL != nullptr) { // check that it wasn't deleted by concurrent thread
                        NodeLockGuard nLLock(nL);
                        rotateLeftOverRight(grand, parent, node, nL);
                        parent->rcnt = nL->lcnt;
                        node->lcnt = nL->rcnt;
                        nL->lcnt += pcnt;
                        nL->rcnt += ncnt;
                        return true;
                    }
                } // else lost for another concurrent update
            } // else lost for another concurrent update
        } 
        return false;
    }

    void rotateRightOverLeft(nodeptr const nParent, nodeptr const n, nodeptr const nL, nodeptr const nLR) {
        const int64_t nodeOVL = n->changeOVL;
        const int64_t leftOVL = nL->changeOVL;
        const int64_t leftROVL = nLR->changeOVL;

        nodeptr const nPL = nParent->left;
        nodeptr const nLRL = nLR->left;
        nodeptr const nLRR = nLR->right;

        n->changeOVL = beginShrink(nodeOVL);
        nL->changeOVL = beginShrink(leftOVL);
        nLR->changeOVL = beginGrow(leftROVL);

        n->left = nLRR;
        nL->right = nLRL;
        nLR->left = nL;
        nLR->right = n;
        if (nPL == n) {
            nParent->left = nLR;
        } else {
            nParent->right = nLR;
        }

        nLR->parent = nParent;
        nL->parent = nLR;
        n->parent = nLR;
        if (nLRR != nullptr) {
            nLRR->parent = n;
        }
        if (nLRL != nullptr) {
            nLRL->parent = nL;
        }

        nLR->changeOVL = endGrow(leftROVL);
        nL->changeOVL = endShrink(leftOVL);
        n->changeOVL = endShrink(nodeOVL);
    }

    void rotateLeftOverRight(nodeptr const nParent, nodeptr const n, nodeptr const nR, nodeptr const nRL) {
        const int64_t nodeOVL = n->changeOVL;
        const int64_t rightOVL = nR->changeOVL;
        const int64_t rightLOVL = nRL->changeOVL;

        nodeptr const nPL = nParent->left;
        nodeptr const nRLL = nRL->left;
        nodeptr const nRLR = nRL->right;

        n->changeOVL = beginShrink(nodeOVL);
        nR->changeOVL = beginShrink(rightOVL);
        nRL->changeOVL = beginGrow(rightLOVL);

        n->right = nRLL;
        nR->left = nRLR;
        nRL->right = nR;
        nRL->left = n;
        if (nPL == n) {
            nParent->left = nRL;
        } else {
            nParent->right = nRL;
        }

        nRL->parent = nParent;
        nR->parent = nRL;
        n->parent = nRL;
        if (nRLL != nullptr) {
            nRLL->parent = n;
        }
        if (nRLR != nullptr) {
            nRLR->parent = nR;
        }

        nRL->changeOVL = endGrow(rightLOVL);
        nR->changeOVL = endShrink(rightOVL);
        n->changeOVL = endShrink(nodeOVL);
    }

    // nParent, n, nL must be locked.
    // nLR can be not locked.
    // if it is changed by other thread it is grandParent otherwise its parent would be locked as well.
    // the grand->parent is not important to other thread (and it is the only field that is changed here)
    void rotateRight(nodeptr const nParent, nodeptr const n, nodeptr const nL, nodeptr const nLR) {
        const int64_t nodeOVL = n->changeOVL;
        const int64_t leftOVL = nL->changeOVL;

        nodeptr const nPL = nParent->left;

        n->changeOVL = beginShrink(nodeOVL);
        nL->changeOVL = beginGrow(leftOVL);

        // Down links originally to shrinking nodes should be the last to change,
        // because if we change them early a search might bypass the OVL that
        // indicates its invalidity.  Down links originally from shrinking nodes
        // should be the first to change, because we have complete freedom when to
        // change them.  s/down/up/ and s/shrink/grow/ for the parent links.

        n->left = nLR;
        nL->right = n;

        if (nPL == n) {
            nParent->left = nL;
        } else {
            nParent->right = nL;
        }

        nL->parent = nParent;
        n->parent = nL;
        if (nLR != nullptr) {
            nLR->parent = n;
        }

        nL->changeOVL = endGrow(leftOVL);
        n->changeOVL = endShrink(nodeOVL);
    }

    // see comment on rotateRight
    void rotateLeft(nodeptr const nParent, nodeptr const n, nodeptr const nR, nodeptr const nRL) {
        const int64_t nodeOVL = n->changeOVL;
        const int64_t rightOVL = nR->changeOVL;

        nodeptr const nPL = nParent->left;

        n->changeOVL = beginShrink(nodeOVL);
        nR->changeOVL = beginGrow(rightOVL);

        n->right = nRL;
        nR->left = n;
        if (nPL == n) {
            nParent->left = nR;
        } else {
            nParent->right = nR;
        }

        nR->parent = nParent;
        n->parent = nR;
        if (nRL != nullptr) {
            nRL->parent = n;
        }

        nR->changeOVL = endGrow(rightOVL);
        n->changeOVL = endShrink(nodeOVL);
    }

    // void dfs(nodeptr n, String what, int height) {
    //     if (n == nullptr)
    //         return;

    //     for (int i = 0; i < height; i++)
    //         printf("       ");
    //     printf("%s: [%ld] ncnt=%ld lcnt=%ld rcnt=%ld>\n", what, (int64_t) n->key, n->ncnt, n->lcnt, n->rcnt);

    //     dfs(n->left, "left", height + 1);
    //     dfs(n->right, "right", height + 1);
    // }

    // void print(int mode) {
    //     dfs(rootHolder->right, "root", 0);
    // }
};
