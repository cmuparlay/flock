#pragma once

#include <cassert>


#define MAX_KCAS 21
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

#define IS_MARKED(word) (word & 0x1)

template<typename K, typename V>
struct Node {
    casword<K> key;
    casword<casword_t> vNumMark;
    casword<Node<K, V> *> left;
    casword<Node<K, V> *> right;
    casword<Node<K, V> *> parent;
    casword<int> height;
    casword<V> value;
};

enum RetCode: int {
    RETRY = 0,
    UNNECCESSARY = 0,
    FAILURE = -1,
    SUCCESS = 1,
    SUCCESS_WITH_HEIGHT_UPDATE = 2
};

template<class RecordManager, typename K, typename V>
class InternalKCAS {
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
        ObservedNode path[MAX_PATH_SIZE];
        volatile char padding[PADDING_BYTES];
    };


    volatile char padding0[PADDING_BYTES];
    //Debugging, used to validate that no thread's parent can't be NULL, save for the root
    bool init = false;
    const int numThreads;
    const int minKey;
    const long long maxKey;
    volatile char padding4[PADDING_BYTES];
    Node<K, V> * root;
    volatile char padding5[PADDING_BYTES];
    RecordManager * const recmgr;
    volatile char padding7[PADDING_BYTES];
    PathContainer paths[MAX_THREADS];
    volatile char padding8[PADDING_BYTES];

public:

    InternalKCAS(const int _numThreads, const int _minKey, const long long _maxKey);

    ~InternalKCAS();

    bool contains(const int tid, const K &key);

    V insertIfAbsent(const int tid, const K &key, const V &value);

    V erase(const int tid, const K &key);

    bool validate();

    void printDebuggingDetails();

    Node<K, V> * getRoot();

    void initThread(const int tid);

    void deinitThread(const int tid);

    int getHeight(Node<K, V> * node);

    RecordManager * const debugGetRecMgr() {
        return recmgr;
    }

private:
    Node<K, V> * createNode(const int tid, Node<K, V> * parent, K key, V value);

    void freeSubtree(const int tid, Node<K, V> * node);

    long validateSubtree(Node<K, V> * node, long smaller, long larger, std::unordered_set<casword_t> &keys, ofstream &graph, ofstream &log, bool &errorFound);

    int internalErase(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, const K &key);

    int internalInsert(const int tid, ObservedNode &parentObserved, const K &key, const V &value);

    int countChildren(const int tid, Node<K, V> * node);

    int getSuccessor(const int tid, Node<K, V> * node, ObservedNode &succObserved, const K &key);

    bool validatePath(const int tid, const int &size, const K &key, ObservedNode path[]);

    int search(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, const K &key);

    int rotateRight(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, ObservedNode &leftChildObserved);

    int rotateLeft(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, ObservedNode &rightChildObserved);

    int rotateLeftRight(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, ObservedNode &leftChildObserved, ObservedNode &leftRigthChildObserved);

    int rotateRightLeft(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, ObservedNode &rightChildObserved, ObservedNode &rightLeftChildObserved);

    void fixHeightAndRebalance(const int tid, Node<K, V> * node);

    int fixHeight(const int tid, ObservedNode &observedNode);
};

template<class RecordManager, typename K, typename V>
Node<K, V> * InternalKCAS<RecordManager, K, V>::createNode(const int tid, Node<K, V> * parent, K key, V value) {
    Node<K, V> * node = recmgr->template allocate<Node<K, V> >(tid);
    //No node, save for root, should have a NULL parent
    assert(!init || parent->key < maxKey);
    node->key.setInitVal(key);
    node->value.setInitVal(value);
    node->parent.setInitVal(parent);
    node->vNumMark.setInitVal(0);
    node->left.setInitVal(NULL);
    node->right.setInitVal(NULL);
    node->height.setInitVal(1);
    return node;
}

template<class RecordManager, typename K, typename V>
InternalKCAS<RecordManager, K, V>::InternalKCAS(const int _numThreads, const int _minKey, const long long _maxKey)
: numThreads(_numThreads), minKey(_minKey), maxKey(_maxKey), recmgr(new RecordManager(numThreads)) {
    assert(_numThreads < MAX_THREADS);
    int tid = 0;
    initThread(tid);
    root = createNode(0, NULL, (maxKey + 1 & 0x00FFFFFFFFFFFFFF), NULL);
    init = true;
}

template<class RecordManager, typename K, typename V>
InternalKCAS<RecordManager, K, V>::~InternalKCAS() {
    int tid = 0;
    initThread(tid);
    freeSubtree(tid, root);
    deinitThread(tid);
    delete recmgr;
}

template<class RecordManager, typename K, typename V>
inline Node<K, V> * InternalKCAS<RecordManager, K, V>::getRoot() {
    return root->left;
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::initThread(const int tid) {
    recmgr->initThread(tid);
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::deinitThread(const int tid) {
    recmgr->deinitThread(tid);
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::getHeight(Node<K, V> * node) {
    return node == NULL ? 0 : node->height;
}

/* getSuccessor(const int tid, Node * node, ObservedNode &succObserved, int key)
 * ### Gets the successor of a given node in it's subtree ###
 * returns the successor of a given node stored within an ObservedNode with the
 * observed version number.
 * Returns an integer, 1 indicating the process was successful, 0 indicating a retry
 */
template<class RecordManager, typename K, typename V>
inline int InternalKCAS<RecordManager, K, V>::getSuccessor(const int tid, Node<K, V> * node, ObservedNode &oSucc, const K &key) {
    auto & path = paths[tid].path;

    while (true) {
        Node<K, V> * succ = node->right;
        path[0].node = node;
        path[0].oVNumMark = node->vNumMark;
        int currSize = 1;

        while (succ != NULL) {
            assert(currSize < MAX_PATH_SIZE - 1);
            path[currSize].node = succ;
            path[currSize].oVNumMark = succ->vNumMark;
            currSize++;
            succ = succ->left;
        }

        if (validatePath(tid, currSize, key, path) && currSize > 1) {
            oSucc = path[currSize - 1];
            return RetCode::SUCCESS;
        } else {
            return RetCode::RETRY;
        }
    }
}

template<class RecordManager, typename K, typename V>
inline bool InternalKCAS<RecordManager, K, V>::contains(const int tid, const K &key) {
    assert(key <= maxKey);
    int result;
    ObservedNode oNode;
    ObservedNode oParent;
    auto guard = recmgr->getGuard(tid);

    while ((result = search(tid, oParent, oNode, key)) == RetCode::RETRY) {
        /* keep trying until we get a result */
    }
    return result == RetCode::SUCCESS;
}

/* search(const int tid, ObservedNode &predObserved, ObservedNode &parentObserved, ObservedNode &nodeObserved, const int &key)
 * A proposed successor-predecessor pair is generated by searching for a given key,
 * if the key is not found, the path is then validated to ensure it was not missed.
 * Where appropriate, the predecessor (predObserved), parent (parentObserved) and node
 * (nodeObserved) are provided to the caller.
 */
template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::search(const int tid, ObservedNode &oParent, ObservedNode &oNode, const K &key) {
    assert(key <= maxKey);

    K currKey;
    casword_t nodeVNumMark;

    ObservedNode * path = paths[tid].path;
    path[0].node = root;
    path[0].oVNumMark = root->vNumMark;

    Node<K, V> * node = root->left;

    int currSize = 1;

    while (true) {
        assert(currSize < MAX_PATH_SIZE - 1);
        //We have hit a terminal node without finding our key, must validate
        if (node == NULL) {
            if (validatePath(tid, currSize, key, path)) {
                oParent = path[currSize - 1];
                return RetCode::FAILURE;
            } else {
                return RetCode::RETRY;
            }
        }

        nodeVNumMark = node->vNumMark;

        currKey = node->key;

        path[currSize].node = node;
        path[currSize].oVNumMark = nodeVNumMark;
        currSize++;

        if (key > currKey) {
            node = node->right;
        } else if (key < currKey) {
            node = node->left;
        }            //no validation required on finding a key
        else {
            oParent = path[currSize - 2];
            oNode = path[currSize - 1];
            return RetCode::SUCCESS;
        }
    }
}

/* validatePath(const int tid, const int size, const int key, ObservedNode path[MAX_PATH_SIZE])
 * ### Validates all nodes in a path such that they are not marked and their version numbers have not changed ###
 * validated a given path, ensuring that all version numbers of observed nodes still match
 * the version numbers stored locally within nodes within the tree. This provides the caller
 * with certainty that there was a time that this path existed in the tree
 * Returns true for a valid path
 * Returns false for an invalid path (some node version number changed)
 */
template<class RecordManager, typename K, typename V>
inline bool InternalKCAS<RecordManager, K, V>::validatePath(const int tid, const int &size, const K &key, ObservedNode path[]) {
    assert(size > 0 && size < MAX_PATH_SIZE);

    for (int i = 0; i < size; i++) {
        ObservedNode oNode = path[i];
        if (oNode.node->vNumMark != oNode.oVNumMark || IS_MARKED(oNode.oVNumMark)) {
            return false;
        }
    }
    return true;
}

template<class RecordManager, typename K, typename V>
inline V InternalKCAS<RecordManager, K, V>::insertIfAbsent(const int tid, const K &key, const V &value) {
    ObservedNode oParent;
    ObservedNode oNode;

    while (true) {
        auto guard = recmgr->getGuard(tid);

        int res;
        while ((res = (search(tid, oParent, oNode, key))) == RetCode::RETRY) {
            /* keep trying until we get a result */
        }

        if (res == RetCode::SUCCESS) {
            return (V)oNode.node->value;
        }

        assert(res == RetCode::FAILURE);
        if (internalInsert(tid, oParent, key, value)) {
            return 0;
        }
    }
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::internalInsert(const int tid, ObservedNode &oParent, const K &key, const V &value) {
    /* INSERT KCAS (K = 2-3)
     * predecessor's version number*:	vNumber	 ->  vNumber + 1
     * parent's version number:		vNumber  ->  vNumber + 1
     * parent's child pointer:		NULL     ->  newNode
     */

    kcas::start();
    Node<K, V> * parent = oParent.node;

    Node<K, V> * newNode = createNode(tid, parent, key, value);

    if (key > parent->key) {
        kcas::add(&parent->right, (Node<K, V> *)NULL, newNode);
    } else if (key < parent->key) {
        kcas::add(&parent->left, (Node<K, V> *)NULL, newNode);
    } else {
        recmgr->deallocate(tid, newNode);
        return RetCode::RETRY;
    }

    kcas::add(&parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2);

    if (kcas::execute()) {
        fixHeightAndRebalance(tid, parent);
        return RetCode::SUCCESS;
    }

    recmgr->deallocate(tid, newNode);

    return RetCode::RETRY;
}

template<class RecordManager, typename K, typename V>
inline V InternalKCAS<RecordManager, K, V>::erase(const int tid, const K &key) {
    ObservedNode oParent;
    ObservedNode oNode;

    while (true) {
        auto guard = recmgr->getGuard(tid);

        int res = 0;
        while ((res = (search(tid, oParent, oNode, key))) == RetCode::RETRY) {
            /* keep trying until we get a result */
        }

        if (res == RetCode::FAILURE) {
            return 0;
        }

        assert(res == RetCode::SUCCESS);
        if ((res = internalErase(tid, oParent, oNode, key))) {
            return (V)oNode.node->value;
        }
    }
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::internalErase(const int tid, ObservedNode &oParent, ObservedNode &oNode, const K &key) {
    Node<K, V> * parent = oParent.node;
    Node<K, V> * node = oNode.node;

    int numChildren = countChildren(tid, node);

    kcas::start();

    if (IS_MARKED(oParent.oVNumMark) || IS_MARKED(oNode.oVNumMark)) {
        return RetCode::RETRY;
    }

    if (numChildren == 0) {
        /* No-Child Delete
         * Unlink node
         */

        if (key > parent->key) {
            kcas::add(&parent->right, node, (Node<K, V> *)NULL);
        } else if (key < parent->key) {
            kcas::add(&parent->left, node, (Node<K, V> *)NULL);
        } else {
            return RetCode::RETRY;
        }

        kcas::add(
            &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2,
            &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 3
        );

        if (kcas::execute()) {
            assert(IS_MARKED(node->vNumMark));
            recmgr->retire(tid, node);
            fixHeightAndRebalance(tid, parent);

            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    } else if (numChildren == 1) {
        /* One-Child Delete
         * Reroute parent pointer around removed node
         */

        Node<K, V> * left = node->left;
        Node<K, V> * right = node->right;
        Node<K, V> * reroute;

        //determine which child will be the replacement
        if (left != NULL) {
            reroute = left;
        } else if (right != NULL) {
            reroute = right;
        } else {
            return RetCode::RETRY;
        }

        casword_t rerouteVNum = reroute->vNumMark;

        if (IS_MARKED(rerouteVNum)) {
            return RetCode::RETRY;
        }

        if (key > parent->key) {
            kcas::add(&parent->right, node, reroute);
        } else if (key < parent->key) {
            kcas::add(&parent->left, node, reroute);
        } else {
            return RetCode::RETRY;
        }

        kcas::add(
            &reroute->parent, node, parent,
            &reroute->vNumMark, rerouteVNum, rerouteVNum + 2,
            &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 3,
            &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2
        );

        if (kcas::execute()) {
            assert(IS_MARKED(node->vNumMark));
            recmgr->retire(tid, node);
            fixHeightAndRebalance(tid, parent);

            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    } else if (numChildren == 2) {
        /* Two-Child Delete
         * Promotion of descendant successor to this node by replacing the key/value pair at the node
         */

        ObservedNode oSucc;

        //the (decendant) successor's key will be promoted
        if (getSuccessor(tid, node, oSucc, key) == RetCode::RETRY) {
            return RetCode::RETRY;
        }

        if (oSucc.node == NULL) {
            return RetCode::RETRY;
        }

        Node<K, V> * succ = oSucc.node;
        Node<K, V> * succParent = succ->parent;

        ObservedNode oSuccParent;
        oSuccParent.node = succParent;
        oSuccParent.oVNumMark = succParent->vNumMark;

        if (oSuccParent.node == NULL) {
            return RetCode::RETRY;
        }

        K succKey = succ->key;

        assert(succKey <= maxKey);

        if (IS_MARKED(oSuccParent.oVNumMark)) {
            return RetCode::RETRY;
        }

        Node<K, V> * succRight = succ->right;

        if (succRight != NULL) {
            casword_t succRightVNum = succRight->vNumMark;

            if (IS_MARKED(succRightVNum)) {
                return RetCode::RETRY;
            }

            kcas::add(
                &succRight->parent, succ, succParent,
                &succRight->vNumMark, succRightVNum, succRightVNum + 2
            );
        }

        if (succParent->right == succ) {
            kcas::add(&succParent->right, succ, succRight);
        } else if (succParent->left == succ) {
            kcas::add(&succParent->left, succ, succRight);
        } else {
            return RetCode::RETRY;
        }

        V nodeVal = node->value;
        V succVal = succ->value;

        kcas::add(
            &node->value, nodeVal, succVal,
            &node->key, key, succKey,
            &succ->vNumMark, oSucc.oVNumMark, oSucc.oVNumMark + 3,
            &succParent->vNumMark, oSuccParent.oVNumMark, oSuccParent.oVNumMark + 2
        );

        if (succParent != node) {
            kcas::add(&node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2);
        }

        if (kcas::execute()) {
            assert(IS_MARKED(succ->vNumMark));
            recmgr->retire(tid, succ);
            //successor's parent is the only node that's height will have been impacted
            fixHeightAndRebalance(tid, succParent);
            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    }
    assert(false);
    return RetCode::RETRY;
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::fixHeightAndRebalance(const int tid, Node<K, V> * node) {
    ObservedNode oNode;
    ObservedNode oParent;

    int propRes;

    while (node != root) {
        ObservedNode oRight;
        ObservedNode oLeft;
        ObservedNode oRightLeft;
        ObservedNode oLeftRight;
        ObservedNode oRightRight;
        ObservedNode oLeftLeft;

        oNode.node = node;
        oNode.oVNumMark = node->vNumMark;

        oParent.node = node->parent;
        oParent.oVNumMark = oParent.node->vNumMark;

        if (IS_MARKED(oNode.oVNumMark)) {
            return;
        }

        Node<K, V> * left = node->left;
        if (left != NULL) {
            oLeft.node = left;
            oLeft.oVNumMark = left->vNumMark;
        }

        Node<K, V> * right = node->right;
        if (right != NULL) {
            oRight.node = right;
            oRight.oVNumMark = right->vNumMark;
        }

        int localBalance = getHeight(left) - getHeight(right);

        if (localBalance >= 2) {
            if (left == NULL || IS_MARKED(oLeft.oVNumMark)) {
                continue;
            }

            Node<K, V> * leftRight = left->right;
            Node<K, V> * leftLeft = left->left;

            if (leftRight != NULL) {
                oLeftRight.node = leftRight;
                oLeftRight.oVNumMark = leftRight->vNumMark;
            }

            if (leftLeft != NULL) {
                oLeftLeft.node = leftLeft;
                oLeftLeft.oVNumMark = leftLeft->vNumMark;
            }

            int leftBalance = getHeight(leftLeft) - getHeight(leftRight);

            if (leftBalance < 0) {
                if (leftRight == NULL) {
                    continue;
                }
                if (rotateLeftRight(tid, oParent, oNode, oLeft, oLeftRight)) {
                    //node is now the lowest on the tree, so it must be rebalanced first cannot simply loop...
                    fixHeightAndRebalance(tid, node);
                    fixHeightAndRebalance(tid, left);
                    fixHeightAndRebalance(tid, leftRight);
                    node = oParent.node;
                }
            } else {
                if (rotateRight(tid, oParent, oNode, oLeft) == RetCode::SUCCESS) {
                    fixHeightAndRebalance(tid, node);
                    fixHeightAndRebalance(tid, left);
                    node = oParent.node;
                }
            }
        } else if (localBalance <= -2) {
            if (right == NULL || IS_MARKED(oRight.oVNumMark)) {
                continue;
            }

            Node<K, V> * rightLeft = right->left;
            Node<K, V> * rightRight = right->right;

            if (rightLeft != NULL) {
                oRightLeft.node = rightLeft;
                oRightLeft.oVNumMark = rightLeft->vNumMark;
            }

            if (rightRight != NULL) {
                oRightRight.node = rightRight;
                oRightRight.oVNumMark = rightRight->vNumMark;
            }

            int rightBalance = getHeight(rightLeft) - getHeight(rightRight);

            if (rightBalance > 0) {
                if (rightLeft == NULL) {
                    continue;
                }

                if (rotateRightLeft(tid, oParent, oNode, oRight, oRightLeft)) {
                    fixHeightAndRebalance(tid, node);
                    fixHeightAndRebalance(tid, right);
                    fixHeightAndRebalance(tid, rightLeft);
                    node = oParent.node;
                }
            } else {
                if (rotateLeft(tid, oParent, oNode, oRight) == RetCode::SUCCESS) {
                    fixHeightAndRebalance(tid, node);
                    fixHeightAndRebalance(tid, right);
                    node = oParent.node;
                }
            }
        } else {
            //no rebalance occurred? check if the height is still ok
            if ((propRes = fixHeight(tid, oNode)) == RetCode::FAILURE) {
                continue;
            } else if (propRes == RetCode::SUCCESS_WITH_HEIGHT_UPDATE) {
                node = node->parent;
            }
            else {
                return;
            }
        }
    }
    return;
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::fixHeight(const int tid, ObservedNode &oNode) {

    Node<K, V> * node = oNode.node;
    Node<K, V> * left = node->left;
    Node<K, V> * right = node->right;

    casword_t leftOVNumMark;
    casword_t rightOVNumMark;

    kcas::start();

    if (left != NULL) {
        leftOVNumMark = left->vNumMark;
        kcas::add(&left->vNumMark, leftOVNumMark, leftOVNumMark);
    }

    if (right != NULL) {
        rightOVNumMark = right->vNumMark;
        kcas::add(&right->vNumMark, rightOVNumMark, rightOVNumMark);
    }

    int oldHeight = node->height;

    int newHeight = 1 + max(getHeight(left), getHeight(right));

    //Check if rebalance is actually necessary
    if (oldHeight == newHeight) {
        if (node->vNumMark == oNode.oVNumMark
                && (left == NULL || left->vNumMark == leftOVNumMark)
                && (right == NULL || right->vNumMark == rightOVNumMark)) {
            return RetCode::UNNECCESSARY;
        } else {
            return RetCode::FAILURE;
        }
    }

    kcas::add(
        &node->height, oldHeight, newHeight,
        &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2
    );

    if (kcas::execute()) {
        return RetCode::SUCCESS_WITH_HEIGHT_UPDATE;
    }

    return RetCode::FAILURE;
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::rotateRight(const int tid, ObservedNode &oParent, ObservedNode &oNode, ObservedNode &oLeft) {
    Node<K, V> * parent = oParent.node;
    Node<K, V> * node = oNode.node;
    Node<K, V> * left = oLeft.node;

    kcas::start();

    /***Pointers to Parents and Children***/
    //could fail fast here, should consider
    if (parent->right == node) {
        kcas::add(&parent->right, node, left);
    } else if (parent->left == node) {
        kcas::add(&parent->left, node, left);
    } else {
        return RetCode::FAILURE;
    }

    Node<K, V> * leftRight = left->right;
    if (leftRight != NULL) {
        casword_t leftRightOVNumMark = leftRight->vNumMark;
        kcas::add(
            &leftRight->parent, left, node,
            &leftRight->vNumMark, leftRightOVNumMark, leftRightOVNumMark + 2
        );
    }

    Node<K, V> * leftLeft = left->left;
    if (leftLeft != NULL) {
        casword_t leftLeftOVNumMark = leftLeft->vNumMark;
        kcas::add(&leftLeft->vNumMark, leftLeftOVNumMark, leftLeftOVNumMark);
    }

    Node<K, V> * right = node->right;
    if (right != NULL) {
        casword_t rightOVNumMark = right->vNumMark;
        kcas::add(&right->vNumMark, rightOVNumMark, rightOVNumMark);
    }

    int oldNodeHeight = node->height;
    int oldLeftHeight = left->height;

    int newNodeHeight = 1 + max(getHeight(leftRight), getHeight(right));
    int newLeftHeight = 1 + max(getHeight(leftLeft), newNodeHeight);

    kcas::add(
        &left->parent, node, parent,
        &node->left, left, leftRight,
        &left->right, leftRight, node,
        &node->parent, parent, left,
        &node->height, oldNodeHeight, newNodeHeight,
        &left->height, oldLeftHeight, newLeftHeight,
        &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2,
        &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2,
        &left->vNumMark, oLeft.oVNumMark, oLeft.oVNumMark + 2
    );

    if (kcas::execute()) return RetCode::SUCCESS;
    return RetCode::FAILURE;
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::rotateLeft(const int tid, ObservedNode &oParent, ObservedNode &oNode, ObservedNode &oRight) {
    Node<K, V> * parent = oParent.node;
    Node<K, V> * node = oNode.node;
    Node<K, V> * right = oRight.node;

    kcas::start();

    /***Pointers to Parents and Children***/
    //could fail fast here, should consider
    if (parent->right == node) {
        kcas::add(&parent->right, node, right);
    } else if (parent->left == node) {
        kcas::add(&parent->left, node, right);
    } else {
        return RetCode::FAILURE;
    }

    Node<K, V> * rightLeft = right->left;
    if (rightLeft != NULL) {
        casword_t rightLeftOVNumMark = rightLeft->vNumMark;
        kcas::add(
            &rightLeft->parent, right, node,
            &rightLeft->vNumMark, rightLeftOVNumMark, rightLeftOVNumMark + 2
        );
    }

    Node<K, V> * rightRight = right->right;
    if (rightRight != NULL) {
        casword_t rightRightOVNumMark = rightRight->vNumMark;
        kcas::add(&rightRight->vNumMark, rightRightOVNumMark, rightRightOVNumMark);
    }

    Node<K, V> * left = node->left;
    if (left != NULL) {
        casword_t leftOVNumMark = left->vNumMark;
        kcas::add(&left->vNumMark, leftOVNumMark, leftOVNumMark);
    }

    int oldNodeHeight = node->height;
    int oldRightHeight = right->height;

    int newNodeHeight = 1 + max(getHeight(left), getHeight(rightLeft));
    int newRightHeight = 1 + max(newNodeHeight, getHeight(rightRight));


    kcas::add(
        &right->parent, node, parent,
        &node->right, right, rightLeft,
        &right->left, rightLeft, node,
        &node->parent, parent, right,
        &node->height, oldNodeHeight, newNodeHeight,
        &right->height, oldRightHeight, newRightHeight,
        &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2,
        &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2,
        &right->vNumMark, oRight.oVNumMark, oRight.oVNumMark + 2
    );

    if (kcas::execute()) return RetCode::SUCCESS;
    return RetCode::FAILURE;
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::rotateLeftRight(const int tid, ObservedNode &oParent, ObservedNode &oNode, ObservedNode &oLeft, ObservedNode &oLeftRight) {
    Node<K, V> * parent = oParent.node;
    Node<K, V> * node = oNode.node;
    Node<K, V> * left = oLeft.node;
    Node<K, V> * leftRight = oLeftRight.node;

    kcas::start();

    /***Pointers to Parents and Children***/
    //could fail fast here, should consider
    if (parent->right == node) {
        kcas::add(&parent->right, node, leftRight);
    } else if (parent->left == node) {
        kcas::add(&parent->left, node, leftRight);
    } else {
        return RetCode::FAILURE;
    }

    Node<K, V> * leftRightLeft = leftRight->left;
    if (leftRightLeft != NULL) {
        casword_t leftRightLeftOVNumMark = leftRightLeft->vNumMark;
        kcas::add(
            &leftRightLeft->parent, leftRight, left,
            &leftRightLeft->vNumMark, leftRightLeftOVNumMark, leftRightLeftOVNumMark + 2
        );
    }

    Node<K, V> * leftRightRight = leftRight->right;
    if (leftRightRight != NULL) {
        casword_t leftRightRightOVNumMark = leftRightRight->vNumMark;
        kcas::add(
            &leftRightRight->parent, leftRight, node,
            &leftRightRight->vNumMark, leftRightRightOVNumMark, leftRightRightOVNumMark + 2
        );
    }

    Node<K, V> * right = node->right;
    if (right != NULL) {
        casword_t rightOVNumMark = right->vNumMark;
        kcas::add(&right->vNumMark, rightOVNumMark, rightOVNumMark);
    }

    Node<K, V> * leftLeft = left->left;
    if (leftLeft != NULL) {
        casword_t leftLeftOVNumMark = leftLeft->vNumMark;
        kcas::add(&leftLeft->vNumMark, leftLeftOVNumMark, leftLeftOVNumMark);
    }

    int oldNodeHeight = node->height;
    int oldLeftHeight = left->height;
    int oldLeftRightHeight = leftRight->height;

    int newNodeHeight = 1 + max(getHeight(leftRightRight), getHeight(right));
    int newLeftHeight = 1 + max(getHeight(leftLeft), getHeight(leftRightLeft));
    int newLeftRightHeight = 1 + max(newNodeHeight, newLeftHeight);

    kcas::add(
        &leftRight->parent, left, parent,
        &leftRight->left, leftRightLeft, left,
        &left->parent, node, leftRight,
        &leftRight->right, leftRightRight, node,
        &node->parent, parent, leftRight,
        &left->right, leftRight, leftRightLeft,
        &node->left, left, leftRightRight,
        &node->height, oldNodeHeight, newNodeHeight,
        &left->height, oldLeftHeight, newLeftHeight,
        &leftRight->height, oldLeftRightHeight, newLeftRightHeight,
        &leftRight->vNumMark, oLeftRight.oVNumMark, oLeftRight.oVNumMark + 2,
        &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2,
        &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2,
        &left->vNumMark, oLeft.oVNumMark, oLeft.oVNumMark + 2
    );

    if (kcas::execute()) return RetCode::SUCCESS;
    return RetCode::FAILURE;
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::rotateRightLeft(const int tid, ObservedNode &oParent, ObservedNode &oNode, ObservedNode &oRight, ObservedNode &oRightLeft) {
    Node<K, V> * parent = oParent.node;
    Node<K, V> * node = oNode.node;
    Node<K, V> * right = oRight.node;
    Node<K, V> * rightLeft = oRightLeft.node;

    kcas::start();

    if (parent->right == node) {
        kcas::add(&parent->right, node, rightLeft);
    } else if (parent->left == node) {
        kcas::add(&parent->left, node, rightLeft);
    } else {
        return RetCode::FAILURE;
    }

    Node<K, V> * rightLeftRight = rightLeft->right;
    if (rightLeftRight != NULL) {
        casword_t rightLeftRightOVNumMark = rightLeftRight->vNumMark;

        if (IS_MARKED(rightLeftRightOVNumMark)) return RetCode::FAILURE;

        kcas::add(
            &rightLeftRight->parent, rightLeft, right,
            &rightLeftRight->vNumMark, rightLeftRightOVNumMark, rightLeftRightOVNumMark + 2
        );
    }

    Node<K, V> * rightLeftLeft = rightLeft->left;
    if (rightLeftLeft != NULL) {
        casword_t rightLeftLeftOVNumMark = rightLeftLeft->vNumMark;

        if (IS_MARKED(rightLeftLeftOVNumMark)) return RetCode::FAILURE;

        kcas::add(
            &rightLeftLeft->parent, rightLeft, node,
            &rightLeftLeft->vNumMark, rightLeftLeftOVNumMark, rightLeftLeftOVNumMark + 2
        );
    }

    Node<K, V> * left = node->left;
    if (left != NULL) {
        casword_t leftOVNumMark = left->vNumMark;
        kcas::add(&left->vNumMark, leftOVNumMark, leftOVNumMark);
    }

    Node<K, V> * rightRight = right->right;
    if (rightRight != NULL) {
        casword_t rightRightOVNumMark = rightRight->vNumMark;
        kcas::add(&rightRight->vNumMark, rightRightOVNumMark, rightRightOVNumMark);
    }

    int oldNodeHeight = node->height;
    int oldRightHeight = right->height;
    int oldRightLeftHeight = rightLeft->height;

    int newNodeHeight = 1 + max(getHeight(rightLeftLeft), getHeight(left));
    int newRightHeight = 1 + max(getHeight(rightRight), getHeight(rightLeftRight));
    int newRightLeftHeight = 1 + max(newNodeHeight, newRightHeight);

    kcas::add(
        &rightLeft->parent, right, parent,
        &rightLeft->right, rightLeftRight, right,
        &right->parent, node, rightLeft,
        &rightLeft->left, rightLeftLeft, node,
        &node->parent, parent, rightLeft,
        &right->left, rightLeft, rightLeftRight,
        &node->right, right, rightLeftLeft,
        &node->height, oldNodeHeight, newNodeHeight,
        &right->height, oldRightHeight, newRightHeight,
        &rightLeft->height, oldRightLeftHeight, newRightLeftHeight,
        &rightLeft->vNumMark, oRightLeft.oVNumMark, oRightLeft.oVNumMark + 2,
        &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2,
        &node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2,
        &right->vNumMark, oRight.oVNumMark, oRight.oVNumMark + 2
    );


    if (kcas::execute()) return RetCode::SUCCESS;
    return RetCode::FAILURE;
}

template<class RecordManager, typename K, typename V>
inline int InternalKCAS<RecordManager, K, V>::countChildren(const int tid, Node<K, V> * node) {
    return (node->left == NULL ? 0 : 1) +
            (node->right == NULL ? 0 : 1);
}

template<class RecordManager, typename K, typename V>
long InternalKCAS<RecordManager, K, V>::validateSubtree(Node<K, V> * node, long smaller, long larger, std::unordered_set<casword_t> &keys, ofstream &graph, ofstream &log, bool &errorFound) {

    if (node == NULL) return 0;
    graph << "\"" << node << "\"" << "[label=\"K: " << node->key << " - H: "
            << node->height << "\"];\n";

    if (IS_MARKED(node->vNumMark)) {
        log << "MARKED NODE! " << node->key << "\n";
        errorFound = true;
    }
    Node<K, V> * nodeLeft = node->left;
    Node<K, V> * nodeRight = node->right;

    if (nodeLeft != NULL) {
        graph << "\"" << node << "\" -> \"" << nodeLeft << "\"";
        if (node->key < nodeLeft->key) {
            assert(false);
            graph << "[color=red]";
        } else {
            graph << "[color=blue]";
        }

        graph << ";\n";
    }

    if (nodeRight != NULL) {
        graph << "\"" << node << "\" -> \"" << nodeRight << "\"";
        if (node->key > nodeRight->key) {
            assert(false);
            graph << "[color=red]";
        } else {
            graph << "[color=green]";
        }
        graph << ";\n";
    }


    Node<K, V> * parent = node->parent;
    graph << "\"" << node << "\" -> \"" << parent << "\"" "[color=grey];\n";
    casword_t height = node->height;

    if (!(keys.count(node->key) == 0)) {
        log << "DUPLICATE KEY! " << node->key << "\n";
        errorFound = true;
    }

    if (!((nodeLeft == NULL || nodeLeft->parent == node) &&
            (nodeRight == NULL || nodeRight->parent == node))) {
        log << "IMPROPER PARENT! " << node->key << "\n";
        errorFound = true;
    }

    if ((node->key < smaller) || (node->key > larger)) {
        log << "IMPROPER LOCAL TREE! " << node->key << "\n";
        errorFound = true;
    }

    if (nodeLeft == NULL && nodeRight == NULL && getHeight(node) > 1) {
        log << "Leaf with height > 1! " << node->key << "\n";
        errorFound = true;
    }

    keys.insert(node->key);

    long lHeight = validateSubtree(node->left, smaller, node->key, keys, graph, log, errorFound);
    long rHeight = validateSubtree(node->right, node->key, larger, keys, graph, log, errorFound);

    long ret = 1 + max(lHeight, rHeight);

    if (node->height != ret) {
        log << "Node " << node->key << " with height " << ret << " thinks it has height " << node->height << "\n";
        errorFound = true;
    }

    if (abs(lHeight - rHeight) > 1) {
        log << "Imbalanced Node! " << node->key << "(" << lHeight << ", " << rHeight << ") - " << node->height << "\n";
        errorFound = true;
    }

    return ret;
}

template<class RecordManager, typename K, typename V>
bool InternalKCAS<RecordManager, K, V>::validate() {
    std::unordered_set<casword_t> keys = {};
    bool errorFound;

    rename("graph.dot", "graph_before.dot");
    ofstream graph;
    graph.open("graph.dot");
    graph << "digraph G {\n";

    ofstream log;
    log.open("log.txt", std::ofstream::out | std::ofstream::app);

    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    log << "Run at: " << std::put_time(&tm, "%d-%m-%Y %H-%M-%S") << "\n";

    long ret = validateSubtree(root->left, minKey, maxKey, keys, graph, log, errorFound);
    graph << "}";
    graph.close();

    if (!errorFound) {
        log << "Validated Successfully!\n";
    }

    log.close();

    return !errorFound;
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::printDebuggingDetails() {
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::freeSubtree(const int tid, Node<K, V> * node) {
    if (node == NULL) return;
    freeSubtree(tid, node->left);
    freeSubtree(tid, node->right);
    recmgr->deallocate(tid, node);
}
