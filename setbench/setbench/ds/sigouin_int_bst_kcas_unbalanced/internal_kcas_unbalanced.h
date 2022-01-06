#pragma once

#include <cassert>
#define MAX_KCAS 16
#include "kcas.h"

#include <unordered_set>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <ctime>

using namespace std;

#define MAX_THREADS 200
#define MAX_PATH_SIZE 128
#define PADDING_BYTES 128
#define KCAS_MAX_K 16

#define IS_MARKED(word) (word & 0x1)

template<typename K, typename V>
struct Node {
    casword<K> key;
    casword<casword_t> vNumMark;
    casword<Node<K, V> *> left;
    casword<Node<K, V> *> right;
    casword<V> value;
};

enum RetCode : int {
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

    RecordManager * const debugGetRecMgr() {
        return recmgr;
    }

private:
    Node<K, V> * createNode(const int tid, K key, V value);

    void freeSubtree(const int tid, Node<K, V> * node);

    long validateSubtree(Node<K, V> * node, long smaller, long larger, std::unordered_set<casword_t> &keys, ofstream &graph, ofstream &log, bool &errorFound);

    int internalErase(const int tid, ObservedNode &parentObserved, ObservedNode &nodeObserved, const K &key);

    int internalInsert(const int tid, ObservedNode &predecessorObserved, ObservedNode &parentObserved, const K &key, const V &value);

    int countChildren(const int tid, Node<K, V> * node);

    int getSuccessor(const int tid, Node<K, V> * node, ObservedNode &succObserved, ObservedNode &oSuccParent, const K &key);

    bool validatePath(const int tid, const int &size, const K &key, ObservedNode path[]);

    int search(const int tid, ObservedNode &predObserved, ObservedNode &parentObserved, ObservedNode &nodeObserved, const K &key);
};

template<class RecordManager, typename K, typename V>
Node<K, V> * InternalKCAS<RecordManager, K, V>::createNode(const int tid, K key, V value) {
    Node<K, V> * node = recmgr->template allocate<Node<K, V> >(tid);
    //No node, save for root, should have a NULL parent
    node->key.setInitVal(key);
    node->value.setInitVal(value);
    node->vNumMark.setInitVal(0);
    node->left.setInitVal(NULL);
    node->right.setInitVal(NULL);
    return node;
}

template<class RecordManager, typename K, typename V>
InternalKCAS<RecordManager, K, V>::InternalKCAS(const int _numThreads, const int _minKey, const long long _maxKey)
        : numThreads(_numThreads), minKey(_minKey), maxKey(_maxKey), recmgr(new RecordManager(_numThreads)) {

    root = createNode(0, (maxKey + 1 & 0x00FFFFFFFFFFFFFF), NULL);
    init = true;
}

template<class RecordManager, typename K, typename V>
InternalKCAS<RecordManager, K, V>::~InternalKCAS() {
    // auto guard = recmgr->getGuard(tid);
    freeSubtree(0, root);
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

/* getSuccessor(const int tid, Node * node, ObservedNode &succObserved, int key)
 * ### Gets the successor of a given node in it's subtree ###
 * returns the successor of a given node stored within an ObservedNode with the
 * observed version number.
 * Returns an integer, 1 indicating the process was successful, 0 indicating a retry
 */
template<class RecordManager, typename K, typename V>
inline int InternalKCAS<RecordManager, K, V>::getSuccessor(const int tid, Node<K, V> * node, ObservedNode &oSucc, ObservedNode &oSuccParent, const K &key) {
    ObservedNode path[MAX_PATH_SIZE];

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

	if(currSize < 2){
	    return RetCode::RETRY;
	}

	oSuccParent = path[currSize - 2];
	oSucc = path[currSize - 1];
        if (IS_MARKED(oSuccParent.oVNumMark) || IS_MARKED(oSucc.oVNumMark)) {
            return RetCode::RETRY;
        } else {
	    return RetCode::SUCCESS;
        }
    }
}

template<class RecordManager, typename K, typename V>
inline bool InternalKCAS<RecordManager, K, V>::contains(const int tid, const K &key) {
    assert(key <= maxKey);
    int result;
    ObservedNode oNode;
    ObservedNode oParent;
    ObservedNode oPred;

    while ((result = search(tid, oPred, oParent, oNode, key)) == RetCode::RETRY) {/* keep trying until we get a result */}
    return result == RetCode::SUCCESS;
}

/* search(const int tid, ObservedNode &predObserved, ObservedNode &parentObserved, ObservedNode &nodeObserved, const int &key)
 * A proposed successor-predecessor pair is generated by searching for a given key,
 * if the key is not found, the path is then validated to ensure it was not missed.
 * Where appropriate, the predecessor (predObserved), parent (parentObserved) and node
 * (nodeObserved) are provided to the caller.
 */
template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::search(const int tid, ObservedNode &oPred, ObservedNode &oParent, ObservedNode &oNode, const K &key) {
    assert(key <= maxKey);

    K currKey;
    casword_t nodeVNumMark;

    ObservedNode * path = paths[tid].path;
    path[0].node = root;
    path[0].oVNumMark = root->vNumMark;

    Node<K, V> * node = root->left;

    ObservedNode * oPredPtr = NULL;
    ObservedNode * oSuccPtr = &path[0];

    int currSize = 1;

    while (true) {
        assert(currSize < MAX_PATH_SIZE - 1);
	//We have hit a terminal node without finding our key, must validate
        if (node == NULL) {
            if (oPredPtr != NULL) {
		oPred = *oPredPtr;

		//The path could be valid, but we could be in the wrong sub-tree
                if (key <= oPredPtr->node->key || key >= oSuccPtr->node->key) {
		    return RetCode::RETRY;
                }
	    //The path could be valid, but we could be in the wrong sub-tree
            } else if (key >= oSuccPtr->node->key) {
		return RetCode::RETRY;
            }

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


	if(key > currKey){
	    node = node->right;
	    oPredPtr = &path[currSize - 1];
	}
	else if(key < currKey){
	    node = node->left;
	    oSuccPtr = &path[currSize - 1];
	}
	//no validation required on finding a key
	else {
	    if(oPredPtr != NULL){
		oPred = *oPredPtr;
	    }

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
inline bool InternalKCAS<RecordManager, K, V>::validatePath(const int tid, const int &size, const K &key, ObservedNode path[MAX_PATH_SIZE]) {
    assert(size > 0);

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
    ObservedNode oPred;
    auto guard = recmgr->getGuard(tid);

    while (true) {

        int res;
        while ((res = (search(tid, oPred, oParent, oNode, key))) == RetCode::RETRY) {/* keep trying until we get a result */}

        if (res == RetCode::SUCCESS) {
            return (V)oNode.node->value;
        }

        assert(res == RetCode::FAILURE);
        if (internalInsert(tid, oPred, oParent, key, value)) {
            return 0;
        }
    }
}

template<class RecordManager, typename K, typename V>
int InternalKCAS<RecordManager, K, V>::internalInsert(const int tid, ObservedNode &oPred, ObservedNode &oParent, const K &key, const V &value) {
    /* INSERT KCAS (K = 2-3)
     * predecessor's version number*:	vNumber	 ->  vNumber + 1
     * parent's version number:		vNumber  ->  vNumber + 1
     * parent's child pointer:		NULL     ->  newNode
     */

    kcas::start();
    Node<K, V> * parent = oParent.node;

    Node<K, V> * pred = oPred.node;

    if (pred != NULL) {
        if (pred->key == key) {
            return RetCode::RETRY;
        } else if (pred != parent) {
            kcas::add(
		&pred->vNumMark, oPred.oVNumMark, oPred.oVNumMark
	    );
        }
    }

    Node<K, V> * newNode = createNode(tid, key, value);

    if (key > parent->key) {
	kcas::add(&parent->right, (Node<K, V> *)NULL, newNode);
    } else if(key < parent->key) {
	kcas::add(&parent->left, (Node<K, V> *)NULL, newNode);
    } else {
	return RetCode::RETRY;
    }

    kcas::add(&parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2);

    if (kcas::execute()) {
        return RetCode::SUCCESS;
    }

    recmgr->retire(tid, newNode);

    return RetCode::RETRY;
}

template<class RecordManager, typename K, typename V>
inline V InternalKCAS<RecordManager, K, V>::erase(const int tid, const K &key) {
    ObservedNode oPred;
    ObservedNode oParent;
    ObservedNode oNode;
    auto guard = recmgr->getGuard(tid);

    while (true) {
        int res = 0;
        while ((res = (search(tid, oPred, oParent, oNode, key))) == RetCode::RETRY) {/* keep trying until we get a result */}

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

    if(IS_MARKED(oParent.oVNumMark) || IS_MARKED(oNode.oVNumMark)){
	return RetCode::RETRY;
    }

    if (numChildren == 0) {
	/* ERASE KCAS - NO CHILDREN (K = 4)
	 * node's mark:			false    ->  true
	 * parent's child pointer:	node     ->  NULL
	 * parent's version number:	vNumber  ->  vNumber + 1
	 * node's version number:	vNumber  ->  vNumber + 1
	 */

        if (key > parent->key) {
            kcas::add(&parent->right, node, (Node<K, V> *)NULL);
        } else if(key < parent->key) {
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

            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    } else if (numChildren == 1) {
	/* ERASE KCAS - 1 CHILD (K = 6)
	 * reroute child's version number:  vNumber  ->  vNumber + 1
	 * reroute child's parent:	    node    ->	parent
	 * node's mark:			    false    ->  true
	 * parent's child pointer:	    node     ->  reroute child
	 * parent's version number:	    vNumber  ->  vNumber + 1
	 * node's version number:	    vNumber  ->  vNumber + 1
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
        } else if(key < parent->key) {
            kcas::add(&parent->left, node, reroute);
        }
	else {
            return RetCode::RETRY;
	}

        kcas::add(
	    &reroute->vNumMark,rerouteVNum, rerouteVNum + 2,
	    &node->vNumMark,oNode.oVNumMark, oNode.oVNumMark + 3,
	    &parent->vNumMark, oParent.oVNumMark, oParent.oVNumMark + 2
	);

        if (kcas::execute()) {
	    assert(IS_MARKED(node->vNumMark));
	    recmgr->retire(tid, node);

            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    } else if (numChildren == 2) {
	/* ERASE KCAS - 2 CHILD (K = 6 or 8)
	 * successor's right child version number*:	vNumber  ->  vNumber + 1
	 * successor's right child parent*:		node -> parent
	 * node's key					key -> successor's key
	 * successor's parent's child pointer:		successor -> successor's right child
	 * successor's mark:				false -> true
	 * successor's version number:			vNumber -> vNumber + 1
	 * successor's parent's version number:		vNumber -> vNumber + 1
	 * node's version number:			vNumber -> vNumber + 1
	 * node's value:				value	-> succ's value
	 */

	ObservedNode oSucc;
        ObservedNode oSuccParent;

	//the (decendant) successor's key will be promoted
        if (getSuccessor(tid, node, oSucc, oSuccParent, key) == RetCode::RETRY) {
            return RetCode::RETRY;
        }

        if (oSucc.node == NULL)  {
            return RetCode::RETRY;
        }

        Node<K, V> * succ = oSucc.node;
        Node<K, V> * succParent = oSuccParent.node;

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
	    &succ->vNumMark,oSucc.oVNumMark, oSucc.oVNumMark + 3,
	    &succParent->vNumMark, oSuccParent.oVNumMark, oSuccParent.oVNumMark + 2
	);

	if(succParent != node){
	    kcas::add(
		&node->vNumMark, oNode.oVNumMark, oNode.oVNumMark + 2
	    );
	}

        if (kcas::execute()) {
	    assert(IS_MARKED(succ->vNumMark));
	    recmgr->retire(tid, succ);
            return RetCode::SUCCESS;
        }

        return RetCode::RETRY;
    }
    assert(false);
    return RetCode::RETRY;
}

template<class RecordManager, typename K, typename V>
inline int InternalKCAS<RecordManager, K, V>::countChildren(const int tid, Node<K, V> * node) {
    return (node->left == NULL ? 0 : 1) +
           (node->right == NULL ? 0 : 1);
}

template<class RecordManager, typename K, typename V>
long InternalKCAS<RecordManager, K, V>::validateSubtree(Node<K, V> * node, long smaller, long larger, std::unordered_set<casword_t> &keys, ofstream &graph, ofstream &log, bool &errorFound) {

    if (node == NULL) return 0;
    graph << "\"" << node << "\"" << "[label=\"K: " << node->key << "\"];\n";

    if(IS_MARKED(node->vNumMark)) {
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

    if (!(keys.count(node->key) == 0)) {
        log << "DUPLICATE KEY! " << node->key << "\n";
        errorFound = true;
    }

    if ((node->key < smaller) || (node->key > larger)) {
        log << "IMPROPER LOCAL TREE! " << node->key << "\n";
        errorFound = true;
    }

    keys.insert(node->key);
    long ret = 1 + max(
           validateSubtree(node->left, smaller,node->key, keys, graph, log, errorFound),
           validateSubtree(node->right, node->key, larger, keys, graph, log, errorFound));

    return ret;
}

template<class RecordManager, typename K, typename V>
bool InternalKCAS<RecordManager, K, V>::validate() {
    std::unordered_set<casword_t> keys = {};
    bool errorFound = false;

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

    if(!errorFound){
	log << "Validated Successfully!\n";
    }

    log.close();

    return !errorFound;
}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::printDebuggingDetails() {}

template<class RecordManager, typename K, typename V>
void InternalKCAS<RecordManager, K, V>::freeSubtree(const int tid, Node<K, V> * node) {
    if (node == NULL) return;
    freeSubtree(tid, node->left);
    freeSubtree(tid, node->right);
    recmgr->retire(tid,node);
}