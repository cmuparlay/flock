/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   avl_tree.h
 * Author: ginns
 *
 * Created on March 18, 2021, 5:10 PM
 */

#ifndef AVL_TREE_H
#define AVL_TREE_H

#include <algorithm>    /* for max */
#include <queue>        /* used in BFS */
#include <iostream>
#include "interfaces.h"

#define INVALID_KEY     0

/**
 * Sequential AVL Tree 
 * Heavily based on the implementation found in the original CA Tree source code
 * http://www.it.uu.se/research/group/languages/software/ca_tree/ca_tree_range_ops
 */
using namespace std;

template <typename K, typename V>
class AVLTree : public IOSet {
private:
    struct Node {
        K key;
        Node * left;
        Node * right;
        Node * parent;
        int balance;
        V val;
        
        Node(K _key, V _val, Node * _parent, Node * _left, Node * _right) :
            key(_key), val(_val), parent(_parent), left(_left), right(_right), balance(0) {
            
        }
    };
    
    Node * root;
    const V NO_VALUE = (V) 0;
    
    /* AVL Tree maintenance and balancing methods */
    int computeHeight();
    pair<K,V> minKey();
    pair<K,V> maxKey();
    Node * getAVLNode(const K & key);
    void rotateLeft(Node * prevNode);
    void rotateRight(Node * prevNode);
    void rotateDoubleRight(Node * prevNode);
    void rotateDoubleLeft(Node * prevNode);
    bool replaceWithRightmost(Node * toReplaceInNode);
    bool deleteBalanceLeft(Node * currentNode);
    bool deleteBalanceRight(Node * currentNode);
    
    
    /* Debugging methods */
    void printInOrderTraversal(Node * node);
    bool doesAVLHold(Node * node);
    size_t numKeysHelper(Node * node);
    size_t sumOfKeysHelper(Node * node);
    void printBFSOrder(Node * node);
    
    /* Memory reclamation */
    void freeTraversal(Node * node);
    
public:
   
    AVLTree();
    ~AVLTree();
    
    /* Dictionary operations  */
    virtual V find(const int tid, const K & key);
    virtual V insert(const int tid, const K & key, const V& val);
    virtual V erase(const int tid, const K & key);
    
    /* Set operations */
    IOSet * join(const int tid, IOSet * rightTree);
    std::tuple<K, IOSet *, IOSet *> split(const int tid);
    
    /* Useful methods */
    bool isEmpty();
    
    /* Debugging methods */
    size_t numKeys();
    size_t sumOfKeys();
    void printInOrderTraversal();
    void printBFSOrder();
    bool checkAVL( );
    
};

template <typename K, typename V>
int AVLTree<K, V>::computeHeight() {
    if (root == NULL) {
        return 0;
    }
    else {
        Node * currentNode = root;
        int heightSoFar = 1;
        while (currentNode->left != NULL || currentNode->right != NULL) {
            if (currentNode->balance == -1) {
                currentNode = currentNode->left;
            }
            else {
                currentNode = currentNode->right;
            }
            heightSoFar++;
        }
        return heightSoFar;
    }
}

template <typename K, typename V>
pair<K,V> AVLTree<K, V>::minKey() {
    Node * currentNode = root;
    while (currentNode->left != NULL) {
        currentNode = currentNode->left;
    }
    if ( currentNode == NULL ) {
        return make_pair(INVALID_KEY, NO_VALUE);
    }
    else {
        return make_pair(currentNode->key, currentNode->val);
    }
}

template <typename K, typename V>
pair<K,V> AVLTree<K, V>::maxKey() {
    Node * currentNode = root;
    while (currentNode->right != NULL) {
        currentNode = currentNode->right;
    }
    if ( currentNode == NULL ) {
        return make_pair(INVALID_KEY, NO_VALUE);
    }
    else {
        return make_pair(currentNode->key, currentNode->val);
    }
}

template <typename K, typename V>
typename AVLTree<K, V>::Node * AVLTree<K, V>::getAVLNode(const K & key) {
    Node * currentNode = root;
    while (currentNode != NULL) {
        K nodeKey = currentNode->key;
        if ( key < nodeKey ) {
            currentNode = currentNode->left;
        }
        else if ( key > nodeKey ) {
            currentNode = currentNode->right;
        }
        else {
            return currentNode;
        }
    }
    return NULL;
}

template <typename K, typename V>
void AVLTree<K, V>::rotateLeft(Node* prevNode) {
    /* Single left rotation */
    Node * leftChild = prevNode->left;
    Node * prevNodeParent = prevNode->parent;
    prevNode->left = leftChild->right;
    if (prevNode->left != NULL) {
        prevNode->left->parent = prevNode;
    }
    leftChild->right = prevNode;
    prevNode->parent = leftChild;
    prevNode->balance = 0;
    if (prevNodeParent == NULL) {
        root = leftChild;
    }
    else if (prevNodeParent->left == prevNode) {
        prevNodeParent->left = leftChild;
    }
    else {
        prevNodeParent->right = leftChild;
    }
    leftChild->parent = prevNodeParent;
    leftChild->balance = 0;
}

template <typename K, typename V>
void AVLTree<K, V>::rotateRight(Node * prevNode) {
    /* Single right rotation */
    Node * rightChild = prevNode->right;
    Node * prevNodeParent = prevNode->parent;
 
    prevNode->right = rightChild->left;
    if (prevNode->right != NULL) {
        prevNode->right->parent = prevNode;
    }
    rightChild->left = prevNode;
    prevNode->parent = rightChild;
    prevNode->balance = 0;
    if (prevNodeParent == NULL) {
        root = rightChild;
    }
    else if (prevNodeParent->left == prevNode) {
        prevNodeParent->left = rightChild;
    }
    else  {
        prevNodeParent->right = rightChild;
    }
    rightChild->parent = prevNodeParent;
    rightChild->balance = 0;
   
}

template <typename K, typename V>
void AVLTree<K, V>::rotateDoubleRight(Node * prevNode) {
    Node * prevNodeParent = prevNode->parent;
    Node * leftChild = prevNode->left;
    Node * leftChildRightChild = leftChild->right;
    
    leftChild->right = leftChildRightChild->left;
    if (leftChildRightChild->left != NULL) {
        leftChildRightChild->left->parent = leftChild;
    }
    
    leftChildRightChild->left = leftChild;
    leftChild->parent = leftChildRightChild;
    
    prevNode->left = leftChildRightChild->right;
    if (leftChildRightChild->right != NULL) {
        leftChildRightChild->right->parent = prevNode;
    }
    
    leftChildRightChild->right = prevNode;
    prevNode->parent = leftChildRightChild;
    
    prevNode->balance = (leftChildRightChild->balance == -1) ? +1 : 0;
    leftChild->balance = (leftChildRightChild->balance == 1) ? -1 : 0;
    
    if (prevNodeParent == NULL) {
        root = leftChildRightChild;
    }
    else if (prevNodeParent->left == prevNode) {
        prevNodeParent->left = leftChildRightChild;
    }
    else {
        prevNodeParent->right = leftChildRightChild;
    }
    
    leftChildRightChild->parent = prevNodeParent;
    leftChildRightChild->balance = 0;   
}

template <typename K, typename V>
void AVLTree<K, V>::rotateDoubleLeft(Node* prevNode) {
    Node * prevNodeParent = prevNode->parent;
    Node * rightChild = prevNode->right;
    Node * rightChildLeftChild = rightChild->left;
    
    rightChild->left = rightChildLeftChild->right;
    if (rightChildLeftChild->right != NULL) {
        rightChildLeftChild->right->parent = rightChild;
    }
    
    rightChildLeftChild->right = rightChild;
    rightChild->parent = rightChildLeftChild;
    
    prevNode->right = rightChildLeftChild->left;
    if (rightChildLeftChild->left != NULL) {
        rightChildLeftChild->left->parent = prevNode;
    }
    
    rightChildLeftChild->left = prevNode;
    prevNode->parent = rightChildLeftChild;
    
    prevNode->balance = (rightChildLeftChild->balance == 1) ? -1: 0;
    rightChild->balance = (rightChildLeftChild->balance == -1) ? 1 : 0;
    if (prevNodeParent == NULL) {
        root = rightChildLeftChild;
    }
    else if (prevNodeParent->left == prevNode) {
        prevNodeParent->left = rightChildLeftChild;
    }
    else {
        prevNodeParent->right = rightChildLeftChild;
    }
    rightChildLeftChild->parent = prevNodeParent;
    rightChildLeftChild->balance = 0;
}

template <typename K, typename V>
bool AVLTree<K, V>::replaceWithRightmost(Node * toReplaceInNode) {
    Node * currentNode = toReplaceInNode->left;
    int replacePos = 0;
    while (currentNode->right != NULL) {
        replacePos = replacePos + 1;
        currentNode = currentNode->right;
    }
    toReplaceInNode->key = currentNode->key;
    toReplaceInNode->val = currentNode->val;
    if (currentNode->parent->right == currentNode) {
        currentNode->parent->right = currentNode->left;
    }
    else {
        currentNode->parent->left = currentNode->left;
    }
    
    if (currentNode->left != NULL) {
        currentNode->left->parent = currentNode->parent;
    }

    // currentNode is now unlinked
    Node* tmp = currentNode->parent;
    delete(currentNode);
    
    bool continueBalance = true;
    currentNode = tmp;

    while (replacePos > 0 && continueBalance) {
        Node * operateOn = currentNode;
        currentNode = currentNode->parent;
        replacePos = replacePos - 1;
        continueBalance = deleteBalanceRight(operateOn);
    }
    return continueBalance;
}

template <typename K, typename V>
bool AVLTree<K, V>::deleteBalanceLeft(Node * currentNode) {
    bool continueBalance = true;
    if (currentNode->balance == -1) {
        currentNode->balance = 0;
    }
    else if (currentNode->balance == 0) {
        currentNode->balance = 1;
        continueBalance = false;
    }
    else {
        Node * currentNodeParent = currentNode->parent;
        Node * rightChild = currentNode->right;
        int rightChildBalance = rightChild->balance;
        if (rightChildBalance >= 0) {
            rotateRight(currentNode);
            if (rightChildBalance == 0) {
                currentNode->balance = 1;
                rightChild->balance = -1;
                continueBalance = false;
            }
        }
        else {
            Node * rightChildLeftChild = rightChild->left;
            int rightChildLeftChildBalance = rightChildLeftChild->balance;
            rightChild->left = rightChildLeftChild->right;
            if (rightChildLeftChild->right != NULL) {
                rightChildLeftChild->right->parent = rightChild;
            }
            rightChildLeftChild->right = rightChild;
            rightChild->parent = rightChildLeftChild;
            currentNode->right = rightChildLeftChild->left;
            if (rightChildLeftChild->left != NULL) {
                rightChildLeftChild->left->parent = currentNode;
            }
            rightChildLeftChild->left = currentNode;
            currentNode->parent = rightChildLeftChild;
            currentNode->balance = (rightChildLeftChildBalance == 1) ? -1 : 0;
            rightChild->balance = (rightChildLeftChildBalance == -1) ? 1 : 0;
            rightChildLeftChild->balance = 0;
            if (currentNodeParent == NULL) {
                root = rightChildLeftChild;
            }
            else if (currentNodeParent->left == currentNode) {
                currentNodeParent->left = rightChildLeftChild;
            }
            else {
                currentNodeParent->right = rightChildLeftChild;
            }
            rightChildLeftChild->parent = currentNodeParent;
        }
    }
    return continueBalance;
}

template <typename K, typename V>
bool AVLTree<K, V>::deleteBalanceRight(Node * currentNode) {
    bool continueBalance = true;
    if (currentNode->balance == 1) {
        currentNode->balance = 0;
    }
    else if (currentNode->balance == 0) {
        currentNode->balance = -1;
        continueBalance = false;
    }
    else {
        Node * currentNodeParent = currentNode->parent;
        Node * leftChild = currentNode->left;
        int leftChildBalance = leftChild->balance;
        if (leftChildBalance <= 0) {
            rotateLeft(currentNode);
            if (leftChildBalance == 0) {
                currentNode->balance = -1;
                leftChild->balance = 1;
                continueBalance = false;
            }
        }
        else {
            Node * leftChildRightChild = leftChild->right;
            int leftChildRightChildBalance = leftChildRightChild->balance;
            leftChild->right = leftChildRightChild->left;
            if (leftChildRightChild->left != NULL) {
                leftChildRightChild->left->parent = leftChild;
            }
            leftChildRightChild->left = leftChild;
            leftChild->parent = leftChildRightChild;
            currentNode->left = leftChildRightChild->right;
            if (leftChildRightChild->right != NULL) {
                leftChildRightChild->right->parent = currentNode;
            }
            leftChildRightChild->right = currentNode;
            currentNode->parent = leftChildRightChild;
            currentNode->balance = (leftChildRightChildBalance == -1) ? 1 : 0;
            leftChild->balance = (leftChildRightChildBalance == 1) ? -1 : 0;
            leftChildRightChild->balance = 0;
            if (currentNodeParent == NULL) {
                root = leftChildRightChild;
            }
            else if (currentNodeParent->left == currentNode) {
                currentNodeParent->left = leftChildRightChild;
            }
            else {
                currentNodeParent->right = leftChildRightChild;
            }
            leftChildRightChild->parent = currentNodeParent;
        }
    }
    return continueBalance;
}

template <typename K, typename V>
AVLTree<K, V>::AVLTree() {
    root = NULL;
}

template <typename K, typename V>
void AVLTree<K, V>::freeTraversal(Node * node) {
    assert(node != NULL);
    if ( node->left != NULL )
        freeTraversal(node->left);
    
    if ( node->right != NULL)
        freeTraversal(node->right);
    
    delete(node);
}

template <typename K, typename V>
AVLTree<K, V>::~AVLTree() {
    if (root != NULL) {
        freeTraversal(root);
    }
}


template <typename K, typename V>
V AVLTree<K, V>::find(const int tid, const K & key) {
    Node* node = getAVLNode(key);
    if (node == nullptr) return NO_VALUE;
    return node->val;
}

/* Semantics: Only insert if key is absent */
template <typename K, typename V>
V AVLTree<K, V>::insert(const int tid, const K & key, const V& val) {
    Node * prevNode = NULL;
    Node * currentNode = root;
    bool dirLeft = true;

    while (currentNode != NULL) {
        K nodeKey = currentNode->key;
        if ( key < nodeKey ) {
            dirLeft = true;
            prevNode = currentNode;
            currentNode = currentNode->left;
        }
        else if (key > nodeKey ) {
            dirLeft = false;
            prevNode = currentNode;
            currentNode = currentNode->right;
        }
        else {
            /* key is found, return false */
            return currentNode->val;
        }
    }
    
    /* Unique key, create new node and insert */
    
    currentNode = new Node(key, val, prevNode, NULL, NULL);

    if (prevNode == NULL) {
        root = currentNode;
    }
    else if (dirLeft) {
        prevNode->left = currentNode;
    }
    else {
        prevNode->right = currentNode;
    }
    
    /* Rotate and/or update balance of nodes */
    while (prevNode != NULL) {
        if (prevNode->left == currentNode) {
            if (prevNode->balance == -1) {
                Node * leftChild = prevNode->left;
                if (leftChild->balance == -1) {
                    rotateLeft(prevNode);
                }
                else {
                    rotateDoubleRight(prevNode);
                }
                break;
            }
            else if (prevNode->balance == 0) {
                prevNode->balance = -1;
            }
            else {
                prevNode->balance = 0;
                break;
            }
        }
        else {
            if (prevNode->balance == 1) {
                Node * rightChild = prevNode->right;
                if (rightChild->balance == 1) {
                    rotateRight(prevNode);
                }
                else {
                    rotateDoubleLeft(prevNode);
                }
                break;
            }
            else if (prevNode->balance == 0) {
                prevNode->balance = 1;
            }
            else {
                prevNode->balance = 0;
                break;
            }
        }
        currentNode = prevNode;
        prevNode = prevNode->parent;
    }

    return NO_VALUE;
}

template <typename K, typename V>
V AVLTree<K, V>::erase(const int tid, const K & key) {   
    bool dirLeft = true;
    Node * currentNode = root;
    while (currentNode != NULL) {
        K nodeKey = currentNode->key;
        if (key < nodeKey) {
            dirLeft = true;
            currentNode = currentNode->left;
        }
        else if (key > nodeKey) {
            dirLeft = false;
            currentNode = currentNode->right;
        }
        else {
            break;
        }
    }
    
    if (currentNode == NULL) {
        return NO_VALUE; //key not found, return false
    }
    V retval = currentNode->val;
    /* else, found key, delete it */
    Node * prevNode = currentNode->parent;
    bool continueFix = true;
    if (currentNode->left == NULL) {
        if (prevNode == NULL) {
            root = currentNode->right;
        }
        else if (dirLeft) {
            prevNode->left = currentNode->right;
        }
        else {
            prevNode->right = currentNode->right;
        }
        
        if (currentNode->right != NULL) {
            currentNode->right->parent = prevNode;
        }
        Node* tmp = currentNode->right;
        delete(currentNode);
        currentNode = tmp;
    }
    else if (currentNode->right == NULL) {
        if (prevNode == NULL) {
            root = currentNode->left;
        }
        else if (dirLeft) {
            prevNode->left = currentNode->left;
        }
        else {
            prevNode->right = currentNode->left;
        }
        
        if (currentNode->left != NULL) {
            currentNode->left->parent = prevNode;
        }
        Node* tmp = currentNode->left;
        delete(currentNode);
        currentNode = tmp;
    }
    else {
        // replaceWithRightmost frees the deleted node
        if (prevNode == NULL) {
            continueFix = replaceWithRightmost(currentNode);
            currentNode = root->left;
            prevNode = root;
        }
        else if (prevNode->left == currentNode) {
            continueFix = replaceWithRightmost(currentNode);
            prevNode = prevNode->left;
            currentNode = prevNode->left;
            dirLeft = true;
        }
        else {
            continueFix = replaceWithRightmost(currentNode);
            prevNode = prevNode->right;
            currentNode = prevNode->left;
            dirLeft = true;
        }
    }

    
    /* Rebalance */
    while (continueFix && prevNode != NULL) {
        Node * nextPrevNode = prevNode->parent;
        if (nextPrevNode != NULL) {
            bool findCurrentLeftDir = true;
            if (nextPrevNode->left == prevNode) {
                findCurrentLeftDir = true;
            }
            else {
                findCurrentLeftDir = false;
            }
            
            if (currentNode == NULL) {
                if (dirLeft) 
                    continueFix = deleteBalanceLeft(prevNode);
                else 
                    continueFix = deleteBalanceRight(prevNode);
            }
            else {
                if (prevNode->left == currentNode)
                    continueFix = deleteBalanceLeft(prevNode);
                else
                    continueFix = deleteBalanceRight(prevNode);
            }
            
            if (findCurrentLeftDir)
                currentNode = nextPrevNode->left;
            else
                currentNode = nextPrevNode->right;
            prevNode = nextPrevNode;
        }
        else {
            if (currentNode == NULL) {
                if (dirLeft)
                    continueFix = deleteBalanceLeft(prevNode);
                else
                    continueFix = deleteBalanceRight(prevNode);
            }
            else {
                if (prevNode->left == currentNode) 
                    continueFix = deleteBalanceLeft(prevNode);
                else
                    continueFix = deleteBalanceRight(prevNode);
            }
            prevNode = NULL;
        }
    }
    
    return retval;
}

template <typename K, typename V>
IOSet * AVLTree<K, V>::join(const int tid, IOSet * rightSet) {
    /* Assumption: rightTree's smallest key > this tree's largest key */
    AVLTree * const rightTree = dynamic_cast<AVLTree *>(rightSet);
    if (rightTree == nullptr) {
        assert(false); /* incorrect type */
    }
    
    AVLTree * newTree = new AVLTree();
    Node * prevNode = NULL;
    Node * currentNode = NULL;
    
    AVLTree * const leftTree = this;
    
    /* Check if left tree has keys */
    if(leftTree->root == NULL){
        newTree->root = rightTree->root;
        // Don't accidentally free the returned tree's data
        rightTree->root = NULL;
        return newTree;
    }
    
    /* Check if right tree has keys */
    if(rightTree->root == NULL){
        newTree->root = leftTree->root;
        // Don't accidentally free the returned tree's data
        leftTree->root = NULL; 
        return newTree;
    }
    
    /* Both trees are non-empty, non-trivial join */
    int leftHeight = leftTree->computeHeight();
    int rightHeight = rightTree->computeHeight();
    
    if ( leftHeight >= rightHeight ) {
        pair<K,V> minKey = rightTree->minKey();
        assert(minKey.first != INVALID_KEY); /* minKey == NULL only if rightTree is empty */
        
        rightTree->erase(tid, minKey.first);
        Node * newRoot = new Node(minKey.first, minKey.second, NULL, NULL, NULL);
        int newRightHeight = rightTree->computeHeight();
        
        prevNode = NULL;
        currentNode = leftTree->root;
        int currHeight = leftHeight;
        while (currHeight > newRightHeight + 1) {
            if (currentNode->balance == -1) {
                currHeight = currHeight - 2;
            }
            else {
                currHeight = currHeight - 1;
            }
            prevNode = currentNode;
            currentNode = currentNode->right;
        }
        Node * oldCurrentNodeParent = prevNode;
        newRoot->left = currentNode;
        if ( currentNode != NULL ) {
            currentNode->parent = newRoot;
        }
        newRoot->right = rightTree->root;
        if (rightTree->root != NULL) {
            rightTree->root->parent = newRoot;
        }
        
        newRoot->balance = newRightHeight - currHeight;
        
        if ( oldCurrentNodeParent == NULL ) {
            newTree->root = newRoot;
        }
        else if (oldCurrentNodeParent->left == currentNode) {
            prevNode->left = newRoot;
            newRoot->parent = oldCurrentNodeParent;
            newTree->root = leftTree->root;
        }
        else {
            oldCurrentNodeParent->right = newRoot;
            newRoot->parent = oldCurrentNodeParent;
            newTree->root = leftTree->root;
        }
        currentNode = newRoot;
    }
    else { /* symmetric case */
        pair<K,V> maxKey = leftTree->maxKey();
        assert(maxKey.first != INVALID_KEY); /* minKey == NULL only if righTree is empty */
        leftTree->erase(tid, maxKey.first);
        Node * newRoot = new Node(maxKey.first, maxKey.second, NULL, NULL, NULL);
        int newLeftHeight = leftTree->computeHeight();
        
        prevNode = NULL;
        currentNode = rightTree->root;
        int currHeight = rightHeight;
        while (currHeight > newLeftHeight + 1) {
            if (currentNode->balance == 1) {
                currHeight = currHeight - 2;
            }
            else {
                currHeight = currHeight - 1;
            }
            prevNode = currentNode;
            currentNode = currentNode->left;
        }
        Node * oldCurrentNodeParent = prevNode;
        newRoot->right = currentNode;
        if ( currentNode != NULL ) {
            currentNode->parent = newRoot;
        }
        
        newRoot->left = leftTree->root;
        if (leftTree->root != NULL) {
            leftTree->root->parent = newRoot;
        }   
        newRoot->balance = currHeight - newLeftHeight;
        if ( oldCurrentNodeParent == NULL ) {
            newTree->root = newRoot;
        }
        else if (oldCurrentNodeParent->left == currentNode) {
            oldCurrentNodeParent->left = newRoot;
            newRoot->parent = oldCurrentNodeParent;
            newTree->root = rightTree->root;
        }
        else {
            oldCurrentNodeParent->right = newRoot;
            newRoot->parent = oldCurrentNodeParent;
            newTree->root = rightTree->root;
        }
        currentNode = newRoot;
    }
    
    leftTree->root = NULL;
    rightTree->root = NULL;
    while (prevNode != NULL) {
        if (prevNode->left == currentNode) {
            if (prevNode->balance == -1) {
                Node * leftChild = prevNode->left;
                if (leftChild->balance == -1) {
                    newTree->rotateLeft(prevNode);
                }
                else {
                    newTree->rotateDoubleRight(prevNode);
                }
                return newTree;
            }
            else if (prevNode->balance == 0) {
                prevNode->balance = -1;
            }
            else {
                prevNode->balance = 0;
                break;
            }
        }
        else {
            if (prevNode->balance == 1) {
                Node * rightChild = prevNode->right;
                if (rightChild->balance == 1) {
                    newTree->rotateRight(prevNode);
                }
                else {
                    newTree->rotateDoubleLeft(prevNode);
                }
                return newTree;
            }
            else if (prevNode->balance == 0) {
                prevNode->balance = 1;
            }
            else {
                prevNode->balance = 0;
                break;
            }
        }
        currentNode = prevNode;
        prevNode = prevNode->parent;
    }
    return newTree;
}

template <typename K, typename V>
std::tuple<K, IOSet *, IOSet *> AVLTree<K, V>::split(const int tid) {
    Node * leftRoot = NULL;
    Node * rightRoot = NULL;
    
    K splitKey;
    V splitVal;
    if (root == NULL) {
        /* empty tree */
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    else if (root->left == NULL && root->right == NULL) {
        /* Tree only has one node */
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    else if (root->left == NULL) {
        /* root's left child is NULL, right child is non-NULL */
        splitKey = root->right->key;
        splitVal = root->right->val;
        rightRoot = root->right;
        rightRoot->parent = NULL;
        rightRoot->balance = 0;
        root->right = NULL;
        leftRoot = root;
        leftRoot->balance = 0;
    }
    else {
        /* root's left child is non-NULL */
        splitKey = root->key;
        splitVal = root->val;
        leftRoot = root->left;
        leftRoot->parent = NULL;
        root->left = NULL;
       
        if (root->right == NULL) {
            /* root does not have a right child */
            rightRoot = root;
            rightRoot->balance = 0;
        }
        else {
            /* re-insert splitKey into right tree */
            Node * oldRoot = root;
            root = root->right;
            root->parent = NULL;   
            delete(oldRoot);
            insert(tid, splitKey, splitVal);
            rightRoot = root;
            
        }
    }
    AVLTree * leftTree = new AVLTree();
    leftTree->root = leftRoot;
    AVLTree * rightTree = new AVLTree();
    rightTree->root = rightRoot;
     
    // Don't accidently delete the data of the returned trees
    root = NULL;
    return std::make_tuple(splitKey, leftTree, rightTree);
}

template <typename K, typename V>
bool AVLTree<K, V>::isEmpty() {
    return root == NULL;
}

template <typename K, typename V>
size_t AVLTree<K, V>::sumOfKeysHelper(Node * node) {
    if (node == NULL)
        return 0;
    return node->key + sumOfKeysHelper(node->left) + sumOfKeysHelper(node->right);
}

template <typename K, typename V>
size_t AVLTree<K, V>::sumOfKeys() {
    return sumOfKeysHelper(root);
}

template <typename K, typename V>
size_t AVLTree<K, V>::numKeysHelper(Node * node) {
    if (node == NULL)
        return 0;
    return 1 + numKeysHelper(node->left) + numKeysHelper(node->right);
}

template <typename K, typename V>
size_t AVLTree<K, V>::numKeys() {
    return numKeysHelper(root);
}

template <typename K, typename V>
void AVLTree<K, V>::printInOrderTraversal(Node * node) {
    if (node->left != NULL)
        printInOrderTraversal(node->left);
    
    printf("%d-", node->key);
    
    if ( node->right != NULL)
        printInOrderTraversal(node->right);
}

template <typename K, typename V>
void AVLTree<K, V>::printInOrderTraversal() {
    printf("start-");
    printInOrderTraversal(root);
    printf("end\n");
}

template <typename K, typename V>
bool AVLTree<K, V>::doesAVLHold(Node * node) {
    bool holds = true;
    if ( node != NULL ) {
        if (node->left != NULL)
            holds *= doesAVLHold(node->left);
        if (node->right != NULL)
            holds *= doesAVLHold(node->right);    

        holds *= abs(node->balance) <= 1 ? true : false;
    }
   
    return holds;  
}

template <typename K, typename V>
bool AVLTree<K, V>::checkAVL( ) {
    return doesAVLHold(root);
}

template <typename K, typename V>
void AVLTree<K, V>::printBFSOrder(Node * node) {
    if (node != NULL) {
        queue<Node *> q;;
        q.push(node);
        printf("start-");
        while ( !q.empty() ) {
            Node * curr = q.front();
            q.pop();
            K parentKey = curr->parent == NULL? 0 : curr->parent->key;
            printf("%d(p(%d)->", curr->key, parentKey);
            if (curr->left != NULL)
                q.push(curr->left);
            if (curr->right != NULL)
                q.push(curr->right);
        }
        printf("end\n"); 
    }
}

template <typename K, typename V>
void AVLTree<K, V>::printBFSOrder() {
    printBFSOrder(root);
}

#endif /* AVL_TREE_H */

