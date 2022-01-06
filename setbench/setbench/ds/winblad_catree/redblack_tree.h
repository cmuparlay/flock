/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   redblack_tree.h
 * Author: ginns
 *
 * Created on April 21, 2021, 11:42 AM
 */

#ifndef REDBLACK_TREE_H
#define REDBLACK_TREE_H

#include <algorithm>    /* for max */
#include <queue>        /* used in BFS */
#include <cassert>
#include <iostream>


#include "interfaces.h"

#define INVALID_KEY     0

using namespace std;
/**
 * Based on implementation found in Introduction to Algorithms [CLRS] and the 
 * Multiset implementation found in the C++ library CGAL
 * https://github.com/CGAL/cgal/blob/master/STL_Extension/include/CGAL/Multiset.h
 */

template <class RecordManager, typename K, typename V>
class RedBlackTree : public IOSet {
private:
    enum Color { RED, BLACK };
    struct Node {
        K key;
        V val;
        Node * parent;
        Node * left;
        Node * right;
        Color color;
        
        Node(K _key, V _val, Node * _parent, Node * _left, Node * _right, Color _color) :
            key(_key), val(_val), parent(_parent), left(_left), right(_right), color(_color) {
            
        }
        
        bool isBlack() {
            return color == BLACK;
        }
    };
    
    void insertFixup(Node * node);
#if 0
    void eraseFixup(Node * node);
#else
    void eraseFixup(Node * node, Node * parent);
#endif
    void transplant(Node * u, Node * v);
    Node * tree_minimum(Node * node);
    Node * tree_maximum(Node * node);
    void leftRotate(Node * x);
    void rightRotate(Node * x);
    
    int calcBlackHeight(Node * node);
    void printBFSOrder(Node * node);
    size_t numKeysHelper(Node * node);
    size_t sumOfKeysHelper(Node * node);
    Color getColor(Node * node);
    void setColor(Node * node, Color newColor);
    
    Node * root;
    int blackHeight;
    const V NO_VALUE = (V) 0;
    
public:
    
    RedBlackTree();
    ~RedBlackTree();
        
    V find(const int tid, const K & key);
    V insert(const int tid, const K & key, const V& val);
    V erase(const int tid, const K & key);
    
    IOSet * join(const int tid, IOSet * rightSet);
    std::tuple<K, IOSet *, IOSet *> split(const int tid);
    
    size_t numKeys();
    size_t sumOfKeys();
    int getSize();
    void printKeys();
    
    int verifyBlackHeight();
    void printBFSOrder();
    
};

/**
 * 
 */


template <class RecordManager, typename K, typename V>
RedBlackTree<RecordManager, K, V>::RedBlackTree() {
    root = NULL;
    blackHeight = 0;
}

template <class RecordManager, typename K, typename V>
RedBlackTree<RecordManager, K, V>::~RedBlackTree() {
    
}


template <class RecordManager, typename K, typename V>
V RedBlackTree<RecordManager, K, V>::find(const int tid, const K & key) {
    Node * currNode = root;
    while (currNode != NULL) {
        K nodeKey = currNode->key;
        if ( key < nodeKey ) {
            currNode = currNode->left;
        }
        else if ( key > nodeKey ) {
            currNode = currNode->right;
        }
        else { /* key found */
            return currNode->val;
        }
    }
    return NO_VALUE;
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::leftRotate(Node * x) {
    assert(x != NULL);
    
    Node * y = x->right;
    assert(y != NULL);
    
    x->right = y->left;
    if (y->left != NULL) {
        y->left->parent = x;
    }
    y->parent = x->parent;
 
    if (x->parent == NULL) {
        root = y;
    }
    else if (x == x->parent->left) {
        x->parent->left = y;
    }
    else  {
        x->parent->right = y;
    }
    y->left = x;
    x->parent = y;
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::rightRotate(Node * x) {
    assert(x != NULL);
    
    Node * y = x->left;
    assert(y != NULL);
    
    x->left = y->right;
    if (y->right != NULL) {
        y->right->parent = x;
    }
    y->parent = x->parent;
 
    if (x->parent == NULL) {
        root = y;
    }
    else if (x == x->parent->left) {
        x->parent->left = y;
    }
    else  {
        x->parent->right = y;
    }
    y->right = x;
    x->parent = y;
}

template <class RecordManager, typename K, typename V>
typename RedBlackTree<RecordManager, K, V>::Color RedBlackTree<RecordManager, K, V>::getColor(Node * node) {
    if (node == NULL)
        return BLACK;
    else
        return node->color;
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::setColor(Node * node, Color newColor) {
    if (node != NULL)
        node->color = newColor;
}


template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::insertFixup(Node * z) {
    while ( (z != root) && getColor(z->parent) == RED) {
        if (z->parent == z->parent->parent->left) {
            Node * y = z->parent->parent->right;
            if ( getColor(y) == RED ) {
                setColor(z->parent, BLACK);
                setColor(y, BLACK);
                setColor(z->parent->parent, RED);
                z = z->parent->parent;
            }
            else {
                if (z == z->parent->right) {
                    z = z->parent;
                    leftRotate(z);
                }
                setColor(z->parent, BLACK);
                setColor(z->parent->parent, RED);
                rightRotate(z->parent->parent);
            }
        }
        else {
            /* symmetric case */
            Node * y = z->parent->parent->left;
            if ( getColor(y) == RED ) {
                setColor(z->parent, BLACK);
                setColor(y, BLACK);
                setColor(z->parent->parent, RED);
                z = z->parent->parent;
            }
            else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rightRotate(z);
                }
                setColor(z->parent, BLACK);
                setColor(z->parent->parent, RED);
                leftRotate(z->parent->parent);
            }
        }
    }
    
    if ( getColor(root) == RED ) {
        setColor(root, BLACK);
        blackHeight++;
    }
}

template <class RecordManager, typename K, typename V>
V RedBlackTree<RecordManager, K, V>::insert(const int tid, const K & key, const V& val) {
    Node * prevNode = NULL;
    Node * currNode = root;
    while (currNode != NULL) {
        prevNode = currNode;
        if (key < currNode->key) {
            currNode = currNode->left;
        }
        else if (key > currNode->key) {
            currNode = currNode->right;
        }
        else {
            /* key already exists */
            return currNode->val;
        }
    }
    
    /* New key, insert into tree */
    Node * newNode = new Node(key, val, prevNode, NULL, NULL, RED);
    if ( prevNode == NULL ) {
        root = newNode;
    }
    else if (newNode->key < prevNode->key) {
        prevNode->left = newNode;
    }
    else {
        prevNode->right = newNode;
    }
    insertFixup(newNode);
    
    return NO_VALUE;
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::transplant(Node* u, Node* v) {
# if 0
    if (u->parent == NULL) {
        root = v;
    }
    else if (u == u->parent->left) {
        u->parent->left = v;
    }
    else {
        u->parent->right = v;
    }
    if (v != NULL)
        v->parent = u->parent;
    
# else
    /* save u's properties */
    Color uColor = u->color;
    Node * uParent = u->parent;
    Node * uRight = u->right;
    Node * uLeft = u->left;

    u->color = v->color;
    
    if ( u != v->parent ) {
        if (v->parent == NULL)
            root = u;
        else {
            if ( v->parent->left == v)
                v->parent->left = u;
            else
                v->parent->right = u;
        }
        u->parent = v->parent;
    }
    else {
        u->parent = v;
    }
    
    if (u != v->right) {
        if (v->right != NULL)
            v->right->parent = u;
        
        u->right = v->right;
    }
    else {
        u->right = v;
    }
    
    if (u != v->left) {
        if (v->left != NULL)
            v->left->parent = u;
        
        u->left = v->left;
    }
    else {
        u->left = v;
    }
    
    /* Move u's saved properties to v */
    v->color = uColor;
    if (v != uParent) {
        if (uParent == NULL)
            root = v;
        else {
            if (uParent->left == u)
                uParent->left = v;
            else
                uParent->right = v;
            
        }
        
        v->parent = uParent;
    }
    else {
        v->parent = u;
    }
    
    if (v != uRight) {
        if (uRight != NULL)
            uRight->parent = v;
        
        v->right = uRight;
    }
    else {
        v->right = u;
    }
    
    if (v != uLeft) {
        if (uLeft != NULL)
            uLeft->parent = v;
        
        v->left = uLeft;
    }
    else {
        v->left = u;
    }
#endif
}

template <class RecordManager, typename K, typename V>
typename RedBlackTree<RecordManager, K, V>::Node * RedBlackTree<RecordManager, K, V>::tree_minimum(Node * node) {
    Node * curr = node;
    while (curr->left != NULL) {
        curr = curr->left;
    }
    return curr;
}

template <class RecordManager, typename K, typename V>
typename RedBlackTree<RecordManager, K, V>::Node * RedBlackTree<RecordManager, K, V>::tree_maximum(Node * node) {
    Node * curr = node;
    while (curr->right != NULL) {
        curr = curr->right;
    }
    return curr;
}

#if 0
void RedBlackTree<RecordManager, K, V>::eraseFixup(Node* node) {
    assert(node != NULL);
    Node * x = node;
    Node * w;
    while ( (x != root) && (getColor(x) == BLACK) ) {
        if (x == x->parent->left) {
            w = x->parent->right;
            if (getColor(w) == RED) {
                setColor(w, BLACK);
                setColor(x->parent, RED);
                leftRotate(x->parent);
                w = x->parent->right;
            }
            if ( (getColor(w->left) == BLACK) && (getColor(w->right) == BLACK) ) {
                setColor(w, RED);
                x = x->parent;
                
                if (x == root)
                    blackHeight--;
            }
            else {
                if (getColor(w->right) == BLACK) {
                    setColor(w->left, BLACK);
                    setColor(w, RED);
                    rightRotate(w);
                    w = x->parent->right;
                }
                setColor(w, getColor(x->parent));
                setColor(x->parent, BLACK);
                if ( w->right != NULL)
                    setColor(w->right, BLACK);
                leftRotate(x->parent);
                x = root;
            }
        }
        else {
            /* Symmetric case */
            w = x->parent->left;
            if (getColor(w) == RED) {
                setColor(w, BLACK);
                setColor(x->parent, RED);
                rightRotate(x->parent);
                w = x->parent->left;
            }
            if ( (getColor(w->right) == BLACK) && (getColor(w->left) == BLACK) ) {
                setColor(w, RED);
                x = x->parent;
                
                if (x == root)
                    blackHeight--;
            }
            else {
                if (getColor(w->left) == BLACK) {
                    setColor(w->right, BLACK);
                    setColor(w, RED);
                    leftRotate(w);
                    w = x->parent->left;
                }
                setColor(w, getColor(x->parent));
                setColor(x->parent, BLACK);
                if (w->left != NULL)
                    setColor(w->left, BLACK);
                rightRotate(x->parent);
                x = root;
            }
        }
    }
    
    if (getColor(x) == RED) {
        setColor(x, BLACK);
        if (x == root)
            blackHeight++;
    }
    
}

#else
template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::eraseFixup(Node* node, Node * parent) {
    Node * curr = node;
    Node * currParent = parent;
    Node * sibling;
    
    while (curr != root && getColor(curr) == BLACK ) {
        if (curr == currParent->left) {
            sibling = currParent->right;
        
            if (getColor(sibling) == RED) {
                setColor(sibling, BLACK);
                setColor(currParent, RED);
                leftRotate(currParent);
                sibling = currParent->right;
            }
            
            assert(sibling != NULL);
            if ((getColor(sibling->left) == BLACK) && (getColor(sibling->right) == BLACK)) {
                setColor(sibling, RED);
                
                curr = currParent;
                currParent = currParent->parent;
                
                if (curr == root)
                    blackHeight--;
            }
            else {
                if ( getColor(sibling->right) == BLACK ) {
                    setColor(sibling->left, BLACK);
                    setColor(sibling, RED);
                    rightRotate(sibling);
                    sibling = currParent->right;
                }
                setColor(sibling, getColor(currParent));
                setColor(currParent, BLACK);
                if (sibling->right != NULL)
                    setColor(sibling->right, BLACK);
                leftRotate(currParent);
                curr = root;
            }    
        }
        else { /* symmetric case */
            sibling = currParent->left;
            
            if ( getColor(sibling) == RED ) {
                setColor(sibling, BLACK);
                setColor(currParent, RED);
                rightRotate(currParent);
                sibling = currParent->left;
            }
            
            assert(sibling != NULL);
            if ((getColor(sibling->left) == BLACK) && (getColor(sibling->right) == BLACK)) {
                setColor(sibling, RED);
                
                curr = currParent;
                currParent = currParent->parent;
                
                if (curr == root)
                    blackHeight--;
            }
            else {
                if ( getColor(sibling->left) == BLACK ) {
                    setColor(sibling->right, BLACK);
                    setColor(sibling, RED);
                    leftRotate(sibling);
                    sibling = currParent->left;
                }
                setColor(sibling, getColor(currParent));
                setColor(currParent, BLACK);
                if (sibling->left != NULL)
                    setColor(sibling->left, BLACK);
                rightRotate(currParent);
                curr = root;
            }
            
        }
    }
    if ( getColor(curr) == RED ) {
        setColor(curr, BLACK);
        
        if (curr == root)
            blackHeight++;
    } 
    
}
#endif

template <class RecordManager, typename K, typename V>
V RedBlackTree<RecordManager, K, V>::erase(const int tid, const K & key) {
#if 0
    /* Find node */
    Node * z = root;
    while (z != NULL) {
        if (key < z->key) {
            z = z->left;
        }
        else if (key > z->key) {
            z = z->right;
        }
        else { /* found node with key */
            break;
        }
    }
    
    
    if ( z == NULL )
        return false;  /* key is not in tree */
    
    
    
    /* Node z contains the key */
    Node * x;
    Node * y = z;
    Node * zParent;
    Color yOriginalColor = getColor(y);
    if (z->left == NULL) {  /* Does not have left child, might have right */
        x = z->right;
        transplant(z, z->right);
    }
    else if (z->right == NULL) { /* Only has right child */
        x = z->left;
        transplant(z, z->left);
    }
    else { /* */
        y = tree_minimum(z->right);
        yOriginalColor = getColor(y);
        x = y->right;
        if ( (y->parent == z) && (x != NULL) ) {
            x->parent = y;
        }
        else {
            transplant(y, y->right);
            y->right = z->right;
            if (y->right != NULL)
                y->right->parent = y;
        }
        transplant(z, y);
        y->left = z->left;
        if (y->left != NULL)
            y->left->parent = y;
        setColor(y, getColor(z));
    }
    
    if ( yOriginalColor == BLACK )
        eraseFixup(x);
    

    return true;
#else
     /* Find node */
    Node * node = root;
    V retval;
    while (node != NULL) {
        if (key < node->key) {
            node = node->left;
        }
        else if (key > node->key) {
            node = node->right;
        }
        else { /* found node with key */
            retval = node->val;
            break;
        }
    }
    
    
    if ( node == NULL )
        return NO_VALUE;  /* key is not in tree */
    
    if ( node == root && 
            (node->left == NULL) && (node->right == NULL) ) {
        root = NULL;
        blackHeight = 0;
        return retval;
    }
    
    if ( (node->left != NULL) && (node->right != NULL) ) {
        /* Node has children */
        Node * succ = tree_minimum(node->right);
        
        transplant(node, succ);
    }
    
    
    Node * child = NULL;
    if (node->left != NULL) 
        child = node->left;
    else
        child = node->right;
    
    if (child != NULL)
        child->parent = node->parent;
    
    if (node->parent == NULL) {
        root = child;
        
        if (getColor(node) == BLACK)
            blackHeight--;
    }
    else {
        if (node == node->parent->left)
            node->parent->left = child;
        else
            node->parent->right = child;
    }
    
    if (getColor(node) == BLACK)
        eraseFixup(child, node->parent);
    

    return retval;
#endif
}

template <class RecordManager, typename K, typename V>
IOSet * RedBlackTree<RecordManager, K, V>::join(const int tid, IOSet * rightSet) {
    RedBlackTree * rightTree;
    if ( (rightTree = dynamic_cast<RedBlackTree *>(rightSet)) == nullptr ) {
        assert(false); /* incorrect type */
    }
    
    /* check if either set is empty */
    if ( rightTree->root == NULL )
        return this;
    else if ( root == NULL )
        return rightSet;
    
    
    /* Both sets are not empty */
    Node * maxT1 = tree_maximum(root);
    Node * minT2 = rightTree->tree_minimum(rightTree->root);
    assert(maxT1->key < minT2->key);
    
    
    Node * aux = NULL;
    if ( maxT1 != root ) {
        maxT1->parent->right = maxT1->left;
        
        if ( maxT1->left != NULL)
            maxT1->left->parent = maxT1->parent;
        
        if (getColor(maxT1) == BLACK)
            eraseFixup(maxT1->left, maxT1->parent);
        
        aux = maxT1;
    }
    else if (minT2 != rightTree->root) {
        minT2->parent->left = minT2->right;
        
        if ( minT2->right != NULL )
            minT2->right->parent = minT2->parent;
        
        if (getColor(minT2) == BLACK)
            rightTree->eraseFixup(minT2->right, minT2->parent);
        
        aux = minT2;
    }
    else {
        root->right = minT2;
        minT2->parent = root;
        setColor(minT2, RED);
        minT2->left = NULL;
        
        if (minT2->right != NULL) {
            insertFixup(minT2->right);
        }

        
        return this;
    }
    
    Node * node1 = root;
    Node * node2 = rightTree->root;
    int currBHeight;
    
    if (blackHeight <= rightTree->blackHeight) {
        /* This tree is smaller than rightTree */
        node2 = rightTree->root;
        currBHeight = rightTree->blackHeight;
        
        /* Find node in leftmost path of rightTree with 
         * blackHeight == our tree's blackHeight */
        while (currBHeight > blackHeight) {
            if (getColor(node2) == BLACK)
                currBHeight--;
            node2 = node2->left;
        }
        
        if (getColor(node2) == RED)
            node2 = node2->left;
    }
    else {
        /* This tree is taller */
        node1 = root;
        currBHeight = blackHeight;
        /* Find node in rightmost path of our tree with
         * blackHeight == rightTree's blackHeight */
        while (currBHeight > rightTree->blackHeight) {
            if (getColor(node1) == BLACK)
                currBHeight--;
            node1 = node1->right;
        }
        
        if (getColor(node1) == RED)
            node1 = node1->right;
        
    }
    
    Node * newRoot = NULL;
    Node * parent;
    
    if (node1 == root) {
        parent = node2->parent;
        if (parent == NULL)
            newRoot = aux;
        else {
            newRoot = rightTree->root;
            
            parent->left = aux;
        }
    }
    else {
        parent = node1->parent;
        assert(parent != NULL);
        newRoot = root;
        parent->right = aux;
    }
    
    aux->parent = parent;
    setColor(aux, RED);
    aux->left = node1;
    aux->right = node2;
    
    node1->parent = aux;
    node2->parent = aux;
    
    if ( root != newRoot ) {
        blackHeight = rightTree->blackHeight;
        root = newRoot;
    }

    
    insertFixup(aux);
    
    return this;
}


template <class RecordManager, typename K, typename V>
std::tuple<K, IOSet *, IOSet *> RedBlackTree<RecordManager, K, V>::split(const int tid) {
    K splitKey;
    if (root == NULL) {
        /* empty tree */
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    else if (root->left  == NULL && root->right == NULL) {\
        /* Tree only has one node */
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    else if (root->left == NULL) {
        /* Root only has a right child */
        K leftRootKey = root->key;
        V leftRootVal = root->val;
        K rightRootKey = root->right->key;
        V rightRootVal = root->right->val;
        splitKey = rightRootKey;
        
        RedBlackTree * leftTree = new RedBlackTree();
        RedBlackTree * rightTree = new RedBlackTree();
        
        leftTree->insert(tid, leftRootKey, leftRootVal);
        rightTree->insert(tid, rightRootKey, rightRootVal);
        
        delete(root->right);
        delete(root);
        
        return std::make_tuple(splitKey, leftTree, rightTree);
    }
    else if (root->right == NULL) {
        /* Root only has a left child */
        K leftRootKey = root->left->key;
        V leftRootVal = root->left->val;
        K rightRootKey = root->key;
        V rightRootVal = root->val;
        splitKey = rightRootKey;
        
        RedBlackTree * leftTree = new RedBlackTree();
        RedBlackTree * rightTree = new RedBlackTree();
        
        leftTree->insert(tid, leftRootKey, leftRootVal);
        rightTree->insert(tid, rightRootKey, rightRootVal);
        
        delete(root->left);
        delete(root);
        
        return std::make_tuple(splitKey, leftTree, rightTree);
    }
    else {
        /* root has two children (difficult case) */
        RedBlackTree * leftTree = new RedBlackTree();
        RedBlackTree * rightTree = new RedBlackTree();
        int currBHeight = blackHeight;
        Node * spineRight = NULL;
        Node * child = NULL;
        Node * next = NULL;
        Node * splitNode = root;
        K splitKey = splitNode->key;
        
        Node * curr = root;
        assert(curr != NULL);
        assert(getColor(curr) == BLACK);
        currBHeight--;
        
        /* Create right tree */
        child = curr->right;
        next = curr->left;
        
        rightTree->root = child;
        rightTree->blackHeight = currBHeight;
        rightTree->root->parent = NULL;
        if (getColor(rightTree->root) == RED) {
            setColor(rightTree->root, BLACK);
            rightTree->blackHeight++;
        }
       
        spineRight = rightTree->root;
                
        /* Create left tree */
        child = curr->left;
        next = curr->right;
        
        leftTree->root = child;
        leftTree->blackHeight = currBHeight;
        leftTree->root->parent = NULL;
        if (getColor(leftTree->root) == RED) {
            setColor(leftTree->root, BLACK);
            leftTree->blackHeight++;
        }
        
        /* Insert split node (root) into right tree */
        while ( spineRight->left != NULL ) {
            spineRight = spineRight->left;
        }
        
        splitNode->parent = spineRight;
        setColor(splitNode, RED);
        splitNode->right = NULL;
        splitNode->left = NULL;
        spineRight->left = splitNode;
        
        rightTree->insertFixup(splitNode);
        
        return std::make_tuple(splitKey, leftTree, rightTree);
    }
    
}

template <class RecordManager, typename K, typename V>
size_t RedBlackTree<RecordManager, K, V>::sumOfKeysHelper(Node* node) {
    if (node == NULL)
        return 0;
    return node->key + sumOfKeysHelper(node->left) + sumOfKeysHelper(node->right);
}

template <class RecordManager, typename K, typename V>
size_t RedBlackTree<RecordManager, K, V>::sumOfKeys() {
    return sumOfKeysHelper(root);
}

template <class RecordManager, typename K, typename V>
size_t RedBlackTree<RecordManager, K, V>::numKeysHelper(Node* node) {
    if (node == NULL)
        return 0;
    return 1 + numKeysHelper(node->left) + numKeysHelper(node->right);
}

template <class RecordManager, typename K, typename V>
size_t RedBlackTree<RecordManager, K, V>::numKeys() {
    return numKeysHelper(root);
}


/* lazy evaluation */
template <class RecordManager, typename K, typename V>
int RedBlackTree<RecordManager, K, V>::getSize() {
    /* Split operation makes it difficult to keep a member variable for size */
    /* This will have to be a tree traversal */
    // TODO implement
    return 0;
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::printKeys() {
    
}

/**
 * From StackOverflow 
 * https://stackoverflow.com/questions/13848011/how-to-check-the-black-height-of-a-node-for-all-paths-to-its-descendent-leaves
 */
template <class RecordManager, typename K, typename V>
int RedBlackTree<RecordManager, K, V>::calcBlackHeight(Node* node) {
    if (node == NULL) {
        return 1; /* leaf (nil) node */
    }
    
    int leftBlackHeight = calcBlackHeight(node->left);
    if (leftBlackHeight == 0)
        return leftBlackHeight;
    
    int rightBlackHeight = calcBlackHeight(node->right);
    if (rightBlackHeight == 0)
        return rightBlackHeight;
    
    assert(leftBlackHeight != 0);
    assert(rightBlackHeight != 0);
    
    if (leftBlackHeight != rightBlackHeight) {
        return 0;
    }
    else {
        int blackHeight = leftBlackHeight + (node->isBlack() ? 1 : 0);
        //printf("Node(%d) black height: %d\n", node->key, blackHeight);
        return blackHeight;
    }
}


template <class RecordManager, typename K, typename V>
int RedBlackTree<RecordManager, K, V>::verifyBlackHeight() {
    return calcBlackHeight(root);
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::printBFSOrder(Node * node) {
    if (node != NULL) {
        queue<Node *> q;
        q.push(node);
        printf("start-");
        while ( !q.empty() ) {
            Node * curr = q.front();
            q.pop();
            K parentKey = curr->parent == NULL? INVALID_KEY : curr->parent->key;
            char c = curr->color == BLACK? 'b' : 'r';
            printf("%d_%c(p(%d))->", curr->key, c, parentKey);
            if (curr->left != NULL)
                q.push(curr->left);
            if (curr->right != NULL)
                q.push(curr->right);
        }
        printf("end\n"); 
    }
}

template <class RecordManager, typename K, typename V>
void RedBlackTree<RecordManager, K, V>::printBFSOrder() {
    printBFSOrder(root);
}

#endif /* REDBLACK_TREE_H */
