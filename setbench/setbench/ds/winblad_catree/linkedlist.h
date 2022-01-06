/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   linkedlist.h
 * Author: ginns
 *
 * Created on April 19, 2021, 9:05 PM
 */

#ifndef LINKEDLIST_H
#define LINKEDLIST_H
#include <cassert>
#include <iostream>

#include "interfaces.h"

#define INVALID_KEY     0

/**
 * Sequential Linked List with a head and tail pointer
 */
using namespace std;

template <class RecordManager, typename K, typename V>
class LinkedList : public IOSet {
private:
    struct Node {
        K key;
        V val;
        Node * next;
        
        Node(K _key, V _val, Node * _next) : key(_key), val{_val}, next(_next) {}
    };
    
    Node * head;
    Node * tail;
    size_t size;
    const V NO_VALUE = (V) 0;
    
public:
    
    LinkedList();
    ~LinkedList();
     
    /* Dictionary operations  */
    V find(const int tid, const K & key);
    V insert(const int tid, const K & key, const V& val);
    V erase(const int tid, const K & key);
    
    /* */
    IOSet * join(const int tid, IOSet * rightList);
    std::tuple<K, IOSet *, IOSet *> split(const int tid);
    
    size_t getSize();
    bool isEmpty();
    size_t numKeys();
    size_t sumOfKeys();
    void printKeys();
    bool checkSortedOrder();
   
};

template <class RecordManager, typename K, typename V>
LinkedList<RecordManager, K, V>::LinkedList() {
    size = 0;
    head = NULL;
    tail = NULL;
}

template <class RecordManager, typename K, typename V>
LinkedList<RecordManager, K, V>::~LinkedList() {
    
    Node * prev = NULL;
    Node * curr = head;;
    
    while (curr != NULL) {
        prev = curr;
        curr = curr->next;
        delete(prev);
    }
    
    size = 0;
}

template <class RecordManager, typename K, typename V>
V LinkedList<RecordManager, K, V>::find(const int tid, const K & key) {
    Node * curr = head;
    while ( (curr != NULL) && (curr->key < key) ) {
        curr = curr->next;
    }
    
    if (curr != NULL && curr->key == key)
        return curr->val;
    else
        return NO_VALUE;
}

template <class RecordManager, typename K, typename V>
V LinkedList<RecordManager, K, V>::insert(const int tid, const K & key, const V& val) {
    if (head == NULL) {
        /* empty list */
        Node * newNode = new Node(key, val, NULL);
        head = newNode;
        tail = newNode;
    }
    else {
        Node * prev = NULL;
        Node * curr = head;
        while ( (curr != NULL) && (curr->key < key) ) {
            prev = curr;
            curr = curr->next;
        }
        
        if (curr != NULL && curr->key == key)
            return curr->val; // key already present
        
        /* Insert key into list */
        Node * newNode = new Node(key, val, curr);
        
        if (prev == NULL) {
            assert(key < curr->key);
            head = newNode;
        }
        else {
            prev->next = newNode;
            if (prev == tail)
                tail = newNode;
        }
    }
    size++;
    return NO_VALUE;
}

template <class RecordManager, typename K, typename V>
V LinkedList<RecordManager, K, V>::erase(const int tid, const K& key) {
    V retval;
    if (head == NULL) {
        return NO_VALUE; /* empty list */
    }
    else {
        Node * prev = NULL;
        Node * curr = head;
        while ( (curr != NULL) && (curr->key < key) ) {
            prev = curr;
            curr = curr->next;
        }
        
        if (curr == NULL || curr->key != key)
            return NO_VALUE; // key not in list
        
        /* Delete key from list */
        
        assert(curr->key == key);
        retval = curr->val;
        
        if ( prev == NULL ) {
            assert(curr == head);
            if (head == tail) {
                head = NULL;
                tail = NULL;
            }
            else {
                head = curr->next;               
            }
        }
        else {
            prev->next = curr->next;
            if (curr == tail) {
                tail = prev;
            }
        }
        delete curr;
        
    }
    size--;
    return retval;
}

template <class RecordManager, typename K, typename V>
size_t LinkedList<RecordManager, K, V>::getSize() {
    return size;
}

template <class RecordManager, typename K, typename V>
bool LinkedList<RecordManager, K, V>::isEmpty() {
    return head == NULL;
}

template <class RecordManager, typename K, typename V>
size_t LinkedList<RecordManager, K, V>::numKeys() {
    int sum = 0;
    Node * curr = head;
    while (curr != NULL) {
        ++sum;
        curr = curr->next;
    }
    return sum;
}

template <class RecordManager, typename K, typename V>
size_t LinkedList<RecordManager, K, V>::sumOfKeys() {
    int sum = 0;
    Node * curr = head;
    while (curr != NULL) {
        sum += curr->key;
        curr = curr->next;
    }
    return sum;
}

template <class RecordManager, typename K, typename V>
void LinkedList<RecordManager, K, V>::printKeys() {
    cout << "start-";
    Node * curr = head;
    while (curr != NULL) {
        cout << curr->key << "-";
        curr = curr->next;
    }
    cout << "end" << endl;
}

template <class RecordManager, typename K, typename V>
bool LinkedList<RecordManager, K, V>::checkSortedOrder() {
    bool sorted = true;
    Node * prev = NULL;
    Node * curr = head;
    while ( curr != NULL ) {
        prev = curr;
        curr = curr->next;
        if (curr != NULL)
            sorted = prev->key < curr->key; 
        if (sorted == false)
            break;
    }
    return sorted;
}

template <class RecordManager, typename K, typename V>
IOSet * LinkedList<RecordManager, K, V>::join(const int tid, IOSet * rightSet) {
        
    LinkedList * rightList;
    if ( (rightList = dynamic_cast<LinkedList *>(rightSet)) == nullptr ) {
        assert(false); /* incorrect type */
    }
    
    LinkedList * newList = new LinkedList();
    LinkedList * leftList = this;
    
    /* Check if left list has keys */
    if (leftList->isEmpty()) {
        newList->head = rightList->head;
        newList->tail = rightList->tail;
        newList->size = leftList->getSize() + rightList->getSize();
        return newList;
    }
    
    /* Check if right list has keys */
    if ( rightList->isEmpty() ) {
        newList->head = leftList->head;
        newList->tail = leftList->tail;
        newList->size = newList->size = leftList->getSize() + rightList->getSize();
        return newList;
    }
    
    assert(leftList->tail->key < rightList->head->key);
    
    newList->head = leftList->head;
    newList->tail = rightList->tail;
    leftList->tail->next = rightList->head;
    newList->size = leftList->getSize() + rightList->getSize();
    return newList;
}

template <class RecordManager, typename K, typename V>
std::tuple<K, IOSet *, IOSet *> LinkedList<RecordManager, K, V>::split(const int tid) {
    
    if ( isEmpty() ) {
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    else if ( size == 1 ) {
        return std::make_tuple(INVALID_KEY, nullptr, nullptr);
    }
    
    int origSize = size;
    int splitPoint = origSize / 2;
    int count = 0;
    Node * prev = NULL;
    Node * curr = head;
    while ( count < splitPoint ) {
        prev = curr;
        curr = curr->next;
        count++;
    }

    int splitKey = curr->key;

    LinkedList * leftList = new LinkedList<RecordManager, K, V>();
    LinkedList * rightList = new LinkedList<RecordManager, K, V>();

    leftList->head = head;
    leftList->tail = prev;
    leftList->tail->next = NULL;
    leftList->size = splitPoint;

    rightList->head = curr;
    rightList->tail = tail;
    rightList->size = origSize - splitPoint;

    assert(leftList->tail->next == NULL);
    assert(rightList->tail->next == NULL);
    
    return std::make_tuple(splitKey, leftList, rightList); 
}



#endif /* LINKEDLIST_H */

