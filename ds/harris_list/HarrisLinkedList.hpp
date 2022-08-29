/*
 * Copyright 2020
 *   Andreia Correia <andreia.veiga@unine.ch>
 *   Pedro Ramalhete <pramalhe@gmail.com>
 *   Pascal Felber <pascal.felber@unine.ch>
 *
 * This work is published under the MIT license. See LICENSE.txt
 */
#pragma once

#include <atomic>
#include <thread>
#include <forward_list>
#include <set>
#include <iostream>
#include <string>

#include <flock/epoch.h>

/**
 * Original Harris Linked list.
 *
 * https://www.microsoft.com/en-us/research/wp-content/uploads/2001/10/2001-disc.pdf
 * This is unsuitable to be used with Hazard Pointers as explained by Cohen in
 * "Every data structure deseres lock-free reclamation":
 * https://dl.acm.org/doi/10.1145/3276513
 *
 * Memory reclamation is done with OrcGC, which is the only scheme compatible with it
 * 
 * <p>
 * This set has three operations:
 * <ul>
 * <li>add(x)      - Lock-Free
 * <li>remove(x)   - Lock-Free
 * <li>contains(x) - Lock-Free
 * </ul><p>
 * <p>
 */
template<typename T, typename V>
class HarrisLinkedList {

public:
    struct alignas(128) Node {
        T key;
        V value;
        std::atomic<Node*> next {nullptr};

        Node(T key, V value) : key{key}, value(value) { }
    };

    // Pointers to head and tail sentinel nodes of the list
    alignas(128) Node* head;
    alignas(128) Node* tail;

public:

    HarrisLinkedList() {
        head = node_pool.new_obj(T{}, V{});
        tail = node_pool.new_obj(T{}, V{});
        head->next = tail;
    }


    // We don't expect the destructor to be called if this instance can still be in use
    ~HarrisLinkedList() {
        Node* node = head;
        while(node != nullptr) {
            Node* next = getUnmarked(node->next);
            node_pool.destruct(node);
            node = next;
        }
    }

    static mem_pool<Node> node_pool;

    static std::string className() { return "HarrisLinkedListSet" ; }

    size_t get_size() {
        size_t size = 0;
        Node* node = head->next;
        while(node != tail) {
            size++;
            node = getUnmarked(node->next);
        }
        return size;
    }

    /**
     * This method is named 'Insert()' in the original paper.
     * Taken from Figure 7 of the paper:
     * "High Performance Dynamic Lock-Free Hash Tables and List-Based Sets"
     * <p>
     * Progress Condition: Lock-Free
     *
     */
    bool add(T key, V value) {
        return with_epoch([&] () -> bool {
            Node* new_node = node_pool.new_obj(key, value);
            Node* left_node;
            do {
                Node* right_node = search (key, left_node);
                if ((right_node != tail) && (right_node->key == key)) /*T1*/ {
                    node_pool.destruct(new_node);
                    return false;
                }
                new_node->next.store(right_node);
                if (left_node->next.compare_exchange_strong(right_node, new_node)) /*C2*/
                    return true;
            } while (true); /*B3*/
        });
    }


    /**
     * This method is named 'Delete()' in the original paper.
     * Taken from Figure 7 of the paper:
     * "High Performance Dynamic Lock-Free Hash Tables and List-Based Sets"
     */
    bool remove(T key) {
        return with_epoch([&] () -> bool {
            Node* right_node;
            Node* right_node_next;
            Node* left_node;
            do {
                right_node = search (key, left_node);
                if ((right_node == tail) || (right_node->key != key)) /*T1*/
                    return false;
                right_node_next = right_node->next.load();
                if (!isMarked(right_node_next))
                    if (right_node->next.compare_exchange_strong(right_node_next, getMarked(right_node_next))) break;
            } while (true); /*B4*/
            if (!left_node->next.compare_exchange_strong(right_node, right_node_next)) /*C4*/
                right_node = search (right_node->key, left_node);
            else node_pool.retire(getUnmarked(right_node));
            return true;
        });
    }


    /**
     * This is named 'Search()' on the original paper
     * Taken from Figure 7 of the paper:
     * "High Performance Dynamic Lock-Free Hash Tables and List-Based Sets"
     * <p>
     * Progress Condition: Lock-Free
     */
    std::optional<V> find(T key) {
        return with_epoch([&] () -> std::optional<V> {
            Node* right_node;
            Node* left_node;
            right_node = search (key, left_node);
            if ((right_node == tail) || (right_node->key != key))
                return {};
            else
                return right_node->value;
        });
    }


private:

    /**
     * Progress Condition: Lock-Free
     */
    Node* search (T search_key, Node*& left_node) {
        search_again:
        do {
            Node* left_node_next;
            Node* right_node = head;
            Node* t_next = right_node->next.load(); /* 1: Find left_node and right_node */
            do {
                if (!isMarked(t_next)) {
                    left_node = right_node;
                    left_node_next = t_next;
                }
                right_node = getUnmarked(t_next);
                if (right_node == tail) break;
                t_next = right_node->next.load();
            } while (isMarked(t_next) || (right_node->key < search_key)); /*B1*/
            /* 2: Check nodes are adjacent */
            if (left_node_next == right_node)
                if ((right_node != tail) && isMarked(right_node->next.load())) goto search_again; /*G1*/
            else
                return right_node; /*R1*/
            /* 3: Remove one or more marked nodes */
            if (left_node->next.compare_exchange_strong(left_node_next, right_node)) /*C1*/  {
                Node* to_free = getUnmarked(left_node_next);
                while(to_free != right_node) {
                    node_pool.retire(to_free);
                    to_free = getUnmarked(to_free->next);
                }
                if ((right_node != tail) && isMarked(right_node->next.load())) 
                    goto search_again; /*G2*/
                else
                    return right_node; /*R2*/
            }
                
        } while (true); /*B2*/
    }

    bool isMarked(Node * node) {
        return ((size_t) node & 0x1ULL);
    }

    Node * getMarked(Node * node) {
        return (Node*)((size_t) node | 0x1ULL);
    }

    Node * getUnmarked(Node * node) {
        return (Node*)((size_t) node & (~0x1ULL));
    }
};

template<typename T, typename V>
mem_pool<typename HarrisLinkedList<T,V>::Node> HarrisLinkedList<T,V>::node_pool;
