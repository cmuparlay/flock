/**
 * Preliminary C++ implementation of binary search tree using LLX/SCX.
 *
 * Copyright (C) 2014 Trevor Brown
 * This preliminary implementation is CONFIDENTIAL and may not be distributed.
 */

#ifndef SCXRECORD_H
#define	SCXRECORD_H

#define MAX_NODES 6

#include <atomic>
#include <iostream>
#include <iomanip>
#include <string>

#include "descriptors.h"

namespace bst_ns {

    template <class K, class V>
    class Node;

    std::string const NAME_OF_TYPE[33] = {
        std::string("INS"),
        std::string("DEL"),
        std::string("BLK"),
        std::string("RB1"),
        std::string("RB2"),
        std::string("PUSH"),
        std::string("W1"),
        std::string("W2"),
        std::string("W3"),
        std::string("W4"),
        std::string("W5"),
        std::string("W6"),
        std::string("W7"),
        std::string("DBL1"),
        std::string("DBL2"),
        std::string("DBL3"),
        std::string("DBL4"),
        std::string("RB1SYM"),
        std::string("RB2SYM"),
        std::string("PUSHSYM"),
        std::string("W1SYM"),
        std::string("W2SYM"),
        std::string("W3SYM"),
        std::string("W4SYM"),
        std::string("W5SYM"),
        std::string("W6SYM"),
        std::string("W7SYM"),
        std::string("DBL1SYM"),
        std::string("DBL2SYM"),
        std::string("DBL3SYM"),
        std::string("DBL4SYM"),
        std::string("REPLACE"),
        std::string("NOOP")
    };

    //int const NUM_INSERTED[33] = {
    //        3, 1, 3, 2, 3, 3,                               // ins, del, blk, rb1-2, push
    //        4, 4, 5, 5, 4, 4, 3,                            // w1-7
    //        5, 3, 5, 3,                                     // dbl1-4
    //        2, 3, 3,                                        // rb1-2sym, pushsym
    //        4, 4, 5, 5, 4, 4, 3,                            // w1-7sym
    //        5, 3, 5, 3,                                     // dbl1-4sym
    //        1,                                              // replace
    //        0                                               // no-op (dummy)
    //};
    //
    //int const NUM_TO_FREEZE[33] = {
    //        1, 3, 4, 3, 4, 4,                               // ins, del, blk, rb1-2, push
    //        5, 5, 6, 6, 5, 5, 4,                            // w1-7
    //        6, 4, 6, 4,                                     // dbl1-4
    //        3, 4, 4,                                        // rb1-2sym, pushsym
    //        5, 5, 6, 6, 5, 5, 4,                            // w1-7sym
    //        6, 4, 6, 4,                                     // dbl1-4sym
    //        1,                                              // replace
    //        0                                               // no-op (dummy)
    //};
    //
    //int const NUM_OF_NODES[33] = {
    //        2, 4, 4, 3, 4, 4,                               // ins, del, blk, rb1-2, push
    //        5, 5, 6, 6, 5, 5, 4,                            // w1-7
    //        6, 4, 6, 4,                                     // dbl1-4
    //        3, 4, 4,                                        // rb1-2sym, pushsym
    //        5, 5, 6, 6, 5, 5, 4,                            // w1-7sym
    //        6, 4, 6, 4,                                     // dbl1-4sym
    //        2,                                              // replace
    //        0                                               // no-op (dummy)
    //};

    template <class K, class V>
    class SCXRecord {
    public:
        const static int TYPE_FIND      = -1;
        const static int TYPE_INS       = 0;
        const static int TYPE_DEL       = 1;
        const static int TYPE_BLK       = 2;
        const static int TYPE_RB1       = 3;
        const static int TYPE_RB2       = 4;
        const static int TYPE_PUSH      = 5;
        const static int TYPE_W1        = 6;
        const static int TYPE_W2        = 7;
        const static int TYPE_W3        = 8;
        const static int TYPE_W4        = 9;
        const static int TYPE_W5        = 10;
        const static int TYPE_W6        = 11;
        const static int TYPE_W7        = 12;
        const static int TYPE_DBL1      = 13;
        const static int TYPE_DBL2      = 14;
        const static int TYPE_DBL3      = 15;
        const static int TYPE_DBL4      = 16;
        const static int TYPE_RB1SYM    = 17;
        const static int TYPE_RB2SYM    = 18;
        const static int TYPE_PUSHSYM   = 19;
        const static int TYPE_W1SYM     = 20;
        const static int TYPE_W2SYM     = 21;
        const static int TYPE_W3SYM     = 22;
        const static int TYPE_W4SYM     = 23;
        const static int TYPE_W5SYM     = 24;
        const static int TYPE_W6SYM     = 25;
        const static int TYPE_W7SYM     = 26;
        const static int TYPE_DBL1SYM   = 27;
        const static int TYPE_DBL2SYM   = 28;
        const static int TYPE_DBL3SYM   = 29;
        const static int TYPE_DBL4SYM   = 30;
        const static int TYPE_REPLACE   = 31;
        const static int TYPE_NOOP      = 32;
        const static int NUM_OF_OP_TYPES = 33;

        const static int STATE_INPROGRESS = 0;
        const static int STATE_COMMITTED = 1;
        const static int STATE_ABORTED = 2;

        // bitfield (least significant first):
        // 1 bit allFrozen
        // 2 bits state
        // remaining bits sequence #
        struct {
            volatile mutables_t mutables;                                       // reserved by weak descriptor transformation
            Node<K,V> * newNode;
            Node<K,V> * volatile * field;
            int numberOfNodes;
            int numberOfNodesToFreeze;
            Node<K,V> * nodes[MAX_NODES];                // array of pointers to nodes ; these are CASd to NULL as pointers nodes[i]->scxPtr are changed so that they no longer point to this scx record.
            SCXRecord<K,V> * scxRecordsSeen[MAX_NODES];  // array of pointers to scx records

            // for rqProvider
            Node<K,V> * insertedNodes[MAX_NODES+1];
            Node<K,V> * deletedNodes[MAX_NODES+1];
        } c;
        PAD; // prevent false sharing

        const static int size = sizeof(c);

        SCXRecord() { /* left blank for efficiency with custom allocators */ }
        SCXRecord(const SCXRecord<K,V>& op) { /* left blank for efficiency with custom allocators */ }
    };

}

#endif	/* OPERATION_H */

