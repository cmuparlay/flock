/* 
 * File:   intlf.h
 * Author: Trevor Brown
 *
 * Substantial improvements to interface, memory reclamation and bug fixing.
 *
 * Created on June 7, 2017, 4:00 PM
 */

#ifndef INTLF_H
#define INTLF_H

#include "record_manager.h"

#define LEFT 0
#define RIGHT 1

#define KEY_MASK 0x8000000000000000
#define ADDRESS_MASK 15

#define NULL_BIT 8
#define INJECT_BIT 4
#define DELETE_BIT 2
#define PROMOTE_BIT 1

#define isIFlagSet(p) (((uintptr_t) (p) & INJECT_BIT) != 0)
#define isNull(p) (((uintptr_t) (p) & NULL_BIT) != 0)
#define isDFlagSet(p) (((uintptr_t) (p) & DELETE_BIT) != 0)
#define isPFlagSet(p) (((uintptr_t) (p) & PROMOTE_BIT) != 0)
#define isKeyMarked(key) (((key) & KEY_MASK) == KEY_MASK)
#define setIFlag(p) ((node_t<skey_t, sval_t> *) ((uintptr_t) (p) | INJECT_BIT))
#define setNull(p) ((node_t<skey_t, sval_t> *) ((uintptr_t) (p) | NULL_BIT))
#define setDFlag(p) ((node_t<skey_t, sval_t> *) ((uintptr_t) (p) | DELETE_BIT))
#define setPFlag(p) ((node_t<skey_t, sval_t> *) ((uintptr_t) (p) | PROMOTE_BIT))
#define setReplaceFlagInKey(key) ((key) | KEY_MASK)
#define getKey(key) ((key) & ~KEY_MASK)
#define getAddress(p) ((node_t<skey_t, sval_t> *) ((uintptr_t) (p) & ~ADDRESS_MASK))

#define CAS(parent, which, oldChild, newChild) CASB(&parent->child[which], oldChild, newChild)

typedef enum {
    INJECTION, DISCOVERY, CLEANUP
} Mode;

typedef enum {
    SIMPLE, COMPLEX
} Type;

typedef enum {
    DELETE_FLAG, PROMOTE_FLAG
} Flag;

template <typename skey_t, typename sval_t>
struct node_t {
    volatile skey_t markAndKey; //format <markFlag,address>
    node_t<skey_t, sval_t> * volatile child[2]; //format <address,NullBit,InjectFlag,DeleteFlag,PromoteFlag>
    volatile unsigned long readyToReplace;
    sval_t value;
};

template <typename skey_t, typename sval_t>
struct edge {
    node_t<skey_t, sval_t> * parent;
    node_t<skey_t, sval_t> * child;
    int which;
};

template <typename skey_t, typename sval_t>
struct seekRecord {
    edge<skey_t, sval_t> lastEdge;
    edge<skey_t, sval_t> pLastEdge;
    edge<skey_t, sval_t> injectionEdge;
};

template <typename skey_t, typename sval_t>
struct anchorRecord {
    node_t<skey_t, sval_t> * node;
    skey_t key;
};

template <typename skey_t, typename sval_t>
struct stateRecord {
    int depth;
    edge<skey_t, sval_t> targetEdge;
    edge<skey_t, sval_t> pTargetEdge;
    skey_t targetKey;
    skey_t currentKey;
    //sval_t oldValue;
    Mode mode;
    Type type;
    seekRecord<skey_t, sval_t> successorRecord;
};

template <typename skey_t, typename sval_t>
struct tArgs {
    int tid;
    node_t<skey_t, sval_t> * newNode;
    bool isNewNodeAvailable;
    seekRecord<skey_t, sval_t> targetRecord;
    seekRecord<skey_t, sval_t> pSeekRecord;
    stateRecord<skey_t, sval_t> myState;
    struct anchorRecord<skey_t, sval_t> anchorRecord;
    struct anchorRecord<skey_t, sval_t> pAnchorRecord;
};

template <typename skey_t, typename sval_t, class RecMgr>
class intlf {
private:
PAD;
    const unsigned int idx_id;
PAD;
    node_t<skey_t, sval_t> * R;
    node_t<skey_t, sval_t> * S;
    node_t<skey_t, sval_t> * T;
PAD;
    const int NUM_THREADS;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
PAD;
    RecMgr * const recmgr;
PAD;
    int init[MAX_THREADS_POW2] = {0,}; // this suffers from false sharing, but is only touched once per thread! so no worries.
PAD;

    void populateEdge(edge<skey_t, sval_t>* e, node_t<skey_t, sval_t> * parent, node_t<skey_t, sval_t> * child, int which);
    void copyEdge(edge<skey_t, sval_t>* d, edge<skey_t, sval_t>* s);
    void copySeekRecord(seekRecord<skey_t, sval_t>* d, seekRecord<skey_t, sval_t>* s);
    node_t<skey_t, sval_t> * newLeafNode(tArgs<skey_t, sval_t>*, skey_t key, sval_t value);
    void seek(tArgs<skey_t, sval_t>*, skey_t, seekRecord<skey_t, sval_t>*);
    void initializeTypeAndUpdateMode(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    void updateMode(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    void inject(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    void findSmallest(tArgs<skey_t, sval_t>*, node_t<skey_t, sval_t> *, seekRecord<skey_t, sval_t>*);
    void findAndMarkSuccessor(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    void removeSuccessor(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    bool cleanup(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*);
    bool markChildEdge(tArgs<skey_t, sval_t>*, stateRecord<skey_t, sval_t>*, bool);
    void helpTargetNode(tArgs<skey_t, sval_t>*, edge<skey_t, sval_t>*, int);
    void helpSuccessorNode(tArgs<skey_t, sval_t>*, edge<skey_t, sval_t>*, int);
    node_t<skey_t, sval_t> * simpleSeek(skey_t key, seekRecord<skey_t, sval_t>* s);
    sval_t search(tArgs<skey_t, sval_t>* t, skey_t key);
    sval_t lf_insert(tArgs<skey_t, sval_t>* t, skey_t key, sval_t value);
    bool lf_remove(tArgs<skey_t, sval_t>* t, skey_t key);
    
public:

    intlf(const int _NUM_THREADS, const skey_t& _KEY_MIN, const skey_t& _KEY_MAX, const sval_t& _VALUE_RESERVED, unsigned int id)
    : idx_id(id), NUM_THREADS(_NUM_THREADS), KEY_MIN(_KEY_MIN), KEY_MAX(_KEY_MAX), NO_VALUE(_VALUE_RESERVED), recmgr(new RecMgr(NUM_THREADS)) {
        const int tid = 0;
        initThread(tid);

        recmgr->endOp(tid); // enter an initial quiescent state.
        tArgs<skey_t, sval_t> args = {0}; 
        args.tid = tid;
        
        R = newLeafNode(&args, KEY_MAX, NO_VALUE);
        R->child[RIGHT] = newLeafNode(&args, KEY_MAX, NO_VALUE);
        S = R->child[RIGHT];
	S->child[RIGHT] = newLeafNode(&args, KEY_MAX, NO_VALUE);
	T = S->child[RIGHT];
    }

    ~intlf() {
        recmgr->printStatus();
        delete recmgr;
    }

    void initThread(const int tid) {
        if (init[tid]) return;
        else init[tid] = !init[tid];
        recmgr->initThread(tid);
    }

    void deinitThread(const int tid) {
        if (!init[tid]) return;
        else init[tid] = !init[tid];
        recmgr->deinitThread(tid);
    }

    node_t<skey_t, sval_t> * get_root() {
        return R;
    }
    
    RecMgr * debugGetRecMgr() {
        return recmgr;
    }

    sval_t insert(const int tid, skey_t key, sval_t item) {
        assert(key < KEY_MAX);
        tArgs<skey_t, sval_t> args = {0}; 
        args.tid = tid;
        return lf_insert(&args, key, item);
    }

    bool remove(const int tid, skey_t key) {
        assert(key < KEY_MAX);
        tArgs<skey_t, sval_t> args = {0}; 
        args.tid = tid;
        return lf_remove(&args, key);
    }

    sval_t find(const int tid, skey_t key) {
        tArgs<skey_t, sval_t> args = {0}; 
        args.tid = tid;
        return search(&args, key);
    }
};

template <typename skey_t, typename sval_t, class RecMgr>
inline node_t<skey_t, sval_t> * intlf<skey_t, sval_t, RecMgr>::newLeafNode(tArgs<skey_t, sval_t>* t, skey_t key, sval_t value) {
    auto result = recmgr->template allocate<node_t<skey_t, sval_t>>(t->tid);
    if (((uint64_t) result) & ADDRESS_MASK) setbench_error("node address has invalid alignment for this tree---node address must be a multiple of 16 (this data structure implicitly assumes an allocator that gives you nodes with this alignment)");
    result->markAndKey = key;
    result->value = value;
    result->child[LEFT] = setNull(NULL);
    result->child[RIGHT] = setNull(NULL);
    result->readyToReplace = false;
    return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::populateEdge(struct edge<skey_t, sval_t>* e, node_t<skey_t, sval_t> * parent, node_t<skey_t, sval_t> * child, int which) {
    e->parent = parent;
    e->child = child;
    e->which = which;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::copyEdge(struct edge<skey_t, sval_t>* d, struct edge<skey_t, sval_t>* s) {
    d->parent = s->parent;
    d->child = s->child;
    d->which = s->which;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::copySeekRecord(seekRecord<skey_t, sval_t>* d, seekRecord<skey_t, sval_t>* s) {
    copyEdge(&d->lastEdge, &s->lastEdge);
    copyEdge(&d->pLastEdge, &s->pLastEdge);
    copyEdge(&d->injectionEdge, &s->injectionEdge);
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::seek(tArgs<skey_t, sval_t>* t, skey_t key, seekRecord<skey_t, sval_t>* s) {
    anchorRecord<skey_t, sval_t>* pAnchorRecord;
    anchorRecord<skey_t, sval_t>* anchorRecord;

    struct edge<skey_t, sval_t> pLastEdge;
    struct edge<skey_t, sval_t> lastEdge;

    node_t<skey_t, sval_t> * curr;
    node_t<skey_t, sval_t> * next;
    node_t<skey_t, sval_t> * temp;
    int which;

    bool n;
    bool d;
    bool p;
    skey_t cKey;
    skey_t aKey;

    pAnchorRecord = &t->pAnchorRecord;
    anchorRecord = &t->anchorRecord;

    pAnchorRecord->node = S;
    pAnchorRecord->key = KEY_MAX;

    while (true) {
        //initialize all variables used in traversal
        populateEdge(&pLastEdge, R, S, RIGHT);
        populateEdge(&lastEdge, S, T, RIGHT);
        curr = T;
        anchorRecord->node = S;
        anchorRecord->key = KEY_MAX;
        while (true) {
            //read the key stored in the current node
            cKey = getKey(curr->markAndKey);
            //find the next edge to follow
            which = key < cKey ? LEFT : RIGHT;
            temp = curr->child[which];
            n = isNull(temp);
            d = isDFlagSet(temp);
            p = isPFlagSet(temp);
            next = getAddress(temp);
            //std::cout<<"ckey="<<cKey<<" search key="<<key<<" which="<<which<<" n="<<n<<" d="<<d<<" p="<<p<<" next="<<next<<std::endl;
            //check for completion of the traversal
            if (key == cKey || n) {
                //either key found or no next edge to follow. Stop the traversal
                s->pLastEdge = pLastEdge;
                s->lastEdge = lastEdge;
                populateEdge(&s->injectionEdge, curr, next, which);
                if (key == cKey) {
                    //key matches. So return
                    return;
                } else {
                    break;
                }
            }
            if (which == RIGHT) {
                //the next edge that will be traversed is a right edge. Keep track of the current node and its key
                anchorRecord->node = curr;
                anchorRecord->key = cKey;
            }
            //traverse the next edge
            pLastEdge = lastEdge;
            populateEdge(&lastEdge, curr, next, which);
            curr = next;
        }
        //key was not found. check if can stop
        temp = anchorRecord->node->child[RIGHT];
        d = isDFlagSet(temp);
        p = isPFlagSet(temp);
        if (!d && !p) {
            //the anchor node is part of the tree. Return the results of the current traversal
            aKey = getKey(anchorRecord->node->markAndKey);
            if (anchorRecord->key == aKey) return;
        } else {
            if (pAnchorRecord->node == anchorRecord->node && pAnchorRecord->key == anchorRecord->key) {
                //return the results of previous traversal
                copySeekRecord(s, &t->pSeekRecord);
                return;
            }
        }
        //store the results of the current traversal and restart
        copySeekRecord(&t->pSeekRecord, s);
        pAnchorRecord->node = anchorRecord->node;
        pAnchorRecord->key = anchorRecord->key;
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::initializeTypeAndUpdateMode(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    node_t<skey_t, sval_t> * node;
    //retrieve the address from the state record
    node = state->targetEdge.child;
    if (isNull(node->child[LEFT]) || isNull(node->child[RIGHT])) {
        //one of the child pointers is null
        if (isKeyMarked(node->markAndKey)) {
            state->type = COMPLEX;
        } else {
            state->type = SIMPLE;
        }
    } else {
        //both the child pointers are non-null
        state->type = COMPLEX;
    }
    updateMode(t, state);
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::updateMode(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    node_t<skey_t, sval_t> * node;
    //retrieve the address from the state record
    node = state->targetEdge.child;

    //update the operation mode
    if (state->type == SIMPLE) {
        //simple delete
        state->mode = CLEANUP;
    } else {
        //complex delete
        if (node->readyToReplace) {
            assert(isKeyMarked(node->markAndKey));
            state->mode = CLEANUP;
        } else {
            state->mode = DISCOVERY;
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::inject(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    node_t<skey_t, sval_t> * parent;
    node_t<skey_t, sval_t> * node;
    struct edge<skey_t, sval_t> targetEdge;
    int which;
    bool result;
    bool i;
    bool d;
    bool p;
    node_t<skey_t, sval_t> * temp;

    targetEdge = state->targetEdge;
    parent = targetEdge.parent;
    node = targetEdge.child;
    which = targetEdge.which;

    result = CAS(parent, which, node, setIFlag(node));
    if (!result) {
        //unable to set the intention flag on the edge. help if needed
        temp = parent->child[which];
        i = isIFlagSet(temp);
        d = isDFlagSet(temp);
        p = isPFlagSet(temp);

        if (i) {
            helpTargetNode(t, &targetEdge, 1);
        } else if (d) {
            helpTargetNode(t, &state->pTargetEdge, 1);
        } else if (p) {
            helpSuccessorNode(t, &state->pTargetEdge, 1);
        }
        return;
    }
    //mark the left edge for deletion
    result = markChildEdge(t, state, LEFT);
    if (!result) return;
    //mark the right edge for deletion
    result = markChildEdge(t, state, RIGHT);
//    state->oldValue = state->targetEdge.child->value; /////// modification to allow deletion to return the old value

    //initialize the type and mode of the operation
    initializeTypeAndUpdateMode(t, state);
}

template <typename skey_t, typename sval_t, class RecMgr>
bool intlf<skey_t, sval_t, RecMgr>::markChildEdge(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state, bool which) {
    node_t<skey_t, sval_t> * node;
    struct edge<skey_t, sval_t> edge;
    Flag flag;
    node_t<skey_t, sval_t> * address;
    node_t<skey_t, sval_t> * temp;
    bool n;
    bool i;
    bool d;
    bool p;
    struct edge<skey_t, sval_t> helpeeEdge;
    node_t<skey_t, sval_t> * oldValue;
    node_t<skey_t, sval_t> * newValue;
    bool result;

    if (state->mode == INJECTION) {
        edge = state->targetEdge;
        flag = DELETE_FLAG;
    } else {
        edge = state->successorRecord.lastEdge;
        flag = PROMOTE_FLAG;
    }
    node = edge.child;
    while (true) {
        temp = node->child[which];
        n = isNull(temp);
        i = isIFlagSet(temp);
        d = isDFlagSet(temp);
        p = isPFlagSet(temp);
        address = getAddress(temp);
        if (i) {
            populateEdge(&helpeeEdge, node, address, which);
            helpTargetNode(t, &helpeeEdge, state->depth + 1);
            continue;
        } else if (d) {
            if (flag == PROMOTE_FLAG) {
                helpTargetNode(t, &edge, state->depth + 1);
                return false;
            } else {
                return true;
            }
        } else if (p) {
            if (flag == DELETE_FLAG) {
                helpSuccessorNode(t, &edge, state->depth + 1);
                return false;
            } else {
                return true;
            }
        }
        oldValue = n ? setNull(address) : address;
        newValue = (flag == DELETE_FLAG) ? setDFlag(oldValue) : setPFlag(oldValue);
        if (!CAS(node, which, oldValue, newValue)) {
            continue;
        } else {
            break;
        }
    }
    return true;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::findSmallest(tArgs<skey_t, sval_t>* t, node_t<skey_t, sval_t> * node, seekRecord<skey_t, sval_t>* s) {
    node_t<skey_t, sval_t> * curr;
    node_t<skey_t, sval_t> * left;
    node_t<skey_t, sval_t> * temp;
    bool n;
    struct edge<skey_t, sval_t> lastEdge;
    struct edge<skey_t, sval_t> pLastEdge;

    //find the node with the smallest key in the subtree rooted at the right child
    //initialize the variables used in the traversal
    auto right = getAddress(node->child[RIGHT]);
    populateEdge(&lastEdge, node, right, RIGHT);
    populateEdge(&pLastEdge, node, right, RIGHT);
    while (true) {
        curr = lastEdge.child;
        temp = curr->child[LEFT];
        n = isNull(temp);
        left = getAddress(temp);
        if (n) break;
        //traverse the next edge
        pLastEdge = lastEdge;
        populateEdge(&lastEdge, curr, left, LEFT);
    }
    //initialize seek record and return
    s->lastEdge = lastEdge;
    s->pLastEdge = pLastEdge;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::findAndMarkSuccessor(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    struct edge<skey_t, sval_t> successorEdge;
    bool m;
    bool n;
    bool d;
    bool p;
    node_t<skey_t, sval_t> * temp;
    node_t<skey_t, sval_t> * left;

    //retrieve the addresses from the state record
    auto node = state->targetEdge.child;
    auto s = &state->successorRecord;
    while (true) {
        //read the mark flag of the key in the target node
        m = isKeyMarked(node->markAndKey);
        //find the node with the smallest key in the right subtree
        findSmallest(t, node, s);
        if (m) {
            //successor node has already been selected before the traversal
            break;
        }
        //retrieve the information from the seek record
        successorEdge = s->lastEdge;
        temp = successorEdge.child->child[LEFT];
        n = isNull(temp);
        p = isPFlagSet(temp);
        left = getAddress(temp);
        if (!n) {
            continue;
        }
        //read the mark flag of the key under deletion
        m = isKeyMarked(node->markAndKey);
        if (m) {
            //successor node has already been selected
            if (p) {
                break;
            } else {
                continue;
            }
        }
        //try to set the promote flag on the left edge
        if (CAS(successorEdge.child, LEFT, setNull(left), setPFlag(setNull(node)))) break;
        //attempt to mark the edge failed; recover from the failure and retry if needed
        temp = successorEdge.child->child[LEFT];
        n = isNull(temp);
        d = isDFlagSet(temp);
        p = isPFlagSet(temp);
        if (p) break;
        if (!n) continue; //the node found has since gained a left child
        if (d) helpTargetNode(t, &s->lastEdge, state->depth + 1); //the node found is undergoing deletion; need to help
    }
    // update the operation mode
    updateMode(t, state);
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::removeSuccessor(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    node_t<skey_t, sval_t> * node;
    seekRecord<skey_t, sval_t>* s;
    struct edge<skey_t, sval_t> successorEdge;
    struct edge<skey_t, sval_t> pLastEdge;
    node_t<skey_t, sval_t> * temp;
    node_t<skey_t, sval_t> * right;
    node_t<skey_t, sval_t> * address;
    node_t<skey_t, sval_t> * oldValue;
    node_t<skey_t, sval_t> * newValue;
    bool n;
    bool d;
    bool p;
    bool i;
    bool dFlag;
    bool which;
    bool result;

    //retrieve addresses from the state record
    node = state->targetEdge.child;
    s = &state->successorRecord;
    findSmallest(t, node, s);
    //extract information about the successor node
    successorEdge = s->lastEdge;
    //ascertain that the seek record for the successor node contains valid information
    temp = successorEdge.child->child[LEFT];
    p = isPFlagSet(temp);
    address = getAddress(temp);
    if (address != node) {
        node->readyToReplace = true;
        updateMode(t, state);
        return;
    }
    if (!p) {
        node->readyToReplace = true;
        updateMode(t, state);
        return;
    }

    //mark the right edge for promotion if unmarked
    temp = successorEdge.child->child[RIGHT];
    p = isPFlagSet(temp);
    if (!p) {
        //set the promote flag on the right edge
        markChildEdge(t, state, RIGHT);
    }
    //promote the key
    node->markAndKey = setReplaceFlagInKey(successorEdge.child->markAndKey);
    while (true) {
        //check if the successor is the right child of the target node itself
        if (successorEdge.parent == node) {
            dFlag = true;
            which = RIGHT;
        } else {
            dFlag = false;
            which = LEFT;
        }
        i = isIFlagSet(successorEdge.parent->child[which]);
        temp = successorEdge.child->child[RIGHT];
        n = isNull(temp);
        right = getAddress(temp);
        if (n) {
            //only set the null flag. do not change the address
            if (i) {
                if (dFlag) {
                    oldValue = setIFlag(setDFlag(successorEdge.child));
                    newValue = setNull(setDFlag(successorEdge.child));
                } else {
                    oldValue = setIFlag(successorEdge.child);
                    newValue = setNull(successorEdge.child);
                }
            } else {
                if (dFlag) {
                    oldValue = setDFlag(successorEdge.child);
                    newValue = setNull(setDFlag(successorEdge.child));
                } else {
                    oldValue = successorEdge.child;
                    newValue = setNull(successorEdge.child);
                }
            }
            result = CAS(successorEdge.parent, which, oldValue, newValue);
        } else {
            if (i) {
                if (dFlag) {
                    oldValue = setIFlag(setDFlag(successorEdge.child));
                    newValue = setDFlag(right);
                } else {
                    oldValue = setIFlag(successorEdge.child);
                    newValue = right;
                }
            } else {
                if (dFlag) {
                    oldValue = setDFlag(successorEdge.child);
                    newValue = setDFlag(right);
                } else {
                    oldValue = successorEdge.child;
                    newValue = right;
                }
            }
            result = CAS(successorEdge.parent, which, oldValue, newValue);
        }
        if (result) break;
        if (dFlag) break;
        temp = successorEdge.parent->child[which];
        d = isDFlagSet(temp);
        pLastEdge = s->pLastEdge;
        if (d && pLastEdge.parent != NULL) {
            helpTargetNode(t, &pLastEdge, state->depth + 1);
        }
        findSmallest(t, node, s);
        if (s->lastEdge.child != successorEdge.child) {
            //the successor node has already been removed
            break;
        } else {
            successorEdge = s->lastEdge;
        }
    }
    node->readyToReplace = true;
    updateMode(t, state);
}

template <typename skey_t, typename sval_t, class RecMgr>
bool intlf<skey_t, sval_t, RecMgr>::cleanup(tArgs<skey_t, sval_t>* t, stateRecord<skey_t, sval_t>* state) {
    node_t<skey_t, sval_t> * parent;
    node_t<skey_t, sval_t> * node;
    node_t<skey_t, sval_t> * left;
    node_t<skey_t, sval_t> * right;
    node_t<skey_t, sval_t> * address;
    node_t<skey_t, sval_t> * temp;
    bool pWhich;
    bool nWhich;
    bool result;
    bool n;

    //retrieve the addresses from the state record
    parent = state->targetEdge.parent;
    node = state->targetEdge.child;
    pWhich = state->targetEdge.which;

    if (state->type == COMPLEX) {
        //replace the node with a new copy in which all the fields are unmarked
        auto newNode = recmgr->template allocate<node_t<skey_t, sval_t>>(t->tid);
        newNode->markAndKey = getKey(node->markAndKey);
        newNode->value = 0;
        newNode->readyToReplace = false;
        left = getAddress(node->child[LEFT]);
        newNode->child[LEFT] = left;
        temp = node->child[RIGHT];
        n = isNull(temp);
        right = getAddress(temp);
        newNode->child[RIGHT] = n ? setNull(NULL) : right;

        //switch the edge at the parent
        result = CAS(parent, pWhich, setIFlag(node), newNode);
    } else {
        //remove the node. determine to which grand child will the edge at the parent be switched
        nWhich = (isNull(node->child[LEFT])) ? RIGHT : LEFT;
        temp = node->child[nWhich];
        n = isNull(temp);
        address = getAddress(temp);
        result = CAS(parent, pWhich, setIFlag(node), n ? setNull(node) : address);
    }
    return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::helpTargetNode(tArgs<skey_t, sval_t>* t, struct edge<skey_t, sval_t>* helpeeEdge, int depth) {
    // intention flag must be set on the edge
    // obtain new state record and initialize it
    auto state = recmgr->template allocate<stateRecord<skey_t, sval_t>>(t->tid);
    state->targetEdge = *helpeeEdge;
    state->depth = depth;
    state->mode = INJECTION;

    if (!markChildEdge(t, state, LEFT)) return; // mark the left and right edges if unmarked
    markChildEdge(t, state, RIGHT);
    initializeTypeAndUpdateMode(t, state);
    if (state->mode == DISCOVERY) {
        findAndMarkSuccessor(t, state);
    }

    if (state->mode == DISCOVERY) {
        removeSuccessor(t, state);
    }
    if (state->mode == CLEANUP) {
        cleanup(t, state);
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
void intlf<skey_t, sval_t, RecMgr>::helpSuccessorNode(tArgs<skey_t, sval_t>* t, struct edge<skey_t, sval_t>* helpeeEdge, int depth) {
    node_t<skey_t, sval_t> * parent;
    node_t<skey_t, sval_t> * node;
    node_t<skey_t, sval_t> * left;
    seekRecord<skey_t, sval_t>* s;
    // retrieve the address of the successor node
    parent = helpeeEdge->parent;
    node = helpeeEdge->child;
    // promote flag must be set on the successor node's left edge
    // retrieve the address of the target node
    left = getAddress(node->child[LEFT]);
    // obtain new state record and initialize it
    auto state = recmgr->template allocate<stateRecord<skey_t, sval_t>>(t->tid);
    populateEdge(&state->targetEdge, NULL, left, LEFT);
    state->depth = depth;
    state->mode = DISCOVERY;
    s = &state->successorRecord;
    // initialize the seek record in the state record
    s->lastEdge = *helpeeEdge;
    populateEdge(&s->pLastEdge, NULL, parent, LEFT);
    // remove the successor node
    removeSuccessor(t, state);
}

template <typename skey_t, typename sval_t, class RecMgr>
node_t<skey_t, sval_t> * intlf<skey_t, sval_t, RecMgr>::simpleSeek(skey_t key, seekRecord<skey_t, sval_t>* s) {
    anchorRecord<skey_t, sval_t> pAnchorRecord;
    anchorRecord<skey_t, sval_t> anchorRecord;

    node_t<skey_t, sval_t> * lastTraversalResult = NULL;
    node_t<skey_t, sval_t> * curr;
    node_t<skey_t, sval_t> * next;
    node_t<skey_t, sval_t> * temp;
    int which;

    bool n;
    bool d;
    bool p;
    skey_t cKey;
    skey_t aKey;

    pAnchorRecord.node = S;
    pAnchorRecord.key = KEY_MAX;

    while (true) {
        //initialize all variables used in traversal
        //populateEdge(&pLastEdge,R,S,RIGHT);
        //populateEdge(&lastEdge,S,T,RIGHT);
        curr = T;
        anchorRecord.node = S;
        anchorRecord.key = KEY_MAX;
        while (true) {
            //read the key stored in the current node
            cKey = getKey(curr->markAndKey);
            //find the next edge to follow
            which = key < cKey ? LEFT : RIGHT;
            temp = curr->child[which];
            n = isNull(temp);
            d = isDFlagSet(temp);
            p = isPFlagSet(temp);
            next = getAddress(temp);
            //check for completion of the traversal
            if (key == cKey || n) {
                //either key found or no next edge to follow. Stop the traversal
                //s->pLastEdge = pLastEdge;
                //s->lastEdge = lastEdge;
                //populateEdge(&s->injectionEdge,curr,next,which);
                if (key == cKey) {
                    //key matches. So return
                    return curr;
                } else {
                    break;
                }
            }
            if (which == RIGHT) {
                //the next edge that will be traversed is a right edge. Keep track of the current node and its key
                anchorRecord.node = curr;
                anchorRecord.key = cKey;
            }
            //traverse the next edge
            //pLastEdge = lastEdge;
            //populateEdge(&lastEdge,curr,next,which);
            curr = next;
        }
        //key was not found. check if can stop
        aKey = getKey(anchorRecord.node->markAndKey);
        if (anchorRecord.key == aKey) {
            temp = anchorRecord.node->child[RIGHT];
            d = isDFlagSet(temp);
            p = isPFlagSet(temp);
            if (!d && !p) {
                //the anchor node is part of the tree. Return the results of the current traversal
                return NULL;
            }
            if (pAnchorRecord.node == anchorRecord.node && pAnchorRecord.key == anchorRecord.key) {
                //return the results of previous traversal
                return lastTraversalResult;
            }
        }
        //store the results of the current traversal and restart
        lastTraversalResult = curr;
        pAnchorRecord.node = anchorRecord.node;
        pAnchorRecord.key = anchorRecord.key;
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t intlf<skey_t, sval_t, RecMgr>::search(tArgs<skey_t, sval_t>* t, skey_t key) {
    auto guard = recmgr->getGuard(t->tid, true);

#ifndef SIMPLE_SEEK
    seek(t, key, &t->targetRecord);
    auto node = t->targetRecord.lastEdge.child;
    skey_t nKey = getKey(node->markAndKey);
    return (nKey == key) ? node->value : NO_VALUE;
#else
    node = simpleSeek(key, &t->targetRecord);
    return (node == NULL) ? NO_VALUE : node->value;
#endif
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t intlf<skey_t, sval_t, RecMgr>::lf_insert(tArgs<skey_t, sval_t>* t, skey_t key, sval_t value) {
    //std::cout<<"insert "<<key<<std::endl;
    while (true) {
        auto guard = recmgr->getGuard(t->tid);
        
        seek(t, key, &t->targetRecord);
        auto node = t->targetRecord.lastEdge.child;
        auto nKey = getKey(node->markAndKey);
        if (nKey == key) return node->value;

        // create a new node and initialize its fields
        auto newNode = newLeafNode(t, key, value);
        newNode->markAndKey = key;
        newNode->value = value;
        auto which = t->targetRecord.injectionEdge.which;
        auto address = t->targetRecord.injectionEdge.child;
        if (CAS(node, which, setNull(address), newNode)) {
            return NO_VALUE;
        } else {
            recmgr->deallocate(t->tid, newNode);
        }
        
        auto temp = node->child[which];
        if (isDFlagSet(temp)) {
            helpTargetNode(t, &t->targetRecord.lastEdge, 1);
        } else if (isPFlagSet(temp)) {
            helpSuccessorNode(t, &t->targetRecord.lastEdge, 1);
        }
    }
}

template <typename skey_t, typename sval_t, class RecMgr>
bool intlf<skey_t, sval_t, RecMgr>::lf_remove(tArgs<skey_t, sval_t>* t, skey_t key) {
    // initialize the state record
    auto myState = &t->myState;
    myState->depth = 0;
    myState->targetKey = key;
    myState->currentKey = key;
    myState->mode = INJECTION;
    //myState->oldValue = NO_VALUE;
    auto targetRecord = &t->targetRecord;

    while (true) {
        auto guard = recmgr->getGuard(t->tid);
        
        seek(t, myState->currentKey, targetRecord);
        auto targetEdge = targetRecord->lastEdge;
        auto pTargetEdge = targetRecord->pLastEdge;
        auto nKey = getKey(targetEdge.child->markAndKey);
        if (myState->currentKey != nKey) {
            if (myState->mode == INJECTION) {
                return false;
                //return NO_VALUE; //the key does not exist in the tree
            } else {
                return true;
                //return myState->oldValue; // set by inject()
            }
        }
        //perform appropriate action depending on the mode
        if (myState->mode == INJECTION) {
            //store a reference to the target node
            myState->targetEdge = targetEdge;
            myState->pTargetEdge = pTargetEdge;
            //attempt to inject the operation at the node
            inject(t, myState);
        }
        //mode would have changed if the operation was injected successfully
        if (myState->mode != INJECTION) {
            //if the target node found by the seek function is different from the one stored in state record, then return
            if (myState->targetEdge.child != targetEdge.child) return true; //return myState->oldValue;
            //update the target edge using the most recent seek
            myState->targetEdge = targetEdge;
        }
        if (myState->mode == DISCOVERY) findAndMarkSuccessor(t, myState);
        if (myState->mode == DISCOVERY) removeSuccessor(t, myState);
        if (myState->mode == CLEANUP) {
            if (cleanup(t, myState)) {
                return true; //myState->oldValue;
            } else {
                nKey = getKey(targetEdge.child->markAndKey);
                myState->currentKey = nKey;
            }
        }
    }
}

#endif /* INTLF_H */
