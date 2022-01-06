/* 
 * Modular implementation of LLX and SCX.
 * 
 * These primitives were introduced in my PODC'13 paper:
 *  Pragmatic primitives for non-blocking data structures.
 * 
 * This implementation applies the weak descriptor optimization described in my DISC 2017 paper:
 *  Reuse, don't recycle -- transforming lock-free algorithms that throw away descriptors.
 * 
 * Usage:
 *  1. Include this header.
 *  2. Create an SCXProvider object, specifying the template parameter
 *     MaxNodeDependenciesPerSCX, which should be an upper bound on the
 *     maximum number of nodes that any single SCX operation will depend on.
 *  3. In an update that will perform SCX, you will ultimately invoke scxExecute
 *     to perform an SCX. Before you can do this, you must invoke scxInit,
 *     and then perform one or more invocations of scxAddNode to add nodes
 *     that the SCX operation will depend on. Before performing
 *     scxAddNode(tid, node, ...), you must perform LLX(tid, node, ...),
 *     and the return value of this LLX must satisfy isSuccessfulLLXResult.
 *     The return value of this LLX is then fed to scxAddNode.
 *     After performing your successful LLX(s), scxInit and scxAddNode(s),
 *     you can invoke scxExecute, which will return true if it succeeded,
 *     and false otherwise.
 * 
 * Author: Trevor Brown (C) 2018
 * Created on April 4, 2018, 4:11 PM
 */

#ifndef SCX_H
#define SCX_H

#include "plaf.h"
#include "descriptors.h"
#include <cstring>

// NodeT must contain fields:
//   volatile size_t marked                                                     --- note: any primitive type will do, as long as it is word aligned, and is the only data stored in its word
//   volatile scx_handle_t scxPtr                                               --- note: this must be word aligned (and should be word sized)
//
// The user must call SCXProvider::initNode(node) on each allocated node (to initialize its marked and scxPtr fields) before inserting the node into the data structure.

typedef tagptr_t scx_handle_t;

template <typename NodeT, int MaxNodeDependenciesPerSCX>
class SCXProvider {
private:
    
    struct SCXRecord {
        const static int STATE_INPROGRESS = 0;
        const static int STATE_COMMITTED = 1;
        const static int STATE_ABORTED = 2;    
        
        struct {
            volatile mutables_t mutables;                           // mutable fields of the scx record (state and sequence number)

            void * oldVal;                                          // old value that we expect to find in *field (to prevent helpers from making erroneous changes)
            void * newVal;                                          // new value to store to *field
            void * volatile * field;                                // pointer to the field that should be changed
            int numNodes;                                           // largest valid index of nodes[...]
            NodeT * nodes[MaxNodeDependenciesPerSCX];               // nodes[i] is the ith node that we depend on
            bool finalize[MaxNodeDependenciesPerSCX];               // finalize[i] = should we finalize nodes[i]?
            scx_handle_t scxPtrsSeen[MaxNodeDependenciesPerSCX];    // scxPtrs[i] is the value returned by the last llx(nodes[i]) by this process
        } c;
        PAD;
        const static int size = sizeof(c);
    };
    
    // descriptor reduction algorithm
    #define DESC1_ARRAY records
    #define DESC1_T SCXRecord
    #define MUTABLES1_OFFSET_ALLFROZEN 0
    #define MUTABLES1_OFFSET_STATE 1
    #define MUTABLES1_MASK_ALLFROZEN 0x1
    #define MUTABLES1_MASK_STATE 0x6
    #define MUTABLES1_NEW(mutables) \
        ((((mutables)&MASK1_SEQ)+(1<<OFFSET1_SEQ)) \
        | (SCXRecord::STATE_INPROGRESS<<MUTABLES1_OFFSET_STATE))
    #define MUTABLES1_INIT_DUMMY SCXRecord::STATE_COMMITTED<<MUTABLES1_OFFSET_STATE | MUTABLES1_MASK_ALLFROZEN<<MUTABLES1_OFFSET_ALLFROZEN
    #include "descriptors_impl.h"
PAD;
    DESC1_T DESC1_ARRAY[LAST_TID1+1] __attribute__ ((aligned(64)));
PAD;

    static const scx_handle_t INIT_SCX_HANDLE = ((scx_handle_t) TAGPTR1_STATIC_DESC(0));

public:

    static const scx_handle_t FINALIZED = ((scx_handle_t) TAGPTR1_DUMMY_DESC(1));
    static const scx_handle_t FAILED = ((scx_handle_t) TAGPTR1_DUMMY_DESC(2));
        
private:

    int help(const int tid, tagptr_t const tagptr, SCXRecord const * const snap, bool helpingOther) {
        SCXRecord *ptr = TAGPTR1_UNPACK_PTR(tagptr);
        for (int i=helpingOther; i<snap->c.numNodes; ++i) {
            bool successfulCAS = __sync_bool_compare_and_swap(&snap->c.nodes[i]->scxPtr, (void *) snap->c.scxPtrsSeen[i], tagptr);
            auto exp = snap->c.nodes[i]->scxPtr;
            if (successfulCAS || exp == (scx_handle_t) tagptr) continue; // if node is already frozen for our operation

            // note: we can get here only if:
            // 1. the state is inprogress, and we just failed a cas, and every helper will fail that cas (or an earlier one), so the scx must abort, or
            // 2. the state is committed or aborted
            // (this suggests that it might be possible to get rid of the allFrozen bit)

            // read mutable allFrozen field of descriptor
            bool succ;
            bool allFrozen = DESC1_READ_FIELD(succ, ptr->c.mutables, tagptr, MUTABLES1_MASK_ALLFROZEN, MUTABLES1_OFFSET_ALLFROZEN);
            if (!succ) return SCXRecord::STATE_ABORTED;

            if (allFrozen) {
                return SCXRecord::STATE_COMMITTED;
            } else {
                const int newState = SCXRecord::STATE_ABORTED;
                MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, newState, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
                return newState;
            }
        }
        MUTABLES1_WRITE_BIT(ptr->c.mutables, snap->c.mutables, MUTABLES1_MASK_ALLFROZEN);
        SOFTWARE_BARRIER;
        for (int i=0; i<snap->c.numNodes; ++i) {
            if (!snap->c.finalize[i]) continue;
            snap->c.nodes[i]->marked = true;
        }

        // CAS in the new sub-tree (update CAS)
        __sync_bool_compare_and_swap(snap->c.field, snap->c.oldVal, snap->c.newVal);

        MUTABLES1_WRITE_FIELD(ptr->c.mutables, snap->c.mutables, SCXRecord::STATE_COMMITTED, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
        return SCXRecord::STATE_COMMITTED; // success
    }

    void helpOther(const int tid, tagptr_t tagptr) {
        if (tagptr == INIT_SCX_HANDLE) {
            return; // deal with the dummy descriptor
        }
        SCXRecord snap;
        if (DESC1_SNAPSHOT(&snap, tagptr, SCXRecord::size)) {
            assert((UNPACK1_SEQ(tagptr) & 0x1));
            assert((UNPACK1_SEQ(snap.c.mutables) & 0x1));
            assert(UNPACK1_SEQ(tagptr) == UNPACK1_SEQ(snap.c.mutables));
            help(tid, tagptr, &snap, true);
        }
    }
    
public:

    const int numThreads;

    SCXProvider(const int _numThreads)
    : numThreads(_numThreads)
    {
        DESC1_INIT_ALL(numThreads);
        SCXRecord * dummy = TAGPTR1_UNPACK_PTR(INIT_SCX_HANDLE);
        dummy->c.mutables = MUTABLES1_INIT_DUMMY;
        for (int i=0;i<numThreads;++i) {
            DESC1_NEW(i); // add a DESC1_NEW call at the start for each thread, since we only perform DESC1_NEW AFTER an scx
        }
        //std::cout<<"address of dummy: "<<((uintptr_t) dummy)<<" address of dummy->c.mutables: "<<((uintptr_t) &dummy->c.mutables)<<" address of dummy->c.{end}: "<<((uintptr_t) &dummy->c.scxPtrsSeen[MaxNodeDependenciesPerSCX])<<" size="<<dummy->size<<std::endl;
    }
    
    void initNode(NodeT * const node) {
        node->marked = false;
        node->scxPtr = INIT_SCX_HANDLE;
    }
    
    scx_handle_t llx(const int tid, NodeT const * const srcNode, NodeT * const dest = NULL, const int bytesToCopy = sizeof(NodeT)) {
        const bool marked = srcNode->marked;
        SOFTWARE_BARRIER;
        tagptr_t tagptr = (tagptr_t) srcNode->scxPtr;

        // read mutable state field of descriptor
        bool succ;
        int state = DESC1_READ_FIELD(succ, TAGPTR1_UNPACK_PTR(tagptr)->c.mutables, tagptr, MUTABLES1_MASK_STATE, MUTABLES1_OFFSET_STATE);
        if (!succ) state = SCXRecord::STATE_COMMITTED;
        // note: special treatment for alg in the case where the descriptor has already been reallocated (impossible before the transformation, assuming safe memory reclamation)
        SOFTWARE_BARRIER;

        if (state == SCXRecord::STATE_ABORTED || ((state == SCXRecord::STATE_COMMITTED) && !srcNode->marked)) {
            // read snapshot fields (if outputSnapshot is non-NULL)
            if (dest) memcpy(dest, srcNode, bytesToCopy);
            if ((tagptr_t) srcNode->scxPtr == tagptr) return (scx_handle_t) tagptr; // we have a snapshot
        }

        if (state == SCXRecord::STATE_INPROGRESS) {
            helpOther(tid, tagptr);
        }
        return (marked ? FINALIZED : FAILED);
    }
    
    bool isSuccessfulLLXResult(scx_handle_t const handle) {
        return (handle != FINALIZED && handle != FAILED);
    }
    
    inline void scxInit(const int tid) {
        SCXRecord * scxptr = &DESC1_ARRAY[tid];
        assert(!(UNPACK1_SEQ(scxptr->c.mutables) & 0x1));
        scxptr->c.numNodes = 0;
    }
    
    inline void scxAddNode(const int tid, NodeT * const node, bool finalize, scx_handle_t const llxResult) {
        SCXRecord * scxptr = &DESC1_ARRAY[tid];
        assert(!(UNPACK1_SEQ(scxptr->c.mutables) & 0x1));
        int * ix = &scxptr->c.numNodes;
        scxptr->c.nodes[*ix] = node;
        scxptr->c.finalize[*ix] = finalize;
        scxptr->c.scxPtrsSeen[*ix] = llxResult;
        ++(*ix);
    }
    
    inline bool scxExecute(const int tid, void * volatile * const field, void * const oldVal, void * const newVal) {
        SCXRecord * scxptr = &DESC1_ARRAY[tid];
        scxptr->c.oldVal = oldVal;
        scxptr->c.newVal = newVal;
        scxptr->c.field = field;
        DESC1_INITIALIZED(tid);
        auto tagptr = TAGPTR1_NEW(tid, scxptr->c.mutables);
        assert((UNPACK1_SEQ(tagptr) & 0x1));
        auto result = help(tid, tagptr, scxptr, false);
        DESC1_NEW(tid);
        assert(!(UNPACK1_SEQ(scxptr->c.mutables) & 0x1));
        return (result == SCXRecord::STATE_COMMITTED);
    }
    
};

#endif /* SCX_H */
