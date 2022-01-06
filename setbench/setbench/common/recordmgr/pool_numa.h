                                                                                                                                            /**
 * NUMA aware bounded object pool
 *
 * Copyright (C) 2019 Trevor Brown
 *
 */

#ifndef POOL_NUMA_H
#define	POOL_NUMA_H

#include <cassert>
#include <iostream>
#include <sstream>
#include "blockbag.h"
#include "blockpool.h"
#include "lockfreeblockstack.h"
#include "pool_interface.h"
#include "plaf.h"
#include "errors.h"
//#include "globals.h"
#ifdef GSTATS_HANDLE_STATS
#   include "globals_extern.h"
#endif
#include "numa_tools.h"

#ifdef GSTATS_HANDLE_STATS
#   ifndef __AND
#      define __AND ,
#   endif
#   define GSTATS_HANDLE_STATS_POOL_NUMA(gstats_handle_stat) \
        gstats_handle_stat(LONG_LONG, pool_cpu_get, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_reclaimer_to_cpu, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_cpu_to_node, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_node_to_global, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_global_to_alloc, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_alloc_to_cpu, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_global_to_cpu, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \
        gstats_handle_stat(LONG_LONG, move_block_node_to_cpu, 1, { \
                gstats_output_item(PRINT_RAW, SUM, TOTAL) \
        }) \

    // define a variable for each stat above
    GSTATS_HANDLE_STATS_POOL_NUMA(__DECLARE_EXTERN_STAT_ID);
#endif

template <typename T = void, class Alloc = allocator_interface<T> >
class pool_numa : public pool_interface<T, Alloc> {
private:
    //PAD; // not needed after superclass layout
    int cpuBlockUB;
    int nodeBlockUB;
    int globalBlockUB;

    lfbstack<T> * globalPool;
    lfbstack<T> ** nodePools;
    blockbag<T> ** cpuPools;
    PAD;

    // possible optimization: have low and high thresholds,
    //     and when pulling or pushing, pull/push to (lo+hi)/2

    void tryPushBlocks(const int tid) {
        if (cpuPools[tid]->getSizeInBlocks() <= cpuBlockUB) return; // common case ; note: past this line, we are guaranteed to move at least one block to the node pool
        auto node = __numa.get_node_periodic();

        // TODO: I AM FORGETTING TO FREE THE ACTUAL BLOCKS????

        // move blocks from cpu pool to node pool
        while (cpuPools[tid]->getSizeInBlocks() > cpuBlockUB) {
            auto b = cpuPools[tid]->removeFullBlock();
            nodePools[node]->addBlock(b);
#ifdef GSTATS_HANDLE_STATS
            GSTATS_ADD(tid, move_block_cpu_to_node, 1);
#endif
        }

        // move blocks from node pool to global pool
        bool movedToGlobalPool = false;
        while (nodePools[node]->sizeInBlocks() > nodeBlockUB) {
            auto b = nodePools[node]->getBlock();
            if (b) {
                globalPool->addBlock(b);
                movedToGlobalPool = true;
#ifdef GSTATS_HANDLE_STATS
                GSTATS_ADD(tid, move_block_node_to_global, 1);
#endif
            }
        }
        if (!movedToGlobalPool) return;

        // release blocks from global pool to the allocator
        // NOTE: this could have a MUCH more efficient implementation
        while (globalPool->sizeInBlocks() > globalBlockUB) {
            auto b = globalPool->getBlock();
            if (b) {
                while (!b->isEmpty()) {
                    auto obj = b->pop();
                    this->alloc->deallocate(tid, obj);
                }
#ifdef GSTATS_HANDLE_STATS
                GSTATS_ADD(tid, move_block_global_to_alloc, 1);
#endif
            }
        }
    }

    void pullBlock(const int tid) {
        // check if we already have a non-empty block
        if (!cpuPools[tid]->isEmpty()) return;

        // try node pool
        auto node = __numa.get_node_periodic();
        //std::cout<<"node="<<node<<std::endl;
        auto b = nodePools[node]->getBlock();
        if (b) {
#ifdef GSTATS_HANDLE_STATS
            GSTATS_ADD(tid, move_block_node_to_cpu, 1);
#endif
            cpuPools[tid]->addFullBlock(b);
            return;
        }

        // try global pool
        b = globalPool->getBlock();
        if (b) {
            cpuPools[tid]->addFullBlock(b);
#ifdef GSTATS_HANDLE_STATS
            GSTATS_ADD(tid, move_block_global_to_cpu, 1);
#endif
            return;
        }

        // TODO: currently there is no movement of blocks from global pools down to node pools. only directly to cpu pools.

        // allocate a full block of objects
        for (int i=0;i<BLOCK_SIZE;++i) {
            auto obj = this->alloc->allocate(tid);
            cpuPools[tid]->add(obj);
        }
#ifdef GSTATS_HANDLE_STATS
        GSTATS_ADD(tid, move_block_alloc_to_cpu, 1);
#endif
    }
public:
    template <typename _Tp1>
    struct rebindAlloc {
        typedef typename Alloc::template rebind<_Tp1>::other other;
    };
    template<typename _Tp1>
    struct rebind {
        typedef pool_numa<_Tp1, Alloc> other;
    };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 {
        typedef pool_numa<_Tp1, _Tp2> other;
    };

    std::string getSizeString() {
        return "";
    }

    inline T* get(const int tid) {
        //MEMORY_STATS2 this->alloc->debug->addFromPool(tid, 1);
#ifdef GSTATS_HANDLE_STATS
        GSTATS_ADD(tid, pool_cpu_get, 1);
#endif
        pullBlock(tid); // after this, we are guaranteed to have a non-empty cpuPool
        return cpuPools[tid]->remove();
    }
    inline void add(const int tid, T* ptr) {
        //MEMORY_STATS2 this->debug->addToPool(tid, 1);
#ifdef GSTATS_HANDLE_STATS
        GSTATS_ADD(tid, pool_cpu_get, 1);
#endif
        cpuPools[tid]->add(ptr);
        tryPushBlocks(tid);
    }
    inline void addMoveFullBlocks(const int tid, blockbag<T> *bag, block<T> * const predecessor) {
        setbench_error("unsupported operation");
    }
    inline void addMoveFullBlocks(const int tid, blockbag<T> *bag) {
        // OLD COMMENT (unclear why this would be true... at any rate it's only for debugging output): WARNING: THE FOLLOWING DEBUG COMPUTATION GETS THE WRONG NUMBER OF BLOCKS.
        //MEMORY_STATS2 this->debug->addToPool(tid, (bag->getSizeInBlocks()-1)*BLOCK_SIZE);

        auto sizeBefore = cpuPools[tid]->getSizeInBlocks();
        cpuPools[tid]->appendMoveFullBlocks(bag);
        auto sizeAfter = cpuPools[tid]->getSizeInBlocks();
#ifdef GSTATS_HANDLE_STATS
        GSTATS_ADD(tid, move_block_reclaimer_to_cpu, sizeAfter - sizeBefore);
#endif

        tryPushBlocks(tid);
    }
    inline void addMoveAll(const int tid, blockbag<T> *bag) {
        MEMORY_STATS2 this->debug->addToPool(tid, bag->computeSize());
        cpuPools[tid]->appendMoveAll(bag);
        tryPushBlocks(tid);
    }
    inline int computeSize(const int tid) {
        return cpuPools[tid]->computeSize();
    }

    void debugPrintStatus(const int tid) {}

    void initThread(const int tid) {}
    void deinitThread(const int tid) {}

    pool_numa(const int numProcesses, Alloc * const _alloc, debugInfo * const _debug)
            : pool_interface<T, Alloc>(numProcesses, _alloc, _debug) {
        VERBOSE DEBUG COUTATOMIC("constructor pool_numa"<<std::endl);

        // example: suppose 256b objects and block size 64
        cpuBlockUB = 8; // then this is 128kb per thread
        nodeBlockUB = 64 * __numa.get_num_cpus() / __numa.get_num_nodes(); // and with 48 threads per socket, this is 768kb per socket PER BLOCK, or 50mb per socket
        globalBlockUB = 8 * __numa.get_num_cpus(); // and with 192 threads total, this is 25mb
        // for a total of 24mb + 200mb + 25mb = 250mb (per 256b object type)

        globalPool = new lfbstack<T>();

        nodePools = new lfbstack<T> * [__numa.get_num_nodes()];
        for (int node=0;node<__numa.get_num_nodes();++node) {
            nodePools[node] = new lfbstack<T>();
        }

        cpuPools = new blockbag<T> * [numProcesses];
        for (int tid=0;tid<numProcesses;++tid) {
            cpuPools[tid] = new blockbag<T>(tid, this->blockpools[tid]);
        }
    }
    ~pool_numa() {
        VERBOSE DEBUG COUTATOMIC("destructor pool_numa"<<std::endl);
        const int dummyTid = 0;

        // clean up global pool
        block<T> *fullBlock;
        while (fullBlock = globalPool->getBlock()) {
            while (!fullBlock->isEmpty()) {
                T * const ptr = fullBlock->pop();
                this->alloc->deallocate(dummyTid, ptr);
            }
            this->blockpools[dummyTid]->deallocateBlock(fullBlock);
        }
        delete globalPool;

        // clean up node pools
        for (int node=0;node<__numa.get_num_nodes();++node) {
            auto p = nodePools[node];
            while (fullBlock = p->getBlock()) {
                while (!fullBlock->isEmpty()) {
                    T * const ptr = fullBlock->pop();
                    this->alloc->deallocate(dummyTid, ptr);
                }
                this->blockpools[dummyTid]->deallocateBlock(fullBlock);
            }
            delete p;
        }
        delete[] nodePools;

        // clean up free bags
        for (int tid=0;tid<this->NUM_PROCESSES;++tid) {
            auto p = cpuPools[tid];
            this->alloc->deallocateAndClear(tid, p);
            delete p;
        }
        delete[] cpuPools;
    }
};

#endif

