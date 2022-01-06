/**
 * This class lets us write methods to gather tree structure statistics once,
 * and apply them to many data structures.
 */
#ifndef TREE_STATS_H
#define TREE_STATS_H

#ifdef USE_TREE_STATS

#include <cassert>
#include <sstream>
#include <string>
#include <vector>
#include <limits>
#include "plaf.h"

#define MAX_HEIGHT (1<<10)

/**
 * TODO: extend tree_stats.h to start tracking memory layout issues
 * (avg cache line crossings,
 *  avg cache set occupancy (need to demarcate search data),
 *  neighbouring object types,
 *  page crossings,
 *  avg page density,
 *  alignment histogram,
 *  page occupancy visualizations,
 *  unique pages needed,
 *  unique cache lines needed)
 */

template <typename NodeHandlerT>
class TreeStats {
private:
    typedef typename NodeHandlerT::NodePtrType nodeptr;
    PAD;
    size_t internalsAtDepth[MAX_HEIGHT];
    size_t leavesAtDepth[MAX_HEIGHT];
    size_t keysAtDepth[MAX_HEIGHT];
    size_t keysInLeavesAtDepth[MAX_HEIGHT];
    size_t keysInInternalsAtDepth[MAX_HEIGHT];
    size_t sumOfKeys;
#ifdef TREE_STATS_BYTES_AT_DEPTH
    size_t bytesAtDepth[MAX_HEIGHT];
#endif
    PAD;

    void computeStats(NodeHandlerT * handler, nodeptr node, size_t depth, size_t maxDepth = std::numeric_limits<size_t>::max()) {
        if (!handler) return;

        //std::cout<<"nodeAddr="<<(size_t)node<<" depth="<<depth<<" degree="<<node->size<<" internal?="<<NodeHandlerT::isInternal(node)<<std::endl;
        if (!node || depth > maxDepth) return;
        size_t numKeys = handler->getNumKeys(node);
        keysAtDepth[depth] += numKeys;
        sumOfKeys += handler->getSumOfKeys(node);
#ifdef TREE_STATS_BYTES_AT_DEPTH
        bytesAtDepth[depth] += handler->getSizeInBytes(node);
#endif
        if (handler->isLeaf(node)) {
            ++leavesAtDepth[depth];
            keysInLeavesAtDepth[depth] += numKeys;
        } else {
            ++internalsAtDepth[depth];
            keysInInternalsAtDepth[depth] += numKeys;
            auto it = handler->getChildIterator(node);
            while (it.hasNext()) {
                auto child = it.next();
                computeStats(handler, child, 1+depth, maxDepth);
            }
        }
    }

public:
    TreeStats(NodeHandlerT * handler, nodeptr root, bool parallelConstruction, bool freeHandler = true) {
        for (size_t d=0;d<MAX_HEIGHT;++d) {
            internalsAtDepth[d] = 0;
            leavesAtDepth[d] = 0;
            keysAtDepth[d] = 0;
            keysInLeavesAtDepth[d] = 0;
            keysInInternalsAtDepth[d] = 0;
#ifdef TREE_STATS_BYTES_AT_DEPTH
            bytesAtDepth[d] = 0;
#endif
        }
        sumOfKeys = 0;
#ifdef _OPENMP

        if (!handler) return;

        if (!parallelConstruction) {
            computeStats(handler, root, 0);

        } else {
            /**
             * PARALLEL constructor
             */
            std::cout<<"computing tree_stats in PARALLEL..."<<std::endl;
            #ifdef _OPENMP
                const size_t minNodes = 4*omp_get_max_threads();
            #else
                const size_t minNodes = 1;
            #endif

            std::vector<nodeptr> qn;    // queue of node pointers
            std::vector<size_t> qd;     // queue of depths
            qn.reserve(minNodes*2);
            qd.reserve(minNodes*2);
            qn.push_back(root);
            qd.push_back(0);

            size_t ix = 0; // index of top in qn and qd
            size_t currDepth = 0;
            size_t ixStartOfDepth = 0;
            size_t nodesSeenAtDepth = 0;

            #ifdef _OPENMP
                const size_t ompThreads = omp_get_max_threads();
            #else
                const size_t ompThreads = 1;
            #endif
            std::cout<<"bounded depth BFS to partition into subtrees for parallel computation ("<<ompThreads<<" threads)..."<<std::endl;
            while (ix < qn.size()) {
                auto node = qn[ix];
                auto depth = qd[ix];
                TRACE COUTATOMIC("tree_stats: queue visiting node "<<(uintptr_t) node<<" at depth "<<depth<<std::endl);
                ++ix;
                TRACE COUTATOMIC("tree_stats: new ix="<<ix<<std::endl);

                if (depth != currDepth) {
                    if (nodesSeenAtDepth >= minNodes) {
                        TRACE COUTATOMIC("tree_stats: we have seen enough nodes ("<<nodesSeenAtDepth<<") at depth "<<currDepth<<" so we cut off the bfs"<<std::endl);
                        --ix;
                        TRACE COUTATOMIC("tree_stats: new new ix="<<ix<<std::endl);
                        break;
                    }
                    currDepth = depth;
                    nodesSeenAtDepth = 0;
                    ixStartOfDepth = ix-1;
                    TRACE COUTATOMIC("tree_stats: new depth "<<depth<<" ixStartOfDepth="<<ixStartOfDepth<<std::endl);
                }
                ++nodesSeenAtDepth;
                TRACE COUTATOMIC("tree_stats: nodesSeenAtDepth "<<currDepth<<" changed to "<<nodesSeenAtDepth<<std::endl);

                // add any children to the queue
                if (node && !handler->isLeaf(node)) {
                    auto it = handler->getChildIterator(node);
                    while (it.hasNext()) {
                        auto child = it.next();
                        TRACE COUTATOMIC("tree_stats: add child "<<(uintptr_t) child<<" of node "<<(uintptr_t) node<<" to queue at depth "<<(1+depth)<<std::endl);
                        qn.push_back(child);
                        qd.push_back(1+depth);
                    }
                }
            }
            if (nodesSeenAtDepth < minNodes) {
                currDepth = 0;
                ix = 1;
                ixStartOfDepth = 0;
                TRACE COUTATOMIC("tree_stats: not enough nodes seen ("<<nodesSeenAtDepth<<") at any depth just partition into ONE tree"<<std::endl);
            }

            // we now have at least minNodes subtrees to process (in qn[ixStartOfDepth ... ix]),
            // so we can use openmp parallel for to construct TreeStats for these subtrees in parallel.
            std::cout<<"partitioned into "<<(ix-ixStartOfDepth)<<" subtrees; running parallel for..."<<std::endl;
            #pragma omp parallel for schedule(dynamic, 1)
            for (size_t i=ixStartOfDepth;i<ix;++i) {
                TreeStats<NodeHandlerT> * ts = new TreeStats(handler, qn[i], false, false);
                TRACE COUTATOMIC("tree_stats: sequential compute at depth "<<currDepth<<std::endl);
                for (size_t d=0;d<MAX_HEIGHT-currDepth;++d) {
                    FAA(&internalsAtDepth[d+currDepth], ts->internalsAtDepth[d]);
                    FAA(&leavesAtDepth[d+currDepth], ts->leavesAtDepth[d]);
                    FAA(&keysAtDepth[d+currDepth], ts->keysAtDepth[d]);
                    FAA(&keysInLeavesAtDepth[d+currDepth], ts->keysInLeavesAtDepth[d]);
                    FAA(&keysInInternalsAtDepth[d+currDepth], ts->keysInInternalsAtDepth[d]);
#ifdef TREE_STATS_BYTES_AT_DEPTH
                    FAA(&bytesAtDepth[d+currDepth], ts->bytesAtDepth[d]);
#endif
                }
                FAA(&sumOfKeys, ts->sumOfKeys);
                delete ts;
            }

//            std::cout<<"currDepth="<<currDepth<<std::endl;
//            std::cout<<(ix-ixStartOfDepth+1)<<" subtrees computed in parallel... addresses:"<<std::endl;
//            for (int i=ixStartOfDepth;i<ix;++i) {
//                std::cout<<" "<<qn[i]<<"[depth "<<qd[i]<<"]";
//            }
//            std::cout<<std::endl;

            // compute stats for the top of the tree, ABOVE the parallel constructed subtrees.
            std::cout<<"computing stats for the top of the tree (above the partitions)..."<<std::endl;
            if (currDepth > 0) computeStats(handler, root, 0, currDepth - 1);

        }
#else
        computeStats(handler, root, 0);
#endif
        if (freeHandler) delete handler;
    }

    size_t getInternalsAtDepth(size_t d) {
        assert(d < MAX_HEIGHT);
        return internalsAtDepth[d];
    }
    size_t getLeavesAtDepth(size_t d) {
        assert(d < MAX_HEIGHT);
        return leavesAtDepth[d];
    }
    size_t getNodesAtDepth(size_t d) {
        assert(d < MAX_HEIGHT);
        return getInternalsAtDepth(d) + getLeavesAtDepth(d);
    }
    size_t getHeight() {
        size_t d=0;
        while (d < MAX_HEIGHT && getNodesAtDepth(d) > 0) {
            ++d;
        }
        return d;
    }
    size_t getInternals() {
        size_t maxDepth = getHeight();
        size_t result = 0;
        for (size_t d=0;d<maxDepth;++d) {
            result += getInternalsAtDepth(d);
        }
        return result;
    }
    size_t getLeaves() {
        size_t maxDepth = getHeight();
        size_t result = 0;
        for (size_t d=0;d<maxDepth;++d) {
            result += getLeavesAtDepth(d);
        }
        return result;
    }
    size_t getNodes() {
        return getInternals() + getLeaves();
    }
    size_t getPointersAtDepth(size_t d) {
        assert(d+1 < MAX_HEIGHT);
        return getNodesAtDepth(d+1);
    }
    size_t getKeysAtDepth(size_t d) {
        assert(d < MAX_HEIGHT);
        return keysAtDepth[d];
    }
    size_t getKeys() {
        size_t maxDepth = getHeight();
        size_t result = 0;
        for (size_t d=0;d<maxDepth;++d) {
            result += getKeysAtDepth(d);
        }
        return result;
    }
    size_t getKeysInLeaves() {
        size_t maxDepth = getHeight();
        size_t result = 0;
        for (size_t d=0;d<maxDepth;++d) {
            result += keysInLeavesAtDepth[d];
        }
        return result;
    }
    size_t getKeysInInternals() {
        size_t maxDepth = getHeight();
        size_t result = 0;
        for (size_t d=0;d<maxDepth;++d) {
            result += keysInInternalsAtDepth[d];
        }
        return result;
    }
    double getAverageDegreeLeavesAtDepth(size_t d) {
        double denom = getLeavesAtDepth(d);
        return (denom == 0) ? 0 : getKeysAtDepth(d) / denom;
    }
    double getAverageDegreeLeaves() {
        double denom = getLeaves();
        return (denom == 0) ? 0 : getKeysInLeaves() / denom;
    }
    double getAverageDegreeInternalsAtDepth(size_t d) {
        double denom = getInternalsAtDepth(d);
        return (denom == 0) ? 0 : getPointersAtDepth(d) / denom;
    }
    double getAverageDegreeInternals() {
        double denom = getInternals();
        return (denom == 0) ? 0 : getNodes() / denom;
    }
    double getAverageDegreeAtDepth(size_t d) {
        // double denom = getNodesAtDepth(d);
        // return (getPointersAtDepth(d) + getKeysAtDepth(d)) / denom;
        return (getPointersAtDepth(d) + keysInLeavesAtDepth[d]) / (double) getNodesAtDepth(d);
    }
    double getAverageDegree() {
        double denom = getNodes();
        return (denom == 0) ? 0 : (denom + getKeysInLeaves()) / denom;
    }
    double getAverageKeyDepth() {
        size_t height = getHeight();
        size_t sumDepths = 0;
        for (size_t d=0;d<height;++d) {
            sumDepths += keysAtDepth[d] * d;
        }
        double denom = getKeys();
        return (denom == 0) ? 0 : sumDepths / denom;
    }
#ifdef TREE_STATS_BYTES_AT_DEPTH
    size_t getBytesAtDepth(size_t d) {
        return bytesAtDepth[d];
    }
    size_t getSizeInBytes() {
        size_t height = getHeight();
        size_t bytes = 0;
        for (size_t d=0;d<height;++d) {
            bytes += bytesAtDepth[d];
        }
        return bytes;
    }
#endif
    size_t getSumOfKeys() {
        return sumOfKeys;
    }
    std::string toString() {
        std::stringstream ss;
        size_t height = getHeight();

        ss<<"tree_stats_numInternalsAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getInternalsAtDepth(d);
        }
        ss<<std::endl;

        ss<<"tree_stats_numLeavesAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getLeavesAtDepth(d);
        }
        ss<<std::endl;

        ss<<"tree_stats_numNodesAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getNodesAtDepth(d);
        }
        ss<<std::endl;

//        ss<<"tree_stats_numPointersAtDepth=";
//        for (size_t d=0;d<height;++d) {
//            ss<<(d?" ":"")<<getPointersAtDepth(d);
//        }
//        ss<<std::endl;

        ss<<"tree_stats_numKeysAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getKeysAtDepth(d);
        }
        ss<<std::endl;

//        ss<<"tree_stats_avgDegreeLeavesAtDepth=";
//        for (size_t d=0;d<height;++d) {
//            ss<<(d?" ":"")<<getAverageDegreeLeavesAtDepth(d);
//        }
//        ss<<std::endl;
//
//        ss<<"tree_stats_avgDegreeInternalsAtDepth=";
//        for (size_t d=0;d<height;++d) {
//            ss<<(d?" ":"")<<getAverageDegreeInternalsAtDepth(d);
//        }
//        ss<<std::endl;

        ss<<"tree_stats_avgDegreeAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getAverageDegreeAtDepth(d);
        }
        ss<<std::endl;

        ss<<std::endl;
        ss<<"tree_stats_height="<<height<<std::endl;
        ss<<"tree_stats_numInternals="<<getInternals()<<std::endl;
        ss<<"tree_stats_numLeaves="<<getLeaves()<<std::endl;
        ss<<"tree_stats_numNodes="<<getNodes()<<std::endl;
        ss<<"tree_stats_numKeys="<<getKeys()<<std::endl;
        ss<<std::endl;

        ss<<"tree_stats_avgDegreeInternal="<<getAverageDegreeInternals()<<std::endl;
        ss<<"tree_stats_avgDegreeLeaves="<<getAverageDegreeLeaves()<<std::endl;
        ss<<"tree_stats_avgDegree="<<getAverageDegree()<<std::endl;
        ss<<"tree_stats_avgKeyDepth="<<getAverageKeyDepth()<<std::endl;

#ifdef TREE_STATS_BYTES_AT_DEPTH
        ss<<std::endl;
        ss<<"tree_stats_bytesAtDepth=";
        for (size_t d=0;d<height;++d) {
            ss<<(d?" ":"")<<getBytesAtDepth(d);
        }
        ss<<std::endl;
        ss<<"tree_stats_sizeInBytes="<<getSizeInBytes()<<std::endl;
#endif

        return ss.str();
    }
};

#endif

#endif /* TREE_STATS_H */

