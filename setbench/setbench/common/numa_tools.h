/* 
 * File:   numa_tools.h
 * Author: t35brown
 *
 * Created on May 7, 2019, 3:21 PM
 */

#ifndef NUMA_TOOLS_H
#define NUMA_TOOLS_H

#include "errors.h"
#include "plaf.h"
#include <numa.h>
#include <sched.h>

#define __PAD_INTS (PREFETCH_SIZE_WORDS*2)

static __thread int __currNode = -1;
static __thread int __currCPU = -1;
static __thread int __callsNode = 0;
static __thread int __callsCPU = 0;

class NumaTools {
private:
    PAD;
    int callsPerUpdate;
    int numNodes;
    int numCPUs;
    int * cpuToNode;
    PAD;
public:
    NumaTools(int _callsPerUpdate = 100) {
        callsPerUpdate = _callsPerUpdate;
        if (numa_available() == -1) {
            setbench_error("libnuma returned -1 from numa_available(); ensure libnuma is setup correctly.");
        }
        numNodes = numa_num_configured_nodes();
        numCPUs = numa_num_configured_cpus();
        cpuToNode = new int[__PAD_INTS + __PAD_INTS*numCPUs + __PAD_INTS] + __PAD_INTS; // shift start of this array forward by __PAD_INTS slots
        for (int i=0;i<numCPUs;++i) {
            cpuToNode[__PAD_INTS*i] = numa_node_of_cpu(i);
        }
    }
    ~NumaTools() {
        delete[] (cpuToNode - __PAD_INTS); // shift start back
    }
    int get_cpu_cached() {
        return __currCPU;
    }
    int get_cpu_slow() {
        return (__currCPU = sched_getcpu());
    }
    int get_cpu_periodic() {
        return ((__callsCPU++) % callsPerUpdate) ? get_cpu_cached() : get_cpu_slow();
    }
    int get_node_for_cpu(int cpu) {
        return cpuToNode[__PAD_INTS*cpu];
    }
    int get_node_slow() {
        return (__currNode = get_node_for_cpu(get_cpu_slow()));
    }
    int get_node_cached() {
        return __currNode;
    }
    int get_node_periodic() {
        return ((__callsNode++) % callsPerUpdate) ? get_node_cached() : get_node_slow();
    }
    int get_num_nodes() {
        return numNodes;
    }
    int get_num_cpus() {
        return numCPUs;
    }
};

static NumaTools __numa;

#endif /* NUMA_TOOLS_H */

