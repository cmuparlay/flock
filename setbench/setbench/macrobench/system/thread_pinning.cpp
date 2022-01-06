using namespace std;
#include "thread_pinning.h"

namespace thread_pinning {

    cpu_set_t ** cpusets;
    int * customBinding;
    int numCustomBindings;
    
    void setbench_deinit(const int numThreads) {
        const int potentialThreads = max(LOGICAL_PROCESSORS, numThreads);
        if (cpusets) {
            for (int i=0;i<potentialThreads;++i) {
                CPU_FREE(cpusets[i]);
            }
            delete[] cpusets;
            cpusets = NULL;
        }
        if (customBinding) {
            delete[] customBinding;
            customBinding = NULL;
        }
    }
    
    void configurePolicy(const int numThreads, string policy) {
        const int potentialThreads = max(LOGICAL_PROCESSORS, numThreads);
        cpusets = new cpu_set_t * [potentialThreads];
        customBinding = new int[MAX_THREADS_POW2];
        parseCustom(policy);
        if (numCustomBindings > 0) {
            // create cpu sets for binding threads to cores
            int size = CPU_ALLOC_SIZE(LOGICAL_PROCESSORS);
            for (int i=0;i<potentialThreads;++i) {
                cpusets[i] = CPU_ALLOC(LOGICAL_PROCESSORS);
                CPU_ZERO_S(size, cpusets[i]);
                CPU_SET_S(customBinding[i%numCustomBindings], size, cpusets[i]);
            }
        }
    }

    void bindThread(const int tid) {
        if (numCustomBindings > 0) {
            doBindThread(tid, LOGICAL_PROCESSORS);
        }
    }

    int getActualBinding(const int tid) {
        int result = -1;
        if (numCustomBindings == 0) {
            return result;
        }
        unsigned bindings = 0;
        for (int i=0;i<LOGICAL_PROCESSORS;++i) {
            if (CPU_ISSET_S(i, CPU_ALLOC_SIZE(LOGICAL_PROCESSORS), cpusets[tid%LOGICAL_PROCESSORS])) {
                result = i;
                ++bindings;
            }
        }
        if (bindings > 1) {
            cout<<"ERROR: "<<bindings<<" processor bindings for thread "<<tid<<std::endl;
            exit(-1);
        }
        return result;
    }

    bool isInjectiveMapping(const int numThreads) {
        const int potentialThreads = max(numThreads, LOGICAL_PROCESSORS);
        if (numCustomBindings == 0) {
            return true;
        }
        bool covered[LOGICAL_PROCESSORS];
        for (int i=0;i<LOGICAL_PROCESSORS;++i) covered[i] = 0;
        for (int i=0;i<potentialThreads;++i) {
            int ix = getActualBinding(i);
            if (covered[ix]) return false;
            covered[ix] = 1;
        }
        return true;
    }
    
}