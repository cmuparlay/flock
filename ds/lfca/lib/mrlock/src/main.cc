//------------------------------------------------------------------------------
// 
//     Testing different locking strategies
//
//------------------------------------------------------------------------------

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <set>
#include <thread>
#include <mutex>
#include <boost/random.hpp>
#include <sched.h>
#include "timehelper.h"
#include "threadbarrier.h"
#include "strategy/lockablebase.h"
#include "strategy/mrlockable.h"

void LockThread(unsigned rank, unsigned resource, std::vector<unsigned> contention, unsigned iteration, ResourceAllocatorBase* allocator, 
                ThreadBarrier& barrier, ThreadBarrier& stopBarrier)
{
    //set affinity for each thread
    cpu_set_t cpu;
    CPU_SET(rank, &cpu);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpu);

    boost::random::mt19937 randomGen;
    randomGen.seed(rank + Time::GetWallTime());
    boost::random::uniform_int_distribution<> randomDist(0, resource - 1);

    //The set of resource ids is ordered so that the resourceIdVec is ordered
    std::set<unsigned> randomNumbers;
    std::vector<ResourceAllocatorBase::ResourceIdVec> resourceIdVec;
    resourceIdVec.resize(iteration);

    for (unsigned i = 0; i < iteration; i++)
    {
        randomNumbers.clear();

        while (randomNumbers.size() < contention[i]) 
        {
            randomNumbers.insert(randomDist(randomGen));
        }
        resourceIdVec[i].assign(randomNumbers.begin(), randomNumbers.end());
    }

    barrier.Wait();

    for (unsigned i = 0; i < iteration; i++) 
    {
        LockableBase* resourceLock = allocator->CreateLockable(resourceIdVec[i]);
        resourceLock->Lock();
        allocator->UseResource(resourceIdVec[i]);
        resourceLock->Unlock();
        delete resourceLock;
    }   

    stopBarrier.Wait();
}

int main(int argc, const char *argv[])
{

    unsigned numThread = 4;
    unsigned numResource = 64;
    unsigned numPivot = 7;
    unsigned numContention = 50;
    unsigned numIteration = 10000;
    //lockType 0=mr
    unsigned lockType = 0; 
    
    if(argc > 1) numThread = atoi(argv[1]);
    if(argc > 2) numResource = atoi(argv[2]);
    if(argc > 3) numPivot = atoi(argv[3]);
    if(argc > 4) numContention = atoi(argv[4]);
    if(argc > 5) numIteration = atoi(argv[5]);
    if(argc > 6) lockType = atoi(argv[6]);
    
    //assert(numContention <= numResource);

    //Create resource allocator
    const char* lockName = NULL;
    ResourceAllocatorBase* resourceAlloc = NULL;
    switch(lockType)
    {
    case 0:
        lockName = "MRLock";
        resourceAlloc = new MRResourceAllocator(numResource);
        break;
    }

    printf("Start testing with %d threads %d resources %d pivot %d contention %d iteration using %s\n", numThread, numResource, numPivot, numContention, numIteration, lockName);

    std::vector<std::thread> thread(numThread);
    ThreadBarrier barrier(numThread + 1);
    ThreadBarrier stopBarrier(numThread + 1);

    //generate testing sequence
    boost::random::mt19937 randomGen;
    randomGen.seed(Time::GetWallTime());
    //boost::random::uniform_int_distribution<> randomPercent(0,99);
    boost::random::uniform_int_distribution<> randomHighContention(numPivot + 1, numResource);
    boost::random::uniform_int_distribution<> randomLowContention(2, numPivot);

    //The set of resource ids is ordered so that the resourceIdVec is ordered
    std::vector<unsigned> contentionSpecVec;
    contentionSpecVec.resize(numIteration);

    for (unsigned i = 0; i < numIteration; i++)
    {
        //unsigned dice = randomPercent(randomGen);
        unsigned requestSize = i % 1000 <  numContention ? randomLowContention(randomGen) : randomHighContention(randomGen);
        contentionSpecVec[i] = requestSize;
    }

    //Create joinable threads
    for (unsigned i = 0; i < numThread; i++) 
    {
        thread[i] = std::thread(LockThread, i, numResource, contentionSpecVec, numIteration, resourceAlloc, std::ref(barrier), std::ref(stopBarrier));
    }

    barrier.Wait();

    {
        ScopedTimer timer(true);

        stopBarrier.Wait();
    }
    
    //Wait for the threads to finish
    for (unsigned i = 0; i < thread.size(); i++) 
    {
        thread[i].join();
    }

    delete resourceAlloc;
    
    return 0;
}
