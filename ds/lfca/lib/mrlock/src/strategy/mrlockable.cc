//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------

#include <cassert>
#include "strategy/mrlockable.h"

MRResourceAllocator::MRResourceAllocator(int numResources)
    : ResourceAllocatorBase(numResources)
    , m_mutex64(NULL)
    , m_mutex(NULL)
{
    if(numResources > 64)    
    {
        m_mutex = new MRLock<Bitset>(numResources);
    }
    else
    {
        m_mutex64 = new MRLock<uint64_t>(numResources);
    }
}

MRResourceAllocator::~MRResourceAllocator()
{
    delete m_mutex64;
    delete m_mutex;
}

LockableBase* MRResourceAllocator::CreateLockable(const ResourceIdVec& resources)
{
    LockableBase* lockable = NULL;

    if (m_resource.size() > 64)
    {
        Bitset resourceMask;
        resourceMask.Resize(m_resource.size());
        for (unsigned i = 0; i < resources.size(); i++) 
        {
            resourceMask.Set(resources[i]);
        }

        lockable = new MRLockable<Bitset>(resourceMask, m_mutex); 

    }
    else
    {
        int resourceMask = 0;
        for (unsigned i = 0; i < resources.size(); i++) 
        {
            resourceMask |= 1 << resources[i];
        }

        lockable = new MRLockable<uint64_t>(resourceMask, m_mutex64); 
    }

    return lockable;
}
