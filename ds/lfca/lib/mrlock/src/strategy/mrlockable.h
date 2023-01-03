//------------------------------------------------------------------------------
// 
//     A multi-resource lock
//
//------------------------------------------------------------------------------

#ifndef _MRLOCKABLE_INCLUDED_
#define _MRLOCKABLE_INCLUDED_

#include "mrlock.h"
#include "strategy/lockablebase.h"

template<typename BitsetType>
class MRLockable : public LockableBase
{
public:
    MRLockable(const BitsetType& resourceMask, MRLock<BitsetType>* mutex)
        : m_resourceMask(resourceMask)
        , m_lockHandle(-1)
        , m_mutex(mutex)
    {}

    virtual void Lock()
    {
        m_lockHandle = m_mutex->Lock(m_resourceMask);
    }

    virtual void Unlock()
    {
        m_mutex->Unlock(m_lockHandle);
    }

private:
    BitsetType m_resourceMask;
    int m_lockHandle;
    MRLock<BitsetType>* m_mutex;
};

class MRResourceAllocator : public ResourceAllocatorBase
{
public:
    MRResourceAllocator (int numResources);
    virtual ~MRResourceAllocator ();

    virtual LockableBase* CreateLockable(const ResourceIdVec& resources);

private:
    MRLock<uint64_t>* m_mutex64; //optimized version for less than 64 resources
    MRLock<Bitset>* m_mutex;
};


#endif //_MRLOCKABLE_INCLUDED_
