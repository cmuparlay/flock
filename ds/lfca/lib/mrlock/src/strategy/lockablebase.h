//------------------------------------------------------------------------------
// 
//     SST/Macro tools set
//
//------------------------------------------------------------------------------

#ifndef _LOCKABLEBASE_INCLUDED_
#define _LOCKABLEBASE_INCLUDED_

#include <vector>

class LockableBase 
{
public:
    virtual ~LockableBase ();

    virtual void Lock() = 0;
    virtual void Unlock() = 0;
};

class ResourceAllocatorBase 
{
public:
    typedef int ResourceType;
    typedef int ResourceIdType;
    typedef std::vector<ResourceType> ResourceVec;
    typedef std::vector<ResourceIdType> ResourceIdVec;

public:
    ResourceAllocatorBase (int numResources);
    virtual ~ResourceAllocatorBase ();

    void UseResource(const ResourceIdVec& id);
    virtual LockableBase* CreateLockable(const ResourceIdVec& resources) = 0;

protected:
    ResourceVec m_resource;
};

#endif //_LOCKABLEBASE_INCLUDED_
