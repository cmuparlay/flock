//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------

#include <cstdio>
#include "strategy/lockablebase.h"

LockableBase::~LockableBase()
{}

ResourceAllocatorBase::ResourceAllocatorBase(int numResources)
    : m_resource(numResources)
{}

ResourceAllocatorBase::~ResourceAllocatorBase()
{
    //printf("Counters: { ");
    //for (unsigned i = 0; i < m_resource.size(); i++) 
    //{
        //printf("%d ", m_resource[i]);
    //}
    //printf("}\n");
}

void ResourceAllocatorBase::UseResource(const ResourceIdVec& resourceId)
{
    //very simple usage of the resources
    for (unsigned i = 0; i < resourceId.size(); i++) 
    {
        m_resource[resourceId[i]]++;
    }
}
