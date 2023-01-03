//------------------------------------------------------------------------------
// 
//     A multi-resource lock (no queue, test and lock)
//
//------------------------------------------------------------------------------

#ifndef _CONCEPT_MRSIMPLELOCK_INCLUDED_
#define _CONCEPT_MRSIMPLELOCK_INCLUDED_

#include <atomic>
#include <cassert>
#include <cstdint>

class MRSimpleLock
{
public:
    MRSimpleLock(uint32_t resources)
    {
        m_bits.store(0, std::memory_order_relaxed);
    } 

    ~MRSimpleLock()
    {}

    inline void Lock(uint64_t resources)
    {
        for(;;)
        {
            uint64_t bits = m_bits.load(std::memory_order_relaxed);
            if(!(bits & resources))
            {
                if(m_bits.compare_exchange_weak(bits, bits | resources, std::memory_order_relaxed))
                {
                    break;
                }
            }
        }

        //Good to go
    }

    inline void Unlock(uint64_t resources)
    {
        for(;;)
        {
            uint64_t bits = m_bits.load(std::memory_order_relaxed);
            if(m_bits.compare_exchange_weak(bits, bits & ~resources, std::memory_order_relaxed))
            {
                break;
            }
        }
    }

private:
    std::atomic<uint64_t> m_bits;
};

#endif //_CONCEPT_MRSIMPLELOCK_INCLUDED_
