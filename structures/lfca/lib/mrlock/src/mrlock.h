//------------------------------------------------------------------------------
// 
//     A multi-resource lock (using std::atomic)
//
//------------------------------------------------------------------------------

#ifndef _CONCEPT_MRLOCK_INCLUDED_
#define _CONCEPT_MRLOCK_INCLUDED_

#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>
#include "bitset.h"

//A bit of hack to initialize the bitset class
template<typename BitsetType>
inline void InitializeBitset(BitsetType& b, uint32_t r)
{}

template<>
inline void InitializeBitset<Bitset>(Bitset& b, uint32_t r)
{
    b.Resize(r);
}

template<typename BitsetType>
class MRLock
{
public:
    MRLock(uint32_t resources)
    {
        //We are using mask to wrap the index around cicular array,
        //so the buffer size should be the power of 2
        //We set buffer size equal to or greater than the std::thread::hardware_concurrency()
        uint32_t maxThreads = std::thread::hardware_concurrency(); 
        uint32_t bufferSize = 2;
        while(bufferSize <= maxThreads)
        {
            bufferSize = bufferSize << 1;
        }
        assert((bufferSize >= 2) && ((bufferSize & (bufferSize - 1)) == 0));

        m_buffer = new Cell[bufferSize];
        m_bufferMask = bufferSize - 1;

        for (uint32_t i = 0; i < bufferSize; i++) 
        {
            m_buffer[i].m_sequence.store(i, std::memory_order_relaxed);

            InitializeBitset(m_buffer[i].m_bits, resources);

            //m_bits are initialized to all 1s, and will be set to all 1s when dequeued
            //This ensure that after a thread equeue a new request but before it set the m_bits to
            //proper value, the following request will not pass through
            m_buffer[i].m_bits = ~0;
        }

        m_head.store(0, std::memory_order_relaxed);
        m_tail.store(0, std::memory_order_relaxed);
    }

    ~MRLock()
    {
        delete[] m_buffer;
    }

    inline uint32_t Lock(const BitsetType& resources)
    {
        //Enqueue the resource request at the tail
        //If the queue is full, busy wait at the end
        //So the capacity of the queue actually determine the FIFO fairness
        Cell* cell;
        uint32_t pos;

        for(;;)
        {
            pos = m_tail.load(std::memory_order_relaxed);
            cell = &m_buffer[pos & m_bufferMask];
            uint32_t seq = cell->m_sequence.load(std::memory_order_acquire);
            int32_t dif = (int32_t)seq - (int32_t)pos;

            if(dif == 0)
            {
                if(m_tail.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    break;
                }
            }
        }

        cell->m_bits = resources;
        cell->m_sequence.store(pos + 1, std::memory_order_release);

        //Spin on all previsou locks 
        uint32_t spinPos = m_head;
        while(spinPos != pos)
        {
            //We start from the head moving toward my pos, spin on cell that collide with my request
            //When that cell is freed we move on to the next one util reaching myself
            //we need to check both m_sequence and m_bits, because either of them could be set to 
            //indicate a free cell, and we want to move on quickly
            if(pos - m_buffer[spinPos & m_bufferMask].m_sequence > m_bufferMask 
                    || !(m_buffer[spinPos & m_bufferMask].m_bits & resources))
            {
                spinPos++;
            }
        }

        //Good to go
        return pos;
    }

    inline void Unlock(uint32_t handle)
    {
        //Release my lock by setting the bits to 0
        m_buffer[handle & m_bufferMask].m_bits = 0;

        //Dequeue cells that have been released
        uint32_t pos = m_head.load(std::memory_order_relaxed);
        while(!m_buffer[pos & m_bufferMask].m_bits)
        {
            Cell* cell = &m_buffer[pos & m_bufferMask];    
            uint32_t seq = cell->m_sequence.load(std::memory_order_acquire);
            int32_t dif = (int32_t)seq - (int32_t)(pos + 1);

            if(dif == 0)
            {
                if(m_head.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    cell->m_bits = ~0;
                    cell->m_sequence.store(pos + m_bufferMask + 1, std::memory_order_release);
                }
            }

            pos = m_head.load(std::memory_order_relaxed);
        }
    }

private:
    static const uint32_t CACHELINE_SIZE = 128;

    struct Cell
    {
        std::atomic<uint32_t> m_sequence; 
        BitsetType m_bits;
        //Cells are allocated in contiguous memory, since m_bits and m_sequence value changed frequently
        //we'd better seperate each cell to increase cache hits, and this indeed provides significant speed up
        char m_pad[CACHELINE_SIZE - sizeof(std::atomic<uint32_t>) - sizeof(uint64_t)];   
    };

    char m_pad0[CACHELINE_SIZE];
    Cell* m_buffer;
    uint32_t m_bufferMask;

    char m_pad1[CACHELINE_SIZE - sizeof(Cell*) - sizeof(uint32_t)];
    std::atomic<uint32_t> m_head;

    char m_pad2[CACHELINE_SIZE - sizeof(std::atomic<uint32_t>)];
    std::atomic<uint32_t> m_tail;
};

#endif //_CONCEPT_MRLOCK_INCLUDED_
