//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------

#ifndef _BARRIER_INCLUDED_
#define _BARRIER_INCLUDED_

#include <atomic>
#include <iostream>

class ThreadBarrier 
{
public:
    ThreadBarrier(unsigned numThread)
        : m_numThread(numThread)
        , m_arrived(0)
    {}

    void Wait()
    {
        m_arrived++;

        while(m_arrived < m_numThread);
    }

private:
    unsigned m_numThread;
    std::atomic<unsigned> m_arrived;
};

#endif //_BARRIER_INCLUDED_
