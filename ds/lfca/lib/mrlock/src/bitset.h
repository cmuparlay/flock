//------------------------------------------------------------------------------
// 
//     Bitset
//
//------------------------------------------------------------------------------

#ifndef _BITSET_INCLUDED_
#define _BITSET_INCLUDED_

#include <string.h>

#define WORD_INDEX(INDEX) ((INDEX) >> 5)
#define BIT_INDEX(INDEX) ((INDEX) & 0x0000001f)
#define READ_BIT(ARRAY, INDEX) (ARRAY[WORD_INDEX(INDEX)] & (0x80000000 >> BIT_INDEX(INDEX)))
#define SET_BIT(ARRAY, INDEX) (ARRAY[WORD_INDEX(INDEX)] |= (0x80000000 >> BIT_INDEX(INDEX)))
#define RESET_BIT(ARRAY, INDEX) (ARRAY[WORD_INDEX(INDEX)] &= ~(0x80000000 >> BIT_INDEX(INDEX)))

class Bitset
{
public:
    Bitset()
        : m_size(-1)
        , m_words(-1)
        , m_bits(NULL)
    {}

    Bitset(const Bitset& rhs)
        : m_size(rhs.m_size)
        , m_words(rhs.m_words)
        , m_bits(new int[m_words])
    {
        *this = rhs;
    }

    ~Bitset()
    {
        delete[] m_bits;
    }

    inline void Resize(int size, int flag = 0)
    {
        m_size = size;
        m_words = WORD_INDEX(m_size - 1) + 1;
        delete[] m_bits;
        m_bits = new int[m_words];
        
        memset(m_bits, flag, m_words * sizeof(int));
    }

    inline void operator=(const Bitset& rhs)
    {
        memcpy(m_bits, rhs.m_bits, m_words * sizeof(int));
    }

    inline void operator=(int flag)
    {
        memset(m_bits, flag, m_words * sizeof(int));
    }

    inline operator bool () const
    {
        for (int i = 0; i < m_words; i++) 
        {
            if(m_bits[i])
            {
                return true;
            }
        }

        return false;
    }

    inline bool operator & (const Bitset& rhs) const
    {
        for (int i = 0; i < m_words; i++) 
        {
            if(m_bits[i] & rhs.m_bits[i])
            {
                return true;
            }
        }

        return false;
    }

    inline void Set(int pos = -1)
    {
        if(pos >= 0)
        {
            SET_BIT(m_bits, pos);             
        }
        else
        {
            memset(m_bits, ~0, m_words * sizeof(int));
        }
    }

    inline void Reset(int pos = -1)
    {
        if(pos >= 0)
        {
            RESET_BIT(m_bits, pos);
        }
        else
        {
            memset(m_bits, 0, m_words * sizeof(int));
        }
    }

private:
    int m_size;
    int m_words;
    int* m_bits;
};

#endif //_BITSET_INCLUDED_

