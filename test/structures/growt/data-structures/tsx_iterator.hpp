/*******************************************************************************
 * data-structures/base_iterator.h
 *
 * Part of Project growt - https://github.com/TooBiased/growt.git
 *
 * Copyright (C) 2015-2016 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once

namespace growt
{

// Forward Declarations ********************************************************
template <class, bool> class MappedRefGrowT;
template <class, bool> class ReferenceGrowT;
template <class, bool> class IteratorGrowt;

template <class, bool> class ReferenceTSX;



// Mapped Reference
template <class TSXTable, bool is_const = false> class MappedRefTSX
{
  private:
    using BTable_t       = TSXTable;
    using key_type       = typename BTable_t::key_type;
    using mapped_type    = typename BTable_t::mapped_type;
    using pair_type      = std::pair<key_type, mapped_type>;
    using value_nc       = std::pair<const key_type, mapped_type>;
    using value_intern   = typename BTable_t::value_intern;
    using pointer_intern = value_intern*;

    template <class, bool> friend class MappedRefGrowT;
    template <class, bool> friend class ReferenceGrowT;
    template <class, bool> friend class ReferenceTSX;

  public:
    using value_type =
        typename std::conditional<is_const, const value_nc, value_nc>::type;

    MappedRefTSX(value_type _copy, pointer_intern ptr) : copy(_copy), ptr(ptr)
    {
    }

    inline void refresh() { copy = *ptr; }

    template <bool is_const2 = is_const>
    inline typename std::enable_if<!is_const2>::type
    operator=(const mapped_type& value)
    {
        ptr->setData(value);
    }
    template <class F> inline void update(const mapped_type& value, F f)
    {
        ptr->update(copy.first, value, f);
    }
    inline bool compare_exchange(mapped_type& exp, const mapped_type& val)
    {
        auto temp = value_intern(copy.first, exp);
        if (ptr->CAS(temp, value_intern(copy.first, val)))
        {
            copy.second = val;
            return true;
        }
        else
        {
            copy.second = temp.second;
            exp         = temp.second;
            return false;
        }
    }

    inline operator mapped_type() const { return copy.second; }

  private:
    pair_type      copy;
    pointer_intern ptr;
};




// Reference *******************************************************************
template <class TSXTable, bool is_const = false> class ReferenceTSX
{
  private:
    using BTable_t       = TSXTable;
    using key_type       = typename BTable_t::key_type;
    using mapped_type    = typename BTable_t::mapped_type;
    using pair_type      = std::pair<key_type, mapped_type>;
    using value_nc       = std::pair<const key_type, mapped_type>;
    using value_intern   = typename BTable_t::value_intern;
    using pointer_intern = value_intern*;

    using mapped_ref = MappedRefTSX<TSXTable, is_const>;

    template <class, bool> friend class ReferenceGrowT;
    template <class, bool> friend class MappedRefGrowT;

  public:
    using value_type =
        typename std::conditional<is_const, const value_nc, value_nc>::type;

    ReferenceTSX(value_type _copy, pointer_intern ptr)
        : second(_copy, ptr), first(second.copy.first)
    {
    }

    inline void refresh() { second.refresh(); }

    template <class F> inline void update(const mapped_type& value, F f)
    {
        second.update(value, f);
    }
    inline bool compare_exchange(mapped_type& exp, const mapped_type& val)
    {
        return second.compare_exchange(exp, val);
    }

    inline operator pair_type() const { return second.copy; }
    inline operator value_type() const
    {
        return reinterpret_cast<value_type>(second.copy);
    }

    mapped_ref      second;
    const key_type& first;
};


// Iterator ********************************************************************
template <class TSXTable, bool is_const = false> class IteratorTSX
{
  private:
    using BTable_t       = TSXTable;
    using key_type       = typename BTable_t::key_type;
    using mapped_type    = typename BTable_t::mapped_type;
    using pair_type      = std::pair<key_type, mapped_type>;
    using value_nc       = std::pair<const key_type, mapped_type>;
    using value_intern   = typename BTable_t::value_intern;
    using pointer_intern = value_intern*;

    template <class, bool> friend class IteratorGrowT;
    template <class, bool> friend class ReferenceGrowT;

  public:
    using difference_type = std::ptrdiff_t;
    using value_type =
        typename std::conditional<is_const, const value_nc, value_nc>::type;
    using reference = ReferenceTSX<TSXTable, is_const>;
    // using pointer    = value_type*;
    using iterator_category = std::forward_iterator_tag;

    // template<class T, bool b>
    // friend void swap(IteratorTSX<T,b>& l, IteratorTSX<T,b>& r);
    template <class T, bool b>
    friend bool
    operator==(const IteratorTSX<T, b>& l, const IteratorTSX<T, b>& r);
    template <class T, bool b>
    friend bool
    operator!=(const IteratorTSX<T, b>& l, const IteratorTSX<T, b>& r);

    // Constructors ************************************************************
    IteratorTSX(const pair_type& copy, value_intern* ptr, value_intern* eptr)
        : copy(copy), ptr(ptr), eptr(eptr)
    {
    }

    IteratorTSX(const IteratorTSX& rhs)
        : copy(rhs.copy), ptr(rhs.ptr), eptr(rhs.eptr)
    {
    }
    IteratorTSX& operator=(const IteratorTSX& r)
    {
        copy = r.copy;
        ptr  = r.ptr;
        eptr = r.eptr;
        return *this;
    }

    ~IteratorTSX() = default;

    // Basic Iterator Functionality ********************************************
    inline IteratorTSX& operator++(int = 0)
    {
        ++ptr;
        while (ptr < eptr && (ptr->isEmpty() || ptr->isDeleted())) { ++ptr; }
        if (ptr == eptr)
        {
            ptr  = nullptr;
            copy = std::make_pair(key_type(), mapped_type());
        }
        return *this;
    }

    inline reference operator*() const { return reference(copy, ptr); }
    // pointer   operator->() const { return  ptr; }

    inline bool operator==(const IteratorTSX& rhs) const
    {
        return ptr == rhs.ptr;
    }
    inline bool operator!=(const IteratorTSX& rhs) const
    {
        return ptr != rhs.ptr;
    }

    // Functions necessary for concurrency *************************************
    inline void refresh() { copy = *ptr; }

    inline bool erase()
    {
        auto temp   = value_intern(reinterpret_cast<value_type>(copy));
        auto result = false;
        while (!temp.isDeleted)
        {
            if (ptr->atomicDelete(temp))
            {
                this->operator++();
                return true;
            }
        }
        this->operator++();
        return false;
    }

  private:
    pair_type      copy;
    pointer_intern ptr;
    pointer_intern eptr;
};


} // namespace growt
