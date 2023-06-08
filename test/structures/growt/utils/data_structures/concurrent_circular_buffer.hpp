
#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>

#include "../concurrency/memory_order.hpp"

namespace utils_tm
{

template <class T>
class concurrent_circular_buffer
{
  private:
    using this_type = concurrent_circular_buffer<T>;
    using memo      = concurrency_tm::standard_memory_order_policy;

    size_t                            _bitmask;
    std::unique_ptr<std::atomic<T>[]> _buffer;
    std::atomic_size_t                _push_id;
    std::atomic_size_t                _pop_id;

  public:
    using value_type = T;

    concurrent_circular_buffer(size_t capacity = 128);
    concurrent_circular_buffer(const concurrent_circular_buffer&) = delete;
    concurrent_circular_buffer&
    operator=(const concurrent_circular_buffer&) = delete;
    concurrent_circular_buffer(concurrent_circular_buffer&& other);
    concurrent_circular_buffer& operator=(concurrent_circular_buffer&& rhs);
    ~concurrent_circular_buffer();

    inline void push(T e);
    inline T    pop();

    inline size_t capacity() const;
    inline size_t size() const;
    void          clear();

  private:
    inline size_t mod(size_t i) const { return i & _bitmask; }
};




// CTORS AND DTOR !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
template <class T>
concurrent_circular_buffer<T>::concurrent_circular_buffer(size_t capacity)
{
    size_t tcap = 1;
    while (tcap < capacity) tcap <<= 1;

    _buffer  = std::make_unique<std::atomic<T>[]>(tcap);
    _bitmask = tcap - 1;

    clear();
}

template <class T>
concurrent_circular_buffer<T>::concurrent_circular_buffer(
    concurrent_circular_buffer&& other)
    : _bitmask(other._bitmask), _buffer(std::move(other._buffer)),
      _push_id(other._push_id.load()), _pop_id(other._pop_id.load())
{
    other._buffer = nullptr;
}

template <class T>
concurrent_circular_buffer<T>&
concurrent_circular_buffer<T>::operator=(concurrent_circular_buffer&& other)
{
    if (&other == this) return *this;

    this->~this_type();
    new (this) concurrent_circular_buffer(std::move(other));
    return *this;
}

template <class T>
concurrent_circular_buffer<T>::~concurrent_circular_buffer()
{
    // clear();
    // free(_buffer);
}




// MAIN FUNCTIONALITY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
template <class T>
void concurrent_circular_buffer<T>::push(T e)
{
    auto id = _push_id.fetch_add(1, memo::acquire);
    id      = mod(id + 1);

    auto temp = T();
    while (!_buffer[id].compare_exchange_weak(temp, e, memo::release))
    {
        temp = T();
    }
}

template <class T>
T concurrent_circular_buffer<T>::pop()
{
    auto id = _pop_id.fetch_add(1, memo::acquire);
    id      = mod(id + 1);

    auto temp = _buffer[id].exchange(T(), memo::acq_rel);
    while (temp == T()) { temp = _buffer[id].exchange(T(), memo::acq_rel); }
    return temp;
}


// SIZE AND CAPACITY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

template <class T>
size_t concurrent_circular_buffer<T>::capacity() const
{
    return _bitmask + 1;
}

template <class T>
size_t concurrent_circular_buffer<T>::size() const
{
    return _push_id.load(memo::relaxed) - _pop_id.load(memo::relaxed);
}


// HELPER FUNCTIONS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

template <class T>
void concurrent_circular_buffer<T>::clear()
{
    // This should be fine although not standard compatible
    // Also, this works only for POD objects
    // T* non_atomic_view = reinterpret_cast<T*>(_buffer.get());
    // std::fill(non_atomic_view, non_atomic_view + _bitmask + 1, T());

    for (size_t i = 0; i < capacity(); ++i)
        _buffer[i].store(T(), memo::relaxed);
    _push_id.store(0);
    _pop_id.store(0);
}
} // namespace utils_tm
