#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <optional>

#include "../concurrency/memory_order.hpp"

namespace utils_tm
{

// This structure is owned by one thread, and is meant to pass inputs
// to that thread
template <class T>
class many_producer_single_consumer_buffer
{
  private:
    using this_type = many_producer_single_consumer_buffer<T>;
    using memo      = concurrency_tm::standard_memory_order_policy;

    static constexpr size_t _scnd_buffer_flag = 1ull << 63;
    size_t                  _capacity;
    std::atomic_size_t      _pos;
    size_t                  _read_pos;
    size_t                  _read_end;
    std::atomic<T>*         _buffer;

  public:
    using value_type = T;

    many_producer_single_consumer_buffer(size_t capacity)
        : _capacity(capacity), _pos(0), _read_pos(0), _read_end(0)
    {
        _buffer = new std::atomic<T>[2 * capacity];

        for (size_t i = 0; i < 2 * capacity; ++i)
        {
            _buffer[i].store(T(), memo::relaxed);
        }
    }

    many_producer_single_consumer_buffer(
        const many_producer_single_consumer_buffer&) = delete;
    many_producer_single_consumer_buffer&
    operator=(const many_producer_single_consumer_buffer&) = delete;
    many_producer_single_consumer_buffer(
        many_producer_single_consumer_buffer&& other)
        : _capacity(other._capacity), _pos(other._pos.load(memo::relaxed)),
          _read_pos(other._read_pos), _read_end(other._read_end)
    {
        _buffer       = other._buffer;
        other._buffer = nullptr;
    }
    many_producer_single_consumer_buffer&
    operator=(many_producer_single_consumer_buffer&& rhs)
    {
        if (&rhs == this) return *this;

        this->~this_type();
        new (this) many_producer_single_consumer_buffer(std::move(rhs));
        return *this;
    }
    ~many_producer_single_consumer_buffer() { delete[] _buffer; }


    // can be called concurrently by all kinds of threads
    // returns false if the buffer is full
    bool push_back(const T& e)
    {
        auto tpos = _pos.fetch_add(1, memo::acq_rel);

        if (tpos & _scnd_buffer_flag)
        {
            tpos ^= _scnd_buffer_flag;
            if (tpos >= _capacity * 2) return false;
        }
        else if (tpos >= _capacity)
            return false;

        _buffer[tpos].store(e, memo::relaxed);
        return true;
    }

    template <class iterator_type>
    size_t
    push_back(iterator_type& start, const iterator_type& end, size_t number = 0)
    {
        if (number == 0) number = end - start;

        size_t tpos   = _pos.fetch_add(number, memo::acq_rel);
        size_t endpos = 0;
        if (tpos & _scnd_buffer_flag)
        {
            tpos ^= _scnd_buffer_flag;
            endpos = std::min(tpos + number, _capacity * 2);
        }
        else
            endpos = std::min(tpos + number, _capacity);

        number = endpos - tpos;

        for (; tpos < endpos; ++tpos)
        {
            _buffer[tpos]->store(*start, memo::release);
            start++;
        }
        return number;
    }

    // can be called concurrent to push_backs but only by the owning thread
    // pull_all breaks all previously pulled elements
    std::optional<T> pop()
    {
        if (_read_pos == _read_end)
        {
            fetch_on_empty_read_buffer();
            if (_read_pos == _read_end) return {};
        }
        auto read = _buffer[_read_pos].load(memo::relaxed);
        while (read == T()) read = _buffer[_read_pos].load(memo::relaxed);
        _buffer[_read_pos].store(T(), memo::relaxed);
        ++_read_pos;
        return std::make_optional(read);
    }

  private:
    void fetch_on_empty_read_buffer()
    {
        bool first_to_second = !(_scnd_buffer_flag & _pos.load(memo::relaxed));
        if (first_to_second)
        {
            _read_end =
                _pos.exchange(_capacity | _scnd_buffer_flag, memo::acq_rel);
            _read_end = std::min(_read_end, _capacity);
            _read_pos = 0;
        }
        else
        {
            _read_end = _pos.exchange(0, memo::acq_rel);
            _read_end ^= _scnd_buffer_flag;
            _read_end = std::min(_read_end, 2 * _capacity);
            _read_pos = _capacity;
        }
    }
};
} // namespace utils_tm
