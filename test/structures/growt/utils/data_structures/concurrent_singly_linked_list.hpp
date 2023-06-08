#pragma once

#include <atomic>
#include <iterator>

#include "../concurrency/memory_order.hpp"

namespace utils_tm
{

template <class T>
class concurrent_singly_linked_list
{
  private:
    using this_type = concurrent_singly_linked_list<T>;
    using memo      = concurrency_tm::standard_memory_order_policy;

    class queue_item_type
    {
      public:
        queue_item_type(const T& element) : value(element), next(nullptr) {}
        template <class... Args>
        queue_item_type(Args&&... args)
            : value(std::forward<Args>(args)...), next(nullptr)
        {
        }
        T                             value;
        std::atomic<queue_item_type*> next;
    };

    template <bool is_const>
    class iterator_base
    {
      private:
        using this_type = iterator_base<is_const>;
        using item_ptr  = typename std::conditional<is_const,
                                                   const queue_item_type*,
                                                   queue_item_type*>::type;
        item_ptr _ptr;

      public:
        using difference_type = std::ptrdiff_t;
        using value_type =
            typename std::conditional<is_const, const T, T>::type;
        using reference         = value_type&;
        using pointer           = value_type*;
        using iterator_category = std::forward_iterator_tag;

        iterator_base(item_ptr ptr) : _ptr(ptr) {}
        iterator_base(const iterator_base& other) = default;
        iterator_base& operator=(const iterator_base& other) = default;
        ~iterator_base()                                     = default;

        inline reference operator*() const;
        inline pointer   operator->() const;

        inline iterator_base& operator++();

        inline bool operator==(const iterator_base& other) const;
        inline bool operator!=(const iterator_base& other) const;
    };

    std::atomic<queue_item_type*> _head;

  public:
    using value_type          = T;
    using reference_type      = T&;
    using pointer_type        = T*;
    using iterator_type       = iterator_base<false>;
    using const_iterator_type = iterator_base<true>;

    concurrent_singly_linked_list() : _head(nullptr) {}
    concurrent_singly_linked_list(const concurrent_singly_linked_list&) =
        delete;
    concurrent_singly_linked_list&
    operator=(const concurrent_singly_linked_list&) = delete;
    concurrent_singly_linked_list(concurrent_singly_linked_list&& source);
    concurrent_singly_linked_list&
    operator=(concurrent_singly_linked_list&& source);
    ~concurrent_singly_linked_list();

    template <class... Args>
    inline void emplace(Args&&... args);
    inline void push(const T& element);
    inline void push(queue_item_type* item);

    iterator_type find(const T& element);
    bool          contains(const T& element);

    size_t size() const;

    inline iterator_type       begin();
    inline const_iterator_type begin() const;
    inline const_iterator_type cbegin() const;
    inline iterator_type       end();
    inline const_iterator_type end() const;
    inline const_iterator_type cend() const;
};

template <class T>
concurrent_singly_linked_list<T>::concurrent_singly_linked_list(
    concurrent_singly_linked_list&& source)
{
    auto temp = source._head.exchange(nullptr, memo::acq_rel);
    _head.store(temp, memo::relaxed);
}

template <class T>
concurrent_singly_linked_list<T>& concurrent_singly_linked_list<T>::operator=(
    concurrent_singly_linked_list&& source)
{
    if (this == &source) return *this;
    this->~concurrent_singly_linked_list();
    new (this) concurrent_singly_linked_list(std::move(source));
    return *this;
}

template <class T>
concurrent_singly_linked_list<T>::~concurrent_singly_linked_list()
{
    auto temp = _head.exchange(nullptr, memo::relaxed);
    while (temp)
    {
        auto next = temp->next.load(memo::relaxed);
        delete temp;
        temp = next;
    }
}



template <class T>
template <class... Args>
void concurrent_singly_linked_list<T>::emplace(Args&&... args)
{
    queue_item_type* item = new queue_item_type(std::forward<Args>(args)...);
    push(item);
}

template <class T>
void concurrent_singly_linked_list<T>::push(const T& element)
{
    queue_item_type* item = new queue_item_type{element};
    push(item);
}

template <class T>
void concurrent_singly_linked_list<T>::push(queue_item_type* item)
{
    auto temp = _head.load(memo::acquire);
    do {
        item->next.store(temp, memo::relaxed);
    } while (!_head.compare_exchange_weak(temp, item, memo::acq_rel));
}



template <class T>
typename concurrent_singly_linked_list<T>::iterator_type
concurrent_singly_linked_list<T>::find(const T& element)
{
    return std::find(begin(), end(), element);
}

template <class T>
bool concurrent_singly_linked_list<T>::contains(const T& element)
{
    return find(element) != end();
}

template <class T>
size_t concurrent_singly_linked_list<T>::size() const
{
    auto result = 0;
    for ([[maybe_unused]] const auto& e : *this) { ++result; }
    return result;
}



// ITERATOR STUFF
template <class T>
typename concurrent_singly_linked_list<T>::iterator_type
concurrent_singly_linked_list<T>::begin()
{
    return iterator_type(_head.load(memo::acquire));
}

template <class T>
typename concurrent_singly_linked_list<T>::const_iterator_type
concurrent_singly_linked_list<T>::begin() const
{
    return cbegin();
}

template <class T>
typename concurrent_singly_linked_list<T>::const_iterator_type
concurrent_singly_linked_list<T>::cbegin() const
{
    return const_iterator_type(_head.load(memo::acquire));
}

template <class T>
typename concurrent_singly_linked_list<T>::iterator_type
concurrent_singly_linked_list<T>::end()
{
    return iterator_type(nullptr);
}

template <class T>
typename concurrent_singly_linked_list<T>::const_iterator_type
concurrent_singly_linked_list<T>::end() const
{
    return cend();
}

template <class T>
typename concurrent_singly_linked_list<T>::const_iterator_type
concurrent_singly_linked_list<T>::cend() const
{
    return const_iterator_type(nullptr);
}



// ITERATOR IMPLEMENTATION
template <class T>
template <bool c>
typename concurrent_singly_linked_list<T>::template iterator_base<c>::reference
concurrent_singly_linked_list<T>::iterator_base<c>::operator*() const
{
    return _ptr->value;
}

template <class T>
template <bool c>
typename concurrent_singly_linked_list<T>::template iterator_base<c>::pointer
concurrent_singly_linked_list<T>::iterator_base<c>::operator->() const
{
    return &(_ptr->value);
}

template <class T>
template <bool c>
typename concurrent_singly_linked_list<T>::template iterator_base<
    c>::iterator_base&
concurrent_singly_linked_list<T>::iterator_base<c>::operator++()
{
    _ptr = _ptr->next.load(memo::acquire);
    return *this;
}

template <class T>
template <bool c>
bool concurrent_singly_linked_list<T>::iterator_base<c>::operator==(
    const iterator_base& other) const
{
    return _ptr == other._ptr;
}

template <class T>
template <bool c>
bool concurrent_singly_linked_list<T>::iterator_base<c>::operator!=(
    const iterator_base& other) const
{
    return _ptr != other._ptr;
}
} // namespace utils_tm
