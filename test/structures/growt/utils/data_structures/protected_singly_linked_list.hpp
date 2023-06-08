#pragma once

#include <atomic>

#include "../concurrency/memory_order.hpp"
#include "../debug.hpp"
#include "../mark_pointer.hpp"
#include "../memory_reclamation/hazard_reclamation.hpp"
#include "../memory_reclamation/reclamation_guard.hpp"


namespace utils_tm
{

namespace mark = mark;
namespace dtm  = debug_tm;
namespace rtm  = reclamation_tm;

template <class Value>
using hzrd = utils_tm::reclamation_tm::hazard_manager<Value>;

template <class Value, template <class> class RecMngr = hzrd>
class protected_singly_linked_list
{
  private:
    using this_type = protected_singly_linked_list<Value, RecMngr>;
    using memo      = concurrency_tm::standard_memory_order_policy;

  public:
    class queue_item_type
    {
      public:
        using value_type               = Value;
        using reclamation_manager_type = RecMngr<queue_item_type>;
        using atomic_queue_item_ptr =
            typename reclamation_manager_type::atomic_pointer_type;

        queue_item_type(value_type t) : value(std::move(t)), next(nullptr) {}
        value_type            value;
        atomic_queue_item_ptr next;
    };

    using value_type = Value;
    using reclamation_manager_type =
        typename queue_item_type::reclamation_manager_type;
    using reclamation_handle_type =
        typename reclamation_manager_type::handle_type;
    using atomic_queue_item_ptr =
        typename reclamation_manager_type::atomic_pointer_type;
    using queue_item_ptr = typename reclamation_manager_type::pointer_type;
    using guard_type     = typename reclamation_manager_type::guard_type;

    template <bool is_const>
    class iterator_base
    {
      private:
        using this_type = iterator_base<is_const>;
        using item_ptr  = typename std::conditional<is_const,
                                                   const queue_item_type*,
                                                   queue_item_type*>::type;

        guard_type _guard;

      public:
        using difference_type = std::ptrdiff_t;
        using value_type =
            typename std::conditional<is_const, const Value, Value>::type;
        using reference         = value_type&;
        using pointer           = value_type*;
        using iterator_category = std::forward_iterator_tag;

        iterator_base(guard_type guard) : _guard(std::move(guard)) {}
        iterator_base(const iterator_base& other) = default;
        iterator_base& operator=(const iterator_base& other) = default;
        ~iterator_base()                                     = default;

        inline reference operator*() const;
        inline pointer   operator->() const;

        inline iterator_base& operator++();    // pre
        inline iterator_base& operator++(int); // post

        inline bool operator==(const iterator_base& other) const;
        inline bool operator!=(const iterator_base& other) const;
    };

    using iterator_type       = iterator_base<false>;
    using const_iterator_type = iterator_base<true>;

    protected_singly_linked_list() : _head(nullptr) {}
    protected_singly_linked_list(
        protected_singly_linked_list&& source) noexcept;
    protected_singly_linked_list&
    operator=(protected_singly_linked_list&& source) noexcept;
    ~protected_singly_linked_list();

    inline void push(reclamation_handle_type& h, value_type element);
    inline void push(reclamation_handle_type& h, queue_item_type* item);
    inline iterator_type
                  push_or_find(reclamation_handle_type& h, value_type element);
    inline size_t erase(reclamation_handle_type& h, value_type element);

    iterator_type find(reclamation_handle_type& h, const value_type& element);
    bool contains(reclamation_handle_type& h, const value_type& element);

    size_t size(reclamation_handle_type& h);

    inline iterator_type       begin(reclamation_handle_type& h);
    inline const_iterator_type begin(reclamation_handle_type& h) const;
    inline const_iterator_type cbegin(reclamation_handle_type& h) const;
    inline iterator_type       end(reclamation_handle_type& h);
    inline const_iterator_type end(reclamation_handle_type& h) const;
    inline const_iterator_type cend(reclamation_handle_type& h) const;

  private:
    atomic_queue_item_ptr _head;

    inline iterator_type       make_iterator(guard_type&& guard);
    inline const_iterator_type make_citerator(guard_type&& guard) const;
    inline void                remove(reclamation_handle_type& h,
                                      guard_type&              prev,
                                      guard_type&              curr,
                                      guard_type&              next);

  public:
    class handle_type
    {
      public:
        handle_type(protected_singly_linked_list& list,
                    reclamation_handle_type&      prot)
            : _list(list), _prot(prot)
        {
        }

        inline void          push(value_type element);
        inline void          push(queue_item_type* item);
        inline iterator_type push_or_find(value_type element);
        inline size_t        erase(value_type element);

        iterator_type find(const value_type& element);
        bool          contains(const value_type& element);

        size_t size();

        inline iterator_type       begin();
        inline const_iterator_type begin() const;
        inline const_iterator_type cbegin() const;
        inline iterator_type       end();
        inline const_iterator_type end() const;
        inline const_iterator_type cend() const;

      private:
        protected_singly_linked_list& _list;
        reclamation_handle_type&      _prot;
    };
    handle_type get_handle(reclamation_handle_type& h)
    {
        return handle_type(*this, h);
    }
};


// no concurrent operations on source are allowed
template <class V, template <class> class R>
protected_singly_linked_list<V, R>::protected_singly_linked_list(
    protected_singly_linked_list&& source) noexcept
{
    auto temp = source._head.exchange(nullptr, memo::acq_rel);
    _head.store(temp, memo::relaxed);
}

// no concurrent operations (both on source or target)
template <class V, template <class> class R>
protected_singly_linked_list<V, R>&
protected_singly_linked_list<V, R>::operator=(
    protected_singly_linked_list&& source) noexcept
{
    if (this == &source) return *this;
    this->~protected_singly_linked_list();
    new (this) protected_singly_linked_list(std::move(source));
    return *this;
}

// no concurrent operations allowed
template <class V, template <class> class R>
protected_singly_linked_list<V, R>::~protected_singly_linked_list()
{
    auto temp = _head.exchange(nullptr, memo::relaxed);
    while (temp)
    {
        auto next = temp->next.load(memo::relaxed);
        delete temp;
        temp = next;
    }
}

template <class V, template <class> class R>
void protected_singly_linked_list<V, R>::push(reclamation_handle_type& h,
                                              V                        element)
{
    queue_item_type* item = h.create_pointer(std::move(element));
    push(h, item);
}

template <class V, template <class> class R>
void protected_singly_linked_list<V, R>::push(
    [[maybe_unused]] reclamation_handle_type& h, queue_item_type* item)
{
    auto temp = _head.load(memo::acquire);
    do {
        item->next.store(temp, memo::relaxed);
    } while (!_head.compare_exchange_weak(temp, item, memo::acq_rel));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::push_or_find(reclamation_handle_type& h,
                                                 V element)
{
    auto item = rtm::make_rec_guard(h, h.create_pointer(std::move(element)));

    while (true)
    {
        // the chain is empty -> insert in the beginning
        while (!_head.load(std::memory_order_relaxed))
        {
            queue_item_ptr temp = nullptr;
            if (_head.compare_exchange_weak(temp, item, memo::release))
                return make_iterator(std::move(item));
        }

        auto prev = guard_type(h);
        auto curr = guard(h, _head);
        if (!curr) continue; // queue has been emptied


        while (true)
        {
            // check if the first link is already correct
            if (curr->value == item->value &&
                !mark::is_marked(curr->next.load(
                    memo::acquire))) // go on, if it was on it's way out
            {
                h.delete_raw(item.release());
                return make_iterator(std::move(curr));
            }

            auto next = h.guard(curr->next);

            // curr has no next
            if (!next)
            {
                // -> insert after curr
                queue_item_ptr temp = nullptr;
                if (curr->next.compare_exchange_strong(temp, item,
                                                       memo::acq_rel))
                    return make_iterator(std::move(item));
                continue;
            }
            // we might have to help removing the element
            else if (mark::is_marked(queue_item_ptr(next)))
            {
                remove(h, prev, curr, next);
                break;
            }
            // curr has been removed
            else if (mark::clear(queue_item_ptr(next)) == curr)
            {
                // -> return to the outer loop (i.e. start from head);
                break;
            }

            prev = std::move(curr);
            curr = std::move(next);
        }
    }
}

template <class V, template <class> class R>
size_t
protected_singly_linked_list<V, R>::erase(reclamation_handle_type& h, V element)
{
    while (true)
    {
        // the chain is empty -> insert in the beginning
        while (!_head.load(memo::relaxed))
        {
            // not found
            return 0;
        }

        auto prev = guard_type(h);
        auto curr = guard_type(h, _head);
        if (!curr) continue; // queue has been emptied

        while (true)
        {
            auto next = h.guard(curr->next);

            // check if the first link is already correct
            if (!mark::is_marked(queue_item_ptr(next)) &&
                curr->value == element) // go on, if it was on it's way out
            {
                // todo here
                queue_item_ptr temp = next;
                if (!mark::atomic_mark<1>(curr->next, temp)) { continue; }
                remove(h, prev, curr, next);
                return 1;
            }
            // curr has no next
            else if (!next)
            {
                // not found
                return 0;
            }
            // we might have to help removing the element
            else if (mark::is_marked(queue_item_ptr(next)))
            {
                remove(h, prev, curr, next);
                break;
            }
            // curr has been removed
            else if (mark::clear(queue_item_ptr(next)) == curr)
            {
                // -> return to the outer loop (i.e. start from head);
                break;
            }

            prev = std::move(curr);
            curr = std::move(next);
        }
    }
}



template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::find(reclamation_handle_type& h,
                                         const V&                 element)
{
    while (true)
    {
        // the chain is empty -> insert in the beginning
        auto prev = guard_type(h);
        auto curr = guard(h, _head);
        if (!curr) return end(); // queue has been emptied

        while (true)
        {
            // check if the first link is already correct
            if (curr->value == element &&
                !mark::is_marked(curr->next.load(
                    memo::acquire))) // go on, if it was on it's way out
            {
                return make_iterator(std::move(curr));
            }

            auto next = h.guard(curr->next);

            // curr has no next
            if (!next)
            {
                // -> not found
                return end();
            }
            // we might have to help removing the element
            else if (mark::is_marked(queue_item_ptr(next)))
            {
                remove(h, prev, curr, next);
                break;
            }
            // curr has been removed
            else if (mark::clear(queue_item_ptr(next)) == curr)
            {
                // -> return to the outer loop (i.e. start from head);
                break;
            }

            prev = std::move(curr);
            curr = std::move(next);
        }
    }
}

template <class V, template <class> class R>
bool protected_singly_linked_list<V, R>::contains(reclamation_handle_type& h,
                                                  const V& element)
{
    return find(h, element) != end();
}

template <class V, template <class> class R>
size_t protected_singly_linked_list<V, R>::size(reclamation_handle_type& h)
{
    while (true)
    {
        // the chain is empty -> insert in the beginning
        auto prev = guard_type(h);
        auto curr = guard_type(h, _head);
        if (!curr) return 0; // queue has been emptied

        size_t i = 1;

        while (true)
        {
            auto next = h.guard(curr->next);

            // curr has no next
            if (!next)
            {
                // -> not found
                return i;
            }
            // we might have to help removing the element
            else if (mark::is_marked(queue_item_ptr(next)))
            {
                remove(h, prev, curr, next);
                break;
            }
            // curr has been removed
            else if (mark::clear(queue_item_ptr(next)) == curr)
            {
                // -> return to the outer loop (i.e. start from head);
                break;
            }
            i++;
            prev = std::move(curr);
            curr = std::move(next);
        }
    }
}



// ITERATOR STUFF
template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::begin(reclamation_handle_type& h)
{
    auto temp = h.guard(_head);
    return make_iterator(std::move(temp));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::begin(reclamation_handle_type& h) const
{
    auto temp = h.guard(_head);
    return make_citerator(std::move(temp));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::cbegin(reclamation_handle_type& h) const
{
    auto temp = h.guard(_head);
    return make_citerator(std::move(temp));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::end(reclamation_handle_type& h)
{
    return make_iterator(guard_type(h));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::end(reclamation_handle_type& h) const
{
    return make_citerator(guard_type(h));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::cend(reclamation_handle_type& h) const
{
    return make_citerator(guard_type(h));
}


// MAKE ITERATOR STUFF

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::make_iterator(guard_type&& guard)
{
    return iterator_type(std::move(guard));
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::make_citerator(guard_type&& guard) const
{
    return const_iterator_type(std::move(guard));
}


template <class V, template <class> class R>
void protected_singly_linked_list<V, R>::remove(reclamation_handle_type& h,
                                                guard_type&              prev,
                                                guard_type&              curr,
                                                guard_type&              next)
{
    while (mark::is_marked(queue_item_ptr(next)) && curr)
    {
        queue_item_ptr ctemp = mark::clear(queue_item_ptr(curr));
        queue_item_ptr ntemp = mark::clear(queue_item_ptr(next));

        if (!prev)
        {
            if (!_head.compare_exchange_strong(ctemp, ntemp, memo::acq_rel))
                return;
        }
        else
        {
            if (!prev->next.compare_exchange_strong(ctemp, ntemp,
                                                    memo::acq_rel))
                return;
        }

        ctemp->next.store(ctemp, memo::release);
        curr = std::move(next);
        h.safe_delete(ctemp);
        if (!ntemp) return;
        next = rtm::make_rec_guard(h, ntemp->next);
    }
}







// ITERATOR IMPLEMENTATION
template <class V, template <class> class R>
template <bool c>
typename protected_singly_linked_list<V,
                                      R>::template iterator_base<c>::reference
protected_singly_linked_list<V, R>::iterator_base<c>::operator*() const
{
    return _guard->value;
}

template <class V, template <class> class R>
template <bool c>
typename protected_singly_linked_list<V, R>::template iterator_base<c>::pointer
protected_singly_linked_list<V, R>::iterator_base<c>::operator->() const
{
    return &(_guard->value);
}

template <class V, template <class> class R>
template <bool c>
typename protected_singly_linked_list<V, R>::template iterator_base<
    c>::iterator_base&
protected_singly_linked_list<V, R>::iterator_base<c>::operator++()
{
    guard_type tguard = rtm::add_guard(_guard, _guard->next);
    _guard            = std::move(tguard);
    return *this;
}

template <class V, template <class> class R>
template <bool c>
typename protected_singly_linked_list<V, R>::template iterator_base<
    c>::iterator_base&
protected_singly_linked_list<V, R>::iterator_base<c>::operator++(int)
{
    guard_type tguard = rtm::add_guard(_guard, _guard->next);
    std::swap(_guard, tguard);
    return iterator_base(std::move(tguard));
}

template <class V, template <class> class R>
template <bool c>
bool protected_singly_linked_list<V, R>::iterator_base<c>::operator==(
    const iterator_base& other) const
{
    return queue_item_ptr(_guard) == queue_item_ptr(other._guard);
}

template <class V, template <class> class R>
template <bool c>
bool protected_singly_linked_list<V, R>::iterator_base<c>::operator!=(
    const iterator_base& other) const
{
    return queue_item_ptr(_guard) != queue_item_ptr(other._guard);
}



template <class V, template <class> class R>
void protected_singly_linked_list<V, R>::handle_type::push(value_type element)
{
    _list.push(_prot, element);
}

template <class V, template <class> class R>
void protected_singly_linked_list<V, R>::handle_type::push(
    queue_item_type* item)
{
    _list.push(_prot, item);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::handle_type::push_or_find(
    value_type element)
{
    return _list.push_or_find(_prot, element);
}

template <class V, template <class> class R>
size_t
protected_singly_linked_list<V, R>::handle_type::erase(value_type element)
{
    return _list.erase(_prot, element);
}


template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::handle_type::find(const value_type& element)
{
    return _list.find(_prot, element);
}

template <class V, template <class> class R>
bool protected_singly_linked_list<V, R>::handle_type::contains(
    const value_type& element)
{
    return _list.contains(_prot, element);
}

template <class V, template <class> class R>
size_t protected_singly_linked_list<V, R>::handle_type::size()
{
    return _list.size(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::handle_type::begin()
{
    return _list.begin(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::handle_type::begin() const
{
    return _list.begin(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::handle_type::cbegin() const
{
    return _list.cbegin(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::iterator_type
protected_singly_linked_list<V, R>::handle_type::end()
{
    return _list.end(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::handle_type::end() const
{
    return _list.end(_prot);
}

template <class V, template <class> class R>
typename protected_singly_linked_list<V, R>::const_iterator_type
protected_singly_linked_list<V, R>::handle_type::cend() const
{
    return _list.cend(_prot);
}

} // namespace utils_tm
