#pragma once

#include <atomic>
#include <tuple>
#include <vector>

#include "../mark_pointer.hpp"
#include "../output.hpp"

#include "reclamation_guard.hpp"

namespace utils_tm
{
namespace reclamation_tm
{

// template <class T>
// class stub_atomic_pointer
// {
// private:
//     T* _ptr;
// public:
//     stub_atomic_pointer(T* ptr) : _ptr(ptr) { }

//     stub_atomic_pointer& operator=(T* ptr)
//     {
//         _ptr = ptr;
//         return *this;
//     }

//     T* load() const
//     {
//         return _ptr;
//     }

//     void store(T* ptr)
//     {
//         this->_ptr = ptr;
//     }

//     T* exchange(T* ptr)
//     {
//         auto result = _ptr;
//         _ptr = ptr;
//         return result;
//     }

//     bool compare_exchange_strong(T*& expected, T* target)
//     {
//         if (_ptr == expected)
//         { _ptr = target;   return true; }
//         else
//         { expected = _ptr; return false; }
//     }

//     operator T*() const { return _ptr; }

// };



template <class T> // Should probably have a default parameter for D
class sequential_manager
{
  private:
    using this_type = sequential_manager<T>;

  public:
    using pointer_type        = T*;
    using atomic_pointer_type = std::atomic<T*>;
    using protected_type      = T;

    template <class lT = T>
    struct rebind
    {
        using other = sequential_manager<lT>;
    };

    sequential_manager()                          = default;
    sequential_manager(const sequential_manager&) = delete;
    sequential_manager& operator=(const sequential_manager&) = delete;
    sequential_manager(sequential_manager&& other)           = default;
    sequential_manager& operator=(sequential_manager&& other) = default;
    ~sequential_manager()                                     = default;

    class handle_type
    {
      private:
        using parent_type = sequential_manager<T>;
        using this_type   = handle_type;

      public:
        using pointer_type        = typename parent_type::pointer_type;
        using atomic_pointer_type = typename parent_type::atomic_pointer_type;
        using guard_type          = reclamation_guard<T, this_type>;

        handle_type()                   = default;
        handle_type(const handle_type&) = delete;
        handle_type& operator=(const handle_type&) = delete;
        handle_type(handle_type&& other) noexcept  = default;
        handle_type& operator=(handle_type&& other) noexcept = default;
        ~handle_type()                                       = default;

      public:
        template <class... Args>
        inline T* create_pointer(Args&&... arg) const;

        inline T*   protect(const atomic_pointer_type& aptr) const;
        inline void safe_delete(pointer_type ptr) const;

        inline void protect_raw(pointer_type ptr) const;
        inline void delete_raw(pointer_type ptr) const;
        inline bool is_safe(pointer_type ptr) const;

        inline void       unprotect(pointer_type ptr) const;
        inline void       unprotect(std::vector<T*>& vec) const;
        inline guard_type guard(const atomic_pointer_type& aptr);
        inline guard_type guard(pointer_type ptr);

        void print() const;
    };
    handle_type get_handle() { return handle_type(); }
};





template <class T>
template <class... Args>
T* sequential_manager<T>::handle_type::create_pointer(Args&&... arg) const
{
    return new T(std::forward<Args>(arg)...);
}

template <class T>
T* sequential_manager<T>::handle_type::protect(
    const atomic_pointer_type& aptr) const
{
    return aptr.load();
}

template <class T>
void sequential_manager<T>::handle_type::safe_delete(pointer_type ptr) const
{
    delete mark::clear(ptr);
}


template <class T>
void sequential_manager<T>::handle_type::protect_raw(pointer_type) const
{
    return;
}

template <class T>
void sequential_manager<T>::handle_type::delete_raw(pointer_type ptr) const
{
    delete mark::clear(ptr);
}

template <class T>
bool sequential_manager<T>::handle_type::is_safe(pointer_type) const
{
    return false;
}


template <class T>
void sequential_manager<T>::handle_type::unprotect(pointer_type) const
{
    return;
}

template <class T>
void sequential_manager<T>::handle_type::unprotect(
    std::vector<pointer_type>&) const
{
    return;
}


template <class T>
typename sequential_manager<T>::handle_type::guard_type
sequential_manager<T>::handle_type::guard(const atomic_pointer_type& aptr)
{
    return guard_type(*this, aptr);
}

template <class T>
typename sequential_manager<T>::handle_type::guard_type
sequential_manager<T>::handle_type::guard(pointer_type ptr)
{
    return guard_type(*this, ptr);
}


template <class T>
void sequential_manager<T>::handle_type::print() const
{
    out_tm::out() << "* print sequential reclamation handle *" << std::endl;
    return;
}

} // namespace reclamation_tm
} // namespace utils_tm
