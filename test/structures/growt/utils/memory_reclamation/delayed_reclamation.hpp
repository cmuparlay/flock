#pragma once

#include <atomic>
#include <tuple>
#include <vector>

#include "../concurrency/memory_order.hpp"
#include "../mark_pointer.hpp"
#include "../output.hpp"
#include "reclamation_guard.hpp"

namespace utils_tm
{
namespace reclamation_tm
{

template <class T> // Should probably have a default parameter for D
class delayed_manager
{
  private:
    using this_type = delayed_manager<T>;
    using memo      = concurrency_tm::standard_memory_order_policy;

  public:
    using pointer_type        = T*;
    using atomic_pointer_type = std::atomic<T*>;
    using protected_type      = T;

    template <class lT = T>
    struct rebind
    {
        using other = delayed_manager<lT>;
    };

    delayed_manager()                       = default;
    delayed_manager(const delayed_manager&) = delete;
    delayed_manager& operator=(const delayed_manager&) = delete;
    delayed_manager(delayed_manager&& other)           = default;
    delayed_manager& operator=(delayed_manager&& other) = default;
    ~delayed_manager()                                  = default;

    class handle_type
    {
      private:
        using parent_type = delayed_manager<T>;
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
        ~handle_type();

      private:
        std::vector<pointer_type> freelist;

      public:
        template <class... Args>
        inline T* create_pointer(Args&&... arg) const;

        inline T*   protect(const atomic_pointer_type& ptr) const;
        inline void safe_delete(pointer_type ptr);

        inline void protect_raw(pointer_type ptr) const;
        inline void delete_raw(pointer_type ptr);
        inline bool is_safe(pointer_type ptr);

        inline void unprotect(pointer_type ptr) const;
        inline void unprotect(std::vector<T*>& vec) const;

        inline guard_type guard(const atomic_pointer_type& aptr);
        inline guard_type guard(pointer_type ptr);

        void print() const;
    };
    handle_type get_handle() { return handle_type(); }
    void        delete_raw(pointer_type ptr) { delete mark::clear(ptr); }
};





template <class T>
delayed_manager<T>::handle_type::~handle_type()
{
    for (auto curr : freelist) delete curr;
}


template <class T>
template <class... Args>
T* delayed_manager<T>::handle_type::create_pointer(Args&&... arg) const
{
    return new T(std::forward<Args>(arg)...);
}

template <class T>
T* delayed_manager<T>::handle_type::protect(
    const atomic_pointer_type& ptr) const
{
    return ptr.load(memo::acquire);
}

template <class T>
void delayed_manager<T>::handle_type::safe_delete(pointer_type ptr)
{
    freelist.push_back(mark::clear(ptr));
}


template <class T>
void delayed_manager<T>::handle_type::protect_raw(pointer_type) const
{
    return;
}

template <class T>
void delayed_manager<T>::handle_type::delete_raw(pointer_type ptr)
{
    delete mark::clear(ptr);
}

template <class T>
bool delayed_manager<T>::handle_type::is_safe(pointer_type ptr)
{
    return false;
}

template <class T>
void delayed_manager<T>::handle_type::unprotect(pointer_type) const
{
    return;
}

template <class T>
void delayed_manager<T>::handle_type::unprotect(
    std::vector<pointer_type>&) const
{
    return;
}

template <class T>
typename delayed_manager<T>::handle_type::guard_type
delayed_manager<T>::handle_type::guard(const atomic_pointer_type& aptr)
{
    return make_rec_guard(*this, aptr);
}

template <class T>
typename delayed_manager<T>::handle_type::guard_type
delayed_manager<T>::handle_type::guard(pointer_type ptr)
{
    return make_rec_guard(*this, ptr);
}


template <class T>
void delayed_manager<T>::handle_type::print() const
{
    out_tm::out() << "* print in delayed reclamation strategy "
                  << freelist.size() << " pointer flagged for deletion *"
                  << std::endl;
}

} // namespace reclamation_tm
} // namespace utils_tm
