#pragma once

#include <cstddef>

template <typename T, typename M> M get_member_type(M T::*);
template <typename T, typename M> T get_class_type(M T::*);

template <typename T,
          typename R,
          R T::*M
         >
constexpr std::size_t offset_of()
{
    return reinterpret_cast<std::size_t>(&(((T*)0)->*M));
}

#define OFFSET_OF(m) offset_of<decltype(get_class_type(m)), \
                     decltype(get_member_type(m)), m>()

#ifndef COMMA
    #define COMMA ,
#endif

// struct S
// {
//     int x;
//     int y;
// };
//
// static_assert(OFFSET_OF(&S::x) == 0, "");
