#pragma once

// These functions are inspired by lemire's fastrange blogpost/code
// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/

#include <stdint.h>

namespace utils_tm
{

inline uint32_t fastrange32(uint32_t cap, uint32_t in)
{
    return (uint64_t(in) * uint64_t(cap)) >> 32;
}


#ifdef __SIZEOF_INT128__
constexpr bool is_defined_128 = true;
#else
constexpr bool is_defined_128 = false;
#endif

inline uint64_t fastrange64(uint64_t cap, uint64_t in)
{
    static_assert(is_defined_128,
                  "128bit integers have to be defined for fastrange64");
    return (__uint128_t(in) * __uint128_t(cap)) >> 64;
}

} // namespace utils_tm
