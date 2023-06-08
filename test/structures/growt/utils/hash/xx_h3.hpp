#pragma once

#include <string>
#include <string_view>

// this define ensures, that xxhash is inlined/does not create new compile unit
#define XXH_PRIVATE_API
#include "xxh3.h"


namespace utils_tm
{
namespace hash_tm
{

struct xx_h3
{
    static constexpr std::string_view name               = "xxh3";
    static constexpr size_t           significant_digits = 64;


    xx_h3(size_t s = 13358259232739045019ull) : seed(s) {}

    size_t seed;

    inline uint64_t operator()(const uint64_t k) const
    {
        auto local = k;
        return XXH3_64bits_withSeed(&local, 8, seed);
    }

    inline uint64_t operator()(const uint32_t k) const
    {
        auto local = k;
        return XXH3_64bits_withSeed(&local, 4, seed);
    }


    // targeted at string type classes i.e. data pointer + size
    template <class Type>
    inline uint64_t operator()(const Type& k) const
    {
        return XXH3_64bits_withSeed(k.data(), k.size(), seed);
    }
};

} // namespace hash_tm
} // namespace utils_tm
