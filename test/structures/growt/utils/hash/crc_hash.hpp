#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>


namespace utils_tm
{
namespace hash_tm
{

struct crc_hash
{
    static constexpr std::string_view name               = "crc32";
    static constexpr size_t           significant_digits = 64;


    crc_hash(size_t seed = 12923598712359872066ull)
        : seed0(seed), seed1(seed * 7467732452331123588ull)
    {
    }

    size_t seed0;
    size_t seed1;

    inline uint64_t operator()(const uint64_t& k) const
    {
        return uint64_t(__builtin_ia32_crc32di(k, seed0) |
                        (__builtin_ia32_crc32di(k, seed1) << 32));
    }

    inline uint64_t operator()(const uint32_t& k) const
    {
        return uint64_t(__builtin_ia32_crc32di(k, seed0) |
                        (__builtin_ia32_crc32di(k, seed1) << 32))
    }

    // string keys are not implemented for our crc-based hash function
    template <class Type>
    inline uint64_t operator()(const Type& k) const;
};

} // namespace hash_tm
} // namespace utils_tm
