#pragma once

#include <cstdint>
#include <cassert>
#include "plaf.h"

class InitSplitMixer {
private:
    uint64_t s;
public:
    InitSplitMixer(uint64_t seed) {
        s = seed;
    }
    uint64_t next() {
        uint64_t result = (s += 0x9E3779B97f4A7C15);
        result = (result ^ (result >> 30)) * 0xBF58476D1CE4E5B9;
        result = (result ^ (result >> 27)) * 0x94D049BB133111EB;
        return result ^ (result >> 31);
    }
};

// WARNING: only generates random numbers up to 2^53-1 for the sake of quality...
// xoshiro256+ generator
class Random64 {
private:
    #define NUM_64BIT_WORDS 4
    union {
        PAD;
        uint64_t s[NUM_64BIT_WORDS];
    };
    uint64_t rol64(uint64_t x, int k) {
        return (x << k) | (x >> (64 - k));
    }
public:
    void setSeed(uint64_t seed) {
        InitSplitMixer subseedGenerator (seed);
        for (int i=0;i<NUM_64BIT_WORDS;++i) {
            s[i] = subseedGenerator.next();
        }
    }
    Random64() {}
    Random64(uint64_t seed) {
        setSeed(seed);
    }
    uint64_t next() {
        uint64_t const result = s[0] + s[3];
        uint64_t const t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];

        s[2] ^= t;
        s[3] = rol64(s[3], 45);
        // discard low order bits to preserve only high quality top 53 bits
        return result >> 11;
    }
    uint64_t next(uint64_t n) {
        return next() % n;
    }
};
