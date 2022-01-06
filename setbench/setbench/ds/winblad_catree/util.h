#ifndef UTIL_H
#define UTIL_H

#include <chrono>
#include <atomic>
#include <sstream>
using namespace std;

#ifndef MAX_THREADS
#define MAX_THREADS 256
#endif

#ifndef PADDING_BYTES
#define PADDING_BYTES 128
#endif

#ifndef DEBUG
#define DEBUG if(0)
#define DEBUG1 if(0)
#define DEBUG2 if(0)
#endif

#ifndef VERBOSE
#define VERBOSE if(0)
#endif

#ifndef TRACE
#define TRACE if(0)
#endif

#ifndef TPRINT
#define TPRINT(contents) { stringstream ss; ss<<"tid="<<tid<<": "<<contents<<endl; cout<<ss.str(); }
#endif

#ifndef PRINT
#define PRINT(name) { cout<<(#name)<<"="<<name<<endl; }
#endif

struct PaddedInt64 {
    volatile int64_t v;
    char padding[PADDING_BYTES - sizeof(v)];
};

// class counter {
// private:
//     char padding0[64];
//     PaddedInt64 subcounters[MAX_THREADS];
//     // implied padding here
//     atomic<int64_t> globalCounter;
//     char padding1[64];
//     const int numThreads;
//     char padding2[64];
// public:
//     counter(int _numThreads) : numThreads(_numThreads), globalCounter(0) {
//         for (int i=0;i<MAX_THREADS;++i) subcounters[i].v = 0;
//     }
//     int64_t inc(int tid) {
//         auto val = ++subcounters[tid].v;
//         if (val >= max(1000, 30*numThreads)) {
//             globalCounter.fetch_add(val);
//             subcounters[tid].v = 0;
//         }
//     }
//     int64_t get() {
//         return globalCounter;
//     }
//     int64_t getAccurate() {
//         int64_t ret = 0;
//         for (int i=0;i<MAX_THREADS;++i) {
//             ret += subcounters[i].v;
//         }
//         ret += globalCounter;
//         return ret;
//     }
// };

class ElapsedTimer {
private:
    char padding0[PADDING_BYTES];
    bool calledStart = false;
    char padding1[PADDING_BYTES];
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    char padding2[PADDING_BYTES];
public:
    void startTimer() {
        calledStart = true;
        start = std::chrono::high_resolution_clock::now();
    }
    int64_t getElapsedMillis() {
        if (!calledStart) {
            printf("ERROR: called getElapsedMillis without calling startTimer\n");
            exit(1);
        }
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    }
};

class PaddedRandom {
private:
    volatile char padding[PADDING_BYTES-sizeof(unsigned int)];
    unsigned int seed;
public:
    PaddedRandom(void) {
        this->seed = 0;
    }
    PaddedRandom(int seed) {
        this->seed = seed;
    }
    
    void setSeed(int seed) {
        this->seed = seed;
    }
    
    /** returns pseudorandom x satisfying 0 <= x < n. **/
    unsigned int nextNatural() {
        seed ^= seed << 6;
        seed ^= seed >> 21;
        seed ^= seed << 7;
        return seed;
    }
};

class debugCounter {
private:
    struct PaddedVLL {
        volatile char padding[PADDING_BYTES-sizeof(long long)];
        volatile long long v;
    };
    PaddedVLL data[MAX_THREADS+1];
public:
    void add(const int tid, const long long val) {
        data[tid].v += val;
    }
    void inc(const int tid) {
        add(tid, 1);
    }
    long long get(const int tid) {
        return data[tid].v;
    }
    long long getTotal() {
        long long result = 0;
        for (int tid=0;tid<MAX_THREADS;++tid) {
            result += get(tid);
        }
        return result;
    }
    void clear() {
        for (int tid=0;tid<MAX_THREADS;++tid) {
            data[tid].v = 0;
        }
    }
    debugCounter() {
        clear();
    }
} __attribute__((aligned(PADDING_BYTES)));

// uint32_t murmur3(uint32_t key) {
//     constexpr uint32_t seed = 0x1a8b714c;
//     constexpr uint32_t c1 = 0xCC9E2D51;
//     constexpr uint32_t c2 = 0x1B873593;
//     constexpr uint32_t n = 0xE6546B64;
    
//     uint32_t k = key;
//     k = k * c1;
//     k = (k << 15) | (k >> 17);
//     k *= c2;
    
//     uint32_t h = k ^ seed;
//     h = (h << 13) | (h >> 19);
//     h = h*5 + n;
    
//     h ^= 4;
    
//     h ^= (h>>16);
//     h *= 0x85EBCA6B;
//     h ^= (h>>13);
//     h *= 0xC2B2AE35;
//     h ^= (h>>16);
//     return h;
// }

#endif /* UTIL_H */
