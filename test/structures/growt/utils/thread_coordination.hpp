#pragma once

/*******************************************************************************
 * thread_coordination.hpp
 *
 * Offers low level functionality for thread synchronization
 * and parallel for loops (used to simplify writing tests/benchmarks)
 *
 * See below for an example
 * (or look into the benchmarks of any of my concurrent libraries)
 *
 * Part of my utils library utils_tm - https://github.com/TooBiased/utils_tm.git
 *
 * Copyright (C) 2019 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <type_traits>

#include "concurrency/memory_order.hpp"
#include "output.hpp"

namespace utils_tm
{
namespace thread_tm
{

namespace ctm = concurrency_tm;

// EXAMPLE USE

// static atomic_size_t for_loop_counter(0);

// struct test_function
// {
//     template <class ThreadType>
//     int execute (ThreadType t, ParamType1 p1, ... , ParamTypeN pn)
//     {
//         // Executed by all threads
//         t.out << t.id << " speaking into its own output" << std::endl;
//         output_tm::out() << t.id << " speaking into the common output"
//                          << std::endl;

//         t.synchronize();
//         t.out << "*****************************************" << std::endl;

//         auto data = synchronized(
//             [](InnerParamType1 ip1, ... , InnerParamTypeN ipn)
//             {
//                 // All threads start here at the same time

//                 execute_parallel(for_loop_counter, 1000000
//                                  [](size_t i, InnerInnerParamType1 iip1,...)
//                                  {
//                                      // body of the for loop
//                                      output_tm::out() << i << std::endl;
//                                  }, iip1, ...)


//                 // The main_thread waits for all others to finish
//             }, ip1, ... , ipn)
//     }
// }

// int main(...)
// {
//     return start_threads<test_function>(p, p1, ..., pn);
// }

// POSSIBLE OUTPUT:  p=4
//                  (note access to the same output is not thread safe/fully
//                   synchronized this output is only one of many possible
//                   reorderings but the *-Line should be after the first
//                   sentences, and before the numbers start)

// 0 speaking into its own output
// 0 speaking into the common output
// 3 speaking into the common output
// 1 speaking into the common output
// 2 speaking into the common output
// *****************************************
// 0
// 500912
// 409
// 905900
// ... (all numbers in [0...1000000))




// THREAD CLASSES + SYNCHRONIZATION ********************************************
// The construction of tests (generating thread objects) is explained below

// Each thread object contains the following:
// p   (the number of threads)
// id  (the number of this current thread)
// out (an output, this output is disabled for all non-main-threads,
//      but it can be enabled or forwarded to a file)
// is_main (self-explanatory)
// synchronize() (has to be called by all threads, to synchronize (BARRIER))
// synchronized(...) (executes the function on all threads synchroneously)

static std::atomic_size_t level;
static std::atomic_size_t wait_end;
static std::atomic_size_t wait_start;

// MAIN THREAD CLASS ***********************************************************
template <bool timed>
struct main_thread
{
    main_thread(size_t p, size_t id) : p(p), id(id), _stage(0) {}

    template <typename Functor, typename... Types>
    inline std::pair<typename std::result_of<Functor(Types&&...)>::type, size_t>
    synchronized(Functor f, Types&&... param)
    {
        start_stage(p - 1, ++_stage);
        auto temp = std::forward<Functor>(f)(std::forward<Types>(param)...);
        return std::make_pair(std::move(temp), end_stage(p - 1, ++_stage));
    }

    inline void synchronize()
    {
        start_stage(p - 1, ++_stage);
        end_stage(p - 1, ++_stage);
    }

    size_t                p;
    size_t                id;
    out_tm::output_type   out;
    static constexpr bool is_main = true;

  private:
    size_t                                                      _stage;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;

    inline void start_stage(size_t p, size_t lvl)
    {
        while (wait_start.load(ctm::mo_acquire) < p)
        { /* WATING */
        }
        wait_start.store(0, ctm::mo_release);

        if constexpr (timed)
        {
            start_time = std::chrono::high_resolution_clock::now();
        }

        level.store(lvl, ctm::mo_release);
    }

    inline size_t end_stage(size_t p, size_t lvl)
    {
        while (wait_end.load(ctm::mo_acquire) < p)
            ;
        wait_end.store(0, ctm::mo_release);
        size_t result = 0;
        if constexpr (timed)
        {
            result = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::high_resolution_clock::now() - start_time)
                         .count();
        }
        level.store(lvl, ctm::mo_release);
        return result;
    }
};

using timed_main_thread   = main_thread<true>;
using untimed_main_thread = main_thread<false>;



// SUB THREAD CLASS ************************************************************
template <bool timed>
struct sub_thread
{
    sub_thread(size_t p, size_t id) : p(p), id(id), _stage(0) { out.disable(); }

    template <typename Functor, typename... Types>
    inline std::pair<typename std::result_of<Functor(Types&&...)>::type, size_t>
    synchronized(Functor f, Types&&... param)
    {
        start_stage(++_stage); // wait_for_stage(stage);
        auto temp = std::forward<Functor>(f)(std::forward<Types>(param)...);
        // finished_stage();
        return std::make_pair(temp, end_stage(++_stage));
    }

    inline void synchronize()
    {
        start_stage(++_stage);
        end_stage(++_stage);
    }

    size_t                p;
    size_t                id;
    out_tm::output_type   out;
    static constexpr bool is_main = false;

  private:
    size_t                                                      _stage;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;

    inline void start_stage(size_t lvl)
    {
        wait_start.fetch_add(1, ctm::mo_acq_rel);
        while (level.load(ctm::mo_acquire) < lvl)
        { /* wait */
        }
        if constexpr (timed)
        {
            start_time = std::chrono::high_resolution_clock::now();
        }
    }

    inline size_t end_stage(size_t lvl)
    {
        wait_end.fetch_add(1, ctm::mo_acq_rel);

        size_t result = 0;
        if constexpr (timed)
        {
            result = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::high_resolution_clock::now() - start_time)
                         .count();
        }

        while (level.load(ctm::mo_acquire) < lvl)
        { /* wait */
        }

        return result;
    }
};

// time is measured relative to the global start
using timed_sub_thread   = sub_thread<true>;
using untimed_sub_thread = sub_thread<false>;


// START TEST ******************************************************************
// 1. starts p-1 Subthreads
// 2. executes the Functor as Mainthread
// 3. rejoins the generated threads
template <template <class> class Functor, typename... Types>
inline int start_threads(size_t p, Types&&... param)
{
    std::thread* local_thread = new std::thread[p - 1];

    for (size_t i = 0; i < p - 1; ++i)
    {
        local_thread[i] =
            std::thread(Functor<untimed_sub_thread>::execute,
                        untimed_sub_thread(p, i + 1), std::ref(param)...);
    }

    // int temp =0;
    int temp =
        Functor<timed_main_thread>::execute(timed_main_thread(p, 0), param...);

    // CLEANUP THREADS
    for (size_t i = 0; i < p - 1; ++i) { local_thread[i].join(); }

    delete[] local_thread;

    return temp;
}




// PARALLEL FOR LOOPS **********************************************************
static const size_t block_size = 4096;

// BLOCKWISE EXECUTION IN PARALLEL
template <typename Functor, typename... Types>
inline void execute_parallel(std::atomic_size_t& global_counter,
                             size_t              e,
                             Functor             f,
                             Types&&... param)
{
    auto c_s = global_counter.fetch_add(block_size);
    while (c_s < e)
    {
        auto c_e = std::min(c_s + block_size, e);
        for (size_t i = c_s; i < c_e; ++i) f(i, std::forward<Types>(param)...);
        c_s = global_counter.fetch_add(block_size);
    }
}

template <typename Functor, typename... Types>
inline void execute_blockwise_parallel(std::atomic_size_t& global_counter,
                                       size_t              e,
                                       Functor             f,
                                       Types&&... param)
{
    auto c_s = global_counter.fetch_add(block_size);
    while (c_s < e)
    {
        auto c_e = std::min(c_s + block_size, e);
        f(c_s, c_e, std::forward<Types>(param)...);
        c_s = global_counter.fetch_add(block_size);
    }
}

} // namespace thread_tm
} // namespace utils_tm
