#include <atomic>
#include <iostream>
#include <sstream>
#include <tuple>

#include "command_line_parser.hpp"
#include "memory_reclamation/reclamation_guard.hpp"
#include "pin_thread.hpp"
#include "thread_coordination.hpp"

namespace utm = utils_tm;
namespace otm = utm::out_tm;
namespace ttm = utm::thread_tm;
namespace rtm = utm::reclamation_tm;
using c       = otm::color;


void print_help()
{
    otm::out() << "This is a test for our hazard pointer implementation\n"
               << c::magenta + "* Executable\n"
               << "   bench/hazard_test.cpp.\n"
               << c::magenta + "* Test subject\n"
               << "   " << c::green + "proj::hazard_tm::hazard_manager"
               << " from " << c::yellow + "impl/hazard.h"
               << "\n"
               << c::magenta + "* Process\n"
               << "   Main: the main thread repeats the following it times\n"
               << "     1. wait until the others did incremented a counter\n"
               << "        (simulating some work), also wait for i-2 to be\n"
               << "        deleted (necessary for the order of the output)\n"
               << "     2. create a new foo object\n"
               << "     3. replace the current pointer with the new one\n"
               << "   Sub:  repeatedly acquire the current foo pointer and\n"
               << "         and increment its counter (in blocks of 100)\n"
               << c::magenta + "* Parameters\n"
               << "   -p #(threads)\n"
               << "   -n #(number of increments before a pointer change) \n"
               << "   -it #(repeats of the test)\n"
               << c::magenta + "* Outputs\n"
               << "   i          counts the repeats\n"
               << "   current    the pointer before the exchange\n"
               << "   next       the pointer after the exchange\n"
               << "   deletor    {thread id, pointer nmbr, pointer}\n"
               << std::flush;
}



static thread_local size_t thread_id;

class foo
{
  public:
    foo(size_t i = 0) : id(i), counter(0) {}
    ~foo()
    {
        deleted.store(id);

        otm::buffered_out()
            << c::bred + "DEL    " << otm::width(3) + id << "    ptr  " << this
            << " deleted by " << thread_id << std::endl;
    }

    static std::atomic_int deleted;
    size_t                 id;
    std::atomic_size_t     counter;
};
std::atomic_int foo::deleted = -1;



#include "memory_reclamation/counting_reclamation.hpp"
#include "memory_reclamation/delayed_reclamation.hpp"
#include "memory_reclamation/hazard_reclamation.hpp"
// This would fail #include "memory_reclamation/sequential_reclamation.hpp"

alignas(64) static std::atomic<foo*> the_one;
alignas(64) static std::atomic_bool finished{false};





template <class ReclManager, class ThreadType>
struct test;

template <class ReclManager>
struct test<ReclManager, ttm::timed_main_thread>
{
    static int execute(ttm::timed_main_thread thrd,
                       size_t                 it,
                       size_t                 n,
                       ReclManager&           recl_mngr)
    {
        utm::pin_to_core(thrd.id);
        thread_id   = thrd.id;
        auto handle = recl_mngr.get_handle();

        the_one.store(handle.create_pointer(0));
        otm::buffered_out() << c::bgreen + "NEW"
                            << "      0    start               new "
                            << the_one.load() << std::endl;

        thrd.synchronized([it, n, &handle]() {
            auto current = handle.guard(the_one);

            for (size_t i = 1; i <= it; ++i)
            {
                while (current->counter.load() < n)
                { /* wait */
                }

                auto next = handle.guard(handle.create_pointer(i));

                otm::buffered_out()
                    << c::bgreen + "NEW    " << otm::width(3) + i << "    prev "
                    << static_cast<foo*>(current) << " new "
                    << static_cast<foo*>(next) << std::endl;

                foo* cas_temp = current;
                if (!the_one.compare_exchange_strong(cas_temp, next))
                    otm::out() << "Error: on changing the pointer\n"
                               << std::flush;
                handle.safe_delete(current);
                current = std::move(next);
            }

            finished.store(true);
            foo* cas_temp = current;
            if (!the_one.compare_exchange_strong(cas_temp, nullptr))
                otm::out() << "Error: on changing ghe pointer to nullptr\n"
                           << std::flush;

            handle.safe_delete(current);
            return 0;
        });

        thrd.synchronize();
        return 0;
    }
};

template <class ReclManager>
struct test<ReclManager, ttm::untimed_sub_thread>
{
    static int execute(ttm::untimed_sub_thread thrd,
                       size_t,
                       size_t,
                       ReclManager& recl_mngr)
    {
        utm::pin_to_core(thrd.id);
        thread_id   = thrd.id;
        auto handle = recl_mngr.get_handle();

        thrd.synchronized([&handle]() {
            while (!finished.load())
            {
                auto current = handle.guard(the_one);
                if (!current) continue;
                for (size_t i = 0; i < 100; ++i) current->counter.fetch_add(1);
            }
            return 0;
        });

        thrd.synchronize();
        return 0;
    }
};



template <class ThreadType>
using delayed_test = test<rtm::delayed_manager<foo>, ThreadType>;
template <class ThreadType>
using counting_test = test<rtm::counting_manager<foo>, ThreadType>;
template <class ThreadType>
using hazard_test = test<rtm::hazard_manager<foo>, ThreadType>;

void reset_test()
{
    foo::deleted.store(-1);
    finished.store(false);
}

/* MAIN FUNCTION: READS COMMANDLINE AND STARTS TEST THREADS *******************/
int main(int argn, char** argc)
{
    utm::command_line_parser c{argn, argc};
    size_t                   p  = c.int_arg("-p", 4);
    size_t                   n  = c.int_arg("-n", 1000);
    size_t                   it = c.int_arg("-it", 20);
    if (c.bool_arg("-h"))
    {
        print_help();
        return 0;
    }
    if (!c.report()) return 1;

    otm::out() << otm::color::bblue + "DELAYED RECLAMATION TEST" << std::endl;
    rtm::delayed_manager<foo> delayed_mngr;
    ttm::start_threads<delayed_test>(p, it, n, delayed_mngr);
    reset_test();

    otm::out() << std::endl
               << otm::color::bblue + "COUNTING RECLAMATION TEST" << std::endl;
    rtm::counting_manager<foo> counting_mngr;
    ttm::start_threads<counting_test>(p, it, n, counting_mngr);
    reset_test();

    otm::out() << std::endl
               << otm::color::bblue + "HAZARD RECLAMATION TEST" << std::endl;
    rtm::hazard_manager<foo> hazard_mngr;
    ttm::start_threads<hazard_test>(p, it, n, hazard_mngr);
    reset_test();
    return 0;
}
