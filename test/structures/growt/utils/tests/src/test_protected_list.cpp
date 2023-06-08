#include <atomic>
#include <string>

#include "command_line_parser.hpp"
#include "data_structures/protected_singly_linked_list.hpp"
#include "output.hpp"
#include "pin_thread.hpp"
#include "thread_coordination.hpp"

namespace utm = utils_tm;
namespace rtm = utils_tm::reclamation_tm;
namespace otm = utils_tm::out_tm;
namespace ttm = utils_tm::thread_tm;

using queue_type = utm::protected_singly_linked_list<std::pair<size_t, size_t>>;
using rec_mngr_type = typename queue_type::reclamation_manager_type;

alignas(64) static queue_type queue;
alignas(64) static rec_mngr_type rec_mngr;
alignas(64) static std::atomic_size_t errors;

std::string size_test(size_t exp, size_t real)
{
    if (exp == real) return "";
    errors.fetch_add(1);
    std::string str = "Unexpected Size! expected " + std::to_string(exp) +
                      " found " + std::to_string(real) + "\n";
    return str;
}

template <class ThreadType>
struct test
{
    static int execute(ThreadType thrd, size_t n, size_t it)
    {
        utm::pin_to_core(thrd.id);
        auto rec_handle = rec_mngr.get_handle();

        for (size_t i = 0; i < it; ++i)
        {
            if constexpr (thrd.is_main) { queue = queue_type(); }

            thrd.synchronized([&thrd, &rec_handle, n]() {
                size_t lerrors = 0;

                for (size_t i = 0; i < n; ++i)
                {
                    queue.push(rec_handle, std::make_pair(i, thrd.id));
                }
                int prev = n - 1;
                for (auto it = queue.begin(rec_handle);
                     it != queue.end(rec_handle); ++it)
                {
                    auto [current, p] = *it;
                    if (p == thrd.id && int(current) != prev--)
                    {
                        lerrors++;
                        otm::out() << "Wrong order?" << std::endl;
                    }
                }
                if (prev != -1)
                {
                    otm::out() << "Thread " << thrd.id
                               << " not all elements found?" << std::endl;
                }
                errors.fetch_add(lerrors, std::memory_order_release);
                return 0;
            });

            thrd.out << size_test(thrd.p * n, queue.size(rec_handle))
                     << (!errors.load() ? "Push test successful!"
                                        : "Push test unsuccessful!")
                     << std::endl;
            if (thrd.is_main) errors.store(0);



            thrd.synchronized([&thrd, &rec_handle, n]() {
                size_t lerrors = 0;

                for (size_t i = 0; i < n; ++i)
                {
                    if (!queue.erase(rec_handle, std::make_pair(i, thrd.id)))
                        ++lerrors;
                }
                errors.fetch_add(lerrors, std::memory_order_release);
                return 0;
            });

            thrd.out << size_test(0, queue.size(rec_handle))
                     << (!errors.load() ? "Erase test successful!"
                                        : "Erase test unsuccessful!")
                     << std::endl;

            if (!errors.load())
            {
                thrd.out << otm::color::green + "Test fully successful!"
                         << std::endl;
            }
            else
            {
                thrd.out << otm::color::red + "Test unsuccessful!" << std::endl;
            }
        }

        return 0;
    }
};


int main(int argn, char** argc)
{
    utm::command_line_parser c{argn, argc};
    size_t                   n  = c.int_arg("-n", 1000000);
    size_t                   p  = c.int_arg("-p", 4);
    size_t                   it = c.int_arg("-it", 8);

    otm::out() << otm::color::byellow + "START CORRECTNESS TEST" << std::endl;
    otm::out() << "testing: concurrent_singly_linked_list" << std::endl;


    otm::out() << "All but one thread push increasing elements into the buffer."
               << std::endl
               << "Then iterate through all inserted elements. Test weather"
               << std::endl
               << "each thread inserted all its elements." << std::endl
               << otm::color::bblue //
               << "  1. each thread pushes n elements" << std::endl
               << "  2. each thread iterates over elements and finds its own"
               << std::endl;


    otm::out() << otm::color::bgreen + "START TEST" << std::endl;
    ttm::start_threads<test>(p, n, it);
    otm::out() << otm::color::bgreen + "END CORRECTNESS TEST" << std::endl;

    return 0;
}
