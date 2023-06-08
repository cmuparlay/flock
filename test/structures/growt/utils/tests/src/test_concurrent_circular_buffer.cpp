#include <atomic>

#include "command_line_parser.hpp"
#include "output.hpp"
#include "thread_coordination.hpp"

#include "data_structures/concurrent_circular_buffer.hpp"
#include "pin_thread.hpp"

namespace utm = utils_tm;
namespace otm = utils_tm::out_tm;
namespace ttm = utils_tm::thread_tm;

using buffer_type = utm::concurrent_circular_buffer<size_t>;
alignas(64) static buffer_type buffer;
alignas(64) static std::atomic_size_t counter;

template <class ThreadType>
struct test
{
    static int execute(ThreadType thrd, size_t it, size_t n, size_t w)
    {
        utm::pin_to_core(thrd.id);

        for (size_t i = 0; i < it; ++i)
        {
            if constexpr (thrd.is_main)
            { // initialize the data structure
                for (size_t i = 1; i <= w; ++i) { buffer.push(i); }
            }

            thrd.synchronized([&thrd, n]() -> int {
                ttm::execute_parallel(counter, n, [](int) {
                    auto val = buffer.pop();
                    buffer.push(val);
                });
                return 0;
            });

            if constexpr (thrd.is_main)
            { // Do some checking
                thrd.out << (buffer.size() == w
                                 ? "buffer has the correct number of elements "
                                 : "Error: unexpected number of elements ")
                         << buffer.size() << "/" << w << std::endl;
                auto checker = std::vector<bool>(w + 1, false);
                while (buffer.size())
                {
                    auto val = buffer.pop();
                    if (!val)
                    {
                        thrd.out << "Error: pop returned a dummy element"
                                 << std::endl;
                    }
                    else if (val > w)
                    {
                        thrd.out << "Error: found unexpected value " << val
                                 << " > " << w << std::endl;
                    }
                    else if (checker[val])
                    {
                        thrd.out << "Error: " << val
                                 << " was found for a second time" << std::endl;
                    }
                    else { checker[val] = true; }
                }
                for (size_t i = 1; i <= w; ++i)
                { // 0 should not be found thus we start at one
                    if (!checker[i])
                        thrd.out << "Error: value " << i
                                 << " was not found in the buffer" << std::endl;
                }
                buffer.clear();
            }
        }
        return 0;
    }
};


int main(int argn, char** argc)
{
    utm::command_line_parser c{argn, argc};
    size_t                   it = c.int_arg("-it", 5);
    size_t                   n  = c.int_arg("-n", 1000000);
    size_t                   w  = c.int_arg("-w", 100);
    size_t                   p  = c.int_arg("-p", 4);

    otm::out() << otm::color::byellow + "START CORRECTNESS TEST" << std::endl;
    otm::out() << "testing: concurrent_circular_buffer" << std::endl;


    otm::out()
        << "All threads push a number of initial elements," << std::endl
        << "afterwards threads repeatedly push and pop a random" << std::endl
        << " number of elements." << std::endl
        << "Additionally, the popped elements are tested, wheather they"
        << std::endl
        << "appear too often (or too littles)" << std::endl
        << otm::color::bblue //
        << "  1a. create and prepare data structure" << std::endl
        << "  1b. wait for synchronized operation" << std::endl
        << "  2.  repeat: pop one element and push it back into the queue"
        << std::endl
        << "  3.  evaluate the data-structure" << std::endl
        << otm::color::reset << std::endl;


    otm::out() << otm::color::bgreen + "START TEST with <size_t>" << std::endl;

    buffer = buffer_type{w};

    ttm::start_threads<test>(p, it, n, w);
    otm::out() << otm::color::bgreen + "END CORRECTNESS TEST" << std::endl;

    return 0;
}
