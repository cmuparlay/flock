#include <atomic>

#include "command_line_parser.hpp"
#include "data_structures/many_producer_single_consumer_buffer.hpp"
#include "output.hpp"
#include "pin_thread.hpp"
#include "thread_coordination.hpp"

namespace utm = utils_tm;
namespace otm = utils_tm::out_tm;
namespace ttm = utils_tm::thread_tm;

alignas(64) static utm::many_producer_single_consumer_buffer<size_t> buffer{0};

template <class ThreadType>
struct test;

template <>
struct test<ttm::untimed_sub_thread>
{
    static int execute(ttm::untimed_sub_thread thrd, size_t n, size_t)
    {
        utm::pin_to_core(thrd.id);

        thrd.synchronized([n]() {
            size_t npushed = 0;
            for (size_t i = 1; i <= n; ++i)
            {
                while (!buffer.push_back(i))
                { /* try inserting i while the buffer is full */
                }
                npushed++;
            }
            otm::out() << npushed << " elements pushed" << std::endl;
            return 0;
        });

        return 0;
    }
};

template <>
struct test<ttm::timed_main_thread>
{
    static int execute(ttm::timed_main_thread thrd, size_t n, size_t bsize)
    {
        utm::pin_to_core(thrd.id);
        buffer = utm::many_producer_single_consumer_buffer<size_t>{bsize};
        size_t* counter = new size_t[n + 1];
        std::fill(counter, counter + n, 0);

        thrd.synchronized([&thrd, n, counter]() {
            size_t npopped = 0;
            while (counter[n] < thrd.p - 1)
            {
                auto popped = buffer.pop();
                if (popped)
                {
                    ++npopped;
                    counter[popped.value()]++;
                }
            }
            otm::out() << npopped << " elements popped" << std::endl;
            return 0;
        });

        bool noerror = true;
        for (size_t i = 1; i <= n; ++i)
        {
            if (counter[i] != thrd.p - 1)
            {
                noerror = false;
                otm::out() << otm::color::red
                           << "unexpected element count in element " << i
                           << " count is " << counter[i] << otm::color::reset
                           << std::endl;
                break;
            }
        }
        if (noerror)
            otm::out() << otm::color::green + "test fully successful"
                       << std::endl;

        return 0;
    }
};


int main(int argn, char** argc)
{
    utm::command_line_parser c{argn, argc};
    size_t                   n     = c.int_arg("-n", 1000000);
    size_t                   bsize = c.int_arg("-s", 1000);
    size_t                   p     = c.int_arg("-p", 3);

    otm::out() << otm::color::byellow + "START CORRECTNESS TEST" << std::endl;
    otm::out() << "testing: many_producer_single_consumer_buffer" << std::endl;


    otm::out()
        << "All but one thread push increasing elements into the buffer."
        << std::endl
        << "The first thread pops elements from the buffer and checks."
        << std::endl
        << "Additionally, the popped elements are tested, wheather they"
        << std::endl
        << "appear too often (or too littles)" << std::endl
        << otm::color::bblue << "  1a. create data structure\n"
        << "  1b. wait for synchronized operation\n"
        << "  2a. pop elements and count appearances from each number\n"
        << "  2b. push back elements repeatedly, until 0..n are inserted\n"
        << "      by each thread" << std::endl
        << otm::color::reset << std::endl;


    otm::out() << otm::color::bgreen + "START TEST with <size_t>" << std::endl;
    ttm::start_threads<test>(p, n, bsize);
    otm::out() << otm::color::bgreen + "END CORRECTNESS TEST" << std::endl;

    return 0;
}
