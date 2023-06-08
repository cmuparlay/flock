#include <random>
#include <string>

#include "command_line_parser.hpp"
#include "output.hpp"

#include "data_structures/circular_buffer.hpp"

namespace utm = utils_tm;
namespace otm = utils_tm::out_tm;

class move_checker
{
  public:
    size_t nmbr;

    move_checker(size_t i = 0) : nmbr(i) {}
    move_checker(const move_checker&)            = delete;
    move_checker& operator=(const move_checker&) = delete;
    move_checker(move_checker&&)                 = default;
    move_checker& operator=(move_checker&&)      = default;

    bool operator==(const move_checker& other) { return other.nmbr == nmbr; }
    bool operator!=(const move_checker& other) { return !operator==(other); }
};

void generate_random(size_t n, std::vector<size_t>& container)
{
    std::mt19937_64                       re;
    std::uniform_int_distribution<size_t> dis;

    for (size_t i = 0; i < n; ++i) { container.push_back(dis(re)); }
}

template <class T>
void run_test(size_t n, size_t c, size_t w)
{
    std::vector<size_t> input;
    input.reserve(n);
    generate_random(n, input);

    utm::circular_buffer<T> container{c};
    size_t                  trailing_index = 0;
    bool                    noerror        = true;

    for (size_t i = 0; i < w; i++) { container.emplace_back(input[i]); }

    for (size_t i = w; i < n; i++)
    {
        container.emplace_back(input[i]);
        auto popped = container.pop_front();
        if (!popped)
        {
            noerror = false;
            otm::out() << otm::color::red +
                              "in move right: unsuccessful pop at pos "
                       << i << std::endl;
        }
        else if (popped.value() != T(input[trailing_index++]))
        {
            noerror = false;
            otm::out() << otm::color::red +
                              "in move right: popped the wrong nmbr at pos "
                       << i << std::endl;
        }
    }
    trailing_index = n - 1;
    for (size_t i = n - w - 1; i > 0; i--)
    {
        container.emplace_front(input[i]);
        auto popped = container.pop_back();
        if (!popped)
        {
            noerror = false;
            otm::out() << otm::color::red +
                              "in move left: unsuccessful pop at pos "
                       << i << std::endl;
        }
        else if (popped.value() != T(input[trailing_index--]))
        {
            noerror = false;
            otm::out() << otm::color::red +
                              "in move left: popped the wrong nmbr at pos "
                       << i << std::endl;
        }
    }

    otm::out() << "size after test:     " << container.size() << std::endl;
    otm::out() << "capacity after test: " << container.capacity() << std::endl;

    if (noerror)
        otm::out() << otm::color::green + "test fully successful!" << std::endl;
}


int main(int argn, char** argc)
{
    utm::command_line_parser cline{argn, argc};
    size_t                   n = cline.int_arg("-n", 10000);
    size_t                   c = cline.int_arg("-c", 100);
    size_t                   w = cline.int_arg("-w", 1000);

    if (!cline.report()) return 1;

    otm::out() << otm::color::byellow + "START CORRECTNESS TEST" << std::endl;
    otm::out() << "testing: circular_buffer" << std::endl;


    otm::out() << "Elements are pushed and popped from the buffer." << std::endl
               << "First we test size_t elements then std::string elements:"
               << std::endl
               << otm::color::bblue //
               << "  1. randomly generate keys" << std::endl
               << "  2. push_front and pop_back" << std::endl
               << "  3. push_back and pop_front" << std::endl
               << otm::color::reset << std::endl;


    otm::out() << otm::color::bgreen + "START TEST with <size_t>" << std::endl;

    run_test<size_t>(n, c, w);

    // otm::out() << otm::color::bgreen << "START TEST with <std::string>"
    //            << otm::color::reset << std::endl;

    // run_test<std::string>(n, c);

    otm::out() << otm::color::bgreen + "START TEST with <move_checker>"
               << std::endl;

    run_test<move_checker>(n, c, w);

    otm::out() << otm::color::bgreen + "END CORRECTNESS TEST" << std::endl;

    return 0;
}
