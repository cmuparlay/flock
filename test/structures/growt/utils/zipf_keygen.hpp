#pragma once

/*******************************************************************************
 * zipf_keygen.hpp
 *
 * Random number generator for zipf distributed numbers [1..universe_size]
 *  - universe and exponent are defined at the time of construction
 *  - random numbers can be generated individually, or in larger batches
 *
 * Part of my utils library utils_tm - https://github.com/TooBiased/utils_tm.git
 *
 * Copyright (C) 2019 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <cmath>
#include <memory>
#include <random>

namespace utils_tm
{

class zipf_generator
{
  public:
    zipf_generator(size_t universe = 0, double exp = 0.0001)
    {
        initialize(universe, exp);
    }

    void initialize(size_t universe, double exp)
    {
        _fast_steps = std::min<size_t>(universe, 100);
        _universe   = universe;
        _precomp    = std::make_unique<double[]>(universe + 1);
        _precomp[0] = 0.;
        auto temp   = 0.;

        for (uint i = 1; i <= universe; ++i)
        {
            temp += 1. / std::pow(i, exp);
            _precomp[i] = temp;
        }

        _distribution =
            std::uniform_real_distribution<double>(0., _precomp[universe]);
    }

    template <class RandomEngine> inline size_t generate(RandomEngine& re)
    {
        // CMD = Hks/Hns
        auto t_p = _distribution(re);


        //  SPECIAL TREATMENT FOR FIRST FEW POSSIBLE KEYS
        if (_precomp[_fast_steps] >= t_p)
        {
            for (size_t i = 1; i <= _fast_steps; ++i)
            {
                if (_precomp[i] >= t_p) return i;
            }
        }


        //  BINARY SEARCH METHOD
        size_t l = 0;
        size_t r = _universe;

        while (l < r - 1)
        {
            size_t m = (r + l) / 2; //               |            -------/
            if (_precomp[m] > t_p)  // Hks(m,s)  t_p |======o----/
            {                       //               |   --/|
                r = m;              //               |  /   |
            }                       //               | /    |
            else                    //               |/_____|________________
            {                       //                l    ret   m         r
                l = m;
            }
        }

        return l + 1;
    }

    template <class RandomEngine>
    void generate(RandomEngine& re, size_t* result, size_t length)
    {
        std::unique_ptr<double[]> randoms = std::make_unique<double[]>(length);

        for (size_t i = 0; i < length; ++i)
        {
            // CMD = Hks/Hns
            randoms[i] = _distribution(re);
        }

        for (size_t i = 0; i < length; ++i)
        {
            if (_precomp[_fast_steps] >= randoms[i])
            {
                for (size_t j = 1; j <= _fast_steps; ++j)
                {
                    if (_precomp[j] >= randoms[i])
                    {
                        result[i] = j;
                        break;
                    }
                }
                continue;
            }

            //  BINARY SEARCH METHOD
            size_t l = 0;
            size_t r = _universe;

            while (l < r - 1)
            {
                size_t m = (r + l) / 2; //               |            -------/
                if (_precomp[m] > randoms[i]) // Hks(m,s)  t_p |======o----/
                {                             //               |   --/|
                    r = m;                    //               |  /   |
                }                             //               | /    |
                else //               |/_____|________________
                {    //                l    ret   m         r
                    l = m;
                }
            }

            result[i] = l + 1;
        }
    }

  private:
    size_t                                 _universe; // normalization factor
    size_t                                 _fast_steps;
    std::uniform_real_distribution<double> _distribution;
    std::unique_ptr<double[]>              _precomp;
};

} // namespace utils_tm
