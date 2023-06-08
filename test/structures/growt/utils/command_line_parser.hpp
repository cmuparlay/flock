#pragma once

/*******************************************************************************
 * commandline.hpp
 *
 * Simple tool to read command line parameters
 *
 * Part of my utils library utils_tm - https://github.com/TooBiased/utils_tm.git
 *
 * Copyright (C) 2019 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <clocale>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace utils_tm
{

class command_line_parser
{
  public:
    command_line_parser(int argn, char** argc)
    {
        std::setlocale(LC_ALL, "en_US.UTF-8");
        for (size_t i = 0; i < size_t(argn); ++i)
        {
            _param_vec.emplace_back(argc[i]);
            _flag_vec.push_back(usage_flags::unused);
        }
    }

    std::string str_arg(const std::string& name, const std::string def = "")
    {
        auto ind = find_name(name);
        if (ind + 1 < _param_vec.size())
        {
            _flag_vec[ind + 1] = usage_flags::used;
            return _param_vec[ind + 1];
        }
        else if (ind < _param_vec.size())
        {
            _flag_vec[ind] = usage_flags::error;
            std::cout << "found argument \"" << name
                      << "\" without following integer!" << std::endl;
        }
        return def;
    }

    int int_arg(const std::string& name, int def = 0)
    {
        auto ind = find_name(name);
        if (ind + 1 < _param_vec.size())
        {
            _flag_vec[ind + 1] = usage_flags::used;
            int r              = 0;
            try
            {
                r = std::stoi(_param_vec[ind + 1]);
            }
            catch (std::invalid_argument& e)
            {
                _flag_vec[ind + 1] = usage_flags::error;
                r                  = def;
                std::cout
                    << "error reading int argument \"" << name
                    << "\" from console, got \"invalid_argument exception\""
                    << std::endl;
            }
            return r;
        }
        else if (ind < _param_vec.size())
        {
            _flag_vec[ind] = usage_flags::error;
            std::cout << "found argument \"" << name
                      << "\" without following integer!" << std::endl;
        }
        return def;
    }

    double double_arg(const std::string& name, double def = 0.)
    {
        std::setlocale(LC_ALL, "en_US.UTF-8");
        auto ind = find_name(name);
        if (ind + 1 < _param_vec.size())
        {
            _flag_vec[ind + 1] = usage_flags::used;
            double r           = 0;
            try
            {
                r = std::stod(_param_vec[ind + 1]);
            }
            catch (std::invalid_argument& e)
            {
                _flag_vec[ind + 1] = usage_flags::error;
                r                  = def;
                std::cout
                    << "error reading double argument \"" << name
                    << "\" from console, got \"invalid-argument exception\"!"
                    << std::endl;
            }
            return r;
        }
        else if (ind < _param_vec.size())
        {
            _flag_vec[ind] = usage_flags::error;
            std::cout << "found argument \"" << name
                      << "\" without following double!" << std::endl;
        }
        return def;
    }

    bool bool_arg(const std::string& name)
    {
        return (find_name(name)) < _param_vec.size();
    }

    bool report()
    {
        bool un = true;
        for (size_t i = 1; i < _param_vec.size(); ++i)
        {
            if (_flag_vec[i] != usage_flags::used)
            {
                if (_flag_vec[i] == usage_flags::unused)
                {
                    std::cout << "parameter " << i << " = \"" << _param_vec[i]
                              << "\" was unused!" << std::endl;
                }
                else if (_flag_vec[i] == usage_flags::error)
                {
                    std::cout << "error reading parameter " << i << " = \""
                              << _param_vec[i] << "\"" << std::endl;
                }
                un = false;
            }
        }
        return un;
    }

  private:
    enum class usage_flags
    {
        unused,
        used,
        error
    };

    std::vector<std::string> _param_vec;
    std::vector<usage_flags> _flag_vec;

    size_t find_name(const std::string& name)
    {
        for (size_t i = 0; i < _param_vec.size(); ++i)
        {
            if (_param_vec[i] == name)
            {
                _flag_vec[i] = usage_flags::used;
                return i;
            }
        }
        return _param_vec.size();
    }
};

} // namespace utils_tm
