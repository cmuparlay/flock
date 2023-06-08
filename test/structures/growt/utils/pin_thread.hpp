#pragma once

/*******************************************************************************
 * pin_thread.hpp
 *
 * Simple function, to bind a process to one core
 * (uses pthread, and sets the affinity bitmask).
 *
 * ATTENTION: we should implement a OS independant way to handle this!
 * But currently, this is enough.
 *
 * ATTENTION: we have made the experience, that this is necessary on server
 * architectures even in sequential applications (due to effects of NUMA)
 *
 * Part of my utils library utils_tm - https://github.com/TooBiased/utils_tm.git
 *
 * Copyright (C) 2019 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <pthread.h>

namespace utils_tm
{

void pin_to_core(size_t core)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

} // namespace utils_tm
