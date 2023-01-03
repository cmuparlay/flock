/**
 * Author: Trevor Brown (me [at] tbrown [dot] pro).
 * Copyright 2018.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

using K = unsigned long;
using V = unsigned long;

#include <iostream>
#include <limits>
#include <cassert>

#include "llcode_adapter.h"
#include "../benchmark/test_sets.h"
#include "allocator_parlay.h"


int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-n <size>] [-r <rounds>] [-p <procs>] [-z <zipfian_param>] [-u <update percent>] [-insert_find_delete] [-no_help] [-strict_lock]");
  ordered_set<K,V,allocator_parlay<K>> tree;
  size_t default_size = 100000000;
  test_sets(tree, default_size, P);
}
