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

#include <iostream>
#include <limits>
#include <cassert>


using K = long;
using V = long;

struct pool_t {
public:
  void stats() {}
  void clear() {}
};

pool_t descriptor_pool, log_array_pool;

#include "llcode_adapter.h"
#include "../test_sets.h"
#include "allocator_new.h"


int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-l] [-n <size>] [-r <rounds>]");
  ordered_set<K,V,allocator_new<K>> tree;
  size_t default_size = 100000000;
  test_sets(tree, default_size, P);
}
