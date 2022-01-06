#include "harris_ll_adapter.h"
#include "test_sets.h"

using K = long;
using V = long;
  
int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-l] [-i] [-s] [-n <size>] [-r <rounds>]");
  ordered_set<K,V> list;
  size_t default_size = 1000;
  test_sets(list, default_size, P);
}
