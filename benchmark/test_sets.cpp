using K = long;
using V = long;

#include "test_sets.h"

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-l] [-n <size>] [-r <rounds>]");
  Set<K,V> lst;
  size_t default_size = 100000;
  test_sets(lst, default_size, P);
}
