using K = unsigned long;
using V = unsigned long;

#include <flock/verlib.h>
#include "set.h"
#include "test_sets.h"


int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"[-n <size>] [-r <rounds>] [-p <procs>] [-z <zipfian_param>] [-u <update percent>] [-insert_find_delete] [-no_help] [-strict_lock]");
  Set<K,V> lst;
  size_t default_size = 100000;
  test_sets(lst, default_size, P);
#if defined(Persistent) && ! defined(Recorded_Once)
  //  print_counts();
#endif
#if defined(LazyStamp)
  vl::print_retries();
#endif
}
