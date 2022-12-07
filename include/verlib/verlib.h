// This selects between using versioned objects or regular objects
// Versioned objects are implemented as described in:
//   Wei, Ben-David, Blelloch, Fatourou, Rupert and Sun,
//   Constant-Time Snapshots with Applications to Concurrent Data Structures
//   PPoPP 2021
// They support snapshotting via version chains, and without
// indirection, but pointers to objects (ptr_type) must be "recorded
// once" as described in the paper.
#pragma once

#include "flock/flock.h"

#ifdef Versioned

// versioned objects, ptr_type includes version chains
#ifdef Recorded_Once
#include "versioned_recorded_once.h"
#elif Simple_Recorded_Once_
#include "versioned_recorded_once_simple.h"
#elif FullyIndirect
#include "versioned_indirect.h"
#elif Simple
#include "versioned_simple.h"
#else
#include "versioned_opt.h"
#endif

# else // Not Versioned

namespace vl {

  struct versioned {};

  template <typename T>
  using versioned_ptr = flck::atomic<T*>;

  template <typename F>
  auto with_snapshot(F f) {
    return flck::with_epoch([&] { return f();});
  }
}

#endif

namespace vl {
  using flck::with_epoch;
  using flck::memory_pool;
}


