// This selects between using persistent objects or regular objects
// Persistent objects are implemented as described in:
//   Wei, Ben-David, Blelloch, Fatourou, Rupert and Sun,
//   Constant-Time Snapshots with Applications to Concurrent Data Structures
//   PPoPP 2021
// They support snapshotting via version chains, and without
// indirection, but pointers to objects (ptr_type) must be "recorded
// once" as described in the paper.

// persistent objects, ptr_type includes version chains
#ifdef Persistent
#ifdef Recorded_Once
#include "persistent_recorded_once.h"
#else
#include "persistent.h"
#endif
using ll_head = persistent;

template <typename T>
using ptr_type = persistent_ptr<T>;

template <typename T>
using ptr_type_ = mutable_val<T*>;

template <typename F>
auto with_snap(F f) { return with_snapshot(f);};

// normal non-persistent type
#else
struct ll_head {};

template <typename F>
auto with_snap(F f) { return with_epoch(f);};

#ifdef LongPtr
template <typename T>
using ptr_type = mutable_double<T*>;
template <typename T>
using ptr_type_ = mutable_double<T*>;
#else
template <typename T>
using ptr_type = mutable_val<T*>;
template <typename T>
using ptr_type_ = mutable_val<T*>;
#endif
#endif
