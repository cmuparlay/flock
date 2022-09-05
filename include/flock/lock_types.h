#include <atomic>
#include "epoch.h"
// Public interface for supported data types
//  mutable_val<T> :
//    mutable_val(v) : constructor
//    mutable_val() : default constructor
//    load() : return value
//    store(v) : store value
//    cam(old_v, new_v) : cas but does not return value
//    ** the following two are the same as load and store if uses
//       with regular locks
//    read() : return value, but not idempotent
//    init(v) : like store(v) but only used before anyone other thread
//              has a handle to this mutable
//    ** the following only needed for snapshotting and only
//       if trying to optimized code.
//    read_() : read that does not check for valid timestamp
//    validate() : ensures it has a timestamp
//
//  ** The following is if a value is only changed once from its
//     initial value
//  write_once<T> :
//    write_once(v) : constructor
//    write_once() : default constructor
//    load() : return value
//    store(v) : store value
//    init(v) : like store(v) but only used before anyone other thread
//              has a handle to this mutable
//
//  memory_pool<T> :
//    memory_pool() : constructor
//    new_obj(constructor args) -> *T : new object of type T
//    retire(*T) -> void : returns memory to pool
//    ** the following only needed for lock_free locks
//    new_init(f, constructor args) : applies f to constructed object
//    ** Statistics and others (can be noops)
//    stats()
//    shuffle()
//    reserve()
//    clear()

template <typename V>
struct mutable_val {
private:
  std::atomic<V> v;
public:
  static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value,
    "Type for mutable must be a pointer or at most 4 bytes");
  mutable_val(V v) : v(v) {}
  mutable_val() {}
  void init(V vv) {v = vv;}
  V load() {return v.load();}
  V read() {return v.load();}
  void store(V vv) { v = vv;}
  void cam(V oldv, V newv) {v->compare_exchange_strong(oldv,newv);}
  V operator=(V b) {store(b); return b; }

  // compatibility with multiversioning
  V read_() {return read();}
  V read_fix() {return read();}
  void validate() {}
};

template <typename V>
using write_once = mutable_val<V>;

template <typename T>
using memory_pool = mem_pool<T>;
