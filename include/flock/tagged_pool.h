#pragma once
#include <atomic>
#include <vector>
#include <parlay/alloc.h>
#include "epoch.h"

// A trivial wrapper around a memory pool so that retire will call
// "destruct" instead of "retire" if the object's "acquired" field is
// false.  The object type xT must have an atomic boolean field called
// acquired.  Note that "destruct" frees the memory immediately while
// "retire" puts it aside and does not free until it is safe to do
// so (e.g. two epochs later in an epoch-based pool).
template <typename xT>
struct tagged_pool {
  using T = xT;
  mem_pool<T> pool;
  void reserve(size_t n) { pool.reserve(n);}
  void shuffle(size_t n) { pool.shuffle(n);}
  void stats() { pool.stats();}
  void clear() { pool.clear();}
  void destruct(T* ptr) { pool.destruct(ptr); }
  void acquire(T* ptr) { ptr->acquired = true; }
  template <typename ... Args>
  T* new_obj(Args... args) { return pool.new_obj(args...);} 

  void retire(T* ptr) {
    bool x = (ptr->acquired).load();
    if (!x) destruct(ptr);
    else {
      ptr->acquired = false;
      pool.retire(ptr);
    }
  }
};
