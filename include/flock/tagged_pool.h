#include <atomic>
#include <vector>
#include <parlay/alloc.h>
#include "epoch.h"

template <typename xT>
struct tagged_pool {
  using T = xT;
  mem_pool<T> pool;
  void reserve(size_t n) { pool.reserve(n);}
  void shuffle(size_t n) { pool.shuffle(n);}
  void stats() { pool.stats();}
  void clear() { pool.clear();}
  void destruct(T* ptr) { pool.destruct(ptr); }
  //void acquire(T* ptr) {if (!ptr->acquired) ptr->acquired = true; }
void acquire(T* ptr) {ptr->acquired = true; }
  
  template <typename ... Args>
  T* new_obj(Args... args) {
    T* ptr = pool.new_obj(args...);
    // ptr->acquired = false;
    // for (int i=0; i < 0; i++)
    //   if ((ptr->acquired).load()) {
    // 	std::cout << i << std::endl;
    // 	break;
    //   }
    return ptr;
  } 

  void retire(T* ptr) {
    bool x = (ptr->acquired).load();
    if (!x) destruct(ptr);
    else {
      ptr->acquired = false;
      pool.retire(ptr);
    }
  }
};
