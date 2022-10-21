#include <atomic>
#include <vector>
#include <limits>
#include <parlay/alloc.h>
#include <parlay/random.h>
#include <parlay/primitives.h>
#include <unordered_set>
#include "timestamps.h"

#pragma once

// ***************************
// epoch structure
// ***************************

// timestamps are distinct from epochs and are for multiversioning (snapshots)
// any time stamps less or equal to done_stamp indicate it is safe to
// collect them (noone will travel through them).
  
struct epoch_s {
  TS prev_stamp;
  
  struct alignas(64) announce_slot {
    std::atomic<long> last;
    announce_slot() : last(-1l) {}
  };

  std::vector<announce_slot> announcements;
  std::atomic<long> current_epoch;
  epoch_s() {
    int workers = parlay::num_workers();
    announcements = std::vector<announce_slot>(workers);
    current_epoch = 0;
    prev_stamp = global_stamp.get_stamp();
  }

  long get_current() {
    return current_epoch.load();
  }

  long get_my_epoch() {
    size_t id = parlay::worker_id();
    return announcements[id].last;
  }

  void set_my_epoch(long e) {
    size_t id = parlay::worker_id();
    announcements[id].last = e;
  }

  void announce() {
    size_t id = parlay::worker_id();
    long current_e = get_current();
    // apparently an exchange is faster than a store (write and fence)
    announcements[id].last.exchange(current_e, std::memory_order_acquire);
  }

  void unannounce() {
    size_t id = parlay::worker_id();
    announcements[id].last.store(-1l, std::memory_order_release);
  }

  void update_epoch() {
    size_t id = parlay::worker_id();
    int workers = parlay::num_workers();
    long current_e = get_current();
    bool all_there = true;
    // check if everyone is done with earlier epochs
    for (int j=0; j<2; j++) //do twice
      for (int i=0; i < workers; i++)
	if ((announcements[i].last != -1l) &&
	    announcements[i].last < current_e) 
	  all_there = false;
    // if so then increment current epoch
    if (all_there) {
      // timestamps are for multiversioning (snapshots)
      // we set done_stamp to the stamp from the previous epoch update
      TS current_stamp = global_stamp.get_stamp();
      if (current_epoch.compare_exchange_strong(current_e, current_e+1)) {
	done_stamp = prev_stamp;
	prev_stamp = current_stamp;
      }
    }
  }
};

epoch_s epoch;

// ***************************
// epoch pools
// ***************************

struct Link {
  Link* next;
  void* value;
};

using list_allocator = parlay::type_allocator<Link>;

template <typename xT>
struct mem_pool {
private:

  // each thread keeps one of these
  struct alignas(256) old_current {
    Link* old;  // linked list of retired items from previous epoch
    Link* current; // linked list of retired items from current epoch
    long epoch; // epoch on last retire, updated on a retire
    long count; // number of retires so far, reset on updating the epoch
    old_current() : old(nullptr), current(nullptr), epoch(0), count(0) {}
  };

  std::vector<old_current> pools;
  int workers;

  // destructs and frees a linked list of objects 
  void clear_list(Link* ptr) {
    while (ptr != nullptr) {
      Link* tmp = ptr;
      ptr = ptr->next;
      destruct((T*) tmp->value);
      list_allocator::free(tmp);
    }
  }

public:
  using T = xT;
  using Allocator = parlay::type_allocator<T>;

  mem_pool() {
    workers = parlay::num_workers();
    pools = std::vector<old_current>(workers);
  }

  mem_pool(const mem_pool&) = delete;

  // noop since epoch announce is used for the whole operation
  void acquire(T* p) { }
  
  void reserve(size_t n) { Allocator::reserve(n);}
  void stats() { Allocator::print_stats();}

  void shuffle(size_t n) {
    n = std::max(n, 1000000ul);
    auto ptrs = parlay::tabulate(n, [&] (size_t i) {return Allocator::alloc();});
    ptrs = parlay::random_shuffle(ptrs);
    parlay::parallel_for(0, n, [&] (size_t i) {Allocator::free(ptrs[i]);});
  }

  // destructs and frees the object immediately
  void destruct(T* p) {
     p->~T();
     Allocator::free(p);
  }

  // destructs and frees the object immediately
  void destruct_no_log(T* p) {
     p->~T();
     Allocator::free(p);
  }

  template <typename ... Args>
  T* new_obj(Args... args) {
    T* newv = Allocator::alloc();
    new (newv) T(args...);
    return newv;
  }

  template <typename F, typename ... Args>
  // f is a function that initializes a new object before it is shared
  T* new_init(F f, Args... args) {
    T* x = new_obj(args...);
    f(x);
    return x;
  }

  // destruction and freeing is delayed until a future epoch
  void retire(T* p) {
    auto &pid = pools[parlay::worker_id()];
    if (pid.epoch < epoch.get_current()) {
      clear_list(pid.old);
      pid.old = pid.current;
      pid.current = nullptr;
      pid.epoch = epoch.get_current();
    }
    // a heuristic
    if (++pid.count == 10 * workers) {
      pid.count = 0;
      epoch.update_epoch();
    }
    Link* lnk = list_allocator::alloc();
    lnk->next = pid.current;
    lnk->value = (void*) p;
    pid.current = lnk;
  }

  // clears all the lists and terminates the underlying allocator
  // to be used on termination
  void clear() {
    epoch.update_epoch();
    for (int i=0; i < pools.size(); i++) {
      clear_list(pools[i].old);
      clear_list(pools[i].current);
      pools[i].old = pools[i].current = nullptr;
    }
    Allocator::finish();
  }
};

template <typename Thunk>
typename std::result_of<Thunk()>::type with_epoch(Thunk f) {
  epoch.announce();
  typename std::result_of<Thunk()>::type v = f();
  epoch.unannounce();
  return v;
}
