// Locks acquired as:
//   with_lock(lock, [=] () { ...code... returning a boolean })
// Shared data accessed inside a lock should be wrapped in one of
//    mutable_val<T>
//    write_once<T>
// They have an interface similar to atomic<T> but ensures idempotence in a lock
// In particular to read use o.load()
// and to wire use o = x or o.store(x)
// Also can use
//    try_lock(lock, [=] () { ...code... returning a boolean })
// which can fail, returning false, if lock is already taken.  Otherwise it
// returns the return value.

#pragma once

#include "defs.h"
#include <atomic>
#include <functional>
#include "log.h"
#include<chrono>
#include<thread>

// used for reentrant locks
static thread_local size_t current_id = parlay::worker_id();

// user facing lock
struct descriptor; // for mutual recursive definition
using lock_entry = size_t;
using Tag = tagged<descriptor*>;

// each lock entry will be a pointer to a descriptor tagged with a counter
// to avoid ABA issues
struct lock {
  std::atomic<lock_entry> lck;
  lock() : lck(Tag::init(nullptr)) {}
  lock_entry load() {return lg.commit_value(lck.load()).first;}
  lock_entry read() {return lck.load();}

  // used to take lock for version with no helping
  bool simple_cas(lock_entry oldl, size_t v) {
    return lck.compare_exchange_strong(oldl, (lock_entry) v);
  }

  // used to take lock for version with helping
  bool cas(lock_entry oldl, descriptor* d) {
    lock_entry current = read();
    if (current != oldl) return false;
    return Tag::cas(lck, oldl, d);
  }
  
  void clear(descriptor* d) {
    lock_entry current = lck.load();
    if (Tag::value(current) == d)
      // true indicates this is ABA free since current cannot
      // be reused until all helpers are done with it.
      Tag::cas(lck, current, nullptr, true);
  }
};


// stores the thunk along with the log
struct descriptor {
  std::function<void()> f;
  bool done;
  bool freed; // just for debugging
  std::atomic<bool> acquired;  // indicates thunk is being helped, lives beyond the 'lifetime' of the descriptor
  int thread_id; // used to detect reentrant locks
  //lock* l; // not currently used
  lock_entry current;
  long epoch_num;
  log_array lg_array;
  
  template <typename Thunk>
  descriptor(lock* l, Thunk& g, lock_entry current)
    : //l(l),
    done(false), freed(false), current(current), f([=] {g();}) {
    lg_array.init();
    epoch_num = epoch.get_my_epoch();
    thread_id = current_id;
  }

  ~descriptor() { // just for debugging
    int i = 0;
    //while (i < Log_Len && lg_array[i] != nullptr) lg_array[i++] = nullptr;
    if (debug) {
      if (freed) {
	std::cout << "yikes: double freeing" << std::endl;
	abort();
      }
      freed = true;
    }
  }

  void operator () () {
    if (debug && freed) {
      std::cout << "yikes: calling freed function" << std::endl; 
      abort();
    }
    // run f using log based on lg_array
    with_log_(Log(&lg_array,0), f);
    done = true;
    //std::atomic_thread_fence(std::memory_order_seq_cst);
  }
};

#ifdef EpochDescriptor
// use epoch based collector to reclaim descriptors
memory_pool<descriptor> descriptor_pool;

#else
// Use optimized collector to reclaim descriptors.
// If noone is helping (acquire flag is false), then reclaim
// immediately, otherwise send to epoch-based collector.
// When helping the acquired flag needs to be set.
memory_pool<descriptor,tagged_pool<descriptor>> descriptor_pool;
#endif

// The lock_entry for help is organized as
//  The lowest 48 bits are a pointer to a descriptor if locked, or null if not
//  Highest 16 bits is a tag to avoid ABA problem
namespace Help {
  bool is_locked(lock_entry le) { return Tag::value(le) != nullptr;}
  descriptor* remove_tag(lock_entry le) { return Tag::value(le);}
  bool lock_is_self(lock_entry le) {
    return current_id == remove_tag(le)->thread_id;
  }
}

// The lock_entry for no_help is organized as
//  Lowest 32 bits are a count.  If odd then locked, if even then unlocked.
//  Next 16 bits are the processor id who has the lock, for checking for reentry
namespace NoHelp {
  bool is_locked(lock_entry le) { return (le % 2 == 1);}
  size_t mask_cnt(lock_entry lck) {return lck & ((1ul << 32) - 1);};
  size_t take_lock(lock_entry le) {
    return ((current_id + 1ul) << 32) | mask_cnt(le+1);
  }
  size_t release_lock(lock_entry le) {
    return mask_cnt(le+1);
  }
  size_t get_procid(lock_entry lck) {return (lck >> 32) & ((1ul << 16) - 1);};
  bool lock_is_self(lock_entry le) {
    return current_id + 1 == get_procid(le);
  }
}

// generic versions for either helping or not
bool is_locked(lock_entry le) {
  if (use_help) return Help::is_locked(le);
  else return NoHelp::is_locked(le);
}

// generic versions for either helping or not
bool is_self_locked(lock_entry le) {
  if (use_help) return (Help::is_locked(le) && !Help::lock_is_self(le));
  else return (NoHelp::is_locked(le) && NoHelp::lock_is_self(le));
}

// runs thunk in appropriate epoch and after it is acquired
bool help_descriptor(lock &l, lock_entry le, bool recursive_help=false) {
  //if (delay)
  // std::this_thread::sleep_for(std::chrono::nanoseconds(100));
  if (!recursive_help && helping) return false;
  descriptor* desc = Help::remove_tag(le);
  bool still_locked = (l.read() == le);
  if (!still_locked) return false;
  long my_epoch = epoch.get_my_epoch();
  long other_epoch = desc->epoch_num;
  if (other_epoch < my_epoch)
    epoch.set_my_epoch(other_epoch); // inherit epoch of helpee
  int my_id = current_id; 
  current_id = desc->thread_id; // inherit thread id of helpee
  descriptor_pool.acquire(desc);
  still_locked = (l.read() == le);
  if (still_locked) {
    bool hold_h = helping;
    helping = true;
    (*desc)();
    l.clear(desc);
    helping = hold_h;
  }
  current_id = my_id; // reset thread id
  epoch.set_my_epoch(my_epoch); // reset to my epoch
  return still_locked;
}

void clear_lock(lock &l) {
  if (use_help) {
    lock_entry current = l.load();
    if (!Help::is_locked(current)) return;
    // check if locked by self
    if (Help::lock_is_self(current)) return;
    // last arg needs to be true, otherwise might not help and clear
    help_descriptor(l, current, true);
  } else {
    lock_entry current = l.read();
    while (NoHelp::is_locked(current) &&
	   !NoHelp::lock_is_self(current))
      current = l.read();
  }
}

bool is_locked_(lock &l) { return is_locked(l.load());}

// this is safe to be used inside of another lock (i.e. it is idempotent, kind of)
// The key components to making it effectively idempotent is using an idempotent new_obj,
// and checking if it is done (my_thunk->done)
// it is lock free if no cycles in lock ordering, and otherwise can deadlock
template <typename Thunk>
auto with_lock_help(lock &l, Thunk f) {
  using RT = decltype(f());
  static_assert(sizeof(RT) <= 4 || std::is_pointer<RT>::value,
 		"Result of try_lock_result must be a pointer or at most 4 bytes");
  lock_entry current = l.read();

  auto [my_descriptor, i_own] = descriptor_pool.new_obj_acquired(&l, f, current);
  if (descriptor_pool.is_done(my_descriptor)) // if already retired, then done
  {
    auto ret_val = descriptor_pool.done_val_result<RT>(my_descriptor);
    assert(ret_val.has_value()); // with_lock is guaranteed to succeed
    return ret_val.value(); 
  }
  bool locked = Help::is_locked(current);
  while (true) {
    if (my_descriptor->done // already done
        || Help::remove_tag(current) == my_descriptor // already acquired
	|| (!locked && l.cas(current, my_descriptor))) { // successfully acquired

      // run the body f with the log from my_descriptor
      RT result = with_log(Log(&my_descriptor->lg_array,0), [&] {return f();});

      // mark as done and clear the lock
      my_descriptor->done = true;
      l.clear(my_descriptor);

      // retire the descriptor saving the result in the enclosing descriptor, if any
      descriptor_pool.retire_acquired_result(my_descriptor, i_own, std::optional<RT>(result));
      return result;
    } else if (locked) {
      help_descriptor(l, current);
    }
    current = l.read();
    locked = Help::is_locked(current);
  }
}

template <typename Thunk>
auto try_lock_help(lock &l, Thunk f) {
  using RT = decltype(f());
  std::optional<RT> result = {};
  lock_entry current = l.load();

  // check if reentrant lock (already locked by self)
  // bool reentry = read_only<bool>([&] {
  //     return (is_locked_help(current) &&
  // 	      current_id == remove_tag(current)->thread_id &&
  // 	      l.load() == current);});
  // if (reentry) {
  if (Help::is_locked(current) && Help::lock_is_self(current)) { // && l.load() == current) {
    return std::optional(f()); // if so, run without acquiring
  }

  // idempotent allocation of descriptor
  // storing current into descriptor saves one logging event since they are committed together
  auto [my_descriptor, i_own] = descriptor_pool.new_obj_acquired(&l, f, current);

  // if descriptor is already retired, then done and return value
  if (descriptor_pool.is_done(my_descriptor)) 
    return descriptor_pool.done_val_result<RT>(my_descriptor);

  // retrieve agreed upon current for idempotence
  //current = my_descriptor->current;
  if (!Help::is_locked(current)) {
    // use a CAS to try to acquire the lock
    l.cas(current, my_descriptor);

    // could be a l.load() without the my_descriptor->done test
    // using l.read() is an optimization to avoid a logging event
    current = l.read();
    if (my_descriptor->done || Help::remove_tag(current) == my_descriptor) {

      // run f with log from my_descriptor
      result = with_log(Log(&my_descriptor->lg_array,0), [&] {return f();});

      // mark as done and clear the lock
      my_descriptor->done = true;
      l.clear(my_descriptor);
    }
  } else help_descriptor(l, current);

  // retire the thunk
  descriptor_pool.retire_acquired_result(my_descriptor, i_own, result);
  return result;
}

template <typename Thunk>
auto try_lock_nohelp(lock &l, Thunk f) {
  using RT = decltype(f());
  size_t current = l.read();
  if (!NoHelp::is_locked(current)) { // unlocked
    lock_entry newl = NoHelp::take_lock(current);
    if (l.simple_cas(current, newl)) {  // try lock
      RT result = f();
      l.lck = NoHelp::release_lock(newl);  // release lock
      return std::optional<RT>(result); 
    } else return std::optional<RT>(); // fail
  } else if (NoHelp::lock_is_self(current)) {// reentry
    return std::optional<RT>(f());
  } else {
    return std::optional<RT>(); // fail
  }
}

template <typename Thunk>
auto with_lock_nohelp(lock &l, Thunk f) {
  using RT = decltype(f());
  int locked = 1 + parlay::worker_id();
  while (true) {
    auto current = l.read();
    if (Help::remove_tag(current) == 0 && l.simple_cas(current, locked)) {
      RT result = f();
      l.lck = 0;
      return (RT) result;
    } 
    if (wait_before_retrying_lock)
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
  }
}

template <typename Thunk>
auto with_lock(lock &l, Thunk f) {
  using RT = decltype(f());
  static_assert(sizeof(RT) <= 4 || std::is_pointer<RT>::value,
 		"Result of with_lock must be a pointer or at most 4 bytes");
  return (use_help ? with_lock_help(l, f) : with_lock_nohelp(l, f));
}

template <typename Thunk>
bool try_lock(lock &l, Thunk f, bool try_only=try_only) {
  if (try_only) {
    std::optional<bool> r = use_help ? try_lock_help(l, f) : try_lock_nohelp(l, f);
    return r.has_value() && r.value();
  } else
    return use_help ? with_lock_help(l, f) : with_lock_nohelp(l, f);
}

template <typename Thunk>
auto try_lock_result(lock &l, Thunk f, bool try_only=try_only) {
  using RT = decltype(f());
  if (try_only) 
    return use_help ? try_lock_help(l, f) : try_lock_nohelp(l, f);
  else
    return std::optional<RT>(use_help ? with_lock_help(l, f) : with_lock_nohelp(l, f));
}

// acquire two locks
template <typename Thunk>
bool with_lock(lock& l1, lock& l2, Thunk f) {
  return with_lock(l1, [=, &l2] () {
    return with_lock(l2, [=] () {return f();});});
}

// acquire three locks
template <typename Thunk>
bool with_lock(lock& l1, lock& l2, lock& l3, Thunk f) {
  return with_lock(l1, [=, &l2, &l3] () {
    return with_lock(l2, [=, &l3] () {
      return with_lock(l3, [=] () {return f();});});});
}

int bucket_bits = 16;
size_t mask = ((1ul) << bucket_bits) - 1;
std::vector<lock> locks{1ul << bucket_bits};

// currently does not work with nested locks and reentry...not sure why
template <typename T, typename Thunk>
bool try_lock_loc(T* ptr, Thunk f) {
  size_t lid = parlay::hash64_2((size_t) ptr) & mask;
  return try_lock(locks[lid], f);
}

template <typename T>
void clear_lock_loc(T* ptr) {
  size_t lid = parlay::hash64_2((size_t) ptr) & mask;
  clear_lock(locks[lid]);
}

template <typename T>
bool is_locked_loc(T* ptr) {
  size_t lid = parlay::hash64_2((size_t) ptr) & mask;
  return is_locked(locks[lid].load());
}


