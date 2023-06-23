#include <atomic>
#include <vector>
#include <limits>
#include <parlay/alloc.h>
#include <parlay/primitives.h>

#pragma once

#ifndef NDEBUG
// checks for corruption of bytes before and after the structure, as well as double frees
// requires some extra memory to pad front and back
#define EpochMemCheck 1
#endif

// If defined allows with_epoch to be nested (only outermost will set the epoch).
// Incurs slight overhead due to extra test, but allows wrapping a with_epoch
// around multiple operations which each do a with_epoch.
// This saves time since setting epoch number only needs to be fenced once on the outside.
//#define NestedEpochs 1

// Supports before_epoch_hooks and after_epoch_hooks, which are thunks
// that get run just before incrementing the epoch number and just after.
// The user set them with:
//    flck::internal::epoch.before_epoch_hooks.push_back(<mythunk>);
//    flck::internal::epoch.after_epoch_hooks.push_back(<myotherthunk>);

// ***************************
// epoch structure
// ***************************

namespace flck {

  thread_local int my_id = -1;
  
struct alignas(64) epoch_s {
	
  // functions to run when epoch is incremented
  std::vector<std::function<void()>> before_epoch_hooks;
  std::vector<std::function<void()>> after_epoch_hooks;
  
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
  }

  long get_current() {
    return current_epoch.load();
  }

  int get_my_id() {
    if (my_id == -1) my_id = parlay::worker_id();
    return my_id;
    //return parlay::worker_id();
  }
  
  long get_my_epoch() {
    size_t id = parlay::worker_id();
    return announcements[id].last;
  }

  void set_my_epoch(long e) {
    size_t id = parlay::worker_id();
    announcements[id].last = e;
  }

  int announce() {
    size_t id = get_my_id();
    while (true) {
      long current_e = get_current();
      long tmp = current_e;
      // apparently an exchange is faster than a store (write and fence)
      announcements[id].last.exchange(tmp, std::memory_order_seq_cst);
      if (get_current() == current_e) return id;
    }
  }

  void unannounce(size_t id) {
    announcements[id].last.store(-1l, std::memory_order_release);
  }

  void update_epoch() {
    size_t id = parlay::worker_id();
    int workers = parlay::num_workers();
    long current_e = get_current();
    bool all_there = true;
    // check if everyone is done with earlier epochs
    for (int i=0; i < workers; i++)
      if ((announcements[i].last != -1l) && announcements[i].last < current_e) {
	all_there = false;
	break;
      }
    // if so then increment current epoch
    if (all_there) {
      for (auto h : before_epoch_hooks) h();
      if (current_epoch.compare_exchange_strong(current_e, current_e+1)) {
	for (auto h : after_epoch_hooks) h();
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
  bool skip;
  void* value;
};

  // x should point to the skip field of a link
  void undo_retire(bool* x) { *x = true;}
  void undo_allocate(bool* x) { *x = false;}
  
using list_allocator = parlay::type_allocator<Link>;

  using namespace std::chrono;

template <typename xT>
struct alignas(64) memory_pool {
private:

  static constexpr double milliseconds_between_epoch_updates = 20.0;
  long update_threshold;
  using sys_time = time_point<std::chrono::system_clock>;

  // each thread keeps one of these
  struct alignas(256) old_current {
    Link* old;  // linked list of retired items from previous epoch
    Link* current; // linked list of retired items from current epoch
    long epoch; // epoch on last retire, updated on a retire
    long count; // number of retires so far, reset on updating the epoch
    sys_time time; // time of last epoch update
    old_current() : old(nullptr), current(nullptr), epoch(0) {}
  };

  // only used for debugging (i.e. EpochMemCheck=1).
  struct paddedT {
    long pad;
    std::atomic<long> head;
    xT value;
    std::atomic<long> tail;
  };

  std::vector<old_current> pools;
  int workers;

  bool* add_to_current_list(void* p) {
    auto i = parlay::worker_id();
    auto &pid = pools[i];
    advance_epoch(i, pid);
    Link* lnk = list_allocator::alloc();
    lnk->next = pid.current;
    lnk->value = p;
    lnk->skip = false;
    pid.current = lnk;
    return &(lnk->skip);
  }

  // destructs and frees a linked list of objects 
  void clear_list(Link* ptr) {
    while (ptr != nullptr) {
      Link* tmp = ptr;
      ptr = ptr->next;
      if (!tmp->skip) {
#ifdef EpochMemCheck
	paddedT* x = pad_from_T((T*) tmp->value);
	if (x->head != 10 || x->tail != 10) {
	  if (x->head == 55) std::cout << "double free" << std::endl;
	  else std::cout << "corrupted head" << std::endl;
	  if (x->tail != 10) std::cout << "corrupted tail" << std::endl;
	  assert(false);
	}
#endif
	destruct((T*) tmp->value);
      }
      list_allocator::free(tmp);
    }
  }

  void advance_epoch(int i, old_current& pid) {
    if (pid.epoch + 1 < epoch.get_current()) {
      clear_list(pid.old);
      pid.old = pid.current;
      pid.current = nullptr;
      pid.epoch = epoch.get_current();
    }
    // a heuristic
    auto now = system_clock::now();
    if (++pid.count == update_threshold  ||
	duration_cast<milliseconds>(now - pid.time).count() >
	milliseconds_between_epoch_updates * (1 + ((float) i)/workers)) {
      pid.count = 0;
      pid.time = now;
      epoch.update_epoch();
    }
  }


public:
  using T = xT;
#ifdef  EpochMemCheck
  using Allocator = parlay::type_allocator<paddedT>;
#else
  using Allocator = parlay::type_allocator<T>;
#endif
  
  memory_pool() {
    workers = parlay::num_workers();
    update_threshold = 10 * workers;
    pools = std::vector<old_current>(workers);
    for (int i = 0; i < workers; i++) {
      pools[i].count = parlay::hash64(i) % update_threshold;
      pools[i].time = system_clock::now();
    }
  }

  memory_pool(const memory_pool&) = delete;
  ~memory_pool() {} // clear(); }

  // noop since epoch announce is used for the whole operation
  void acquire(T* p) { }
  
  void reserve(size_t n) { Allocator::reserve(n);}
  void stats() { Allocator::print_stats();}

  paddedT* pad_from_T(T* p) {
     size_t offset = ((char*) &((paddedT*) p)->value) - ((char*) p);
     return (paddedT*) (((char*) p) - offset);
  }
  
  // destructs and frees the object immediately
  void destruct(T* p) {
     p->~T();
#ifdef EpochMemCheck
     paddedT* x = pad_from_T(p);
     x->head = 55;
     Allocator::free(x);
#else
     Allocator::free(p);
#endif
  }

  template <typename ... Args>
  T* new_obj(Args... args) {
#ifdef EpochMemCheck
    paddedT* x = Allocator::alloc();
    x->pad = x->head = x->tail = 10;
    T* newv = &x->value;
    new (newv) T(args...);
    assert(check_not_corrupted(newv));
#else
    T* newv = Allocator::alloc();
    new (newv) T(args...);
#endif
    return newv;
  }

  bool check_not_corrupted(T* ptr) {
#ifdef EpochMemCheck
    paddedT* x = pad_from_T(ptr);
    if (x->pad != 10) std::cout << "memory_pool: pad word corrupted" << std::endl;
    if (x->head != 10) std::cout << "memory_pool: head word corrupted" << std::endl;
     if (x->tail != 10) std::cout << "memory_pool: tail word corrupted" << std::endl;
    return (x->pad == 10 && x->head == 10 && x->tail == 10);
#endif
    return true;
  }
			   
  template <typename F, typename ... Args>
  // f is a function that initializes a new object before it is shared
  T* new_init(F f, Args... args) {
    T* x = new_obj(args...);
    f(x);
    return x;
  }

  // retire and return a pointer if want to undo the retire
  bool* retire(T* p) {
    return add_to_current_list((void*) p);}
  
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

  thread_local bool in_epoch = false;

  template <typename Thunk>
  auto with_epoch(Thunk f) {
    int id = epoch.announce();
    if constexpr (std::is_void_v<std::invoke_result_t<Thunk>>) {
      f();
      epoch.unannounce(id);
    } else {
      auto v = f();
      epoch.unannounce(id);
      return v;
    }
  }

  template <typename F>
  auto try_loop(const F& f, int delay = 200, const int max_multiplier = 10) {
    int multiplier = 1;
    int cnt = 0;
    while (true)  {
      if (cnt++ == 10000000000ul/(delay*max_multiplier)) {
	std::cout << "problably in an infinite retry loop" << std::endl;
	abort(); 
      }
      auto r = f();
      if (r.has_value()) return *r;
      multiplier = std::min(2*multiplier, max_multiplier);
      for (volatile int i=0; i < delay * multiplier; i++);
    }
  }

} // end namespace flck
