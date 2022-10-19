#include <atomic>
#include "tagged_pool.h"
#include "tagged.h"
#include "lf_log.h"

template <typename V>
struct mutable_val {
private:
  using IT = size_t;
  using TV = tagged<V>;

  IT get_val(Log &p) {
    return p.commit_value(v.load()).first; }

public:
  std::atomic<IT> v;
  static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value,
    "Type for mutable must be a pointer or at most 4 bytes");

  // not much to it.  heavy lifting done in TV
  mutable_val(V vv) : v(TV::init(vv)) {}
  mutable_val() : v(TV::init(0)) {}
  void init(V vv) {v = TV::init(vv);}
  V load() {return TV::value(get_val(lg));}
  V read() {return TV::value(v.load());}
  V read_snapshot() {return TV::value(v.load());}
  void store(V vv) {TV::cas(v, get_val(lg), vv);}
  bool single_cas(V old_v, V new_v) {
    IT old_t = v.load();
    return (TV::value(old_t) == old_v &&
	    TV::cas(v, old_t, new_v, true));}
  void cam(V oldv, V newv) {
    IT old_t = get_val(lg);
    if (TV::value(old_t) == oldv)
      TV::cas(v, old_t, newv);}
  V operator=(V b) {store(b); return b; }

  // compatibility with multiversioning
  void validate() {}
  
  // operator V() { return load(); } // implicit conversion
};

template <typename V>
struct mutable_double {
private:
  struct alignas(16) TV {size_t count; V val; };
  TV v;
  void cam(TV oldv, TV newv) {
    __int128 oldvi = *((__int128*) &oldv);
    __int128 newvi = *((__int128*) &newv);
    __sync_bool_compare_and_swap((__int128*) this, oldvi, newvi);
  }
      
public:
  static_assert(sizeof(V) <= 8,
    "Type for mutable_double must be at most 8 bytes");

  mutable_double(V vv) : v({1, vv}) {}
  mutable_double() : v({1, 0}) {}
  V load() {return lg.commit_value_safe(v.val).first;}
  V read() {return v.val;}
  void init(V vv) {v.val = vv;}
  void store(V newv) {
    size_t cnt = lg.commit_value(v.count).first;
#ifdef NoSkip  // used to test performance without optimization
    cam({cnt, v.val}, {cnt+1, newv});
#else
    // skip if done for efficiency
    skip_if_done_no_log([&] { cam({cnt, v.val}, {cnt+1, newv});});
#endif
  }
  V operator=(V b) {
    store(b);
    return b;
  }
};

// a write once variable can be initialized and then written up to once after that
// a read can happen before or after the write
// this avoids the need for a version counter that is needed on a mutable value
template <typename V>
struct write_once {
  static_assert(sizeof(V) <= 6 || std::is_pointer<V>::value,
    "Type for write_once must be a pointer or at most 6 bytes");
  std::atomic<V> v;
  write_once(V initial) : v(initial) {}
  write_once() {}
  V load() {return lg.commit_value_safe(v.load()).first;}
  V read() {return v.load();}
  void init(V vv) { v = vv; }
  void store(V vv) { v = vv; }
  V operator=(V b) { store(b); return b; }
  // inline operator V() { return load(); } // implicit conversion
};

// *****************************
// Memory pool using epoch based collection and safe (idempotent)
// allocation and retire in a lock.
// *****************************

struct lock;

template <typename T, typename Pool=mem_pool<T>>
struct memory_pool {
  Pool pool;

  void reserve(size_t n) { pool.reserve(n);}
  void clear() { pool.clear(); }
  void stats() { pool.stats();}
  void shuffle(size_t n) { pool.shuffle(n);}
  
  void acquire(T* p) { pool.acquire(p);}
  
  void retire(T* p) {
    if (debug && (p == nullptr))
      std::cout << "retiring null value" << std::endl;
    auto x = lg.commit_value_safe(p);
    if (x.second) // only retire if first try
      with_empty_log([=] {pool.retire(p);}); 
  }

  void destruct(T* p) {
    if (debug && (p == nullptr))
      std::cout << "destructing null value" << std::endl;
    auto x = lg.commit_value_safe(p);
    if (x.second) // only retire if first try
      with_empty_log([=] {pool.destruct(p);}); 
  }

  template <typename F, typename ... Args>
  // f is a function that initializes a new object before it is shared
  T* new_init(F f, Args... args) {
    T* newv = with_log(Log(), [&] { //run f without logging (i.e. an empty log)
	T* x = pool.new_obj(args...);
	f(x);
	return x;});
    auto r = lg.commit_value(newv);
    if (!r.second) { pool.destruct(newv); } // destruct if already initialized
    return r.first;
  }

  // Idempotent allocation
  template <typename ... Args>
  T* new_obj(Args ...args) {
    return new_obj_fl(args...).first;
  }

protected:
  friend class lock;

  // The following protected routines are only used internally
  // in the lock code (not accessible to the user)
  
  // Returns a pointer to the new object and a possible pointer
  // to a location in the log containing the pointer.
  // The location is null if this was not the first among thunks
  // to allocate the object.
  // The returned pointer can be one of done_true or done_false
  // if the object is already retired using retire_acquired.
  template <typename ... Args>
  std::pair<T*,log_entry*> new_obj_acquired(Args... args) {
    auto [ptr,fl] = new_obj_fl(args...);
    if (lg.is_empty()) return std::pair(ptr, nullptr);
    log_entry* l = lg.current_entry();
    if (!fl && !is_done(ptr)) {
      pool.acquire(ptr);
      return std::make_pair((T*) l->load(), nullptr);
    } else return std::make_pair(ptr, fl ? l : nullptr);
  }

  // le must be a value returned as the second return value of new_obj_acquired.
  // It will be either be null or a pointer to a log entry containing p.
  // If non-null then it clears p from the log by replacing it with
  // the result (true or false) so that p can be safely reclaimed.
  // It then retires p.
  // It is important that only one of the helping thunks is passed an le that
  // is not null, otherwise could be retired multiple times
  template<typename TT>
  void retire_acquired_result(T* p, log_entry* le, std::optional<TT> result) {
    if (lg.is_empty()) pool.retire(p);
    else if (le != nullptr) {
      *le = tag_result(result);
      pool.retire(p);
    }
  }

  bool is_done(T* p) {return is_done_flag(p);}

  template <typename RT>
  std::optional<RT> done_val_result(T* p) {
    auto r = extract_result(p);
    if (r.has_value()) return (RT) r.value();
    else return {};
  }
  
private:
  bool done_val(T* p) {return extract_bool(p);}

  // this version also returns a flag to specify whether actually allocated
  // vs., having been allocated by another instance of a thunk
  template <typename ... Args>
  std::pair<T*,bool> new_obj_fl(Args... args) {
    // TODO: helpers might do lots of allocates and frees,
    // can potentially optimize by checking if a log value has already been committed.
    T* newv = with_log(Log(), [&] {return pool.new_obj(args...);});
    auto r = lg.commit_value(newv);
    // if already allocated return back to pool
    if (!r.second) { pool.destruct(newv); }
    return r;
  }

  // the following tags a long entry with the return value of a thunk
  // 1 at the 48th bit is true, 2 at the 48th bit is false
  
  bool is_done_flag(T* p) {
    return (((size_t) p) >> 48) > 0;
  }

  void* tag_bool(bool result) {
    return (void*) (result ? (1ul << 48) : (2ul << 48));
  }

  bool extract_bool(T* p) {
    return (((size_t) p) >> 48) == 1ul;
  }

  // a poor mans "optional".  The flag at the 48th bit indicates presence,
  // and the lower 48 bits are the value if present.
  template<typename TT>
  void* tag_result(std::optional<TT> result) {
    if(!result.has_value()) return (void*) (2ul << 48);
    else return (void*) ((1ul << 48) | (size_t) result.value());
  }

  std::optional<size_t> extract_result(T* p) {
    if (extract_bool(p))
      return ((size_t) p) & ((1ul << 48) - 1);
    return {};
  }
  
};

