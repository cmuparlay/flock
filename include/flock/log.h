#include <atomic>
#include "tagged_pool.h"

#pragma once

// default log length.  Will grow if needed.
constexpr int Log_Len = 8;
using log_entry = std::atomic<void*>;
struct log_array;

mem_pool<log_array> log_array_pool;

struct log_array {
  std::array<log_entry,Log_Len> log_entries;
  std::atomic<log_array*> next;

  void init() { // TODO: can this just be a constructor?
    for (int i=0; i < Log_Len; i++) {
      if (log_entries[i].load() != nullptr) 
        log_entries[i].store(nullptr, std::memory_order::memory_order_relaxed);
    }
    if(next.load() != nullptr) 
      next.store(nullptr, std::memory_order::memory_order_relaxed);
  }

  // log_entry operator [](int i) const { return log_entries[i]; }
  log_entry & operator [](int i) { return log_entries[i]; }

  ~log_array() {
    if(next != nullptr) {
      log_array_pool.destruct(next);
      // next.store(nullptr, std::memory_order::memory_order_release);
    }
  }
};


// the log is maintained per thread and keeps track of all committed values for a thunk
// the log_array pointed to by vals is shared among all thread processing the same thunk
// the count is separate for each thread, indicating how many commits it has done
// whichever thread commits first at a given position in the log_array sticks such that all future
// commits will see it (i.e., whoever arrives first wins).
// commit_value commits a value to the log at the current position.  The value cannot be zero (nullptr).
// commit_value_safe allows the value to be zero but uses bit 48 as a marked flag so it only supports up to 6 bytes
// It supports pointers assuming they fit within 6 bytes (i.e. upper 2 bytes are empty).
struct Log {
  log_array* vals;
  int count;
  Log(log_array* pa, int c) : vals(pa), count(c) {}
  Log() : vals(nullptr), count(0) {}
  log_entry* next_entry() {
    assert(!is_empty());
    //if (debug && count > 0 && (*vals)[count-1] == nullptr) {
    //  std::cout << "skipped log entry " << count-1 << std::endl;}
    if (count == Log_Len) {
      count = 0;
      log_array* next_log_array = vals->next;
      if(next_log_array != nullptr) vals = next_log_array;
      else {  // next_log_array == nullptr, try to commit a new log array
        log_array* new_log_array = log_array_pool.new_obj();
        new_log_array->init();
        if(vals->next.compare_exchange_strong(next_log_array, new_log_array))
          vals = new_log_array;
        else {
          vals = next_log_array;
          log_array_pool.destruct(new_log_array);
        }
      }
    }
    return &(*vals)[count++];
  }
  log_entry* current_entry() {return &(*vals)[count-1];}
  bool is_empty() {return vals == nullptr;}

  // commits a value to the log, or returns existing value if already committed
  // along with a false flag.
  // V must be convertible to void*
  // initialized with null pointer, so should not commit a nullptr (or zero)
  // code tags pointers with a count or flag to avoid this
  template<typename V>
  std::pair<V,bool> commit_value(V newv) {
    if (is_empty()) return std::make_pair(newv, true);
    log_entry* l = next_entry();
    void* oldv = l->load();
    if (debug && (void*) newv == nullptr)
      std::cout << "committing null value to log" << std::endl;
    if (oldv == nullptr && l->compare_exchange_strong(oldv, (void*) newv))
      return std::make_pair(newv, true);
    else return std::make_pair((V) (size_t) oldv, false);
  }

  // this version tags 48th bit so value can be zero
  template<typename V>
  std::pair<V,bool> commit_value_safe(V val) {
    static_assert(sizeof(V) <= 6 || std::is_pointer<V>::value,
		  "Type for commit_value_safe must be a pointer or at most 6 bytes");
    if (is_empty()) return std::make_pair(val, true);
    size_t set_bit = (1ul << 48);
    log_entry* l = next_entry();
    void* oldv = l->load();
    void* newv = (void*) (((size_t) val) | set_bit);
    if (oldv == nullptr && l->compare_exchange_strong(oldv, (void*) newv))
      return std::make_pair(val, true);
    //else return std::make_pair((V) (((size_t) oldv) & (set_bit - 1)), false);
    else return std::make_pair((V) (((size_t) oldv) & ~set_bit), false);
  }
};

// Each thread maintains the log for the lock it is currently in using this variable.
// It will be empty if not inside a lock.
static thread_local Log lg;
//static thread_local Log oldlg;

template <typename F>
auto inline with_log(Log newlg, F f) {
  Log holdlg = lg;
  lg = newlg;
  auto r = f();
  lg = holdlg;
  return r;
}

template <typename F>
auto inline with_log_(Log newlg, F f) {
  Log holdlg = lg;
  lg = newlg;
  f();
  lg = holdlg;
}

// skips a chunk of code if finished by another helper on the log
template <typename F>
bool skip_if_done(F f) {
  if (lg.is_empty()) {f(); return true;}
  log_entry* l = lg.next_entry();
  if (l->load() == nullptr) { // check that not already completed
    f();
    // mark as completed
    l->store((void*) 1, std::memory_order::memory_order_release);
    return true;
  }
  return false;
}

// runs read only code without the log, and commits the result to the log
template <typename V, typename Thunk>
static V read_only(Thunk f) {
  // run f with an empty log and then commit its result
  V r = with_log(Log(), [&] {return f();});
  return lg.commit_value_safe(r).first;
}

// *****************************
// to wrap around values to read and write inside of a lock to make idempotent
// *****************************

struct write_annoucements {
  std::atomic<size_t>* announcements;
  const int stride = {16};
  write_annoucements() {
    announcements = new std::atomic<size_t>[parlay::num_workers()*stride];
  }
  ~write_annoucements() {
    delete[] announcements;
  }
  std::vector<size_t> scan() {
    std::vector<size_t> announced_tags;
    for(int i = 0; i < parlay::num_workers()*stride; i += stride)
      announced_tags.push_back(announcements[i]);
    return announced_tags;
  }
  void set(size_t val) {
    int id = parlay::worker_id();
    announcements[id*stride] = val;}
  void clear() {
    int id = parlay::worker_id();
    announcements[id*stride].store(0, std::memory_order::memory_order_release);}
};

write_annoucements announce_write = {};

template <typename V>
struct tagged {
  using IT = size_t;
  static constexpr int tag_bits = 16; // number of bits to use for tag (including panic bit)
  static constexpr IT top_bit = (1ul << 63);
  static constexpr IT cnt_bit = (1ul << (64-tag_bits+1));
  static constexpr IT panic_bit = (1ul << (64-tag_bits));
  static constexpr IT data_mask = panic_bit - 1;
  static constexpr IT cnt_mask = ~data_mask;
  static inline IT init(V v) {return cnt_bit | (IT) v;}
  static inline V value(IT v) {return (V) (v & data_mask);}
  static inline IT add_tag(IT oldv, IT newv) {
    return newv | (oldv & cnt_mask);
  }
  static inline IT next(IT oldv, V newv) { // old version, doesn't handle overflow
    return ((IT) newv) | inc_tag(oldv); 
  }
  static inline IT next(IT oldv, V newv, IT addr) {
    // return next(oldv, newv);
    IT new_count = inc_tag(oldv);

    bool panic = false;
    if((oldv & top_bit) != (new_count & top_bit) || // overflow, unlikely
       (oldv & panic_bit) != 0) { // panic bit set, unlikely
      // if((oldv & top_bit) != (new_count & top_bit)) std::cout << "overflow\n";
      // if((oldv & panic_bit) != 0) std::cout << "panic bit set\n";
      for(IT ann : announce_write.scan()) { // check if we have to panic
        if((ann & data_mask) == (addr & data_mask) && // same mutable_val obj
           (ann & top_bit) == (new_count & top_bit) && // same half of the key range
           (ann & cnt_mask) >= (new_count & cnt_mask & ~panic_bit)) { 
          panic = true;
          break;
        }
      }
    }

    if(panic) { // unlikely
      std::vector<IT> announced_tags = announce_write.scan();
      while(true) { // loop until new_count is not announced
        bool announced = false;
        for(IT ann : announced_tags) {
          if((ann & data_mask) == (addr & data_mask) &&  // same mutable_val obj
             (ann & cnt_mask) == new_count) {
            announced = true;
            break;
          }
        }
        if(!announced) return ((IT) newv) | (new_count | panic_bit);
        new_count = inc_tag(new_count);
      }
    } else return ((IT) newv) | (new_count & ~panic_bit);
  }
  static inline IT inc_tag(IT oldv) {
    IT new_count = (oldv & cnt_mask) + cnt_bit;
    return ((new_count == 0) ? cnt_bit : new_count); // avoid using 0
  }

  // a safe cas that assigns the new value a tag that no concurrent cas
  // on the same location has in its old value
  static bool cas(std::atomic<IT> &loc, IT oldv, V v, bool aba_free=false) {
    if (lg.is_empty() || aba_free) {
      IT newv = next(oldv, v, (IT) &loc);
      return loc.compare_exchange_strong(oldv, newv);
    } else {
      bool r = false;
      // announce the location and tag been written
      announce_write.set(add_tag(oldv, (IT) &loc));
      skip_if_done([&] { // skip both for correctness, and efficiency
	IT newv = next(oldv, v, (IT) &loc);
	r = loc.compare_exchange_strong(oldv, newv);});
      // unannounce the location
      announce_write.clear();
      return r;
    }
  }
};

  
template <typename V>
struct mutable_val {
private:
  using IT = size_t;
  using TV = tagged<V>;
  std::atomic<IT> v;

  IT get_val(Log &p) {
    return p.commit_value(v.load()).first; }

public:
  static_assert(sizeof(V) <= 4 || std::is_pointer<V>::value,
    "Type for mutable must be a pointer or at most 4 bytes");

  // not much to it.  heavy lifting done in TV
  mutable_val(V vv) : v(TV::init(vv)) {}
  mutable_val() : v(TV::init(0)) {}
  void init(V vv) {v = TV::init(vv);}
  V load() {return TV::value(get_val(lg));}
  V read() {return TV::value(v.load());}
  V read_() {return TV::value(v.load());}
  void store(V vv) {TV::cas(v, get_val(lg), vv);}
  void cam(V oldv, V newv) {
    V old_t = get_val(lg);
    if (TV::value(old_t) == oldv)
      TV::cam(v, old_t, newv);}
  V operator=(V b) {store(b); return b; }

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
    skip_if_done([&] { cam({cnt, v.val}, {cnt+1, newv});});
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
  void init(V vv) { v = vv; }
  void store(V vv) { v = vv; }
  V operator=(V b) { store(b); return b; }
  // inline operator V() { return load(); } // implicit conversion
};

// a wrapper for debugging use after free
template <typename V>
class mutable_val_debug {
public:
  log_array* freed;
  log_array* old;
  mutable_val<V> v;
  V load() {
    if (freed) {std::cout << "load from free" << std::endl; abort();}
    return v.load();
  }
  void store(V vv) {
    if (freed) {std::cout << "store to free" << std::endl; abort();}
    v.store(vv);
  }
  V operator=(V b) { store(b); return b; }
  // operator V() { return load(); } // implicit conversion
  ~mutable_val_debug() {
    if (freed) {
      //std::cout << "double free: " << freed << ", " << lg.vals << " : " << old << ", " << oldlg.vals << std::endl; abort();}
      std::cout << "double free: " << freed << ", " << lg.vals << std::endl; abort();}
    freed = lg.vals;
    //old = oldlg.vals;
  }
  mutable_val_debug(V v) : v(v), freed(nullptr) {}
  mutable_val_debug() : freed(nullptr) {}
};


// *****************************
// memory pool using epoch based collection and safe (idempotent) allocation and retire in a lock
// *****************************

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
    if (x.second) pool.retire(p); // only retire if first try
  }

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

  template <typename F, typename ... Args>
  // f is a function that initializes a new object before it is shared
  T* new_init(F f, Args... args) {
    // run f with no logging (i.e. an empty log)
    T* newv = with_log(Log(), [&] {
      T* x = pool.new_obj(args...);
      f(x);
      return x;
    });
    auto r = lg.commit_value(newv);
    if (!r.second) { pool.destruct(newv); }
    return r.first;
  }

  // Idempotent allocation
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
  void retire_acquired(T* p, log_entry* le, bool result) {
    if (lg.is_empty()) pool.retire(p);
    else if (le != nullptr) {
      *le = tag_bool(result);
      pool.retire(p);
    }
  }

  template<typename TT>
  void retire_acquired_result(T* p, log_entry* le, std::optional<TT> result) {
    if (lg.is_empty()) pool.retire(p);
    else if (le != nullptr) {
      *le = tag_result(result);
      pool.retire(p);
    }
  }

  bool is_done(T* p) {return is_done_flag(p);}
  bool done_val(T* p) {return extract_bool(p);}

  template <typename RT>
  std::optional<RT> done_val_result(T* p) {
    auto r = extract_result(p);
    if (r.has_value()) return (RT) r.value();
    else return {};
  }
  
  template <typename ... Args>
  T* new_obj(Args ...args) {
    return new_obj_fl(args...).first;
  }

private:
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

