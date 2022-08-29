// This selects between using a hashlock or inline lock at compile time
// For the hashlock the address of the structure is hashed to one of a
// fixed number of lock locations and there is no need for a lock per
// structure.  However, the hashing can create lock cycles so this
// cannot be used with a strict lock, just a try_lock.

#include "spin_lock.h"
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
  template <typename Lock>
  V read_fix(Lock* lck) {return read();}
  void validate() {}
};

template <typename V>
using write_once = mutable_val<V>;

#include "epoch.h"
template <typename T>
using memory_pool = mem_pool<T>;

#ifdef HashLock
struct lock_type {
  static const int bucket_bits = 16;
  static const size_t mask = ((1ul) << bucket_bits) - 1;
  static std::vector<lock> locks;
  lock* get_lock() {
    return &locks[parlay::hash64_2((size_t) this) & mask];}
  template <typename F>
  bool try_with_lock(F f) {
    return get_lock()->try_lock(f);}
  void clear_the_lock() { get_lock()->clear_lock(); }
  bool is_locked() { return get_lock()->is_locked(); }
};

std::vector<lock> lock_type::locks{1ul << bucket_bits};

#else 
struct lock_type {
  lock lck;
  template <typename F>
  bool try_with_lock(F f) {
    return lck.try_lock(f); }
  template <typename F>
  auto try_with_lock_result(F f) -> std::optional<decltype(f())> {
    return lck.try_lock_result(f); }
  void clear_the_lock() { lck.clear_lock(); }
  bool is_locked() { return lck.is_locked(); }
};
#endif

