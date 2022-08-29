// This selects between using a hashlock or inline lock at compile time
// For the hashlock the address of the structure is hashed to one of a
// fixed number of lock locations and there is no need for a lock per
// structure.  However, the hashing can create lock cycles so this
// cannot be used with a strict lock, just a try_lock.

#define LockFree 1

#ifdef LockFree
#include "lf_lock.h"
#else
#include "spin_lock.h"
#include "lock_types.h"
#endif
#include "ptr_type.h"

#ifdef HashLock
struct lock_type {
private:
  static const int bucket_bits = 16;
  static const size_t mask = ((1ul) << bucket_bits) - 1;
  static std::vector<lock> locks;
  lock* get_lock() {
    return &locks[parlay::hash64_2((size_t) this) & mask];}
public:
  template <typename F>
  bool try_lock(F f) {
    return get_lock()->try_lock(f);}
  void wait_lock() { get_lock()->wait_lock(); }
  bool is_locked() { return get_lock()->is_locked(); }
};

std::vector<lock> lock_type::locks{1ul << bucket_bits};

#else 
using lock_type = lock;
#endif

