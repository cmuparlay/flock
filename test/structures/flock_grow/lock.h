// creates 2^16 lock slots.
// locks.try_lock(i, f) will hash i to the h(i) % 2^16th lock.
// If the lock is not taken then f is run and the try_lock returns the
// boolean result of f then releasing the lock.   Otherwise it returns false.
struct lock_set {
private:
  using lck = std::atomic<bool>;
  const int bucket_bits = 16;
  const size_t mask = ((1ul) << bucket_bits) - 1;
  std::vector<lck> locks;
public:
  template <typename F>
  bool try_lock(long i, F f) {
    bool old = false;
    bool result = false;
    lck& x = locks[parlay::hash64_2(i) & mask];
    if (x.compare_exchange_strong(old, true)) {
      result = f();
      x = false;
    }
    return result;
  }
  lock_set() : locks(std::vector<lck>(1ul << bucket_bits)) {
    std::fill(locks.begin(), locks.end(), false);
  }
};

lock_set locks;
