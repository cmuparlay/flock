#include "absl/container/flat_hash_map.h"

#define AbslLock 1
#ifdef AbslLock
#include "absl/synchronization/mutex.h"
using LockType = absl::Mutex;
void Lock(LockType& l) {l.Lock();}
void Unlock(LockType& l) {l.Unlock();}
// tried actual reader lock, but slower
void ReaderLock(LockType& l) {l.Lock();}
void ReaderUnlock(LockType& l) {l.Unlock();}
#else
#include <mutex>
using LockType = std::mutex;
void Lock(LockType& l) {l.lock();}
void Unlock(LockType& l) {l.unlock();}
void ReaderLock(LockType& l) {l.lock();}
void ReaderUnlock(LockType& l) {l.unlock();}
#endif

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {

  using umap = absl::flat_hash_map<K, V, Hash, KeyEqual>;
  struct alignas(64) entry {
    LockType mutex;
    umap sub_table;
  };

  std::vector<entry> table;
  long num_buckets;

  unsigned int hash_to_shard(const K& k) {
    return (Hash{}(k) * UINT64_C(0xbf58476d1ce4e5b9)) & (num_buckets-1);
  }

  std::optional<V> find(const K& k) {
    size_t idx = hash_to_shard(k);
    ReaderLock(table[idx].mutex);
    auto r = table[idx].sub_table.find(k);
    ReaderUnlock(table[idx].mutex);
    if (r != table[idx].sub_table.end()) return (*r).second;
    else return std::optional<V>();
  }

  std::optional<V> find_(const K& k) {
    return find(k);
  }

  bool insert(const K& k, const V& v) {
    size_t idx = hash_to_shard(k);
    Lock(table[idx].mutex);
    bool result = table[idx].sub_table.insert(std::make_pair(k, v)).second;    
    Unlock(table[idx].mutex);
    return result;
  }

  bool remove(const K& k) {
    size_t idx = hash_to_shard(k);
    Lock(table[idx].mutex);
    bool result = table[idx].sub_table.erase(k) == 1;
    Unlock(table[idx].mutex);
    return result;
  }

  unordered_map(size_t n) {
    int n_bits = std::round(std::log2(n));
    int bits = n_bits - 2;
    num_buckets = 1l << bits; // must be a power of 2

    table = std::vector<entry>(num_buckets);
    for (int i=0; i < num_buckets; i++)
      table[i].sub_table = umap(n/num_buckets);
  }

  long size() {
    long n = 0;
    for (int i = 0; i < num_buckets; i++)
      n += table[i].sub_table.size();
    return n;}
};
