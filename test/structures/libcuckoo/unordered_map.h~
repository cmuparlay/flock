#include <parlay/primitives.h>
#include <verlib/verlib.h>
#define HASH 1

#include "cuckoohash_map.hh"

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {
  using Table = libcuckoo::cuckoohash_map<K, V, Hash, KeyEqual>;

  Table table;
  
  std::optional<V> find(const K& k) {
    V result;
    if (table.find(k, result)) return result;
    else return std::optional<V>();
  }

  std::optional<V> find_(const K& k) {
    return find(k);
  }

  bool insert(const K& k, const V& v) {
    return table.insert(k, v);
  }

  bool remove(const K& k) {
    return table.erase(k);
  }

  unordered_map(size_t n) {
    table.reserve(n);
  }

  void print() {}
  long check() {return table.size();}
  static void clear() {}
  static void stats() {}
  static void reserve(size_t n) {}
  static void shuffle(size_t n) {}
};
