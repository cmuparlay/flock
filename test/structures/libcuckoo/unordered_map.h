#include <parlay/primitives.h>

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

  bool insert(const K& k, const V& v) {
    return table.insert(k, v);
  }

  bool remove(const K& k) {
    return table.erase(k);
  }

  unordered_map(size_t n) {
    table.reserve(n);
  }

  long size() {return table.size();}
};
