#include <unordered_map>

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {

  using Table = std::unordered_map<K, V, Hash, KeyEqual>;
  Table table;

  std::optional<V> find(const K& k) {
    auto r = table.find(k);
    if (r != table.end()) return (*r).second;
    else return std::optional<V>();
    
  }

  bool insert(const K& k, const V& v) {
    return table.insert(std::make_pair(k, v)).second;    
  }

  bool remove(const K& k) {
    return table.erase(k) == 1;
  }

  unordered_map(size_t n) {
    Table map(n);
    table = map;
  }

  void print() {}
  long size() {return table.size();}
};
