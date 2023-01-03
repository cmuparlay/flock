
#ifndef LLCODE_ADAPTER_H
#define LLCODE_ADAPTER_H

#include "HarrisLinkedList.hpp"

#include <parlay/primitives.h>
#include <optional>

using namespace std;

template <typename K, typename V>
class Set {
private:
  using adapter_t = HarrisLinkedList<K,V>;

public:
  void reserve(size_t n) { adapter_t::node_pool.reserve(n); }

  adapter_t* empty(size_t n) {
    return new adapter_t();
  }

  std::optional<V> find(adapter_t* ds, const K key) {
    return ds->find(key);
  } 

  std::optional<V> find_(adapter_t* ds, const K key) {
    return ds->find_(key);
  } 

  bool insert(adapter_t* ds, K key, const V val) {
    return ds->add(key, val);
  }

  bool remove(adapter_t* ds, const K key) {
    return ds->remove(key);
  }

  void print(adapter_t* ds) {
    // ds->ds->printTree();
  }

  void retire(adapter_t* ds) {
    delete ds;
  }

  void clear() {
    adapter_t::node_pool.clear();
  }

  size_t check(adapter_t* ds) {
    return ds->get_size();
  }

  void stats() {
    adapter_t::node_pool.stats();
  }

  void shuffle(size_t n) {
    adapter_t::node_pool.shuffle(n);
  }

};

#endif // LLCODE_ADAPTER_H
