
#ifndef LLCODE_ADAPTER_H
#define LLCODE_ADAPTER_H

#include "random_fnv1a.h"
#include "adapter.h"

#include <verlib/verlib.h>
#include <parlay/primitives.h>
#include <optional>

thread_local int _tid = -1;
std::atomic<int> num_initialized_threads(0);

template <typename K, typename V, class alloc>
class ordered_set {
private:
  using adapter_t = ds_adapter<K,V,reclaimer_debra<K>,alloc,pool_none<K>>;
  inline static const K KEY_NEG_INFTY = std::numeric_limits<K>::min()+1;
  inline static const K KEY_POS_INFTY = std::numeric_limits<K>::max()-1;

public:
  void reserve(size_t n) {
    adapter_t::reserve(n);
  }

  void shuffle(size_t n) {
    adapter_t::shuffle(n);
  }

  adapter_t* empty(size_t n) {
    return new adapter_t(parlay::num_workers(), KEY_NEG_INFTY, KEY_POS_INFTY, KEY_NEG_INFTY, nullptr);
  }

  std::optional<V> find(adapter_t* ds, const K key) {
    init_thread(ds);
    V val = ds->find(_tid, key);
    if(val == (V) ds->getNoValue()) return {};
    else return val;
  } 

  std::optional<V> find_(adapter_t* ds, const K key) {
    init_thread(ds);
    V val = ds->find(_tid, key);
    if(val == (V) ds->getNoValue()) return {};
    else return val;
  } 

  V insert(adapter_t* ds, K key, const V val) {
    init_thread(ds);
    assert(key != 0);
    V r = ds->insertIfAbsent(_tid, key, val); 
    return (r == (V) ds->getNoValue());
  }

  V remove(adapter_t* ds, const K key) {
    init_thread(ds);
    V r = ds->erase(_tid, key);
    return (r != (V) ds->getNoValue());
  }

  void print(adapter_t* ds) {
    // ds->ds->printTree();
  }

  void retire(adapter_t* ds) {
    init_thread(ds);
    for(int i = 0; i < num_initialized_threads; i++)
      ds->deinitThread(i); // all data structures have an additional check to avoid deinitializing twice
    // num_initialized_threads = 0;
    delete ds;
  }

  void clear() {
    // TODO: you may want to clear memory pools here
  }

  size_t check(adapter_t* ds) {
    init_thread(ds);
    auto tree_stats = ds->createTreeStats(KEY_NEG_INFTY, KEY_POS_INFTY);
    std::cout << "average height: " << tree_stats->getAverageKeyDepth() << std::endl;
    size_t size = tree_stats->getKeys();
    delete tree_stats;
    return size;
  }

  void stats() {
    // ds->printSummary();
  }

private:
  inline void init_thread(adapter_t* ds) {
    if(_tid == -1) {
      _tid = num_initialized_threads.fetch_add(1);
      // std::cout << "thread id: " << _tid << std::endl;
    }
    ds->initThread(_tid);  // all data structures have an additional check to avoid initializing twice
  }
};

#endif // LLCODE_ADAPTER_H
