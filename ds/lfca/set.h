#define Range_Search 1
#define LFCA 1

#include<lfca.h>

// #define MAX_TREAPS_NEEDED (2 * NUM_OPS)
// #define MAX_NODES_NEEDED (32 * NUM_OPS)
// #define MAX_RESULT_SETS_NEEDED (2 * NUM_OPS)


template <typename K_, typename V_>
struct Set {
  using adapter_t = LfcaTree;

public:
  void reserve(size_t n) {
    // adapter_t::reserve(n);
  }

  void shuffle(size_t n) {}

  adapter_t* empty(size_t n) {
    int reserved = 100000;
    Treap::Preallocate(reserved);
    node::Preallocate(16*reserved);
    rs::Preallocate(reserved);
    return new adapter_t();
  }

  std::optional<V> find(adapter_t* ds, const K key) {
    return ds->lookup(key);
  } 

  std::optional<V> find_(adapter_t* ds, const K key) {
    return ds->lookup(key);
  } 

  V insert(adapter_t* ds, K key, const V val) {
    return ds->insert(key);
  }

  V remove(adapter_t* ds, const K key) {
    return ds->remove(key);
  }

  void print(adapter_t* ds) {
    // ds->ds->printTree();
  }

  template<typename AddF>
  void range_(adapter_t* ds, AddF& add, K start, K end) {
    ds->rangeQuery(start, end);
  }

  void retire(adapter_t* ds) {
    Treap::Deallocate();
    node::Deallocate();
    rs::Deallocate();
  }

  void clear() {
    // TODO: you may want to clear memory pools here
  }

  size_t check(adapter_t* ds) {
    return ds->rangeQuery(-1, numeric_limits<long>::max()-1).size();
  }

  void stats() {
    // ds->printSummary();
  }
};