#define Range_Search 1
#define LFCA 1

template <typename K_, typename V_>
struct Set {
  using adapter_t = LfcaTree;
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
    delete ds;
  }

  void clear() {
    // TODO: you may want to clear memory pools here
  }

  size_t check(adapter_t* ds) {
    return ds.range(-1, KEY_POS_INFTY).size();
  }

  void stats() {
    // ds->printSummary();
  }
};