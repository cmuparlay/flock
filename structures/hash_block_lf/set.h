#include <parlay/primitives.h>
#include <flock/flock.h>

template <typename K, typename V>
struct Set {

  template <int Size>
  struct Node : ll_head {
    using node = Node<0>;
    struct KV {K key; V value;};
    int cnt;
    KV entries[Size];
    int find(K k) {
      for (int i=0; i < cnt; i++)
	if (entries[i].key == k) return i;
      return -1;
    }
    Node(K k, V v) : cnt(1) { // could this overload with the other constructor if K is a pointer?
      entries[0] = KV{k,v};
    }
    Node(node* old, K k, V v) {
      if (old == nullptr) cnt = 1;
      else {
	cnt = old->cnt + 1;
	for (int i=0; i < old->cnt; i++)
	  entries[i] = ((Node<Size>*) old)->entries[i];
      }
      entries[cnt-1] = KV{k,v};
    }
    Node(node* old, K k) : cnt(old->cnt - 1) {
      for (int i=0, j=0; i < cnt; i++,j++) {
	if (k == old->entries[i].key) j++;
	entries[i] = ((Node<Size>*) old)->entries[j];
      }
    }
  };
  using node = Node<0>;

  struct slot : lock_type {
    ptr_type<node> ptr;
    slot() : ptr(nullptr) {}
  };

  struct Table {
    parlay::sequence<slot> table;
    slot* get_slot(K k) {
      //size_t idx = parlay::hash64(k) & (table.size()-1u);
      size_t idx = (k * 0x9ddfea08eb382d69ULL) & (table.size()-1u);
      return &table[idx];
    }
    Table(size_t n) {
      size_t nn = std::max((size_t) (n*1.5), 1ul << 14);
      size_t size = 1ul << parlay::log2_up(nn);
      table = parlay::sequence<slot>(size);
    }
  };
  
  memory_pool<Node<1>> node_pool_1;
  memory_pool<Node<3>> node_pool_3;
  memory_pool<Node<7>> node_pool_7;
  memory_pool<Node<31>> node_pool_31;

  node* insert_to_node(node* old, K k, V v) {
    if (old == nullptr) return (node*) node_pool_1.new_obj(k, v);
    if (old->cnt < 3) return (node*) node_pool_3.new_obj(old, k, v);
    else if (old->cnt < 7) return (node*) node_pool_7.new_obj(old, k, v);
    else if (old->cnt < 31) return (node*) node_pool_31.new_obj(old, k, v);
    else abort();
  }

  node* remove_from_node(node* old, K k) {
    if (old->cnt == 1) return (node*) nullptr;
    if (old->cnt == 2) return (node*) node_pool_1.new_obj(old, k);
    else if (old->cnt <= 4) return (node*) node_pool_3.new_obj(old, k);
    else if (old->cnt <= 8) return (node*) node_pool_7.new_obj(old, k);
    else return (node*) node_pool_31.new_obj(old, k);
  }

  void retire_node(node* old) {
    if (old == nullptr);
    else if (old->cnt == 1) node_pool_1.retire((Node<1>*) old);
    else if (old->cnt <= 3) node_pool_3.retire((Node<3>*) old);
    else if (old->cnt <= 7) node_pool_7.retire((Node<7>*) old);
    else if (old->cnt <= 31) node_pool_31.retire((Node<31>*) old);
    else abort();
  }

  // should not require gcc attribute, but compiler screws up otherwise
  __attribute__((always_inline))
  std::optional<V> find_at(slot* s, K k) {
    node* x = s->ptr.load();
    if (x == nullptr) return {};
    if (x->entries[0].key == k)
      return x->entries[0].value;
    int i = x->find(k);
    if (i == -1) return {};
    else return x->entries[i].value;
  }
  
  std::optional<V> find_(Table& table, K k) {
    slot* s = table.get_slot(k);
    return find_at(s, k);
  }

  std::optional<V> find(Table& table, K k) {
    slot* s = table.get_slot(k);
    __builtin_prefetch (s);
    auto x = with_epoch([&] { return find_at(s, k);});
    return x;
  }

  static constexpr int init_delay=200;
  static constexpr int max_delay=2000;

  bool insert_at(slot* s, K k, V v) {
    int delay = init_delay;
    while (true) {
      node* x = s->ptr.load();
      if (x != nullptr && x->find(k) != -1) return false;
	    if (s->ptr.load() != x) return false;
      node* new_node = insert_to_node(x, k, v);
      if(s->ptr.cas(x, new_node)) {
        retire_node(x);
        return true;
      } else retire_node(new_node); // TODO: could be destruct
      for (volatile int i=0; i < delay; i++);
	      delay = std::min(2*delay, max_delay);
    }
  }

  bool insert(Table& table, K k, V v) {
    slot* s = table.get_slot(k); 
    return with_epoch([&] {return insert_at(s, k, v);});
  }
  
  bool remove_at(slot* s, K k) {
    int delay = init_delay;
    while (true) {
      node* x = s->ptr.load();
      if (x == nullptr || x->find(k) == -1) return false;
	    if (s->ptr.load() != x) return false;
      node* new_node = remove_from_node(x, k);
      if(s->ptr.cas(x, new_node)) {
        retire_node(x);
        return true;
      } else retire_node(new_node); // TODO: could be destruct
      for (volatile int i=0; i < delay; i++);
      delay = std::min(2*delay, max_delay);
    }
  }

  bool remove(Table& table, K k) {
    slot* s = table.get_slot(k);
    return with_epoch([&] {return remove_at(s, k);});
  }

  Table empty(size_t n) {return Table(n);}

  void print(Table& t) {
    auto& table = t.table;
    for (size_t i=0; i < table.size(); i++) {
      node* x = table[i].ptr.load();
      if (x != nullptr)
	for (int i = 0; i < x->cnt; i++)
	  std::cout << x->entries[i].key << ", ";
    }
    std::cout << std::endl;
  }

  
  void retire(Table& t) {
    auto& table = t.table;
    parlay::parallel_for (0, table.size(), [&] (size_t i) {
      retire_node(table[i].ptr.load());});
    table.clear();
  }
  
  long check(Table& t) {
    auto& table = t.table;
    auto s = parlay::tabulate(table.size(), [&] (size_t i) {
	      node* x = table[i].ptr.load();
	      if (x == nullptr) return 0;
	      else return x->cnt;});
    return parlay::reduce(s);
  }

  void clear() {
    node_pool_1.clear();
    node_pool_3.clear();
    node_pool_7.clear();
    node_pool_31.clear();
  }
  void stats() {
    node_pool_1.stats();
    node_pool_3.stats();
    node_pool_7.stats();
    node_pool_31.stats();
  }
  void reserve(size_t n) {}
  void shuffle(size_t n) {}
};
