#include <flock/lock.h>
#include <parlay/primitives.h>

template <typename K, typename V>
struct Set {

  struct alignas(32) node {
    K key;
    V value;
    mutable_val<node*> next;
    node(K key, V value, node* next) : key(key), value(value), next(next) {};
  };
  
  struct slot {
    mutable_val<node*> head;
    //lock lck;
    mutable_val<unsigned int> version_num;
    slot() : version_num(0), head(nullptr) {}
  };

  using Table = parlay::sequence<slot>;

  memory_pool<node> node_pool;

  slot* get_slot(Table& table, K k) {
    //return &table[parlay::hash64_2(k) & (table.size()-1u)];
    return &table[(k * 0x9ddfea08eb382d69ULL) & (table.size()-1u)];
  }

  auto find_in_slot(slot* s, K k) {
    mutable_val<node*>* cur = &s->head;
    node* nxt = cur->load();
    while (nxt != nullptr && nxt->key != k) {
      cur = &(nxt->next);
      nxt = cur->load();
    }
    return std::make_pair(cur,nxt);
  }

  std::optional<V> find(Table& table, K k) {
    slot* s = get_slot(table, k);
    __builtin_prefetch (s);
    return with_epoch([&] () -> std::optional<V> {
	auto [cur, nxt] = find_in_slot(s, k);
	if (nxt != nullptr) return nxt->value;
	else return {};
      });
  }

  bool insert_at(slot* s, K k, V v) {
    while (true) {
      unsigned int vn = s->version_num.load();
      auto [cur, nxt] = find_in_slot(s, k);
      if (nxt != nullptr) return false;
      if (try_lock_loc(s, [=] {
			    if (s->version_num.load() != vn) return false;
			    *cur = node_pool.new_obj(k, v, nullptr);
			    s->version_num = vn+1;
			    return true;}))
	return true;
    }

  }

  bool insert(Table& table, K k, V v) {
    slot* s = get_slot(table, k);
    return with_epoch([&] {return insert_at(s, k, v);});
  }
			
  bool remove_at(slot* s, K k) {
    while (true) {
      unsigned int vn = s->version_num.load();
      auto [cur, nxt] = find_in_slot(s, k);
      if (nxt == nullptr) return false;
      if (try_lock_loc(s, [=] {
			    if (s->version_num.load() != vn) return false;
			    *cur = nxt->next.load();
			    node_pool.retire(nxt);
			    s->version_num = vn+1;
			    return true;}))
	return true;
    }
  }

  bool remove(Table& table, K k) {
    slot* s = get_slot(table, k);
    return with_epoch([&] {return remove_at(s, k);});
  }
			
  Table empty(size_t n) {
    size_t size = (1ul << parlay::log2_up(n));
    return parlay::sequence<slot>(2*size); 
  }

  void print(Table& table) {
    for (size_t i=0; i < table.size(); i++) {
      auto x = &(table[i].head);
      auto ptr = x->load();
      while (ptr != nullptr) {
	std::cout << ptr->key << ", ";
	ptr = (ptr->next).load();
      }
    }
    std::cout << std::endl;
  }

  void retire_list(node* ptr) {
    if (ptr == nullptr) return;
    else {
      retire_list((ptr->next).load());
      node_pool.retire(ptr);
    }
  }

  
  void retire(Table& table) {
    parlay::parallel_for (0, table.size(), [&] (size_t i) {
	     retire_list(table[i].head.load());});
    table.clear();
  }
  
  long check(Table& table) {
    auto s = parlay::tabulate(table.size(), [&] (size_t i) {
	      node* ptr = table[i].head.load();
	      int cnt = 0;
	      while (ptr != nullptr) {
		cnt++;
		ptr = (ptr->next).load();
	      }
	      return cnt;});
    return parlay::reduce(s);
  }

  void clear() { node_pool.clear();}
  void reserve(size_t n) { node_pool.reserve(n);}
  void shuffle(size_t n) { node_pool.shuffle(n);}
  void stats() { node_pool.stats();}

};
