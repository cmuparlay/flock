#define Range_Search 1
#include <flock/verlib.h>

template <typename K, typename V>
struct Set {

  struct alignas(32) node : vl::versioned {
    vl::versioned_ptr<node> next;
    K key;
    V value;
    bool is_end;
    flck::write_once<bool> removed;
    flck::lock lck;
    node(K key, V value, node* next)
      : key(key), value(value), next(next), is_end(false), removed(false) {};
    node(node* next, bool is_end) // for head and tail
      : next(next), is_end(is_end), removed(false) {};
  };

  vl::memory_pool<node> node_pool;

  auto find_location(node* root, K k) {
    node* cur = root;
    node* nxt = (cur->next).load();
    while (true) {
      node* nxt_nxt = (nxt->next).load(); // prefetch
      if (nxt->is_end || nxt->key >= k) break;
      cur = nxt;
      nxt = nxt_nxt;
    }
    return std::make_pair(cur, nxt);
  }

  static constexpr int init_delay = 200;
  static constexpr int max_delay = 2000;

  bool insert(node* root, K k, V v) {
    return vl::with_epoch([=] {
      int delay = init_delay;
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (!nxt->is_end && nxt->key == k) return false; //already there
	if (cur->lck.try_lock([=] {
	      if (!cur->removed.load() && (cur->next).load() == nxt) {
		node* new_node = node_pool.new_obj(k, v, nxt);
		cur->next = new_node; // splice in
		return true;
	      } else return false;}))
	  return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }});
  }

  bool remove(node* root, K k) {
    return vl::with_epoch([=] {
      int delay = init_delay;
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (nxt->is_end || k != nxt->key) return false; // not found
        if (cur->lck.try_lock([=] {
          if (cur->removed.load() || (cur->next).load() != nxt)
	    return false;
	  return nxt->lck.try_lock([=] {
	    node* nxtnxt = (nxt->next).load();
	    nxt->removed = true;
	    cur->next = nxtnxt; // shortcut
	    node_pool.retire(nxt); 
	    return true;
	  });}))
	  return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }
    });
  }

  std::optional<V> find_(node* root, K k) {
    auto [cur, nxt] = find_location(root, k);
    //(cur->next).validate();
    if (!nxt->is_end && nxt->key == k) return nxt->value; 
    else return {};
  }

  std::optional<V> find(node* root, K k) {
    return vl::with_epoch([&] {return find_(root, k);});
  }

  template<typename AddF>
    void range(node* root, AddF& add, K start, K end) {
    vl::with_snapshot([=] {
      node* nxt = (root->next).load();
      while (true) {
	node* nxt_nxt = (nxt->next).load(); // prefetch
	if (nxt->is_end || nxt->key >= start) break;
	nxt = nxt_nxt;
#ifdef LazyStamp
	if (vl::bad_stamp) return true;
#endif
      }
      while (!nxt->is_end && nxt->key <= end) {
	add(nxt->key, nxt->value);
	nxt = nxt->next.load();
#ifdef LazyStamp
	if (vl::bad_stamp) return true;
#endif
      }
      return true; });
  }

  node* empty() {
    node* tail = node_pool.new_obj(nullptr, true);
    return node_pool.new_obj(tail, false);
  }

  node* empty(size_t n) { return empty(); }

  void print(node* p) {
    node* ptr = (p->next).load();
    while (!ptr->is_end) {
      std::cout << ptr->key << ", ";
      ptr = (ptr->next).load();
    }
    std::cout << std::endl;
  }

  void retire(node* p) {
    if (!p->is_end) retire(p->next.load());
    node_pool.retire(p);
  }
  
  long check(node* p) {
    node* ptr = (p->next).load();
    if (ptr->is_end) return 0;
    K k = ptr->key;
    ptr = (ptr->next).load();
    long i = 1;
    while (!ptr->is_end) {
      i++;
      if (ptr->key <= k) {
	std::cout << "bad key: " << k << ", " << ptr->key << std::endl;
	abort();
      }
      k = ptr->key;
      ptr = (ptr->next).load();
    }
    return i;
  }

  void clear() { node_pool.clear();}
  void reserve(size_t n) { node_pool.reserve(n);}
  void shuffle(size_t n) { node_pool.shuffle(n);}
  void stats() { node_pool.stats();}

};
