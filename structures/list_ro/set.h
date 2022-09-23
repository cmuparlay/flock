#include <limits>
#define Recorded_Once 1
#include <flock/flock.h>

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();
  K key_max = std::numeric_limits<K>::max();

  struct alignas(32) node : ll_head {
    ptr_type<node> next;
    K key;
    V value;
    lock_type lck;
    write_once<bool> removed;
    node(K key, V value, node* next)
      : key(key), value(value), next(next), removed(false) {};
  };

  memory_pool<node> node_pool;

  size_t max_iters = 10000000;

  auto find_location(node* root, K k) {
    node* cur = root;
    node* nxt = (cur->next).read();
    while (true) {
      node* nxt_nxt = (nxt->next).read(); // prefetch
      if (nxt->key >= k) break;
      cur = nxt;
      nxt = nxt_nxt;
    }
    return std::make_pair(cur, nxt);
  }

  bool insert(node* root, K k, V v) {
    return with_epoch([=] {
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (nxt->key == k) return false; //already there
	if (cur->lck.try_lock([=] {
	      if (!cur->removed.load() && (cur->next).load() == nxt) {
		auto new_node = node_pool.new_obj(k, v, nxt);
		cur->next = new_node; // splice in
		return true;
	      } else return false;}))
	  return true;
      }});
  }

  bool remove(node* root, K k) {
    return with_epoch([=] {
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (k != nxt->key) return false; // not found
        if (cur->lck.try_lock([=] {
          if (cur->removed.load() || (cur->next).load() != nxt) return false;
	  return nxt->lck.try_lock([=] {
	    node* nxtnxt = (nxt->next).load();
	    return nxtnxt->lck.try_lock([=] {
	      nxt->removed = true;
	      nxtnxt->removed = true;
	      auto new_node = node_pool.new_init([=] (node* r) {
			   // if tail then need to point to self
			   if (r->next.read() == nxtnxt) r->next.init(r);},
		nxtnxt->key, nxtnxt->value, nxtnxt->next.load());
	      cur->next = new_node; // splice in
	      node_pool.retire(nxt);
	      node_pool.retire(nxtnxt); 
	      return true;
	    });});}))
	  return true;
      }
    });
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	auto [cur, nxt] = find_location(root, k);
	(cur->next).validate();
	if (nxt->key == k) return nxt->value; 
	else return {};
      });
  }

  node* empty() {
    node* tail = node_pool.new_obj(key_max, 0, nullptr);
    tail->next = tail;
    return node_pool.new_obj(key_min, 0, tail);
  }

  node* empty(size_t n) { return empty(); }

  void print(node* p) {
    node* ptr = (p->next).load();
    while (ptr->key != key_max) {
      std::cout << ptr->key << ", ";
      ptr = (ptr->next).load();
    }
    std::cout << std::endl;
  }

  void retire(node* p) {
    node* nxt = p->next.load();
    if (nxt != p) retire(nxt);
    node_pool.retire(p);
  }
  
  long check(node* p) {
    if (p->key != key_min) std::cout << "bad head" << std::endl;
    node* ptr = (p->next).load();
    K k = key_min;
    long i = 0;
    while (ptr->key != key_max) {
      i++;
      if (ptr->key <= k) {
	std::cout << "bad key: " << k << ", " << ptr->key << std::endl;
	abort();
      }
      k = ptr->key;
      ptr = (ptr->next).load();
    }
    if (ptr == nullptr) std::cout << "bad tail: " << std::endl;
    return i;
  }

  void clear() { node_pool.clear();}
  void reserve(size_t n) { node_pool.reserve(n);}
  void shuffle(size_t n) { node_pool.shuffle(n);}
  void stats() { node_pool.stats();}

};
