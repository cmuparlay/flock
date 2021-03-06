#include <limits>
#include <flock/lock_type.h>
#include <flock/ptr_type.h>

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();
  K key_max = std::numeric_limits<K>::max();

  struct alignas(64) node : ll_head, lock_type {
    ptr_type<node> next;
    write_once<bool> removed;
    K key;
    V value;
    node(K key, V value, node* next)
      : key(key), value(value), next(next), removed(false) {};
  };

  memory_pool<node> node_pool;

  size_t max_iters = 10000000;

  auto find_location(node* root, K k) {
    node* cur = root;
    node* nxt = (cur->next).read_();
    //node* nxt = (cur->next).read_fix(cur);
    while (true) {
      //node* nxt_nxt = (nxt->next).read_fix(nxt); // prefetch
      node* nxt_nxt = (nxt->next).read_(); // prefetch
      if (nxt->key >= k) break;
      cur = nxt;
      nxt = nxt_nxt;
    }
    return std::make_pair(cur, nxt);
  }

  bool insert(node* root, K k, V v) {
    return with_epoch([=] {
      int cnt = 0;
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (nxt->key == k) return false; //already there
	if (cur->try_with_lock([=] {
	      if (!cur->removed.load() && (cur->next).load() == nxt) {
		auto new_node = node_pool.new_obj(k, v, nxt);
		cur->next = new_node; // splice in
		return true;
	      } else return false;}))
	  return true;
	// try again if unsuccessful
	// if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
      }});
  }

  bool remove(node* root, K k) {
    return with_epoch([=] {
      int cnt = 0;
      while (true) {
	auto [cur, nxt] = find_location(root, k);
	if (k != nxt->key) return false; // not found
        if (cur->try_with_lock([=] {
	      if (cur->removed.load() || (cur->next).load() != nxt) return false;
	      return nxt->try_with_lock([=] {
		  node* nxtnxt = (nxt->next).load();
		  nxt->removed = true;
		  cur->next = nxtnxt; // shortcut
		  node_pool.retire(nxt); 
		  return true;
		});}))
	  return true;
	// if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
      }
    });
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	auto [cur, nxt] = find_location(root, k);
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
