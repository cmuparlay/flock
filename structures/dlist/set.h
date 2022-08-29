// doubly linked list
#include <limits>
#include <flock/flock.h>

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();
  K key_max = std::numeric_limits<K>::max();

  struct node : ll_head, lock_type {
    ptr_type<node> next;
    ptr_type_<node> prev;
    write_once<bool> removed;
    K key;
    V value;
    node(K key, V value, node* next, node* prev)
      : key(key), value(value), removed(false), next(next), prev(prev) {};
  };

  memory_pool<node> node_pool;

  size_t max_iters = 10000000;
  
  auto find_location(node* root, K k) {
    node* nxt = (root->next).load();
    while (true) {
      node* nxt_nxt = (nxt->next).read(); // prefetch
      if (nxt->key >= k) break;
      nxt = nxt_nxt;
    }
    return nxt;
  }

  bool insert(node* root, K k, V v) {
    return with_epoch([&] {
      int cnt = 0;		 
      while (true) {
	node* next = find_location(root, k);
	if (next->key == k) return false;
	node* prev = (next->prev).load();
	if (prev->key < k && // fails if something inserted before in meantime
	    prev->try_lock([=] {
		if (!prev->removed.load() && (prev->next).load() == next) {
		  auto new_node = node_pool.new_obj(k, v, next, prev);
		  prev->next = new_node;
		  next->prev = new_node;
		  return true;
		} else return false;}))
	  return true;
	// if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
	// try again if unsuccessful
      }
    });
  }

  bool remove(node* root, K k) {
    return with_epoch([&] {
      int cnt = 0;		 
      while (true) {
	node* loc = find_location(root, k);
	if (loc->key != k) return false;
	node* prev = (loc->prev).load();
	if (prev->try_lock([=] {
	      if (prev->removed.load() || (prev->next).load() != loc) return false;
	      return loc->try_lock([=] {
		  node* next = (loc->next).load();
		  loc->removed = true;
		  prev->next = next;
		  next->prev = prev;
		  node_pool.retire(loc);
		  return true;
		});})) {
	  return true;
	}
	// if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
	// try again if unsuccessful
      }
    });
  }
  
  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	node* loc = find_location(root, k);
	if (loc->key == k) return loc->value; 
	else return {};
      });
  }

  node* empty() {
    node* tail = node_pool.new_obj(key_max, 0, nullptr, nullptr);
    node* head = node_pool.new_obj(key_min, 0, tail, nullptr);
    tail->next = tail;
    tail->prev = head;
    return head;
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
    while (ptr != nullptr && ptr->key != key_max) {
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
