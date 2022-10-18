// doubly linked list
#include <flock/flock.h>

template <typename K, typename V>
struct Set {

  struct alignas(64) node : ll_head, lock_type {
    bool is_end;
    write_once<bool> removed;
    ptr_type_<node> prev;
    ptr_type<node> next;
    K key;
    V value;
    node(K key, V value, node* next, node* prev)
      : key(key), value(value), is_end(false), removed(false),
      next(next), prev(prev) {};
    node(node* next)
      : is_end(true), removed(false), next(next), prev(nullptr) {}
  };

  memory_pool<node> node_pool;

  auto find_location(node* root, K k) {
    node* nxt = (root->next).read();
    while (true) {
      node* nxt_nxt = (nxt->next).read(); // prefetch
      if (nxt->is_end || nxt->key >= k) break;
      nxt = nxt_nxt;
    }
    return nxt;
  }

  

  static constexpr int init_delay=200;
  static constexpr int max_delay=2000;

  bool insert(node* root, K k, V v) {
    return with_epoch([&] {
      int delay = init_delay;
      while (true) {
	node* next = find_location(root, k);
	if (!next->is_end && next->key == k) return false;
	node* prev = (next->prev).load();
	// fails if something inserted before in meantime
	if ((prev->is_end || prev->key < k) && 
	    prev->try_lock([=] {
		if (!prev->removed.load() && (prev->next).load() == next) {
		  auto new_node = node_pool.new_obj(k, v, next, prev);
		  prev->next = new_node;
		  next->prev = new_node;
		  return true;
		} else return false;}))
	  return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }
    });
  }

  bool remove(node* root, K k) {
    return with_epoch([&] {
      int delay = init_delay;
      while (true) {
	node* loc = find_location(root, k);
	if (loc->is_end || loc->key != k) return false;
	node* prev = (loc->prev).load();
	if (prev->try_lock([=] {
	      if (prev->removed.load() || (prev->next).load() != loc)
		return false;
	      return loc->try_lock([=] {
		  node* next = (loc->next).load();
		  loc->removed = true;
		  prev->next = next;
		  next->prev = prev;
		  node_pool.retire(loc);
		  return true;
		});}))
	  return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }
    });
  }
  
  std::optional<V> find_(node* root, K k) {
    node* loc = find_location(root, k);
    if (!loc->is_end && loc->key == k) return loc->value; 
    else return {};
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] { return find_(root, k);});
  }

  node* empty() {
    node* tail = node_pool.new_obj(nullptr);
    node* head = node_pool.new_obj(tail);
    tail->prev = head;
    return head;
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

  void retire(node* p, bool is_start = true) {
    if (is_start || !p->is_end) retire(p->next.load(), false);
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
