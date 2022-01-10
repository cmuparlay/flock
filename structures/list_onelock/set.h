// A version with only one lock instead of two for remove.
// Uses clear_lock to clear current owner of a lock without taking the lock.
// Involves a race condition between the writer of a delete flag and the reader.
// Requires clearing prev incase it is half way through its delete
// and has set removed on its next.   This would prevent progress.

// Does not currently work with hashlocks due to cycles (lock a could
// clear lock b while lock b is clearing lock a).
// To make it work need to associate unhashed address with hashed lock
// so clear_the_lock can ignore the lock if addresses do not match
// (i.e. accidental collision).

#include <limits>
#include <flock/lock_type.h>
#include <parlay/primitives.h>

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();
  K key_max = std::numeric_limits<K>::max();

  struct node : lock_type {
    mutable_val<node*> next;
    write_once<bool> removed;
    K key;
    V value;
    node(K key, V value, node* next)
      : key(key), value(value), next(next), removed(false) {};
  };

  memory_pool<node> node_pool;

  auto find_location(node* root, K k) {
    node* prev = nullptr;
    node* cur = root;
    node* nxt = (cur->next).read();
    while (true) {
      node* nxt_nxt = (nxt->next).read(); // prefetch
      if (nxt->key >= k) break;
      cur = nxt;
      nxt = nxt_nxt;
    }
    return std::make_tuple(prev,cur, nxt);
  }

  bool insert(node* root, K k, V v) {
    return with_epoch([=] {
      while (true) {
	auto [prev, cur, nxt] = find_location(root, k);
	if (nxt->key == k) return false; //already there
	if (use_help && prev != nullptr) prev->clear_the_lock(); // important to ensure lock freedom
	if (cur->try_with_lock([=] {
	      if (cur->removed.load() || (cur->next).load() != nxt) return false;
	      auto new_node = node_pool.new_obj(k, v, nxt);
	      cur->next = new_node; // splice in
	      return true;
	    })) return true;
      }});
  }

  bool remove(node* root, K k) {
    return with_epoch([=] {
      while (true) {		 
	auto [prev, cur, nxt] = find_location(root, k);
	if (k != nxt->key) return false; // not found
	if (prev != nullptr) prev->clear_the_lock();
	nxt->clear_the_lock();
	if (cur->try_with_lock([=] {
	      if (cur->removed.load() || (cur->next).load() != nxt
		  || nxt->is_locked()) return false;
	      nxt->removed = true;
	      // important to ensure removed flag is visible
	      nxt->clear_the_lock();
	      auto a = (nxt->next).load();
	      cur->next = a; // shortcut
	      node_pool.retire(nxt);
	      return true;
	    })) return true;
      }});
  }
  
  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	auto [prev, cur, nxt] = find_location(root, k);
	if (nxt->key == k) return nxt->value; 
	else return {};
      });
  }

  node* empty() {
    node* tail = node_pool.new_obj(key_max, 0, nullptr);
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
    if (p == nullptr) return;
    retire(p->next.load());
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
