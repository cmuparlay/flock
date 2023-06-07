// A growable concurrent unordered_map using a hash table
// Supports: insert, remove, find, size
// Each bucket points to a structure (Node) containing an array of entries
// Nodes come in varying sizes.
// On update the node is copied
// If the size of any bucket reaches some level, then the table grows by some factor
// Lock-free despite use of "lock" since only uses try_lock and never loops on a try_lock
// A delayed thread, however, can slow down the data structure when growing
// since it can delay resizing

// WARNING: NOT FULLY DEBUGGED YET

#include <atomic>
#include <optional>
#include <mutex>
#include "epoch.h"

struct lock {
private:
  static const int bucket_bits = 16;
  static const size_t mask = ((1ul) << bucket_bits) - 1;
  static std::vector<std::mutex> locks;
  static std::mutex* get_lock(long i) {
    return &locks[parlay::hash64_2(i) & mask];}
public:
  static bool try_lock(long i) {return get_lock(i)->try_lock();}
  static void unlock(long i) { get_lock(i)->unlock(); }
};

std::vector<std::mutex> lock::locks{1ul << bucket_bits};

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {
  static constexpr int exp_factor = 16;
  static constexpr int block_size = 64;
  static constexpr int overflow_size = 8;

  struct KV {K key; V value;};
  
  template <typename Range>
  static int find_key(const Range& entries, long cnt, const K& k) {
    for (int i=0; i < cnt; i++)
      if (KeyEqual{}(entries[i].key, k)) return i;
    return -1;
  }

  template <typename Range, typename RangeIn>
  static void insert(Range& out, const RangeIn& entries, long cnt, const K& k, const V& v) {
    for (int i=0; i < cnt; i++) out[i] = entries[i];
    out[cnt] = KV{k,v};
  }

  template <typename Range, typename RangeIn>
  static void update(Range& out, const RangeIn& entries, long cnt, const K& k, const V& v) {
    int i = 0;
    while (!KeyEqual{}(k, entries[i].key)) {
      assert(i < cnt);
      out[i] = entries[i];
      i++;
    }
    out[i].key = entries[i].key;
    out[i].value = v;
    i++;
    while (i < cnt) {
      out[i] = entries[i];
      i++;
    }
  }

  template <typename Range, typename RangeIn>
  static void remove(Range& out, const RangeIn& entries, long cnt, const K& k) {
    int i = 0;
    while (!KeyEqual{}(k, entries[i].key)) {
      assert(i < cnt);
      out[i] = entries[i];
      i++;
    }
    while (i < cnt-1) {
      out[i] = entries[i+1];
      i++;
    }
  }

  // what each slot in table points to
  template <int Size>
  struct Node {
    using node = Node<0>;
    int cnt;
    KV entries[Size];
    KV& get_entry(int i) {
      if (cnt <= 31) return entries[i];
      else return ((BigNode*) this)->entries[i];
    }
    
    int find(const K& k) {
      if (cnt <= 31) return find_key(entries, cnt, k);
      else return find_key(((BigNode*) this)->entries, cnt, k);
    }

    std::optional<V> find_value(const K& k) {
      if (cnt <= 31) { // regular node
	int i = find(k);
	if (i == -1) return {};
	else return entries[i].value;
      } else { // big node
	int i = find_key(((BigNode*) this)->entries, cnt, k);
	if (i == -1) return {};
	else return ((BigNode*) this)->entries[i].value;
      }
    }

    Node(node* old, const K& k, const V& v) {
      cnt = (old == nullptr) ? 1 : old->cnt + 1;
      insert(entries, old->entries, cnt-1, k, v);
    }

    Node(node* old, const K& k, const V& v, bool x) : cnt(old->cnt) {
      assert(old != nullptr);
      update(entries, old->entries, cnt, k, v);
    }

    Node(node* old, const K& k) : cnt(old->cnt - 1) {
      if (cnt == 31) remove(entries, ((BigNode*) old)->entries, cnt+1, k);
      else remove(entries, old->entries, cnt+1, k);
    }
  };
  using node = Node<0>;

  // If a node overflows (cnt > 31), then it becomes a big node and its content
  // is stored indirectly in a parlay sequences.
  struct BigNode {
    using entries_type = parlay::sequence<KV>;
    int cnt;
    entries_type entries;

    BigNode(node* old, const K& k, const V& v) : cnt(old->cnt + 1) {
      entries = entries_type(cnt);
      if (old->cnt == 31) insert(entries, old->entries, old->cnt, k, v);
      else insert(entries, ((BigNode*) old)->entries, old->cnt, k, v);
    }

    BigNode(node* old, const K& k, const V& v, bool x) : cnt(old->cnt) {
      entries = entries_type(cnt);
      update(entries, ((BigNode*) old)->entries, cnt, k, v);  }

    BigNode(node* old, const K& k) : cnt(old->cnt - 1) {
      entries = entries_type(cnt);
      remove(entries, ((BigNode*) old)->entries, cnt+1, k); }
  };

  using slot = std::atomic<node*>;
  
  struct prim_table {
    std::atomic<prim_table*> next;
    std::atomic<long> count;
    long bits;
    size_t size;
    parlay::sequence<slot> buckets;
    parlay::sequence<std::atomic<bool>> block_status;
    slot* get_slot(const K& k) {
      return &buckets[get_index(k)];
    }
    long get_index(const K& k) {
      return (Hash{}(k) >> (40 - bits))  & (size-1u);
    }
    prim_table(long n) : next(nullptr), count(0) {
      bits = std::max<long>(6, 1 + parlay::log2_up(n)); 
      size = 1ul << bits; // needs to be at least block_size
      buckets = parlay::sequence<slot>(size);
    }
    prim_table(prim_table* t)
      : next(nullptr), count(0), bits(t->bits + 4), size(t->size*exp_factor),
	buckets(parlay::sequence<std::atomic<node*>>::uninitialized(size)),
	block_status(parlay::tabulate<std::atomic<bool>>(t->size/block_size, [&] (long i) {return false;}, size)) {}
  };
  static flck::memory_pool<prim_table> prim_table_pool;
  prim_table* hash_table;
  unordered_map(size_t n) : hash_table(prim_table_pool.new_obj(n)) {}

  static node* tag_table(prim_table* x) {return (node*) (((size_t) x) | 1);}
  static prim_table* untag_table(node* x) {return (prim_table*) (((size_t) x) & ~1ul);}
  static bool is_tagged(node* x) {return ((size_t) x) & 1;}

  void expand_table() {
    prim_table* ht = hash_table;
    if (ht->next == nullptr) {
      long n = ht->buckets.size();
      prim_table* new_table = prim_table_pool.new_obj(ht);
      prim_table* old = nullptr;
      if (!ht->next.compare_exchange_strong(old, new_table))
	prim_table_pool.retire(new_table);
      else std::cout << "expand to: " << n * exp_factor << std::endl;
    }
  }

  void insert(prim_table* t, KV& key_value) {
    size_t idx = t->get_index(key_value.key);
    node* x = t->buckets[idx].load();
    //std::cout << idx << ", " << key_value.key << std::endl;
    if (is_tagged(x)) std::cout << "ouch: " << x << std::endl;
    assert(!is_tagged(x));
    t->buckets[idx] = insert_to_node(x , key_value.key, key_value.value);
  }

  void remove(prim_table* t, KV& key_value) {
    size_t idx = t->get_index(key_value.key);
    node* x = t->buckets[idx].load();
    if (is_tagged(x)) std::cout << "ouch2: " << x << std::endl;
    assert(!is_tagged(x));
    t->buckets[idx] = remove_from_node(x, key_value.key);
  }

  void copy_if_needed(long hashid) {
    prim_table* t = hash_table;
    prim_table* next = t->next.load();
    if (next != nullptr) {
      long block_num = hashid & (next->block_status.size() -1);
      //std::cout << block_num << " :: " << next->block_status.size() << std::endl;
      if (!next->block_status[block_num]) {
	if (!lock::try_lock(block_num)) {
	  while (!next->block_status[block_num]);
	  return;
	}
	else {
	  long start = block_num * block_size;
	  for (int i = start; i < start + block_size; i++) {
	    long exp_start = i * exp_factor;
	    for (int j = exp_start; j < exp_start + exp_factor; j++)
	      next->buckets[j] = nullptr;
	    while (true) {
	      node* bucket = t->buckets[i].load();
	      assert(!is_tagged(bucket));
	      int cnt = (bucket == nullptr) ? 0 : bucket->cnt;
	      // this will scatter unless use high end for hash bits
	      for (int j=0; j < cnt; j++) {
		if (i != t->get_index(bucket->get_entry(j).key))
		  std::cout << "In wrong bucket: in " << i << " belongs in " << t->get_index(bucket->get_entry(j).key) << ", " << cnt << std::endl;
		insert(next, bucket->get_entry(j));
	      }
	      bool succeeded = t->buckets[i].compare_exchange_strong(bucket,tag_table(next));
	      if (succeeded) break;
	      for (int j=0; j < cnt; j++)
		remove(next, bucket->get_entry(j));
	    } 
	  }
	  next->block_status[block_num] = true;
	  lock::unlock(block_num);
	  
	  if (++next->count == next->block_status.size()) {
	    std::cout << "end expansion" << std::endl;
	    hash_table = next;
	    //prim_table_pool.retire(t);
	  }
	}
      }
    }
  }

  using Node1 = Node<1>;
  using Node3 = Node<3>;
  using Node7 = Node<7>;
  using Node31 = Node<31>;
  static flck::memory_pool<Node1> node_pool_1;
  static flck::memory_pool<Node3> node_pool_3;
  static flck::memory_pool<Node7> node_pool_7;
  static flck::memory_pool<Node31> node_pool_31;
  static flck::memory_pool<BigNode> big_node_pool;

  node* insert_to_node(node* old, const K& k, const V& v) {
    if (old == nullptr) return (node*) node_pool_1.new_obj(old, k, v);
    if (old->cnt < 3) return (node*) node_pool_3.new_obj(old, k, v);
    if (old->cnt < 7) return (node*) node_pool_7.new_obj(old, k, v);
    if (old->cnt > overflow_size) expand_table();
    if (old->cnt < 31) return (node*) node_pool_31.new_obj(old, k, v);
    return (node*) big_node_pool.new_obj(old, k, v);
  }

  static node* update_node(node* old, const K& k, const V& v) {
    if (old == nullptr) return (node*) node_pool_1.new_obj(old, k, v, true);
    if (old->cnt < 3) return (node*) node_pool_3.new_obj(old, k, v, true);
    else if (old->cnt < 7) return (node*) node_pool_7.new_obj(old, k, v, true);
    else if (old->cnt < 31) return (node*) node_pool_31.new_obj(old, k, v, true);
    else return (node*) big_node_pool.new_obj(old, k, v, true);
  }

  static node* remove_from_node(node* old, const K& k) {
    if (old->cnt == 1) return (node*) nullptr;
    if (old->cnt == 2) return (node*) node_pool_1.new_obj(old, k);
    else if (old->cnt <= 4) return (node*) node_pool_3.new_obj(old, k);
    else if (old->cnt <= 8) return (node*) node_pool_7.new_obj(old, k);
    else if (old->cnt <= 32) return (node*) node_pool_31.new_obj(old, k);
    else return (node*) big_node_pool.new_obj(old, k);
  }

  static void retire_node(node* old) {
    return;  /// need to fix
    if (old == nullptr);
    else if (old->cnt == 1) node_pool_1.retire((Node1*) old);
    else if (old->cnt <= 3) node_pool_3.retire((Node3*) old);
    else if (old->cnt <= 7) node_pool_7.retire((Node7*) old);
    else if (old->cnt <= 31) node_pool_31.retire((Node31*) old);
    else big_node_pool.retire((BigNode*) old);
  }

  static void destruct_node(node* old) {
    if (old == nullptr);
    else if (old->cnt == 1) node_pool_1.destruct((Node1*) old);
    else if (old->cnt <= 3) node_pool_3.destruct((Node3*) old);
    else if (old->cnt <= 7) node_pool_7.destruct((Node7*) old);
    else if (old->cnt <= 31) node_pool_31.destruct((Node31*) old);
    else big_node_pool.destruct((BigNode*) old);
  }

  std::optional<V> find_at(prim_table* t, slot* s, const K& k) {
    node* x = s->load();
    if (is_tagged(x)) return find_internal(t->next,  k);
    if (x == nullptr) return std::optional<V>();
    KV& kv = x->get_entry(0);
    if (KeyEqual{}(kv.key, k)) return kv.value;
    return x->find_value(k);
  }

  std::optional<V> find_internal(prim_table* t, const K& k) {
    return find_at(t, t->get_slot(k), k);
  }
		   
  std::optional<V> find(const K& k) {
    slot* s = hash_table->get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {return find_at(hash_table, s, k);});
  }

  std::optional<bool> try_insert_at(prim_table* t, long i, const K& k, const V& v, bool upsert) {
    node* x = t->buckets[i].load();
    if (is_tagged(x)) {
      prim_table* nxt = t->next;
      return try_insert_at(nxt, nxt->get_index(k), k, v, upsert);
    }
    node* new_node;
    bool found = x != nullptr && x->find(k) != -1;
    if (found)
      if (upsert) new_node = update_node(x, k, v);
      else return false;
    else new_node = insert_to_node(x, k, v);
    if (t->buckets[i].compare_exchange_strong(x, new_node)) {
      retire_node(x);
      return true;
    } 
    destruct_node(new_node); 
    return {}; // failed
  }

  bool insert(const K& k, const V& v) {
    prim_table* ht = hash_table;
    long idx = ht->get_index(k);
    __builtin_prefetch (&hash_table[idx]);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {
	  copy_if_needed(idx);
          return try_insert_at(ht, idx, k, v, false);});});
  }

  bool upsert(const K& k, const V& v) {
    slot* s = hash_table->get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_insert_at(hash_table, s, k, v, true);});});
  }

  static std::optional<bool> try_remove_at(prim_table* t, long i, const K& k) {
    node* x = t->buckets[i].load();
    if (is_tagged(x)) {
      prim_table* nxt = t->next;
      return try_remove_at(nxt, nxt->get_index(k), k);
    }
    if (x == nullptr || x->find(k) == -1)
      return false;

    assert(x != nullptr && x->find(k) != -1);
    node* new_node = remove_from_node(x, k);
    if(t->buckets[i].compare_exchange_strong(x, new_node)) {
      retire_node(x);
      return true;
    } 
    destruct_node(new_node);
    return {};
  }

  bool remove(const K& k) {
    prim_table* ht = hash_table;
    long idx = ht->get_index(k);
    __builtin_prefetch (&hash_table[idx]);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {
          return try_remove_at(ht, idx, k);});});
  }

  ~unordered_map() {
    auto& table = hash_table->buckets;
    parlay::parallel_for (0, table.size(), [&] (size_t i) {
      retire_node(table[i].load());});
  }

  void check() {
    prim_table* ht = hash_table;
    for (int i = 0; i < ht->size; i++) {
      node* x = ht->buckets[i].load();
      if (is_tagged(x));
      else if (x == nullptr);
      else {
	for (int j=0; j < x->cnt; j++)
	  if (ht->get_index(x->get_entry(j).key) != i) {
	    std::cout << "mismatch: entry hash = " << ht->get_index(x->get_entry(j).key) << " bucket = " << i << " cnt = " << x->cnt << ", " << ht->size << ", " << x->get_entry(j).key << std::endl;
	    std::abort();
	  }
      }
    }
  }
    
      
  long size() {
    if (hash_table->next != nullptr)
      for (int i=0; i < hash_table->size; i++)
	copy_if_needed(i);
    auto& table = hash_table->buckets;
    auto s = parlay::tabulate(hash_table->size, [&] (size_t i) {
	      node* x = table[i].load();
	      if (x == nullptr) return 0;
	      else return x->cnt;});
    return parlay::reduce(s);
  }
};

template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::Node1> unordered_map<K,V,H,E>::node_pool_1;
template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::Node3> unordered_map<K,V,H,E>::node_pool_3;
template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::Node7> unordered_map<K,V,H,E>::node_pool_7;
template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::Node31> unordered_map<K,V,H,E>::node_pool_31;
template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::BigNode> unordered_map<K,V,H,E>::big_node_pool;
template <typename K, typename V, typename H, typename E>
flck::memory_pool<typename unordered_map<K,V,H,E>::prim_table> unordered_map<K,V,H,E>::prim_table_pool;
