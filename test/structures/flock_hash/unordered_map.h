// MIT license (https://opensource.org/license/mit/)
// Initial Author: Guy Blelloch

// A lock free concurrent unordered_map using a hash table
// Supports: fast atomic insert, upsert, remove, and  find
// along with a non-atomic, and slow, size
// Each bucket points to a structure (Node) containing an array of entries
// Nodes come in varying sizes and on update the node is copied.
// Allows arbitrary growing, but only efficient if not much larger
// than the original given size (i.e. number of buckets is fixes, but
// number of entries per bucket can grow).

#include <atomic>
#include <optional>
#include "epoch.h"
#include "lock.h"
//#define USE_CAS 1

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {
private:
  struct KV {K key; V value;};

  template <typename Range>
  static int find_key(const Range& entries, long cnt, const K& k) {
    for (int i=0; i < cnt; i++)
      if (KeyEqual{}(entries[i].key, k)) return i;
    return -1;
  }

  // copy entries and insert k,v at end
  template <typename Range, typename RangeIn>
  static void insert(Range& out, const RangeIn& entries, long cnt, const K& k, const V& v) {
    for (int i=0; i < cnt; i++) out[i] = entries[i];
    out[cnt] = KV{k,v};
  }

  // copy entries and update entry with key k to have value v
  template <typename Range, typename RangeIn, typename F>
  static void update(Range& out, const RangeIn& entries, long cnt, const K& k, const F& f) {
    int i = 0;
    while (!KeyEqual{}(k, entries[i].key) && i < cnt) {
      assert(i < cnt);
      out[i] = entries[i];
      i++;
    }
    out[i].key = entries[i].key;
    out[i].value = f(entries[i].value);
    i++;
    while (i < cnt) {
      out[i] = entries[i];
      i++;
    }
  }

  // copy entries and remove entry with key k
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

  // what each slot in table points to (if not bignode)
  template <int Size>
  struct Node {
    using node = Node<0>;
    int cnt;
    KV entries[Size];
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

    template <typename F>
    Node(node* old, const K& k, const F& f) : cnt(old->cnt) {
      assert(old != nullptr);
      update(entries, old->entries, cnt, k, f);
    }

    Node(node* old, const K& k) : cnt(old->cnt - 1) {
      if (cnt == 31) remove(entries, ((BigNode*) old)->entries, cnt+1, k);
      else remove(entries, old->entries, cnt+1, k);
    }
  };
  using node = Node<0>;

  // If a node overflows (cnt > 31), then it becomes a big node and its content
  // is stored indirectly in a parlay sequence.
  struct BigNode {
    using entries_type = parlay::sequence<KV>;
    int cnt;
    entries_type entries;

    BigNode(node* old, const K& k, const V& v) : cnt(old->cnt + 1) {
      entries = entries_type(cnt);
      if (old->cnt == 31) insert(entries, old->entries, old->cnt, k, v);
      else insert(entries, ((BigNode*) old)->entries, old->cnt, k, v);
    }

        template <typename F>
    BigNode(node* old, const K& k, const F& f) : cnt(old->cnt) {
      entries = entries_type(cnt);
      update(entries, ((BigNode*) old)->entries, cnt, k, f);  }

    BigNode(node* old, const K& k) : cnt(old->cnt - 1) {
      entries = entries_type(cnt);
      remove(entries, ((BigNode*) old)->entries, cnt+1, k); }
  };

  using slot = std::atomic<node*>;

  struct Table {
    parlay::sequence<slot> table;
    size_t size;
    slot* get_slot(const K& k) {
      size_t idx = Hash{}(k)  & (size-1u);
      return &table[idx];
    }
    Table(size_t n) {
      int bits = 1 + parlay::log2_up(n);
      size = 1ul << bits;
      table = parlay::sequence<slot>(size);
    }
  };

  Table hash_table;

  using Node1 = Node<1>;
  using Node3 = Node<3>;
  using Node7 = Node<7>;
  using Node31 = Node<31>;
  static flck::memory_pool<Node1> node_pool_1;
  static flck::memory_pool<Node3> node_pool_3;
  static flck::memory_pool<Node7> node_pool_7;
  static flck::memory_pool<Node31> node_pool_31;
  static flck::memory_pool<BigNode> big_node_pool;

  static node* insert_to_node(node* old, const K& k, const V& v) {
    if (old == nullptr) return (node*) node_pool_1.new_obj(old, k, v);
    if (old->cnt < 3) return (node*) node_pool_3.new_obj(old, k, v);
    else if (old->cnt < 7) return (node*) node_pool_7.new_obj(old, k, v);
    else if (old->cnt < 31) return (node*) node_pool_31.new_obj(old, k, v);
    else return (node*) big_node_pool.new_obj(old, k, v);
  }

  template <typename F>
  static node* update_node(node* old, const K& k, const F& f) {
    if (old == nullptr) return (node*) node_pool_1.new_obj(old, k, f);
    if (old->cnt < 3) return (node*) node_pool_3.new_obj(old, k, f);
    else if (old->cnt < 7) return (node*) node_pool_7.new_obj(old, k, f);
    else if (old->cnt < 31) return (node*) node_pool_31.new_obj(old, k, f);
    else return (node*) big_node_pool.new_obj(old, k, f);
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

  // try to install a new node in slot s
  static std::optional<bool> try_update(slot* s, node* old_node, node* new_node, bool ret_val) {
#ifdef USE_CAS
    if (s->load() == old_node &&
	s->compare_exchange_strong(old_node, new_node)) {
#else  // use try_lock
    if (locks.try_lock((long) s, [=] {
	    if (s->load() != old_node) return false;
	    *s = new_node;
	    return true;})) {
#endif
      retire_node(old_node);
      return ret_val;
    } 
    destruct_node(new_node);
    return {};
  }

  static std::optional<bool> try_insert_at(slot* s, const K& k, const V& v) {
    node* old_node = s->load();
    if (old_node != nullptr && old_node->find(k) != -1) return false;
    return try_update(s, old_node, insert_to_node(old_node, k, v), true);
  }

  template <typename F>
  static std::optional<bool> try_upsert_at(slot* s, const K& k, F& f) {
    node* old_node = s->load();
    bool found = old_node != nullptr && old_node->find(k) != -1;
    if (!found)
      try_update(s, old_node, insert_to_node(old_node, k, f(std::optional<V>())), true);
    else
#ifdef USE_CAS
    return try_update(s, old_node, update_node(old_node, k, f), false);
#else  // use try_lock
    if (locks.try_lock((long) s, [=] {
        if (s->load() != old_node) return false;
	*s = update_node(old_node, k, f); // f applied within lock
	return true;})) {
      retire_node(old_node);
      return false;
    } else return {};
#endif
  }

  static std::optional<bool> try_remove_at(slot* s, const K& k) {
      node* old_node = s->load();
      if (old_node == nullptr || old_node->find(k) == -1) return false;
      return try_update(s, old_node, remove_from_node(old_node, k), true);
  }

public:
  unordered_map(size_t n) : hash_table(Table(n)) {}
  ~unordered_map() {
    auto& table = hash_table.table;
    parlay::parallel_for (0, table.size(), [&] (size_t i) {
      retire_node(table[i].load());});
  }
  
  std::optional<V> find(const K& k) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      node* x = s->load();
      if (x == nullptr) return std::optional<V>();
      return std::optional<V>(x->find_value(k));
    });
  }

  bool insert(const K& k, const V& v) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_insert_at(s, k, v);});});
  }

  template <typename F>
  bool upsert(const K& k, const F& f) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_update_at(s, k, f);});});
  }

  bool remove(const K& k) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_remove_at(s, k);});});
  }

  long size() {
    auto& table = hash_table.table;
    auto s = parlay::tabulate(table.size(), [&] (size_t i) {
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
