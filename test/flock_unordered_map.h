// A concurrent unordered_map using a hash table
// Supports: insert, remove, find, size
// Each bucket points to a structure (Node) containing an array of entries
// Nodes come in varying sizes.
// On update the node is copied

#include <atomic>
#include <optional>
#include "epoch.h"

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

struct slot {
    std::atomic<node*> ptr;
    slot() : ptr(nullptr) {}
  };

  struct Table {
    parlay::sequence<slot> table;
    slot* get_slot(const K& k) {
      size_t idx = Hash{}(k)  & (table.size()-1u);
      return &table[idx];
    }
    Table(size_t n) {
      size_t size = (1ul << parlay::log2_up(n));
      table = parlay::sequence<slot>(2*size);
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

  static std::optional<V> find_at(node* x, const K& k) {
    if (KeyEqual{}(x->entries[0].key, k))
      return x->entries[0].value;
    return x->find_value(k);
  }

  static bool cas(std::atomic<node*>& src, node* old, node* newv) {
    return (src.load() == old) && src.compare_exchange_strong(old, newv);
  }
		  
  static std::optional<bool> try_insert_at(slot* s, const K& k, const V& v, bool upsert) {
    node* x = s->ptr.load();
    node* new_node;
    bool found = x != nullptr && x->find(k) != -1;
    if (found)
      if (upsert) new_node = update_node(x, k, v);
      else return false;
    else new_node = insert_to_node(x, k, v);
    //if (s->ptr.compare_exchange_strong(x, new_node)) {
    if (cas(s->ptr, x, new_node)) {
      retire_node(x);
      return true;
    } 
    destruct_node(new_node); 
    return {}; // failed
  }

  static std::optional<bool> try_remove_at(slot* s, const K& k) {
      node* x = s->ptr.load();
      if (x == nullptr || x->find(k) == -1)
        return false;

      node* new_node = remove_from_node(x, k);
      //if(s->ptr.compare_exchange_strong(x, new_node)) {
      if (cas(s->ptr, x, new_node)) {
        retire_node(x);
        return true;
      } 
      destruct_node(new_node);
      return {};
  }

public:
  unordered_map(size_t n) : hash_table(Table(n)) {}
  ~unordered_map() {
    auto& table = hash_table.table;
    parlay::parallel_for (0, table.size(), [&] (size_t i) {
      retire_node(table[i].ptr.load());});
  }
  
  std::optional<V> find(const K& k) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      node* x = s->ptr.load();
      if (x == nullptr) return std::optional<V>();
      return find_at(x, k);});
  }

  bool insert(const K& k, const V& v) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_insert_at(s, k, v, false);});});
  }

  bool upsert(const K& k, const V& v) {
    slot* s = hash_table.get_slot(k);
    __builtin_prefetch (s);
    return flck::with_epoch([&] {
      return flck::try_loop([&] {return try_insert_at(s, k, v, true);});});
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
	      node* x = table[i].ptr.load();
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
