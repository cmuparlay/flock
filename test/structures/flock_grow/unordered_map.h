// A extensible concurrent unordered_map using a hash table
// Supports: insert, remove, find, size
// Each bucket points to a structure (Node) containing an array of entries
// Nodes come in varying sizes.
// On update the node is copied
// If the size of any bucket reaches some level, then the table grows by some factor

#include <atomic>
#include <optional>
#include "epoch.h"

template <typename K,
	  typename V,
	  class Hash = std::hash<K>,
	  class KeyEqual = std::equal_to<K>>
struct unordered_map {

private:
  static constexpr int log_exp_factor = 4;
  static constexpr int exp_factor = 1 << log_exp_factor;
  static constexpr int block_size = 64;
  static constexpr int overflow_size = 8;

  struct KV {K key; V value;};
  
  template <typename Range>
  static int find_in_range(const Range& entries, long cnt, const K& k) {
    for (int i=0; i < cnt; i++)
      if (KeyEqual{}(entries[i].key, k)) return i;
    return -1;
  }

  // The following three functions copy a range and
  // insert/update/remove the specified key.  No ordering is assumed
  // within the range.  Insert assumes k does not appear, while
  // update/remove assume it does appear.
  template <typename Range, typename RangeIn>
  static void copy_and_insert(Range& out, const RangeIn& entries, long cnt,
			      const K& k, const V& v) {
    for (int i=0; i < cnt; i++) out[i] = entries[i];
    out[cnt] = KV{k,v};
  }

  template <typename Range, typename RangeIn>
  static void copy_and_update(Range& out, const RangeIn& entries, long cnt,
			      const K& k, const V& v) {
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
  static void copy_and_remove(Range& out, const RangeIn& entries, long cnt, const K& k) {
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

  // Each bucket points to a Node of some Size, or to a BigNode (defined below)
  // A node contains an array of up to Size entries (actual # of entries given by cnt)
  // Sizes are 1, 3, 7, 31
  template <int Size>
  struct Node {
    using node = Node<0>;
    int cnt;
    KV entries[Size];
    KV& get_entry(int i) {
      if (cnt <= 31) return entries[i];
      // if bigger than 31, then it is a BigNode
      else return ((BigNode*) this)->entries[i];
    }

    // return index of key in entries, or -1 if not found
    int find_index(const K& k) {
      if (cnt <= 31) return find_in_range(entries, cnt, k);
      else return find_in_range(((BigNode*) this)->entries, cnt, k);
    }

    // return optional value found in entries given a key
    std::optional<V> find(const K& k) {
      if (cnt <= 31) { // regular node
	int i = find_index(k);
	if (i == -1) return {};
	else return entries[i].value;
      } else { // big node
	int i = find_in_range(((BigNode*) this)->entries, cnt, k);
	if (i == -1) return {};
	else return ((BigNode*) this)->entries[i].value;
      }
    }

    // copy insert
    Node(node* old, const K& k, const V& v) {
      cnt = (old == nullptr) ? 1 : old->cnt + 1;
      copy_and_insert(entries, old->entries, cnt-1, k, v);
    }

    // copy update
    Node(node* old, const K& k, const V& v, bool x) : cnt(old->cnt) {
      assert(old != nullptr);
      copy_and_update(entries, old->entries, cnt, k, v);
    }

    // copy remove
    Node(node* old, const K& k) : cnt(old->cnt - 1) {
      // if cnt==31 then needs to be copied from a BigNode into this regular node
      if (cnt == 31) copy_and_remove(entries, ((BigNode*) old)->entries, cnt+1, k);
      else copy_and_remove(entries, old->entries, cnt+1, k);
    }
  };
  using node = Node<0>;

  // If a node overflows (cnt > 31), then it becomes a big node and its content
  // is stored indirectly in a parlay sequences.
  // This should rarely, if ever, happen.
  struct BigNode {
    using entries_type = parlay::sequence<KV>;
    int cnt;
    entries_type entries;

    // copy insert
    BigNode(node* old, const K& k, const V& v) : cnt(old->cnt + 1) {
      entries = entries_type(cnt);
      // if old->cnt == 31 then need to copy from regular node into this BigNode
      if (old->cnt == 31) copy_and_insert(entries, old->entries, old->cnt, k, v);
      else copy_and_insert(entries, ((BigNode*) old)->entries, old->cnt, k, v);
    }

    // copy update
    BigNode(node* old, const K& k, const V& v, bool x) : cnt(old->cnt) {
      entries = entries_type(cnt);
      copy_and_update(entries, ((BigNode*) old)->entries, cnt, k, v);  }

    // copy remvoe
    BigNode(node* old, const K& k) : cnt(old->cnt - 1) {
      entries = entries_type(cnt);
      copy_and_remove(entries, ((BigNode*) old)->entries, cnt+1, k); }
  };

  // one bucket of a sequence of buckets
  using bucket = std::atomic<node*>;

  // status of a block
  enum status : char {Empty, Working, Done};

  // a single version of the table
  // this can change as the table grows
  struct table_version {
    std::atomic<table_version*> next; // points to next version if created
    std::atomic<long> finished_block_count; //number of blocks finished copying
    long bits;  // log_2 of size
    size_t size; // number of buckets
    parlay::sequence<bucket> buckets; // sequence of buckets
    parlay::sequence<std::atomic<status>> block_status; // status of each block while copying

    long get_index(const K& k) {
      return (Hash{}(k) >> (40 - bits))  & (size-1u);}

    bucket* get_bucket(const K& k) {
      return &buckets[get_index(k)]; }

    // initial table version, n indicating initial size
    table_version(long n)
      : next(nullptr),
	finished_block_count(0),
	bits(1 + parlay::log2_up(std::max<long>(block_size, n))),
	size(1ul << bits), 
	buckets(parlay::sequence<bucket>(size)) {}

    // expanded table versions copied from smaller version t
    table_version(table_version* t)
      : next(nullptr),
	finished_block_count(0),
	bits(t->bits + log_exp_factor),
	size(t->size * exp_factor),
	buckets(parlay::sequence<std::atomic<node*>>::uninitialized(size)),
	block_status(parlay::sequence<std::atomic<status>>(t->size/block_size)) {
      std::fill(block_status.begin(), block_status.end(), Empty);
    }
  };

  // the current table version
  // points to next larger table version if one exists
  std::atomic<table_version*> current_table_version;

  // memory pool for maintaining the table versions
  // allocated dynamically although at most log(final_size) of them 
  static flck::memory_pool<table_version> table_version_pool;

  // A forwarded node indicates that entries should be accessed in the
  // next larger table.  Uses a pointer of value 1.
  static node* forwarded_node() {return (node*) 1;}
  static bool is_forwarded(node* x) {return x == (node*) 1;}

  // called when table should be expanded (i.e. when some bucket is too large)
  // allocates a new table version and links the old one to it
  void expand_table() {
    table_version* ht = current_table_version.load();
    if (ht->next == nullptr) {
      long n = ht->buckets.size();
      table_version* new_table = table_version_pool.new_obj(ht);
      table_version* old = nullptr;
      if (!ht->next.compare_exchange_strong(old, new_table))
	table_version_pool.retire(new_table);
      //else std::cout << "expand to: " << n * exp_factor << std::endl;
    }
  }

  // copies key_value into a new table
  // note this is not thread safe...i.e. only this thread should be
  // updating the bucket corresponding to the key.
  void copy(table_version* t, KV& key_value) {
    size_t idx = t->get_index(key_value.key);
    node* x = t->buckets[idx].load();
    assert(!is_forwarded(x));
    t->buckets[idx] = insert_to_node(x , key_value.key, key_value.value);
    destruct_node(x);
  }

  // If copying is enabled (i.e. next is not null), and if the the hash bucket
  // given by hashid is not already copied, tries to copy block_size buckets, including
  // that of hashid, to the next larger hash_table.
  void copy_if_needed(long hashid) {
    table_version* t = current_table_version.load();
    table_version* next = t->next.load();
    if (next != nullptr) {
      long block_num = hashid & (next->block_status.size() -1);
      status st = next->block_status[block_num];
      status old = Empty;
      if (st == Done) return;
      else if (st == Empty &&
	       next->block_status[block_num].compare_exchange_strong(old, Working)) {
	long start = block_num * block_size;
	// copy block_size buckets
	for (int i = start; i < start + block_size; i++) {
	  long exp_start = i * exp_factor;
	  // Clear exp_factor buckets in the next table to put them in.
	  for (int j = exp_start; j < exp_start + exp_factor; j++)
	    next->buckets[j] = nullptr;
	  // copy bucket to exp_factor new buckets in next table
	  while (true) {
	    node* bucket = t->buckets[i].load();
	    assert(!is_forwarded(bucket));
	    int cnt = (bucket == nullptr) ? 0 : bucket->cnt;
	    // copy each element
	    for (int j=0; j < cnt; j++) 
	      copy(next, bucket->get_entry(j));
	    bool succeeded = t->buckets[i].compare_exchange_strong(bucket,forwarded_node());
	    if (succeeded) {
	      retire_node(bucket);
	      break;
	    }
	    // If the cas failed then someone updated bucket in the meantime so need to retry.
	    // Before retrying need to clear out already added buckets..
	    for (int j = exp_start; j < exp_start + exp_factor; j++) {
	      auto x = next->buckets[j].load();
	      next->buckets[j] = nullptr;
	      destruct_node(x);
	    }
	  }
	  assert(next->next.load() == nullptr);
	}
	assert(next->block_status[block_num] == Working);
	next->block_status[block_num] = Done;
	// if all blocks have been copied then can set hash_table to next
	// and retire the old table
	if (++next->finished_block_count == next->block_status.size()) {
	  current_table_version = next;
	  table_version_pool.retire(t);
	}
      } else {
	// If working then wait until Done
	while (next->block_status[block_num] == Working) {
	  for (volatile int i=0; i < 100; i++);
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

  std::optional<V> find_at(table_version* t, bucket* s, const K& k) {
    node* x = s->load();
    if (is_forwarded(x)) {
      table_version* nxt = t->next.load();
      find_at(nxt, nxt->get_bucket(k), k);
    }
    if (x == nullptr) return std::optional<V>();
    //KV& kv = x->get_entry(0);
    //if (KeyEqual{}(kv.key, k)) return kv.value;
    return x->find(k);
  }
		   
  std::optional<bool> try_insert_at(table_version* t, bucket* s, const K& k, const V& v, bool upsert) {
    node* x = s->load();
    if (is_forwarded(x)) {
      table_version* nxt = t->next.load();
      return try_insert_at(nxt, nxt->get_bucket(k), k, v, upsert);
    }
    node* new_node;
    bool found = x != nullptr && x->find_index(k) != -1;
    if (found)
      if (upsert) new_node = update_node(x, k, v);
      else return false;
    else new_node = insert_to_node(x, k, v);
    if (s->compare_exchange_strong(x, new_node)) {
      retire_node(x);
      return true;
    } 
    destruct_node(new_node); 
    return {}; // failed
  }
  
  static std::optional<bool> try_remove_at(table_version* t, bucket* s, const K& k) {
    node* x = s->load();
    if (is_forwarded(x)) {
      table_version* nxt = t->next;
      return try_remove_at(nxt, nxt->get_bucket(k), k);
    }
    if (x == nullptr || x->find_index(k) == -1)
      return false;

    assert(x != nullptr && x->find_index(k) != -1);
    node* new_node = remove_from_node(x, k);
    if(s->compare_exchange_strong(x, new_node)) {
      retire_node(x);
      return true;
    } 
    destruct_node(new_node);
    return {};
  }

public:

  unordered_map(size_t n) : current_table_version(table_version_pool.new_obj(n)) {}

  ~unordered_map() {
    auto& buckets = current_table_version.load()->buckets;
    parlay::parallel_for (0, buckets.size(), [&] (size_t i) {
      retire_node(buckets[i].load());});
    table_version_pool.retire(current_table_version.load());
  }

  std::optional<V> find(const K& k) {
    table_version* ht = current_table_version.load();
    bucket* s = ht->get_bucket(k);
    __builtin_prefetch (s);
    return flck::with_epoch([=] {return find_at(ht, s, k);});
  }

  bool insert(const K& k, const V& v) {
    table_version* ht = current_table_version.load();
    long idx = ht->get_index(k);
    bucket* s = &ht->buckets[idx];
    __builtin_prefetch (s);
    return flck::with_epoch([=] {
      return flck::try_loop([=] {
	  copy_if_needed(idx);
          return try_insert_at(ht, s, k, v, false);});});
  }

  bool upsert(const K& k, const V& v) {
    table_version* ht = current_table_version.load();
    long idx = ht->get_index(k);
    bucket* s = &ht->buckets[idx];
    __builtin_prefetch (s);
    return flck::with_epoch([=] {
      return flck::try_loop([=] {
	  copy_if_needed(idx);
          return try_insert_at(ht, s, k, v, true);});});
  }

  bool remove(const K& k) {
    table_version* ht = current_table_version.load();
    bucket* s = ht->get_bucket(k);
    __builtin_prefetch (s);
    return flck::with_epoch([=] {
      return flck::try_loop([=] {
          return try_remove_at(ht, s, k);});});
  }

  long size() {
    table_version* ht = current_table_version.load();
    if (ht->next != nullptr)
      for (int i=0; i < ht->size; i++)
	copy_if_needed(i);
    auto& table = ht->buckets;
    auto s = parlay::tabulate(ht->size, [&] (size_t i) {
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
flck::memory_pool<typename unordered_map<K,V,H,E>::table_version> unordered_map<K,V,H,E>::table_version_pool;
