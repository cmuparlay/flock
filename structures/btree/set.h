#define Recorded_Once 1
#define Range_Search 1
#include <flock/flock.h>
#include <parlay/primitives.h>

// A top-down implementation of abtrees
// Nodes are split or joined on the way down to ensure that each node
// can fit one more child, or remove one more child.

template <typename K_, typename V_>
struct Set {
  using K = K_;
  using V = V_;
  
  struct KV {K key; V value;};

  // for 8 byte keys and 8 byte values
  // it should be a length such that l mod 4 = 3
  // this is so it fills a cache line
  static constexpr int leaf_block_size = 15;
  static constexpr int leaf_min_size = 3;
  static constexpr int leaf_join_cutoff = 12;
  static constexpr int node_block_size = 15;
  static constexpr int node_min_size = 3;
  static constexpr int node_join_cutoff = 12;
    
  // ***************************************
  // Internal Nodes
  // ***************************************

  struct leaf;
  enum Status : char { isOver, isUnder, OK};

  struct header : ll_head {
    bool is_leaf;
    Status status;
    char size;
    header(bool is_leaf, Status status, int size)
      : is_leaf(is_leaf), status(status), size((char) size) {}
  };

  // Internal Node
  // Has node_block_size children, and one fewer keys to divide them
  // The only mutable fields are the child pointers (children) and the
  // removed flag, which is only written to once when the node is
  // being removed.
  // A node needs to be copied to add, remove, or rebalance children.
  struct alignas(64) node : header {
    write_once<bool> removed;
    K keys[node_block_size-1];
    ptr_type<node> children[node_block_size];
    lock_type lck;
    
    int find(K k, uint i=0) {
      while (i < header::size-1 && keys[i] <= k) i++;
      return i;
    }

    // create with given size, to be filled in
    node(int size) :
      header{false,
	(size == node_min_size
	 ? isUnder
	 : (size == node_block_size ? isOver : OK)),
	size},
      removed(false) {}

    // create with a single child (just the pointer to the root)
    node(leaf* l) : header{false, OK, 1}, removed(false) {
      children[0].init((node*) l);
    }

    // create with two children separated by a key (a new root node)
    node(std::tuple<node*,K,node*> nec) : header{false, OK, 2}, removed(false) {
	auto [left,k,right] = nec;
	keys[0] = k;
	children[0].init(left);
	children[1].init(right);
      }

  };

  memory_pool<node> node_pool;

  // helper function to copy into a new node
  template <typename Key, typename Child>
  node* copy(int size, Key get_key, Child get_child) {
    return node_pool.new_init([=] (node* new_l) {
	for (int i = 0; i < size; i++) new_l->children[i].init(get_child(i));
        for (int i = 0; i < size-1; i++) new_l->keys[i] = get_key(i);
      }, size);
  }

  node* copy_node(node* p) {
    node* new_r = copy(p->size,
		       [=] (int i) {return p->keys[i];},
		       [=] (int i) {return p->children[i].read();});
    p->removed = true;
    node_pool.retire(p);
    return new_r;
  }
    
  // helper function for split and rebalance
  template <typename Key, typename Child>
  std::tuple<node*,K,node*> split_mid(int size, Key get_key, Child get_child) {
    // copy into new left child
    int lsize = size/2;
    node* new_l = copy(lsize, get_key, get_child);

    // copy into new right child
    node* new_r = copy(size - lsize,
		       [&] (int i) {return get_key(i + lsize);},
		       [&] (int i) {return get_child(i + lsize);});

    // the dividing key
    K mid = get_key(lsize-1);
    return std::make_tuple(new_l, mid, new_r);
  }

  // split an internal node into two nodes returning the new nodes
  // and the key that divides the children
  std::tuple<node*,K,node*> split(node* p) {
    assert(p->size == node_block_size);
    auto result = split_mid(p->size,
			    [=] (int i) {return p->keys[i];},
			    [=] (int i) {return p->children[i].read();});
    return result;
  }

  // rebalances two nodes into two new nodes
  std::tuple<node*,K,node*> rebalance(node* c1, K k, node* c2) {
    auto get_key = [=] (int i) {
		     if (i < c1->size-1) return c1->keys[i];
		     else if (i == c1->size-1) return k;
		     else return c2->keys[i-c1->size];};
    auto get_child = [=] (int i) {
		       if (i < c1->size) return c1->children[i].read();
		       else return c2->children[i-c1->size].read();};
    auto result = split_mid(c1->size + c2->size, get_key, get_child);
    return result;
  }

  // joins two nodes into one
  node* join(node* c1, K k, node* c2) {
    int size = c1->size + c2->size;
    auto get_key = [=] (int i) {
		     if (i < c1->size-1) return c1->keys[i];
		     else if (i == c1->size-1) return k;
		     else return c2->keys[i-c1->size];};
    auto get_child = [=] (int i) {
		       if (i < c1->size) return c1->children[i].read();
		       else return c2->children[i-c1->size].read();};
    node* new_p = copy(size, get_key, get_child); 
    return new_p;
  }

  // Update an internal node given a child that is split into two
  node* add_child(node* p, std::tuple<node*,K,node*> newc, int pos) {
    int size = p->size;
    assert(size < node_block_size);
    auto [c1,k,c2] = newc;
    auto get_key = [=] (int i) {
		     if (i < pos) return p->keys[i];
		     else if (i == pos) return k;
		     else return p->keys[i-1];};
    auto get_child = [=] (int i) {
		       if (i < pos) return p->children[i].read();
		       else if (i == pos) return c1;
		       else if (i == pos+1) return c2;
		       else return p->children[i-1].read();};
    node* new_p = copy(size+1, get_key, get_child);
    p->removed = true;
    return new_p;
  }

  // Update an internal node replacing two adjacent children
  // with a new child
  node* join_children(node* p, node* c, int pos) {
    int size = p->size;
    auto get_key = [=] (int i) {
		     if (i < pos) return p->keys[i];
		     else return p->keys[i+1];};
    auto get_child = [=] (int i) {
		       if (i < pos) return p->children[i].read();
		       else if (i == pos) return c;
		       else return p->children[i+1].read();};
    node* new_p = copy(size-1, get_key, get_child);
    p->removed = true;
    return new_p;
  }

  // Update an internal node replacing two adjacent children
  // with two newly rebalanced children
  node* rebalance_children(node* p, std::tuple<node*,K,node*> newc, int pos) {
    int size = p->size;
    auto [c1,k,c2] = newc;
    auto get_key = [=] (int i) {
		     if (i == pos) return k;
		     else return p->keys[i];};
    auto get_child = [=] (int i) {
		       if (i == pos) return c1;
		       else if (i == pos+1) return c2;
		       else return p->children[i].read();};
    node* new_p = copy(size, get_key, get_child);
    p->removed = true;
    return new_p;
  }

  // ***************************************
  // Leafs
  // ***************************************

  // Leafs are immutable.  Once created and the keyvals set, their values
  // will not be changed.
  struct alignas(64) leaf : header {
    KV keyvals[leaf_block_size];
    
    std::optional<V> find(K k) {
      int i=0;
      while (i < header::size && keyvals[i].key < k) i++;
      if (i == header::size || keyvals[i].key != k) return {};
      else return keyvals[i].value;
    }

    int prev(K k, int i=0) {
      while (i < header::size && keyvals[i].key < k) i++;
      return i;
    }
    
    leaf(int size) :
      header{true,
	size == leaf_min_size ? isUnder : (size == leaf_block_size ? isOver : OK),
	size} {};
  };

  memory_pool<leaf> leaf_pool;

  leaf* copy_leaf(leaf* l) {
    int size = l->size;
    leaf* new_l = leaf_pool.new_obj(size);
    for (int i=0; i < size; i++)
      new_l->keyvals[i] = l->keyvals[i];
    leaf_pool.retire(l);
    return new_l;
  }

  // Insert a new key-value pair by copying into a new leaf.
  // Old is left intact (but retired).
  // The key k must not already be present in the leaf.
  leaf* insert_leaf(leaf* l, K k, V v) {
    int size = l->size;
    assert(size < leaf_block_size);
    leaf* new_l = leaf_pool.new_obj(size+1);
    int i=0;

    // copy part before the new key
    for (;i < size && l->keyvals[i].key < k; i++)
      new_l->keyvals[i] = l->keyvals[i];

    // copy in the new key and value
    new_l->keyvals[i] = KV{k,v};

    // copy the part after the new key
    for (; i < size ; i++ )
      new_l->keyvals[i+1] = l->keyvals[i];

    return new_l;
  }

  // Remove a key-value pair from the leaf that matches the key k.
  // This copies the values into a new leaf.
  leaf* remove_leaf(leaf* l, K k) {
    int size = l->size;
    assert(size > 0);
    leaf* new_l = leaf_pool.new_obj(size-1);
    int i=0;

    // part before the key
    for (;i < size && l->keyvals[i].key < k; i++)
      new_l->keyvals[i] = l->keyvals[i];

    // part after the key, shifted left
    for (; i < size-1 ; i++ )
      new_l->keyvals[i] = l->keyvals[i+1];

    return new_l;
  }

  // helper function for split_leaf and rebalance_leaf
  template <typename KeyVal>
  std::tuple<node*,K,node*> split_mid_leaf(int size, KeyVal get_kv) {

    // create left child and copy into it
    int lsize = size/2;
    leaf* new_l = leaf_pool.new_obj(lsize);
    leaf* new_r = leaf_pool.new_obj(size-lsize);
    // hack to deal with broken g++-10 compiler
    if (true) {
      K tmpK[20];
      V tmpV[20];

      for (int i = 0; i < lsize; i++) {
        auto [key,val] = get_kv(i);
        tmpK[i] = key;
        tmpV[i] = val;
      }
      for (int i = 0; i < lsize; i++) {
        new_l->keyvals[i].key = tmpK[i];
        new_l->keyvals[i].value = tmpV[i];
      }
      for (int i = 0; i < size - lsize; i++) {
        auto [key,val] = get_kv(i+lsize);      
        tmpK[i] = key;
        tmpV[i] = val;
      }
      for (int i = 0; i < size - lsize; i++) {
        new_r->keyvals[i].key = tmpK[i];
        new_r->keyvals[i].value = tmpV[i];
      }
    } else {
      for (int i = 0; i < lsize; i++) new_l->keyvals[i] = get_kv(i);
      for (int i = 0; i < size - lsize; i++) new_r->keyvals[i] = get_kv(i+lsize);
    }

    // separating key (first key in right child)
    K mid = get_kv(lsize).key;
    return std::make_tuple((node*) new_l, mid, (node*) new_r);
  }

  // split a leaf into two leaves returning the new nodes
  // and the first key of the right leaf
  std::tuple<node*,K,node*> split_leaf(node* p) {
    leaf* l = (leaf*) p;
    int size = l->size;
    assert(size == leaf_block_size);
    auto result = split_mid_leaf(size, [=] (int i) {return l->keyvals[i];});
    return result;
  }

  std::tuple<node*,K,node*> rebalance_leaf(node* l, node* r) {
    int size = l->size + r->size;
    auto result = split_mid_leaf(size, [=] (int i) {
         if (i < l->size) return ((leaf*) l)->keyvals[i];
	 else return ((leaf*) r)->keyvals[i - l->size];});
    return result;
  }

  node* join_leaf(node* l, node* r) {
    int size = l->size + r->size;
    leaf* new_l = leaf_pool.new_obj(size);
    for (int i=0; i < size; i++)
      new_l->keyvals[i] = ((i < l->size) 
			   ? ((leaf*) l)->keyvals[i] 
			   : ((leaf*) r)->keyvals[i - l->size]);
    return (node*) new_l;
  }

  // ***************************************
  // Tree code
  // ***************************************

  // Splits an overfull node (i.e. one with block_size entries)
  // for a grandparent gp, parent p, and child c:
  // copies the parent p to replace with new children and
  // updates the grandparent gp to point to the new copied parent.
  void overfull_node(node* gp, int pidx, node* p, int cidx, node* c) {
    gp->lck.try_lock([=] {
	// check that gp has not been removed, and p has not changed
	if (gp->removed.load() || gp->children[pidx].load() != p) return false;
	return p->lck.try_lock([=] {
	    // check that c has not changed
	    if (p->children[cidx].load() != c) return false;
	    if (c->is_leaf) {
	      gp->children[pidx] = add_child(p, split_leaf(c), cidx);
	      leaf_pool.retire((leaf*) c);
	    } else {
		gp->children[pidx] = add_child(p, split(c), cidx);
		node_pool.retire(c);
	    }
	    node_pool.retire(p);
	    return true;
	  });});
  }

  // Joins or rebalances an underfull node (i.e. one with min_size)
  // Picks one of the two neighbors and either joins with it if the sum of
  // the sizes is less than join_cutoff, or rebalances the two otherwise.
  // Copies the parent p to replace with new child or children.
  // Updates the grandparent gp to point to the new copied parent.
  void underfull_node(node* gp, int pidx, node* p, int cidx, node* c) {
    gp->lck.try_lock([=] {
	// check that gp has not been removed, and p has not changed
	if (gp->removed.load() || gp->children[pidx].load() != p) return false;
	return p->lck.try_lock([=] {
	    // join with next if first in block, otherwise with previous
	    node* other_c = p->children[(cidx == 0 ? cidx + 1 : cidx - 1)].load();
	    auto [li, lc, rc] = ((cidx == 0) ?
				 std::make_tuple(0, c, other_c) :  
				 std::make_tuple(cidx-1, other_c, c)); 
	    if (c->is_leaf) { // leaf
	      if (lc->size + rc->size < leaf_join_cutoff)  // join
		gp->children[pidx] = join_children(p, join_leaf(lc, rc), li);
	      else   // rebalance
		gp->children[pidx] = rebalance_children(p, rebalance_leaf(lc, rc), li);
	      node_pool.retire(p);
	      leaf_pool.retire((leaf*) lc);
	      leaf_pool.retire((leaf*) rc);
	      return true;
	    } else { // internal node
	      K k = p->keys[li];
	      // need to lock the other child 
	      return other_c->lck.try_lock([=] {
		  other_c->removed = true;
		  if (lc->size + rc->size < node_join_cutoff)   // join
		    gp->children[pidx] = join_children(p, join(lc, k, rc), li);
		  else // rebalance
		    gp->children[pidx] = rebalance_children(p, rebalance(lc, k, rc), li);
		  node_pool.retire(p);
		  node_pool.retire(lc);
		  node_pool.retire(rc);
		  return true;
		});
	    }
	  });});
  }

  void fix_node(node* gp, int pidx, node* p, int cidx, node* c) {
    if (c->status == isOver)
      overfull_node(gp, pidx, p, cidx, c);
    else 
      underfull_node(gp, pidx, p, cidx, c);
  }

  node* copy_node_or_leaf(node* p) {
    if (p->is_leaf) return (node*) copy_leaf((leaf*) p);
    else return copy_node(p);
  }
  
  // If c (child of root) is overfull it splits it and creates a new
  // internal node with the two split nodes as children.
  // If c is underfull (degree 1), it removes c
  // In both cases it updates the root to point to the new internal node
  // it takes a lock on the root
  void fix_root(node* root, node* c) {
      root->lck.try_lock([=] {
	// check that c has not changed
	if (root->children[0].load() != c) return false;
	if (c->status == isOver) {
	  if (c->is_leaf) {
	    root->children[0] = node_pool.new_obj(split_leaf(c));
	    leaf_pool.retire((leaf*) c);
	  } else {
	    root->children[0] = node_pool.new_obj(split(c));
	    node_pool.retire(c);
	  }
	  return true;
	} else { // c has degree 1 and not a leaf
	  // c needs to be copied so it is not recorded a second time
	  return c->lck.try_lock([=] {
	      root->children[0] = copy_node_or_leaf(c->children[0].load());
	      node_pool.retire(c);
	      return true;});
	}});
  }

  // Finds the leaf containing a given key, returning the parent, the
  // location of the pointer in the parent to the leaf, and the leaf.
  // Along the way if it encounters any underfull or overfull nodes
  // (internal or leaf) it fixes them and starts over (splitting
  // overfull nodes, and joining underfull nodes with a neighbor).
  // This ensures that the returned leaf is not full, and that it is safe
  // to split a child along the way since its parent is not full and can
  // absorb an extra pointer
  std::tuple<node*, int, leaf*> find_and_fix(node* root, K k) {
    int cnt = 0;
    while (true) {
      node* p = root;
      int cidx = 0;
      node* c = p->children[cidx].read();
      if (c->status == isOver || (!c->is_leaf && c->size == 1))
        fix_root(root, c);
      else while (true) {
        if (c->is_leaf) return std::tuple(p, cidx, (leaf*) c);
        node* gp = p;
        int pidx = cidx;
        p = c;
        cidx = c->find(k);
        c = p->children[cidx].load();
        // The following two lines are only useful if hardware
        // prefetching is turned off.  They prefetch the next two
        // cache lines.
        __builtin_prefetch (((char*) c) + 64); 
        __builtin_prefetch (((char*) c) + 128);
	if (c->status != OK) {
          fix_node(gp, pidx, p, cidx, c);
          break;
	}
      }
    }
  }

  static constexpr int init_delay=200;
  static constexpr int max_delay=2000;

  // Inserts by finding the leaf containing the key, and inserting
  // into the leaf.  Since the find ensures the leaf is not full,
  // there will be space for the new key.  It needs a lock on the
  // parent and in the lock needs to check the parent has not been
  // deleted and the child has not changed.  If the try_lock fails it
  // tries again.
  // returns false and does no update if already in tree
  bool insert(node* root, K k, V v) {
    return with_epoch([=] {
      int delay = init_delay;
      while (true) {
	auto [p, cidx, l] = find_and_fix(root, k);
	if (l->find(k).has_value()) return false; // already there
	if (p->lck.try_lock([=] {
	      if (p->removed.load() || (leaf*) p->children[cidx].load() != l)
		return false;
	      p->children[cidx] = (node*) insert_leaf(l, k, v);
	      leaf_pool.retire(l);
	      return true;
	    })) return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }});
  }

  // Deletes by finding the leaf containing the key, and removing from
  // the leaf.  It needs a lock on the parent.  In the lock it needs to
  // check the parent has not been deleted and the child has not
  // changed.  If the try_lock fails it tries again.  Returns false if
  // not found.
  bool remove(node* root, K k) {
    return with_epoch([=] {
      int delay = init_delay;
      while (true) {
        auto [p, cidx, l] = find_and_fix(root, k);
	if (!l->find(k).has_value()) return false; // not there
	if (p->lck.try_lock([=] {
	      if (p->removed.load() || (leaf*) p->children[cidx].load() != l)
		return false;
	      p->children[cidx] = (node*) remove_leaf(l, k);
	      leaf_pool.retire(l);
	      return true;
	    })) return true;
	for (volatile int i=0; i < delay; i++);
	delay = std::min(2*delay, max_delay);
      }});
  }

  void range_internal_(node* a, int& accum,
  		      std::optional<K> start, std::optional<K> end) {
    if (a->is_leaf) {
      leaf* la = (leaf*) a;
      int s = 0;
      int e = la->size;
      if (start.has_value()) s = la->prev(start.value(), s);
      if (end.has_value()) e = la->prev(end.value(), s);
      for (int i = s; i < e; i++) accum++; // accum.push_back(la->keyvals[i]);
    } else {
      int s = 0;
      int e = a->size;
      if (start.has_value()) s = a->find(start.value(), s);
      if (end.has_value()) e = a->find(end.value(), s);
      if (s == e) range_internal_(a->children[s].read(),accum,start,end);
      else {
  	std::optional<K> empty = {};
  	range_internal_(a->children[s].read(), accum, start, empty);
  	for (int i = s+1; i < e; i++)
  	  range_internal_(a->children[s].read(), accum, empty, empty);
  	range_internal_(a->children[e].read(), accum, empty, end);
      }
    }
  }

  void range_internal(node* a, int& accum, K start, K end) {
    while (true) {
      if (a->is_leaf) {
	leaf* la = (leaf*) a;
	int s = la->prev(start, 0);
	int e = la->prev(end, s);
	for (int i = s; i < e; i++) accum++;
	//accum.push_back(la->keyvals[i]);
	return;
      }
      int s = a->find(start);
      int e = a->find(end, s);
      if (s == e) a = a->children[s].read();
      else {
	for (int i = s; i <= e; i++) 
	  range_internal(a->children[i].read(), accum, start, end);
	return;
      }
    }
  }

  auto range(node* root, K start, K end) {
    return with_snap([=] {
			   //std::vector<KV> result;
      int result = 0;
      //range_internal_(root, result, std::optional<K>(start),
      //std::optional<K>(end));
      range_internal(root, result, start, end);
      return result;
    });
  }
    
  // old version, not used
  std::optional<V> find_(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	auto [p, cidx, l] = find_and_fix(root, k);
	return l->find(k);
      });
  }

  // a wait-free version that does not split on way down
  std::optional<V> find_internal(node* root, K k) {
    node* c = root;
    ptr_type<node>* x;
    while (!c->is_leaf) {
      __builtin_prefetch (((char*) c) + 64); 
      __builtin_prefetch (((char*) c) + 128);
      x = &c->children[c->find(k)];
      c = x->read();
    }
    x->validate();
    return ((leaf*) c)->find(k);
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] {return find_internal(root, k);});
  }

  // snapshot version, if snapshoting enabled
  std::optional<V> find__(node* root, K k) {
    return with_snap([&] {return find_internal(root, k);});
  }

  // An empty tree is an empty leaf along with a root pointing tho the
  // leaf.  The root will always contain a single pointer.
  node* empty() {
    leaf* l = leaf_pool.new_obj(0);
    return node_pool.new_obj(l);
  }

  node* empty(size_t n) { return empty(); }

  void retire(node* p) {
    if (p == nullptr) return;
    if (p->is_leaf) {
     leaf_pool.retire((leaf*) p);
    } else {
      parlay::parallel_for(0, p->size, [&] (size_t i) {
	  retire(p->children[i].load());});
      node_pool.retire(p);
    }
  }

  double total_height(node* root) {
    std::function<size_t(node*, size_t)> hrec;
    hrec = [&] (node* p, size_t depth) {
	     if (p->is_leaf) return depth * ((leaf*) p)->size;
	     return parlay::reduce(parlay::tabulate(p->size, [&] (size_t i) {
		   return hrec(p->children[i].load(), depth + 1);}));
	   };
    return hrec(root, 0);
  }

  using rtup = std::tuple<K,K,long>;

  rtup check_recursive(node* p, bool is_root) {
    if (p->is_leaf) {
      leaf* l = (leaf*) p;
      K minv = l->keyvals[0].key;
      K maxv = l->keyvals[0].key;
      for (int i=1; i < l->size; i++) {
	minv = std::min(minv, l->keyvals[i].key);
	maxv = std::max(minv, l->keyvals[i].key);
      }
      return rtup(minv, maxv, l->size);
    }
    if (!is_root && p->size < node_min_size) {
      std::cout << "size " << (int) p->size
		<< " too small for internal node" << std::endl;
      abort();
    }
    auto r = parlay::tabulate(p->size, [&] (size_t i) -> rtup {
		return check_recursive(p->children[i].load(), false);});
    long total = parlay::reduce(parlay::map(r, [&] (auto x) {
		    return std::get<2>(x);}));
    parlay::parallel_for(0, p->size - 1, [&] (size_t i) {
	if (std::get<1>(r[i]) >= p->keys[i] ||
	    p->keys[i] > std::get<0>(r[i+1])) {
	  std::cout << "keys not ordered around key: " << p->keys[i]
		    << " max before = " << std::get<1>(r[i])
		    << " min after = " << std::get<0>(r[i+1]) << std::endl;
	  abort();
	}});
    return rtup(std::get<0>(r[0]),std::get<1>(r[r.size()-1]), total);
  }

  long check(node* root) {
    auto [minv, maxv, cnt] = check_recursive(root->children[0].load(), true);
    if (verbose) std::cout << "average height = "
			   << ((double) total_height(root) / cnt)
			   << std::endl;
    return cnt;
  }

  void print(node* p) {
    std::function<void(node*)> prec;
    prec = [&] (node* p) {
	     if (p == nullptr) return;
	     if (p->is_leaf) {
	       leaf* l = (leaf*) p;
	       for (int i=0; i < l->size; i++) 
		 std::cout << l->keyvals[i].key << ", ";
	     } else {
	       for (int i=0; i < p->size; i++) {
		 prec((p->children[i]).load());
	       }
	     }
	   };
    prec(p);
    std::cout << std::endl;
  }

  void clear() {
    node_pool.clear();
    leaf_pool.clear();
  }

  void reserve(size_t n) {
    node_pool.reserve(n);
    leaf_pool.reserve(n);
  }

  void shuffle(size_t n) {
    node_pool.shuffle(n/8);
    leaf_pool.shuffle(n/8);
  }

  void stats() {
    node_pool.stats();
    leaf_pool.stats();
  }

};
