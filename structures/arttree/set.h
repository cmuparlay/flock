#include <verlib/verlib.h>
#include <parlay/primitives.h>
#define Range_Search 1

template <typename K, typename V>
struct Set {

  enum node_type : char {Full, Indirect, Sparse, Leaf};

  // extracts byte from key at position pos
  // assumes little endian, and currently specific to integer types
  // need to generalize to character or byte strings
  static int get_byte(K key, int pos) {
    return (key >> (8*(sizeof(K)-1-pos))) & 255;
  }

  struct header : vl::versioned {
    K key;
    node_type nt;
    flck::write_once<bool> removed;
    // every node has a byte position in the key
    // e.g. the root has byte_num = 0
    short int byte_num; 
    header(node_type nt) : nt(nt), removed(false) {}
    header(K key, node_type nt, int byte_num)
      : key(key), nt(nt), removed(false), byte_num((short int) byte_num) {}
  };

  // generic node
  struct node : header, flck::lock {};
  using node_ptr = vl::versioned_ptr<node>;

  // 256 entries, one for each value of a byte, null if empty
  struct full_node : header, flck::lock {
    node_ptr children[256];

    bool is_full() {return false;}

    node_ptr* get_child(K k) {
      auto b = get_byte(k, header::byte_num);
      return &children[b];}

    void init_child(int k, node* c) {
      auto b = get_byte(k, header::byte_num);
      children[b].init(c);
    }

    // an empty "full" node
    full_node() : header(Full) {
      //for (int i=0; i < 256; i++) children[i].init(nullptr);
    }
  };

  // up to 64 entries, with array of 256 1-byte pointers to the 64
  // entries.  A new slot can only be added, but when the key is
  // deleted its entry in the 64-pointer array is made null and
  // can be refilled.  Once all 64 slots are used a new node
  // has to be allocated.
  struct indirect_node : header, flck::lock {
    flck::atomic<int> num_used; // could be aba_free since only increases
    flck::write_once<char> idx[256];  // -1 means empty
    node_ptr ptr[64];

    bool is_full() {return num_used.load() == 64;}
    
    node_ptr* get_child(K k) {
      int i = idx[get_byte(k, header::byte_num)].load();
      if (i == -1) return nullptr;
      else return &ptr[i];}

    // Requires that node is not full (i.e. num_used < 64)
    void add_child(K k, node* v) {
      int i = num_used.load();
      idx[get_byte(k, header::byte_num)] = i;
      ptr[i] = v;
      num_used = i+1;
    }

    void init_child(K k, node* c) {
      int i = num_used.load()-1;
      idx[get_byte(k, header::byte_num)] = i;
      ptr[i].init(c);
    }

    // an empty indirect node
    indirect_node() : header(Indirect), num_used(0) {
      for (int i=0; i < 256; i++) idx[i].init(-1);
    };
  };

  // Up to 16 entries each consisting of a key and pointer.  The keys
  // are immutable, but the pointers can be changed.  i.e. Adding a
  // new child requires copying, but updating a child can be done in
  // place.
  struct alignas(64) sparse_node : header, flck::lock {
    int num_used; 
    unsigned char keys[16];
    node_ptr ptr[16];

    bool is_full() {return num_used == 16;}

    node_ptr* get_child(K k) {
      __builtin_prefetch (((char*) ptr) + 64);
      int kb = get_byte(k, header::byte_num);
      for (int i=0; i < num_used; i++) 
	if (keys[i] == kb) return &ptr[i];
      return nullptr;
    }

    void init_child(K k, node* c) {
      int kb = get_byte(k, header::byte_num);
      keys[num_used-1] = kb;
      ptr[num_used-1].init(c);
    }

    // constructor for a new sparse node with two children
    sparse_node(int byte_num, node* v1, K k1, node* v2, K k2)
      : header(k1, Sparse, byte_num), num_used(2) {
      keys[0] = get_byte(k1, byte_num);
      ptr[0].init(v1);
      keys[1] = get_byte(k2, byte_num);
      ptr[1].init(v2);
    }

    // an empty sparse node
    sparse_node() : header(Sparse), num_used(0) {}
  };

  struct leaf : header {
    V value;
    leaf(K key, V value) : header(key, Leaf, sizeof(K)), value(value) {};
  };

  vl::memory_pool<full_node> full_pool;
  vl::memory_pool<indirect_node> indirect_pool;
  vl::memory_pool<sparse_node> sparse_pool;
  vl::memory_pool<leaf> leaf_pool;

  // dispatch based on node type
  // A returned nullptr means no child matching the key
  inline node_ptr* get_child(node* x, K k) {
    switch (x->nt) {
    case Full : return ((full_node*) x)->get_child(k);
    case Indirect : return ((indirect_node*) x)->get_child(k);
    case Sparse : return ((sparse_node*) x)->get_child(k);
    }
    return nullptr;
  }

  // dispatch based on node type
  inline bool is_full(node* p) {
    switch (p->nt) {
    case Full : return ((full_node*) p)->is_full(); // never full
    case Indirect : return ((indirect_node*) p)->is_full();
    case Sparse : return ((sparse_node*) p)->is_full();
    }
    return false;
  }

  // Adds a new child to p with key k and value v
  // gp is p's parent (i.e. grandparent)
  // This might involve copying p either because
  //   p is sparse (can only be copied), or because
  //   p is indirect but full
  // If p is copied, then gp is updated to point to the new one
  // This should never be called on a full node.
  // Returns false if it fails.
  bool add_child(node* gp, node* p, K k, V v) {
    if (p->nt == Indirect && !is_full(p)) {
      // If non-full indirect node try to add a child pointer
      return p->try_lock([=] {
          if (p->removed.load() || is_full(p) ||
	      get_child(p, k) != nullptr) return false;
	  node* c = (node*) leaf_pool.new_obj(k, v);
	  ((indirect_node*) p)->add_child(k, c);
	  return true;
	});
    } else {
      // otherwise we need to create a new node
      return gp->try_lock([=] {
	  auto child_ptr = get_child(gp, p->key);
	  if (gp->removed.load() || child_ptr->load() != p)
	    return false;
	  return p->try_lock([=] {
              if (get_child(p,k) != nullptr) return false;
	      node* c = (node*) leaf_pool.new_obj(k, v);
	      if (p->nt == Indirect) {
		indirect_node* i_n = (indirect_node*) p;
		i_n->removed = true; 

		// copy indirect to full
		*child_ptr = (node*) full_pool.new_init([=] (full_node* f_n) {
 		  f_n->key = i_n->key;
		  f_n->byte_num = i_n->byte_num;
		  for (int i=0; i < 256; i++) {
		    int j = i_n->idx[i].load();
		    if (j != -1) f_n->children[i].init(i_n->ptr[j].load());
		  }

		  // seemingly broken g++ (10.3) compiler makes the following fail
		  //   f_n->init_child(k, c);
		  // inlining by hand fixes it.
		  auto b = get_byte(k, f_n->byte_num);
		  f_n->children[b].init(c);
		});
		indirect_pool.retire(i_n);
	      } else { // (p->nt == Sparse)
		sparse_node* s_n = (sparse_node*) p;
		s_n->removed = true;
		if (is_full(p)) {

		  // copy sparse to indirect
		  *child_ptr = (node*) indirect_pool.new_init([=] (indirect_node* i_n) {
  		    i_n->key = s_n->key;
		    i_n->byte_num = s_n->byte_num;
		    i_n->num_used.init(16 + 1);
		    for (int i=0; i < 16; i++) {
		      i_n->idx[s_n->keys[i]].init(i);
		      i_n->ptr[i].init(s_n->ptr[i].load());
		    }
		    i_n->init_child(k, c);
	          });  
		} else {

		  // copy sparse to sparse
		  *child_ptr = (node*) sparse_pool.new_init([=] (sparse_node* s_c) {
  		    s_c->key = s_n->key;
		    s_c->byte_num = s_n->byte_num;
		    s_c->num_used = s_n->num_used + 1;
		    for (int i=0; i < s_n->num_used; i++) {
		      s_c->keys[i] = s_n->keys[i];
		      s_c->ptr[i].init(s_n->ptr[i].load());
		    }
		    s_c->init_child(k, c);
	          });
		}
		sparse_pool.retire(s_n);
	      }
	      return true;
	    }); // end try_lock(p->lck
	  return true;
	}); // end try_lock(gp->lck
    } // end else
  }

  auto find_location(node* root, K k) {
    int byte_pos = 0;
    node* gp = nullptr;
    node* p = root;
    while (true) {
      node_ptr* cptr = get_child(p, k);
      if (cptr == nullptr) // has no child with given key
	return std::make_tuple(gp, p, cptr, (node*) nullptr, byte_pos);
      // could be read()
      node* c = cptr->load();
      if (c == nullptr) // has empty child with given key
	return std::make_tuple(gp, p, cptr, c, byte_pos);
      byte_pos++;
      while (byte_pos < c->byte_num &&
	     get_byte(k, byte_pos) == get_byte(c->key, byte_pos))
	byte_pos++;
      if (byte_pos != c->byte_num || c->nt == Leaf) // reached leaf
	return std::make_tuple(gp, p, cptr, c, byte_pos);
      gp = p;
      p = c;
    }
  }

  bool insert(node* root, K k, V v) {
    return vl::with_epoch([=] {
      while (true) {		 
	auto [gp, p, cptr, c, byte_pos] = find_location(root, k);
	if (c != nullptr && c->nt == Leaf && c->byte_num == byte_pos)
	  return false; // already in the tree
	if (cptr != nullptr) {// child pointer exists, always true for full node
	    if (p->try_lock([=] {
		// exit and retry if state has changed
		if (p->removed.load() || cptr->load() != c) return false;

		// create new leaf
		node* new_l = (node*) leaf_pool.new_obj(k, v);

		// fill a null pointer with the new leaf
		if (c == nullptr) (*cptr) = new_l;

		// split an existing leaf into a sparse node with two child
		// leaf nodes (one of them the original leaf)
		else {
		  *cptr = (node*) sparse_pool.new_obj(byte_pos, c, c->key,
						      new_l, k);
		}
		return true;
	      })) return true;
	} else { // no child pointer, need to add
	  if (add_child(gp, p, k, v)) return true;
	}
      } // end while
      return true; // should never get here
    });
  }

  // returns other child if node is sparse and has two children, one
  // of which is c, otherwise returns nullptr
  node* single_other_child(node* p, node* c) {
    if (p->nt != Sparse) return nullptr;
    sparse_node* ps = (sparse_node*) p;
    node* result = nullptr;
    for (int i=0; i < ps->num_used; i++) {
      node* oc = ps->ptr[i].load();
      if (oc != nullptr && oc != c)
	if (result != nullptr) return nullptr; // quit if second child
	else result = oc; // set first child
    }
    return result;
  }
				 
  // currently a "lazy" remove that only removes
  //   1) the leaf
  //   2) its parent if it is sparse with just two children
  bool remove(node* root, K k) {
    return vl::with_epoch([=] {
      while (true) {
	auto [gp, p, cptr, c, byte_pos] = find_location(root, k);
	// if not found return
	if (c == nullptr || !(c->nt == Leaf && c->byte_num == byte_pos))
	  return false;
	if (p->try_lock([=] {
	    if (p->removed.load() || cptr->load() != c) return false;

  	    node* other_child = single_other_child(p,c);
	    if (other_child != nullptr) {
	      // if parent will become singleton try to remove parent as well
	      return gp->try_lock([=] {
		  auto child_ptr = get_child(gp, p->key);
		  if (gp->removed.load() || child_ptr->load() != p)
		    return false;
		  *child_ptr = other_child;
		  p->removed = true;
		  sparse_pool.retire((sparse_node*) p);
		  leaf_pool.retire((leaf*) c);
		  return true;});
	    } else { // just remove child
	      *cptr = nullptr; 
	      leaf_pool.retire((leaf*) c);
	      return true;
	    }}))
	  return true;
      }
      // try again
    });
  }

  std::optional<V> find_(node* root, K k) {
    auto [gp, p, cptr, l, pos] = find_location(root, k);
    if (cptr != nullptr) cptr->validate();
    auto ll = (leaf*) l;
    if (ll != nullptr) return std::optional<V>(ll->value); 
    else return {};
  }

  std::optional<V> find(node* root, K k) {
    return vl::with_epoch([&] {return find_(root, k);});
  }

  template<typename AddF>
  void range_internal(node* a, AddF& add,
		      std::optional<K> start, std::optional<K> end, int pos) {
    if (a == nullptr) return;
    std::optional<K> empty;
    if (a->nt == Leaf) {
      if ((!start.has_value() || start.value() <= a->key)
	  && (!end.has_value() || end.value() >= a->key)) 
	add(a->key, ((leaf*) a)->value);
      return;
    }
    for (int i = pos; i < a->byte_num; i++) {
      if (start == empty && end == empty) break;
      if (start.has_value() && get_byte(start.value(), i) > get_byte(a->key, i)
	  || end.has_value() && get_byte(end.value(), i) < get_byte(a->key, i))
	return;
      if (start.has_value() && get_byte(start.value(), i) < get_byte(a->key,i)) 
	start = empty;
      if (end.has_value() && get_byte(end.value(), i) > get_byte(a->key, i)) {
	end = empty;
      }
    }
    int sb = start.has_value() ? get_byte(start.value(), a->byte_num) : 0;
    int eb = end.has_value() ? get_byte(end.value(), a->byte_num) : 255;
    if (a->nt == Full) {
      for (int i = sb; i <= eb; i++) 
	range_internal(((full_node*) a)->children[i].read_snapshot(), add,
		       start, end, a->byte_num);
    } else if (a->nt == Indirect) {
      for (int i = sb; i <= eb; i++) {
	indirect_node* ai = (indirect_node*) a;
	int o = ai->idx[i].read();
	if (o != -1) {
	  range_internal(ai->ptr[o].read_snapshot(), add,
				    start, end, a->byte_num);
	}
      }
    } else { // Sparse
      sparse_node* as = (sparse_node*) a;
      for (int i = 0; i < as->num_used; i++) {
	int b = as->keys[i];
	if (b >= sb && b <= eb)
	  range_internal(as->ptr[i].read_snapshot(), add, start, end, a->byte_num);
      }
    }
  }		       

  template<typename AddF>
  void range_(node* root, AddF& add, K start, K end) {
    range_internal(root, add,
		   std::optional<K>(start), std::optional<K>(end), 0);
  }

  node* empty() {
    auto r = full_pool.new_obj();
    r->byte_num = 0;
    r->key = 0;
    return (node*) r;
  }

  node* empty(size_t n) { return empty(); }
  
  void print(node* p) {
    std::function<void(node*)> prec;
    prec = [&] (node* p) {
	     if (p == nullptr) return;
	     switch (p->nt) {
	     case Leaf : {
	       auto l = (leaf*) p;
	       std::cout << l->key << ", ";
	       return;
	     }
	     case Full : {
	       auto f_n = (full_node*) p;
	       for (int i=0; i < 256; i++) {
		 prec(f_n->children[i].load());
	       }
	       return;
	     }
	     case Indirect : {
	       auto i_n = (indirect_node*) p;
	       for (int i=0; i < 256; i++) {
		 int j = i_n->idx[i].load();
		 if (j != -1) prec(i_n->ptr[j].load());
	       }
	       return;
	     }
	     case Sparse : {
	       using pr = std::pair<int,node*>;
	       auto s_n = (sparse_node*) p;
	       std::vector<pr> v;
	       for (int i=0; i < s_n->num_used; i++)
		 v.push_back(std::make_pair(s_n->keys[i], s_n->ptr[i].load()));
	       std::sort(v.begin(), v.end()); 
	       for (auto x : v) prec(x.second);
	       return;
	     }
	     }
	   };
    prec(p);
    std::cout << std::endl;
  }

  void retire(node* p) {
    if (p == nullptr) return;
    if (p->nt == Leaf) leaf_pool.retire((leaf*) p);
    else if (p->nt == Sparse) {
      auto pp = (sparse_node*) p;
      parlay::parallel_for(0, pp->num_used, [&] (size_t i) {retire(pp->ptr[i].load());});
      sparse_pool.retire(pp);
    } else if (p->nt == Indirect) {
      auto pp = (indirect_node*) p;
      parlay::parallel_for(0, pp->num_used.load(), [&] (size_t i) {retire(pp->ptr[i].load());});
      indirect_pool.retire(pp);
    } else {
      auto pp = (full_node*) p;
      parlay::parallel_for(0, 256, [&] (size_t i) {retire(pp->children[i].load());});
      full_pool.retire(pp);
    }
  }
  
  long check(node* p) {
    std::function<size_t(node*)> crec;
    crec = [&] (node* p) -> size_t {
	     if (p == nullptr) return 0;
	     switch (p->nt) {
	     case Leaf : return 1;
	     case Full : {
	       auto f_n = (full_node*) p;
	       auto x = parlay::tabulate(256, [&] (size_t i) {
						return crec(f_n->children[i].load());});
	       return parlay::reduce(x);
	     }
	     case Indirect : {
	       auto i_n = (indirect_node*) p;
	       auto x = parlay::tabulate(256, [&] (size_t i) {
						int j = i_n->idx[i].load();
						return (j == -1) ? 0 : crec(i_n->ptr[j].load());});
	       return parlay::reduce(x);
	     }
	     case Sparse : {
	       auto s_n = (sparse_node*) p;
	       auto x = parlay::tabulate(s_n->num_used,
					 [&] (size_t i) {return crec(s_n->ptr[i].load());});
	       return parlay::reduce(x);
	     }
	     }
	     return 0;
	   };
    size_t cnt = crec(p);
    return cnt;
  }

  void clear() {
    full_pool.clear();
    indirect_pool.clear();
    sparse_pool.clear();
    leaf_pool.clear();
  }

  void reserve(size_t n) {}
  
  void shuffle(size_t n) {
    full_pool.shuffle(n/100);
    indirect_pool.shuffle(n/10);
    sparse_pool.shuffle(n/5);
    leaf_pool.shuffle(n);
  }

  void stats() {
    full_pool.stats();
    indirect_pool.stats();
    sparse_pool.stats();
    leaf_pool.stats();
  }
  
};
