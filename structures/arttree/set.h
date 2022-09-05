#include <limits>
#include <flock/flock.h>
#include <parlay/primitives.h>

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();

  enum node_type : char {Full, Indirect, Sparse, Leaf};

  // assumes little endian, and currently specific to integer types
  // need to generalize to character or byte strings
  static int get_byte(K key, int pos) {
    return (key >> (8*(sizeof(K)-1-pos))) & 255;
  }

  struct header : ll_head {
    K key;
    node_type nt;
    write_once<bool> removed;
    short int byte_num; // every node has a byte position in the key
    header(node_type nt) : nt(nt), removed(false) {}
    header(K key, node_type nt, int byte_num)
      : key(key), nt(nt), removed(false), byte_num((short int) byte_num) {}
  };

  // generic node
  struct node : header, lock_type {};
  using node_ptr = ptr_type<node>;

  // 256 entries, one for each value of a byte, null if empty
  struct full_node : header, lock_type {
    node_ptr children[256];

    bool is_full() {return false;}
    
    node_ptr* get_child(K k) {
      auto b = get_byte(k, header::byte_num);
      return &children[b];}

    void init_child(int k, node* v) {
      auto b = get_byte(k, header::byte_num);
      children[b].init(v);
    }

    full_node() : header(Full), children{nullptr} {}
  };

  // up to 64 entries, with array of 256 1-byte pointers
  // to the 64 entries.  Entries can only be added, but on deletion
  // the entry pointer can be made null, and later refilled.
  struct indirect_node : header, lock_type {
    mutable_val<int> num_used; // could be aba_free
    write_once<char> idx[256];  // -1 means empty
    node_ptr ptr[64];

    bool is_full() {return num_used.load() == 64;}
    
    node_ptr* get_child(K k) {
      int i = idx[get_byte(k, header::byte_num)].load();
      if (i == -1) return nullptr;
      else return &ptr[i];}

    bool add_child(K k, node* v) {
      int i = num_used.load();
      if (i<64) {
	idx[get_byte(k, header::byte_num)] = i;
	ptr[i] = v;
	num_used = i+1;
	return true;
      } return false; //overflow
    }

    void init_child(K k, node* v) {
      int i = num_used.load()-1;
      idx[get_byte(k, header::byte_num)] = i;
      ptr[i].init(v);
    }

    indirect_node() : header(Indirect) {
      for (int i=0; i < 256; i++) idx[i].init(-1);
    };
  };

  // up to 16 entries each consisting of a key and pointer
  // adding a new child requires copying
  // updating a child can be done in place
  struct alignas(64) sparse_node : header, lock_type {
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

    void init_child(K k, node* v) {
      int kb = get_byte(k, header::byte_num);
      keys[num_used-1] = kb;
      ptr[num_used-1].init(v);
    }
    
    sparse_node(int byte_num, node* v1, K k1, node* v2, K k2)
      : header(k1, Sparse, byte_num), num_used(2) {
      keys[0] = get_byte(k1, byte_num); ptr[0].init(v1);
      keys[1] = get_byte(k2, byte_num); ptr[1].init(v2);
    }

    sparse_node() : header(Sparse) {}
  };

  struct leaf : header {
    V value;
    leaf(K key, V value) : header(key, Leaf, sizeof(K)), value(value) {};
  };

  memory_pool<full_node> full_pool;
  memory_pool<indirect_node> indirect_pool;
  memory_pool<sparse_node> sparse_pool;
  memory_pool<leaf> leaf_pool;

  inline node_ptr* get_child(node* x, K k) {
    switch (x->nt) {
    case Full : return ((full_node*) x)->get_child(k);
    case Indirect : return ((indirect_node*) x)->get_child(k);
    case Sparse : return ((sparse_node*) x)->get_child(k);
    }
    return nullptr;
  }

  inline bool is_full(node* p) {
    switch (p->nt) {
    case Full : return ((full_node*) p)->is_full();
    case Indirect : return ((indirect_node*) p)->is_full();
    case Sparse : return ((sparse_node*) p)->is_full();
    }
    return false;
  }
		 
  // Adds a new child to p
  // This might involve copying p either
  //   because p is sparse (can only be copied) or because
  //   p is indirect but full
  // If p is copies, then gp is updated to point to it
  bool add_child(node* gp, node* p, K k, V v) {
    if (p->nt == Indirect && !is_full(p)) {
      return p->try_lock([=] {
	  if (p->removed.load()) return false;
	  node* c = (node*) leaf_pool.new_obj(k, v);
	  return ((indirect_node*) p)->add_child(k, c);
	});
    } else {
      return gp->try_lock([=] {
	  auto x = get_child(gp, p->key);
	  if (gp->removed.load() || x->load() != p)
	    return false;
	  return p->try_lock([=] {
	      node* c = (node*) leaf_pool.new_obj(k, v);
	      if (p->nt == Indirect) {
		indirect_node* i_n = (indirect_node*) p;
		i_n->removed = true;

		// copy indirect to full
		*x = (node*) full_pool.new_init([=] (full_node* f_n) {
 		  f_n->key = i_n->key;
		  f_n->byte_num = i_n->byte_num;
		  for (int i=0; i < 256; i++) {
		    int j = i_n->idx[i].load();
		    if (j != -1) f_n->children[i].init(i_n->ptr[j].load());
		  }
		  f_n->init_child(k, c);
		});
		indirect_pool.retire(i_n);
	      } else { // (p->nt == Sparse)
		sparse_node* s_n = (sparse_node*) p;
		s_n->removed = true;
		if (is_full(p)) {

		  // copy sparse to indirect
		  *x = (node*) indirect_pool.new_init([=] (indirect_node* i_n) {
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
		  *x = (node*) sparse_pool.new_init([=] (sparse_node* s_c) {
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
    return with_epoch([=] {
      while (true) {		 
	auto [gp, p, cptr, c, byte_pos] = find_location(root, k);
	if (c != nullptr && c->nt == Leaf && c->byte_num == byte_pos)
	  return false; // already in the tree
	if (cptr != nullptr) { // child pointer exists
	    if (p->try_lock([=] {
		// exit and retry if state has changed
		if (p->removed.load() || cptr->load() != c) return false;

		// create new leaf
		node* new_l = (node*) leaf_pool.new_obj(k, v);

		// fill a null pointer with the new leaf
		if (c == nullptr) (*cptr) = new_l;

		// split an existing leaf into a sparse node with two children
		else {
		  *cptr = (node*) sparse_pool.new_obj(byte_pos, c, c->key, new_l, k);
		}
		return true;
	      })) return true;
	} else // no child pointer, need to add
	  if (add_child(gp, p, k, v)) return true;
      } // end while
      return true;
    });
  }

  // currently a "lazy" remove that only removes the leaf
  bool remove(node* root, K k) {
    return with_epoch([=] {
      while (true) {
	auto [gp, p, cptr, c, byte_pos] = find_location(root, k);
	// if not found return
	if (c == nullptr || !(c->nt == Leaf && c->byte_num == byte_pos))
	  return false;
	if (p->try_lock([=] {
	      if (p->removed.load() || cptr->load() != c) return false;
	      *cptr = nullptr; 
	      leaf_pool.retire((leaf*) c);
	      return true;
	    })) return true;
	// try again
      }
    });
  }	       

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
	auto [gp, p, cptr, l, pos] = find_location(root, k);
	if (cptr != nullptr) cptr->validate();
	auto ll = (leaf*) l;
	if (ll != nullptr) return std::optional<V>(ll->value); 
	else return {};
      });
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
