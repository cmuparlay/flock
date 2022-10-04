#include <flock/flock.h>
#include "rebalance.h"

#ifdef BALANCED
bool balanced = true;
#else
bool balanced = false;
#endif

template <typename K_, typename V_>
struct Set {
  using K = K_;
  using V = V_;
  
  struct KV {
    K key;
    V value;
    KV(K key, V value) : key(key), value(value) {}
    KV() {}
  };
  static constexpr int block_size= 368/sizeof(KV) - 1; // to fit in 384 bytes
  //static constexpr int block_size= 240/sizeof(KV) - 1; // to fit in 256 bytes

  struct head : ll_head {
    bool is_leaf;
    bool is_sentinal; // only used by leaf
    write_once<bool> removed; // only used by node
    head(bool is_leaf)
      : is_leaf(is_leaf), is_sentinal(false), removed(false) {}
  };

  // mutable internal node
  struct node : head, lock_type {
    K key;
    ptr_type<node> left;
    ptr_type<node> right;
    node(K k, node* left, node* right)
      : key(k), head{false}, left(left), right(right) {};
    node(node* left) // root node
      : head{false}, left(left), right(nullptr) {};
  };

  // immutable leaf
  struct leaf : head {
    long size;
    KV keyvals[block_size+1];
    leaf() : size(0), head{true} {};
    std::optional<V> find(K k) {
      for (int i=0; i < size; i++) 
	if (keyvals[i].key == k) return keyvals[i].value;
      return {};
    }
  };

  memory_pool<node> node_pool;
  memory_pool<leaf> leaf_pool;

  Rebalance<Set<K,V>> balance;
  Set() : balance(this) {}

  enum direction {left, right};
  
  auto find_location(node* root, K k) {
    node* gp = nullptr;
    bool gp_left = false;
    node* p = root;
    bool p_left = true;
    node* l = (p->left).read();
    while (!l->is_leaf) {
      gp = p;
      gp_left = p_left;
      p = l;
      p_left = (k < p->key);
      l = p_left ? (p->left).read() : (p->right).read();
    }
    return std::make_tuple(gp, gp_left, p, p_left, l);
  }

  // inserts a key into a leaf.  If a leaf overflows (> block_size) then
  // the leaf is split in the middle and parent internal node is created
  // to point to the two leaves.  The code within the locks is
  // idempotent.  All changeable values (left and right) are accessed
  // via an atomic_ptr and the pool allocators, which are idempotent.
  // The other values are "immutable" (i.e. they are either written once
  // without being read, or just read).
  bool insert(node* root, K k, V v) {
    return with_epoch([=] {
      while (true) {
	auto [gp, gp_left, p, p_left, l] = find_location(root, k);
	leaf* old_l = (leaf*) l;
	if (old_l->find(k).has_value()) return false; // already there
	if (p->try_lock([=] {
	      auto ptr = (p_left) ? &(p->left) : &(p->right);

	      // if p has been removed, or l has changed, then exit
	      if (p->removed.load() || ptr->load() != l) return false;
	      leaf* new_l = leaf_pool.new_obj();
	      // if the leaf is the left sentinal then create new node
	      if (old_l->is_sentinal) {
		new_l->size = 1;
		new_l->keyvals[0] = KV(k,v);
		(*ptr) = node_pool.new_obj(k, l, (node*) new_l);
		return true;
	      }

	      // first insert into a new block
	      int i=0;
	      for (;i < old_l->size && old_l->keyvals[i].key < k; i++) {
		new_l->keyvals[i] = old_l->keyvals[i];
	      }
	      int offset = 0;
	      if (i == old_l->size || k != old_l->keyvals[i].key) {
		new_l->keyvals[i] = KV(k,v);
		offset = 1;
	      }
	      if (offset == 0) abort();
	      for (; i < old_l->size ; i++ )
		new_l->keyvals[i+offset] = old_l->keyvals[i];

	      // if the block overlflows, split into another block and
	      // create a parent
	      if (old_l->size + offset > block_size) { 
		leaf* new_ll = leaf_pool.new_obj();  // the other block
		for (int i =0 ; i < block_size/2+1; i++)
		  new_ll->keyvals[i] = new_l->keyvals[i+(block_size+1)/2];
		new_l->size = (block_size+1)/2;
		new_ll->size = block_size/2 + 1;
		(*ptr) = node_pool.new_obj(new_ll->keyvals[0].key,
					   (node*) new_l, (node*) new_ll);
	      } else {
		new_l->size = old_l->size + offset;
		(*ptr) = (node*) new_l;
	      }

	      // retire the old block
	      leaf_pool.retire(old_l);
	      return true;
	    })) {
	  if (balanced) balance.rebalance(p, root, k);
	  return true;
	}
	// try again if unsuccessful
      }
    });
  }

  // Removes a key from the leaf.  If the leaf will become empty by
  // removing it, then both the leaf and its parent need to be deleted.
  bool remove(node* root, K k) {
    return with_epoch([=] {
      while (true) {
	auto [gp, gp_left, p, p_left, l] = find_location(root, k);
	leaf* old_l = (leaf*) l;
	if (!old_l->find(k).has_value()) return false; // not there
	// The leaf has at least 2 keys, so the key can be removed from the leaf
	if (old_l->size > 1) {
	  if (p->try_lock([=] {
		auto ptr = p_left ? &(p->left) : &(p->right);
		if (p->removed.load() || ptr->load() != l) return false;
		leaf* new_l = leaf_pool.new_obj();
	      
		// copy into new node while deleting k, if there
		int i = 0;
		for (;i < old_l->size && old_l->keyvals[i].key < k; i++)
		  new_l->keyvals[i] = old_l->keyvals[i];
		int offset = (old_l->keyvals[i].key == k) ? 1 : 0;
		for (; i+offset < old_l->size ; i++ )
		  new_l->keyvals[i] = old_l->keyvals[i+offset];
		new_l->size = i;

		// update parent to point to new leaf, and retire old
		(*ptr) = (node*) new_l;
		leaf_pool.retire(old_l);
		return true;
	      }))
	    return true;

	  // The leaf has 1 key.  
	} else if (old_l->keyvals[0].key == k) { // check the one key matches k

	  // We need to delete the leaf (l) and its parent (p), and point
	  // the granparent (gp) to the other child of p.
	  if (gp->try_lock([=] {
	      auto ptr = gp_left ? &(gp->left) : &(gp->right);

	      // if p has been removed, or l has changed, then exit
	      if (gp->removed.load() || ptr->load() != p) return false;

	      // lock p and remove p and l
	      return p->try_lock([=] {
		  node* ll = (p->left).load();
		  node* lr = (p->right).load();
		  if (p_left) std::swap(ll,lr);
		  if (lr != l) return false;
		  p->removed = true;
		  (*ptr) = ll; // shortcut
		  node_pool.retire(p);
		  leaf_pool.retire((leaf*) l);
		  return true; });}))
	    return true;
	} else return true;
	// try again if unsuccessful
      }
    });
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
      auto [gp, gp_left, p, p_left, l] = find_location(root, k);
      ((p_left) ? &(p->left) : &(p->right))->validate();
      leaf* ll = (leaf*) l;
      for (int i=0; i < ll->size; i++) 
	if (ll->keyvals[i].key == k) return ll->keyvals[i].value;
      return {};
      // note brute force is faster than binary search
      //auto loc = std::lower_bound(ll->keyvals, ll->keyvals + ll->size, k);
      //return (loc < ll->keyvals+ll->size && *loc == k);
      });
  }

  node* empty() {
    leaf* l = leaf_pool.new_obj();
    l->size = 0;
    l->is_sentinal = true;
    return node_pool.new_obj((node*) l);
  }

  node* empty(size_t n) { return empty(); }

  void retire(node* p) {
    if (p == nullptr) return;
    if (p->is_leaf) leaf_pool.retire((leaf*) p);
    else {
      parlay::par_do([&] () { retire((p->left).load()); },
		     [&] () { retire((p->right).load()); });
      node_pool.retire(p);
    }
  }

  double total_height(node* p) {
    std::function<size_t(node*, size_t)> hrec;
    hrec = [&] (node* p, size_t depth) {
	     if (p->is_leaf) return depth * ((leaf*) p)->size;
	     size_t d1, d2;
	     parlay::par_do([&] () { d1 = hrec((p->left).load(), depth + 1);},
			    [&] () { d2 = hrec((p->right).load(), depth + 1);});
	     return d1 + d2;
	   };
    return hrec(p->left.load(), 1);
  }
  
  long check(node* p) {
    using rtup = std::tuple<K,K,long>;
    std::function<rtup(node*)> crec;
    bool bad_val = false;
    crec = [&] (node* p) {
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
	     node* l = p->left.load();
	     node* r = p->right.load();
	     K lmin,lmax,rmin,rmax;
	     long lsum,rsum;
	     parlay::par_do([&] () { std::tie(lmin,lmax,lsum) = crec(l);},
			    [&] () { std::tie(rmin,rmax,rsum) = crec(r);});
	     if ((lsum > 0 && lmax >= p->key) || rmin < p->key) {
	       std::cout << "bad value: " << lmax << ", " << p->key << ", " << rmin << std::endl;
	       bad_val = true;
	     }
	     if (balanced) balance.check_balance(p, l, r);
	     if (lsum == 0) return rtup(p->key, rmax, rsum);
	     else return rtup(lmin, rmax, lsum + rsum);
	   };
    auto [minv, maxv, cnt] = crec(p->left.load());
    if (verbose) std::cout << "average height = " << ((double) total_height(p) / cnt) << std::endl;
    return bad_val ? -1 : cnt;
  }

  void print(node* p) {
    std::function<void(node*)> prec;
    prec = [&] (node* p) {
	     if (p->is_leaf) {
	       leaf* l = (leaf*) p;
	       for (int i=0; i < l->size; i++) 
		 std::cout << l->keyvals[i].key << ", ";
	     } else {
	       prec((p->left).load());
	       prec((p->right).load());
	     }
	   };
    prec(p->left.load());
    std::cout << std::endl;
  }

  void clear() {
    node_pool.clear();
    leaf_pool.clear();
  }

  void reserve(size_t n) {
    node_pool.reserve(n/8);
    leaf_pool.reserve(n);
  }

  void shuffle(size_t n) {
    node_pool.shuffle(n/8);
    leaf_pool.shuffle(n);
  }

  void stats() {
    node_pool.stats();
    leaf_pool.stats();
  }

};
