#include <flock/flock.h>

template <typename K, typename V>
struct Set {

  // common header for internal nodes and leaves
  struct header {
    K key;
    bool is_leaf;
    bool is_sentinal;
    flck::atomic_write_once<bool> removed; // not used for leaves, but fits here
    header(K key, bool is_leaf)
      : key(key), is_leaf(is_leaf), is_sentinal(false), removed(false) {}
    header(bool is_sentinal) :
      is_leaf(is_sentinal), is_sentinal(is_sentinal), removed(false) {}
  };

  // internal node
  struct node : header, flck::lock {
    flck::atomic<node*> left;
    flck::atomic<node*> right;
    node(K k, node* left, node* right)
      : header{k,false}, left(left), right(right) {};
    node(node* left) // for the root, only has a left pointer
      : header{false}, left(left), right(nullptr) {};
  };

  struct leaf : header {
    V value;
    leaf(K k, V v) : header{k,true}, value(v) {};
    leaf() : header{true} {}; // for the sentinal leaf
  };

  flck::memory_pool<node> node_pool;
  flck::memory_pool<leaf> leaf_pool;

  size_t max_iters = 10000000;
  
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
  
  bool insert(node* root, K k, V v, bool upsert=false) {
    return flck::with_epoch([=] {
      node* prev_leaf = nullptr;
      while (true) {
	auto [gp, gp_left, p, p_left, l] = find_location(root, k);
	if (!l->is_sentinal &&
	    ((!upsert && l->key == k) ||
	     (upsert && prev_leaf != nullptr && prev_leaf->key == k &&
	      l != prev_leaf)))
	  return false;
	prev_leaf = l;
	auto r = p->try_lock([=] {
	      auto ptr = p_left ? &(p->left) : &(p->right);
	      auto l_new = ptr->load();
	      if (p->removed.load() || l_new != l) return false;
	      node* new_l = (node*) leaf_pool.new_obj(k, v);
	      if (k == l->key) *ptr = new_l; // update existing key (only if upsert)
	      else *ptr = ((l->is_sentinal || k > l->key) ?
			   node_pool.new_obj(k, l, new_l) :
			   node_pool.new_obj(l->key, new_l, l));
	      return true;});
	if (r) return (k != l->key);
      }});
  }

  bool remove(node* root, K k) {
    return flck::with_epoch([=] {
       node* prev_leaf = nullptr;
       while (true) {
	 auto [gp, gp_left, p, p_left, l] = find_location(root, k);
	 if (l->is_sentinal || k != l->key ||
	     (prev_leaf != nullptr && prev_leaf != l))
	   return false;
	 prev_leaf = l;
	 if (gp->try_lock([=] {
	       return p->try_lock([=] {
		   auto ptr = gp_left ? &(gp->left) : &(gp->right);
		   if (gp->removed.load() || ptr->load() != p) return false;
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
       }}); 
  }

  std::optional<V> find_(node* root, K k) {
    auto ptr = &(root->left);
    node* l = ptr->read();
    while (!l->is_leaf) {
      auto ptr = (k < l->key) ? &(l->left) : &(l->right);
      l = ptr->read();
    }
    ptr->validate();
    auto ll = (leaf*) l;
    if (!ll->is_sentinal && ll->key == k) return ll->value; 
    else return {};
  }

  std::optional<V> find(node* root, K k) {
    return flck::with_epoch([&] { return find_(root, k);});
  }

  node* empty() {
      return node_pool.new_obj((node*) leaf_pool.new_obj());
  }

  node* empty(size_t n) { return empty(); }
  
  void print(node* p) {
    std::function<void(node*)> prec;
    prec = [&] (node* p) {
	     if (p->is_leaf) std::cout << p->key << ", ";
	     else {
	       prec((p->left).load());
	       prec((p->right).load());
	     }
	   };
    prec(p->left.load());
    std::cout << std::endl;
  }

  void retire(node* p) {
    if (p == nullptr) return;
    if (p->is_leaf) leaf_pool.retire((leaf*) p);
    else {
      parlay::par_do([&] () { retire((p->left).load()); },
		     [&] () { retire((p->right).load()); });
      node_pool.retire(p);
    }
  }
  
  // return total height
  double total_height(node* p) {
    std::function<size_t(node*, size_t)> hrec;
    hrec = [&] (node* p, size_t depth) {
	     if (p->is_leaf) return depth;
	     size_t d1, d2;
	     parlay::par_do([&] () { d1 = hrec((p->left).load(), depth + 1);},
			    [&] () { d2 = hrec((p->right).load(), depth + 1);});
	     return d1 + d2;
	   };
    return hrec(p->left.load(), 1);
  }

  long check(node* p) {
    using rtup = std::tuple<K, K, long>;
    std::function<rtup(node*)> crec;
    crec = [&] (node* p) {
	     if (p->is_sentinal) return rtup(p->key, p->key, 0);
	     if (p->is_leaf) return rtup(p->key, p->key, 1);
	     K lmin,lmax,rmin,rmax;
	     long lsum, rsum;
	     parlay::par_do([&] () { std::tie(lmin,lmax,lsum) = crec((p->left).load());},
			    [&] () { std::tie(rmin,rmax,rsum) = crec((p->right).load());});
	     if ((lsum !=0 && lmax >= p->key) || rmin < p->key)
	       std::cout << "out of order key: " << lmax << ", " << p->key << ", " << rmin << std::endl;
	     if (lsum == 0) return rtup(p->key, rmax, rsum);
	     else return rtup(lmin, rmax, lsum + rsum);
	   };
    auto [minv, maxv, cnt] = crec(p->left.load());
    //if (verbose) std::cout << "average height = " << ((double) total_height(p) / cnt) << std::endl;
    return cnt;
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
    node_pool.shuffle(n);
    leaf_pool.shuffle(n);
  }

  void stats() {
    node_pool.stats();
    leaf_pool.stats();
  }
  
};
