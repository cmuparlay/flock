#include <limits>
#include <algorithm>
#include <cstdlib>
#include "lock.h"

// no parent pointers, serach to key, doesn't work likely because violations are getting rotated off search path
// on read only workloads, it is faster than leaftree and natarajan but slower than bronson, probably because it is external.
// TODO: check that bronson has lower average height than this tree
// inserts and removes are very slow, probably due to the unoptimized rotation code.

// why is this AVL tree significantly less balanced than a red-black tree?

// TODO: minimize number of writes to the log

template <typename K, typename V>
struct Set {

  K key_min = std::numeric_limits<K>::min();
  
  struct node {
    K key;
    bool is_leaf;
    write_once<bool> removed;
    mutable_val<node*> left;
    mutable_val<node*> right;
    mutable_val<int32_t> lefth; // TODO: see if you can get away with storing 1 height
    mutable_val<int32_t> righth;
    lock lck;   
    node(K k, node* left, node* right, int32_t lefth, int32_t righth)
      : key(k), is_leaf(false), left(left), right(right), lefth(lefth), righth(righth), removed(false) {};
  };

  struct leaf {
    K key;
    bool is_leaf;
    V value;
    // mutable_val<node*> parent;
    // leaf(K k, V v, node* p) : key(k), value(v), is_leaf(true), parent(p) {};
    leaf(K k, V v) : key(k), value(v), is_leaf(true) {};
  };

  int32_t height(node* n) {
    if(n->is_leaf) return 1;
    return 1 + std::max(n->lefth.load(), n->righth.load());
  }

  memory_pool<node> node_pool;
  memory_pool<leaf> leaf_pool;

  size_t max_iters = 10000000;
  
  auto find_location(node* root, K k) {
    int cnt = 0;
    node* gp = nullptr;
    bool gp_left = false;
    node* p = root;
    bool p_left = true;
    node* l = (p->left).load();
    while (!l->is_leaf) {
      if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
      gp = p;
      gp_left = p_left;
      p = l;
      p_left = (k < p->key);
      l = p_left ? (p->left).load() : (p->right).load();
    }
    return std::make_tuple(gp, gp_left, p, p_left, l);
  }

  bool correctHeight(node* n) {
    if(n->is_leaf) return true;
    return n->lefth.load() == height(n->left.load()) && 
           n->righth.load() == height(n->right.load());
  }

  int32_t balance(node* n) {
    if(n->is_leaf) return 0;
    return n->lefth.load() - n->righth.load();
  }

  bool noViolations(node* n) {
    return std::abs(balance(n)) <= 1;
  }

  void fixHeight(node* n) {
    // std::cout << "fixed height" << std::endl;
    try_lock(n->lck, [=] () {
      if(n->removed.load()) return false;
      n->lefth = height(n->left.load());
      n->righth = height(n->right.load());
      return true;
    });
  }

  // remember to retire nodes in the rotate methods.

  void rotate(node* p, node* n, node* l, bool rotateRight) {
    bool p_left = (p->left.load() == n);
    try_lock(p->lck, [=] () {
      if(p->removed.load() || n != (p_left ? p->left.load() : p->right.load())) return false;
      return try_lock(n->lck, [=] () {
        if(n->removed.load() || !correctHeight(n) || l != (rotateRight ? n->left.load() : n->right.load()) || (rotateRight && balance(n) < 2) || (!rotateRight && balance(n) > -2)) return false;
        return try_lock(l->lck, [=] () {
          if(l->removed.load() || (rotateRight && balance(l) < 0) || (!rotateRight && balance(l) > 0)) return false;
          node* new_n, *new_l;
          if(rotateRight) {
            new_n = node_pool.new_obj(n->key, l->right.load(), n->right.load(), l->righth.load(), n->righth.load());
            new_l = node_pool.new_obj(l->key, l->left.load(), new_n, l->lefth.load(), height(new_n));
          } else {
            new_n = node_pool.new_obj(n->key, n->left.load(), l->left.load(), n->lefth.load(), l->lefth.load());
            new_l = node_pool.new_obj(l->key, new_n, l->right.load(), height(new_n), l->righth.load());
          }
          if(p_left) p->left = new_l;
          else p->right = new_l;
          n->removed = true; node_pool.retire(n);
          l->removed = true; node_pool.retire(l);
          return true;
        });
      });
    });
  }

  void doubleRotate(node* p, node* n, node* l, bool rotateLR) {
    bool p_left = (p->left.load() == n);
    try_lock(p->lck, [=] () {
      if(p->removed.load() || n != (p_left ? p->left.load() : p->right.load())) return false;
      return try_lock(n->lck, [=] () {
        if(n->removed.load() || !correctHeight(n) || l != (rotateLR ? n->left.load() : n->right.load()) || (rotateLR && balance(n) < 2) || (!rotateLR && balance(n) > -2)) return false;
        return try_lock(l->lck, [=] () {
          if(l->removed.load() || !correctHeight(l) || (rotateLR && balance(l) >= 0) || (!rotateLR && balance(l) <= 0)) return false;
          node* cc = rotateLR ? l->right.load() : l->left.load();
          if(cc->is_leaf) return false;
          return try_lock(cc->lck, [=] () { 
            if(cc->removed.load()) return false;
            node* new_n, *new_l, *new_cc;
            if(rotateLR) {
              new_n = node_pool.new_obj(n->key, cc->right.load(), n->right.load(), cc->righth.load(), n->righth.load());
              new_l = node_pool.new_obj(l->key, l->left.load(), cc->left.load(), l->lefth.load(), cc->lefth.load());
              new_cc = node_pool.new_obj(cc->key, new_l, new_n, height(new_l), height(new_n));
            } else {
              new_n = node_pool.new_obj(n->key, n->left.load(), cc->left.load(), n->lefth.load(), cc->lefth.load());
              new_l = node_pool.new_obj(l->key, cc->right.load(), l->right.load(), cc->righth.load(), l->righth.load());
              new_cc = node_pool.new_obj(cc->key, new_n, new_l, height(new_n), height(new_l));
            }
            if(p_left) p->left = new_cc;
            else p->right = new_cc;
            n->removed = true; node_pool.retire(n);
            l->removed = true; node_pool.retire(l);
            cc->removed = true; node_pool.retire(cc);
            return true;            
          });
        });
      });
    });
  }

  void fixViolations(node* p, node* n) {
    // std::cout << "fixed violation" << std::endl;
    if(balance(n) >= 2) {
      node* c = n->left.load();
      if(c->is_leaf) return; // c can only be a leaf of n's height info is out of date
      if(!correctHeight(c)) fixHeight(c);
      if(balance(c) >= 0) 
        rotate(p, n, c, true); // make sure n.lefth is correct before rotating
      else
        doubleRotate(p, n, c, true);
      // else return rotateLeftRight(n); // make sure n.lefth and n.left.righth are correct before rotating
    } else if(balance(n) <= -2) {
      node* c = n->right.load();
      if(c->is_leaf) return; // c can only be a leaf of n's height info is out of date
      if(!correctHeight(c)) fixHeight(c);
      if(balance(c) <= 0) 
        rotate(p, n, c, false); // make sure n.righth is correct before rotating
      else
        doubleRotate(p, n, c, false);
      // else return rotateRightLeft(n); // make sure n.righth and n.right.lefth are correct before rotating
    }
  }

  // // traverses entire subtree and fixes violations
  // // returns whether or not a violation is found
  // bool fixAll(node* p, node* n) {
  //   if(n->is_leaf) return false;
  //   bool violationFound = false;
  //   if(!correctHeight(n)) {
  //     fixHeight(n);
  //     violationFound = true;
  //   }
  //   if(!noViolations(n)) {
  //     fixViolations(p, n);
  //     violationFound = true;
  //   }
  //   if(violationFound) return true;
  //   else return fixAll(n, n->left.load()) || fixAll(n, n->right.load());
  // }

  int rec[1000006];
  void printTreeHelper(node* p, int depth) {
    if(p == nullptr) return;
    std::cout << "\t";
    for(int i = 0; i < depth; i++)
        if(i == depth-1)
            std::cout << (rec[depth-1]?"\u0371":"\u221F") << "\u2014\u2014\u2014";
        else
            std::cout << (rec[i]?"\u23B8":"  ") << "   ";
    std::cout << p->key;
    if(p->is_leaf) {
        std::cout << std::endl;
        return;
    }
    if(p->removed) std::cout << "'";
    if(p->lck.lck != nullptr) std::cout << "L";
    std::cout << " (" << p->lefth << ", " << p->righth << ")" << std::endl;

    rec[depth] = 1;
    printTreeHelper(p->left, depth+1);
    rec[depth] = 0;
    printTreeHelper(p->right, depth+1);
  }
  
  void printTree(node* p) {
    printTreeHelper(p, 0);
  }

  // TODO: check how many times the outer loop gets executed per update operation
  // TODO: optimize to fix more than one violation each traversal
  // correctness: in a single threaded executions, heights are always fixed before violations (sortof)
  void fixToKey(node* root, K k) {
    node *p, *n;
    while(true) {
      p = root;
      n = root->left.load();
      node* nodeWithViolation = nullptr;
      node* parent = nullptr;
      bool heightViolation;
      while(!n->is_leaf) {
        if(!correctHeight(n)) {
          nodeWithViolation = n;
          heightViolation = true;
        }
        else if(!noViolations(n)) {
          nodeWithViolation = n;
          parent = p;
          heightViolation = false;
        }
        p = n;
        n = (k < n->key) ? n->left.load() : n->right.load();
      }
      if(nodeWithViolation == nullptr) break; // no vioaltions found
      if(heightViolation) fixHeight(nodeWithViolation);
      else fixViolations(parent, nodeWithViolation);
      // printTree(root->left.load());
    }
  }
  
  // bool foundViolations = false;

  bool insert(node* root, K k, V v) {
    // std::cout << "insert " << k << std::endl;
    return with_epoch([=] {
      int cnt = 0;
      while (true) {
        auto [gp, gp_left, p, p_left, l] = find_location(root, k);
        if (k == l->key) return false;
        if (try_lock(p->lck, [=] () {
              auto ptr = p_left ? &(p->left) : &(p->right);
              if (p->removed.load() || ptr->load() != l) return false;
              node* new_l = (node*) leaf_pool.new_obj(k, v);
              node* new_internal = ((k > l->key) ? 
                                     node_pool.new_obj(k, l, new_l, 1, 1) :
                                     node_pool.new_obj(l->key, new_l, l, 1, 1));
              // new_l.parent = new_internal;
              (*ptr) = new_internal;
              return true;
            })) {
          fixToKey(root, k);
          // while(fixAll(root, root->left.load())) {}
          return true;
        }
        if (cnt++ > max_iters) {
          std::cout << "too many iters" << std::endl; abort();
        }
      }
    });
    // check(root);
    // if(foundViolations) abort();
  }

  bool remove(node* root, K k) {
    return with_epoch([=] {
       int cnt = 0;
       while (true) {
   auto [gp, gp_left, p, p_left, l] = find_location(root, k);
   if (k != l->key) return false;
   if (try_lock(gp->lck, [=] () {
       return try_lock(p->lck, [=] () {
            auto ptr = gp_left ? &(gp->left) : &(gp->right);
            if (gp->removed.load() || ptr->load() != p) return false;
            node* ll = (p->left).load();
            node* lr = (p->right).load();
            if (p_left) std::swap(ll,lr);
            if (lr != l) return false;
            p->removed = true;
            node_pool.retire(p);
            leaf_pool.retire((leaf*) l);
            (*ptr) = ll; // shortcut
            return true;
        });}))  {
     fixToKey(root, k);
     // while(fixAll(root, root->left.load())) {}
     return true;
   }
   if (cnt++ > max_iters) {std::cout << "too many iters" << std::endl; abort();}
       }}); 
  }

  std::optional<V> find(node* root, K k) {
    return with_epoch([&] () -> std::optional<V> {
        node* l = (root->left).load();
        while (!l->is_leaf)
          l = (k < l->key) ? (l->left).load() : (l->right).load();
        auto ll = (leaf*) l;
        if (ll->key == k) return ll->value; 
        else return {};
    });
  }

  node* empty() {
    node* l = (node*) leaf_pool.new_obj(key_min, 0);
    node* root = node_pool.new_obj(0, l, nullptr, 1, 0);
    return root;
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
       if (p->is_leaf) return rtup(p->key, p->key, 1);
       if(p->lefth.load() != height(p->left.load())) {
        std::cout << "left height incorrect\n";
        // foundViolations = true;
       }
        
       if(p->righth.load() != height(p->right.load())) {
        std::cout << "right height incorrect\n";
        // foundViolations = true;
       }
        
       if(!noViolations(p)) {
        std::cout << "AVL tree property violated\n";
        // foundViolations = true;
       }
        
       K lmin,lmax,rmin,rmax;
       long lsum, rsum;
       parlay::par_do([&] () { std::tie(lmin,lmax,lsum) = crec((p->left).load());},
          [&] () { std::tie(rmin,rmax,rsum) = crec((p->right).load());});
       if (lmax >= p->key || rmin < p->key)
         std::cout << "out of order key: " << lmax << ", " << p->key << ", " << rmin << std::endl;
       return rtup(lmin, rmax, lsum + rsum);
     };
    auto [minv, maxv, cnt] = crec(p->left.load());
    if (verbose) std::cout << "average height = " << ((double) total_height(p) / cnt) << std::endl;
    return cnt-1;
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
