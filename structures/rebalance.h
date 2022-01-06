
template <typename Tree>
struct Rebalance {
  Tree* tree;
  using node = typename Tree::node;
  using K = typename Tree::K;
  node* new_node(K k, node* l, node* r) {
    return tree->node_pool.new_obj(k, l, r); }
  void retire_node(node* x) {
    (&(tree->node_pool))->retire(x);
  }
  Rebalance(Tree* tree) : tree(tree) {}
  
  size_t priority(node* v) {
    return parlay::hash64_2(v->key);
  }

  // If priority of c (child) is less than p (parent), then it rotates c
  // above p to ensure priorities are in heap order.  Two new nodes are
  // created and gp (grandparent) is updated to point to the new copy of c.
  // The key k is needed to decide the side of p from gp, and c from p.
  bool fix_priority(node* gp, node* p, node* c, int k) {
    return gp->try_with_lock([=] {
	auto ptr = (k < gp->key) ? &(gp->left) : &(gp->right);
	return (!gp->removed.load() && // gp has not been removed
		ptr->load() == p &&    // p has not changed
		p->try_with_lock([=] {
		    bool on_left = (k < p->key);
		    return ((on_left ? (p->left.load() == c) : // c has not changed
			     (p->right.load() == c)) &&
			    c->try_with_lock([=] {
				if (on_left) {  // rotate right to bring left child up
				  node* nc = new_node(p->key, c->right.load(), p->right.load());
				  (*ptr) = new_node(c->key, c->left.load(), nc);
				} else { // rotate left to bring right child up
				  node* nc = new_node(p->key, p->left.load(), c->left.load());
				  (*ptr) = new_node(c->key, nc, c->right.load());
				}
				// retire the old copies, which have been replaced
				p->removed = true; tree->node_pool.retire(p);
				c->removed = true; retire_node(c);
				return true;
			      }));
		  }));
      });
  }

  void fix_path(node* root, int k) {
    while (true) {
      node* gp = root;
      node* p = (gp->left).load();
      if (p->is_leaf) return;
      node* c = (k < p->key) ? (p->left).load() : (p->right).load();
      while (!c->is_leaf && priority(p) >= priority(c)) {
	gp = p;
	p = c;
	c = (k < p->key) ? (p->left).load() : (p->right).load();
      }
      if (c->is_leaf) return;
      fix_priority(gp, p, c, k);
    }
  }


  void rebalance(node* p, node* root, int k) {
    node* c = (k < p->key) ? (p->left).load() : (p->right).load();
    if (!c->is_leaf) fix_path(root, k);
  }

  void check_balance(node* p, node* l, node* r) {
    if (!l->is_leaf && priority(l) > priority(p))
      std::cout << "bad left priority: " << priority(l) << ", " << priority(p) << std::endl;
    if (!r->is_leaf && priority(r) > priority(p))
      std::cout << "bad right priority: " << priority(r) << ", " << priority(p) << std::endl;
  }
};
