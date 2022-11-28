/**
 * The code in this file is from "Lock-free contention adapting search trees",
 * by Kjell Winblad, Konstantinos Sagonas, and Jonsson, Bengt, with slight
 * modification to format and syntax for use with the C++ project.
 *
 * Major modifications:
 * The node structs are combined into a single struct
 * C utilities used in the original implementation, such as stack, now use C++ standard library variants
 * Range query results are stored in vectors instead of treaps
 * Our custom immutable treaps are used in place of the original
 * High contention adaptations (splits) are forced when a treap has reached the maximum size due to our fixed-size treaps
 * Search order has been modified so that the left child can contain all values less than *or equal to* the route node's value, as opposed to strictly less than.
 */

#include "lfca.h"

#include <stack>

using namespace std;

// Forward declare helper functions as needed
node *find_base_stack(node *n, int i, stack<node *> *s);
node *find_base_node(node *n, int i);
node *leftmost_and_stack(node *n, stack<node *> *s);

// Helper functions for do_update
Treap *treap_insert(Treap *treap, int val, bool *result) {
    Treap *newTreap = treap->immutableInsert(val);
    *result = true;  // Inserts always succeed
    return newTreap;
}

Treap *treap_remove(Treap *treap, int val, bool *result) {
    Treap *newTreap = treap->immutableRemove(val, result);
    return newTreap;
}

// Undefined functions that need implementations:

// This function is undefined in the pdf, assume replaces head of stack with n?
void replace_top(stack<node *> *s, node *n) {
    s->pop();
    s->push(n);
    return;
}

// Assuming this finds the leftmost node for a given node (follow left pointer until the end)
node *leftmost(node *n) {
    node *temp = n;
    while (temp->left != nullptr)
        temp = temp->left;
    return temp;
}

// Opposite version of leftmost for secure_join_right
node *rightmost(node *n) {
    node *temp = n;
    while (temp->right != nullptr)
        temp = temp->right;
    return temp;
}

// Help functions
bool LfcaTree::try_replace(node *b, node *new_b) {
    node *expectedB = b;

    if (b->parent == nullptr) {
        return root.compare_exchange_strong(expectedB, new_b);
    }
    else if (b->parent->left.load() == b) {
        return b->parent->left.compare_exchange_strong(expectedB, new_b);
    }
    else if (b->parent->right.load() == b) {
        return b->parent->right.compare_exchange_strong(expectedB, new_b);
    }

    return false;
}

bool is_replaceable(node *n) {
    switch (n->type) {
        case normal:
            return true;

        case join_main:
            return n->neigh2.load() == ABORTED;

        case join_neighbor: {
            node *neigh2local = n->main_node->neigh2.load();
            return (neigh2local == ABORTED || neigh2local == DONE);
        }

        case range:
            return n->storage->result.load() != NOT_SET;

        default:
            return false;
    }
}

// Help functions
void LfcaTree::help_if_needed(node *n) {
    if (n->type == join_neighbor) {
        n = n->main_node;
    }

    if (n->type == join_main && n->neigh2.load() == PREPARING) {
        node *expectedNeigh2 = PREPARING;
        n->neigh2.compare_exchange_strong(expectedNeigh2, ABORTED);
    }
    else if (n->type == join_main && n->neigh2.load() > ABORTED) {
        complete_join(n);
    }
    else if (n->type == range && n->storage->result.load() == NOT_SET) {
        all_in_range(n->lo, n->hi, n->storage);
    }
}

int new_stat(node *n, contention_info info) {
    int range_sub = 0;
    if (n->type == range && n->storage->more_than_one_base.load()) {
        range_sub = RANGE_CONTRIB;
    }

    if (info == contended && n->stat <= HIGH_CONT) {
        return n->stat + CONT_CONTRIB - range_sub;
    }

    if (info == uncontened && n->stat >= LOW_CONT) {
        return n->stat - LOW_CONT_CONTRIB - range_sub;
    }

    return n->stat;
}

void LfcaTree::adapt_if_needed(node *b) {
    if (!is_replaceable(b)) {
        return;
    }
    else if (new_stat(b, noinfo) > HIGH_CONT) {
        high_contention_adaptation(b);
    }
    else if (new_stat(b, noinfo) < LOW_CONT) {
        low_contention_adaptation(b);
    }
}

bool LfcaTree::do_update(Treap *(*u)(Treap *, int, bool *), int i) {
    contention_info cont_info = uncontened;

    while (true) {
        node *base = find_base_node(root.load(), i);

        // If the treap is full, try to split the node and retry the insert
        if (base->data->getSize() >= TREAP_NODES) {
            high_contention_adaptation(base);
            continue;
        }

        if (is_replaceable(base)) {
            bool res;

            node *newb = node::New();
            newb->type = normal;
            newb->parent = base->parent;
            newb->data = u(base->data, i, &res);
            newb->stat = new_stat(base, cont_info);

            if (try_replace(base, newb)) {
                adapt_if_needed(newb);
                return res;
            }
        }

        cont_info = contended;
        help_if_needed(base);
    }
}

// Public interface
LfcaTree::LfcaTree() {
    // Create root node
    node *rootNode = node::New();
    rootNode->type = normal;
    rootNode->data =  Treap::New();
    root.store(rootNode);
}

void LfcaTree::insert(int i) {
    do_update(treap_insert, i);
}

bool LfcaTree::remove(int i) {
    return do_update(treap_remove, i);
}

bool LfcaTree::lookup(int i) {
    node *base = find_base_node(root.load(), i);
    return base->data->contains(i);
}

vector<int> LfcaTree::rangeQuery(int lo, int hi) {
    return all_in_range(lo, hi, nullptr);
}

// Range query helper
node *find_next_base_stack(stack<node *> *s) {
    node *base = s->top();
    s->pop();

    if (s->empty()) {
        return nullptr;
    }

    node *t = s->top();

    if (t->left.load() == base) {
        return leftmost_and_stack(t->right.load(), s);
    }

    int be_greater_than = t->key;
    while (true) {
        if (t->valid.load() && (t->key > be_greater_than)) {
            return leftmost_and_stack(t->right.load(), s);
        }
        else {
            s->pop();

            // Stop looping if the stack is empty
            if (s->empty()) {
                break;
            }

            t = s->top();
        }
    }

    return nullptr;
}

node *new_range_base(node *b, int lo, int hi, rs *s) {
    // Copy the other node
    node *new_base = node::New(*b);

    // Set fields
    new_base->lo = lo;
    new_base->hi = hi;
    new_base->storage = s;

    return new_base;
}

vector<int> LfcaTree::all_in_range(int lo, int hi, rs *help_s) {
    stack<node *> s;
    stack<node *> backup_s;
    vector<node *> done;
    node *b;
    rs *my_s;

find_first:
    b = find_base_stack(root.load(), lo, &s);
    if (help_s != nullptr) {
        if (b->type != range || help_s != b->storage) {
            return *help_s->result.load();
        }
        else {
            my_s = help_s;
        }
    }
    else if (is_replaceable(b)) {
        my_s = rs::New();
        node *n = new_range_base(b, lo, hi, my_s);

        if (!try_replace(b, n)) {
            goto find_first;
        }

        replace_top(&s, n);
    }
    else if (b->type == range && b->hi >= hi) {
        return all_in_range(b->lo, b->hi, b->storage);
    }
    else {
        help_if_needed(b);
        goto find_first;
    }

    while (true) {  // Find remaining base nodes
        done.push_back(b);
        backup_s = s;  // Backup the result set

        // Stop looping if this treap is the last treap to consider for the range query
        if (!(b->data->getSize() == 0)) {
            if (b->data->getMaxValue() >= hi) {
                break;
            }
        }

    find_next_base_node:
        b = find_next_base_stack(&s);
        if (b == nullptr) {
            break;
        }
        else if (my_s->result.load() != NOT_SET) {
            return *my_s->result.load();
        }
        else if (b->type == range && b->storage == my_s) {
            continue;
        }
        else if (is_replaceable(b)) {
            node *n = new_range_base(b, lo, hi, my_s);

            if (try_replace(b, n)) {
                replace_top(&s, n);
                continue;
            }
            else {
                s = backup_s;  // Restore the result set from backup
                goto find_next_base_node;
            }
        }
        else {
            help_if_needed(b);
            s = backup_s;  // Restore the result set from backup
            goto find_next_base_node;
        }
    }

    // stack_array[0] gets the item at the bottom of the stack. Replicate this with a vector
    vector<int> *res = new vector<int>(done.front()->data->rangeQuery(lo, hi));  // done->stack_array[0]->data;
    for (size_t i = 1; i < done.size(); i++) {
        vector<int> resTemp = done.at(i)->data->rangeQuery(lo, hi);
        res->insert(end(*res), begin(resTemp), end(resTemp));
    }

    vector<int> *expectedResult = NOT_SET;
    if (my_s->result.compare_exchange_strong(expectedResult, res)) {
        if (done.size() > 1) {
            my_s->more_than_one_base.store(true);
        }
    }
    else {
        // The result set was already stored. Clean up the local result
        delete res;
    }

    // This call, which randomly adapts a base node from the range query, is ignored.
    // adapt_if_needed(t, done->array[r() % done->size]);

    return *my_s->result.load();
}

// Contention adaptation
node *LfcaTree::secure_join(node *b, bool left) {
    node *n0;
    if (left) {
        n0 = leftmost(b->parent->right.load());
    }
    else {
        n0 = rightmost(b->parent->left.load());
    }

    if (!is_replaceable(n0)) {
        return nullptr;
    }

    // Make sure that the two treaps are small enough to be merged
    if (b->data->getSize() + n0->data->getSize() > TREAP_NODES) {
        return nullptr;
    }

    node *m = node::New(*b);  // Copy b
    m->type = join_main;

    node *expectedNode = b;
    if (left) {
        if (!b->parent->left.compare_exchange_strong(expectedNode, m)) {
            return nullptr;
        }
    }
    else {
        if (!b->parent->right.compare_exchange_strong(expectedNode, m)) {
            return nullptr;
        }
    }

    node *n1 = node::New(*n0);  // Copy n0
    n1->type = join_neighbor;
    n1->main_node = m;

    if (!try_replace(n0, n1)) {
        m->neigh2.store(ABORTED);
        return nullptr;
    }

    expectedNode = nullptr;
    if (!m->parent->join_id.compare_exchange_strong(expectedNode, m)) {
        m->neigh2.store(ABORTED);
        return nullptr;
    }

    node *gparent = parent_of(m->parent);
    expectedNode = nullptr;
    if (gparent == NOT_FOUND || (gparent != nullptr && !gparent->join_id.compare_exchange_strong(expectedNode, m))) {
        m->parent->join_id.store(nullptr);
        m->neigh2.store(ABORTED);
        return nullptr;
    }

    m->gparent = gparent;
    if (left) {
        m->otherb = m->parent->right.load();
    }
    else {
        m->otherb = m->parent->left.load();
    }
    m->neigh1 = n1;

    node *joinedp = m->otherb == n1 ? gparent : n1->parent;
    node *newNeigh2 = node::New(*n1);  // Copy n1
    newNeigh2->type = join_neighbor;
    newNeigh2->parent = joinedp;
    newNeigh2->main_node = m;

    if (left) {
        // The main node has smaller values
        newNeigh2->data = Treap::merge(m->data, n1->data);
    }
    else {
        // The main node has larger values
        newNeigh2->data = Treap::merge(n1->data, m->data);
    }

    expectedNode = PREPARING;
    if (m->neigh2.compare_exchange_strong(expectedNode, newNeigh2)) {
        return m;
    }

    if (gparent != nullptr) {
        gparent->join_id.store(nullptr);
    }

    m->parent->join_id.store(nullptr);
    m->neigh2.store(ABORTED);
    return nullptr;
}

void LfcaTree::complete_join(node *m) {
    node *n2 = m->neigh2.load();
    if (n2 == DONE) {
        return;
    }

    try_replace(m->neigh1, n2);

    m->parent->valid.store(false);

    node *replacement = m->otherb == m->neigh1 ? n2 : m->otherb;
    if (m->gparent == nullptr) {
        node *expected = m->parent;
        root.compare_exchange_strong(expected, replacement);
    }
    else if (m->gparent->left.load() == m->parent) {
        node *expected = m->parent;
        m->gparent->left.compare_exchange_strong(expected, replacement);

        expected = m;
        m->gparent->join_id.compare_exchange_strong(expected, nullptr);
    }
    else if (m->gparent->right.load() == m->parent) {
        node *expected = m->parent;
        m->gparent->right.compare_exchange_strong(expected, replacement);

        expected = m;
        m->gparent->join_id.compare_exchange_strong(expected, nullptr);
    }

    m->neigh2.store(DONE);
}

void LfcaTree::low_contention_adaptation(node *b) {
    if (b->parent == nullptr) {
        return;
    }

    if (b->parent->left.load() == b) {
        node *m = secure_join(b, true);
        if (m != nullptr) {
            complete_join(m);
        }
    }
    else if (b->parent->right.load() == b) {
        node *m = secure_join(b, false);
        if (m != nullptr) {
            complete_join(m);
        }
    }
}

void LfcaTree::high_contention_adaptation(node *b) {
    // Don't split treaps that have too few items
    if (b->data->getSize() < 2) {
        return;
    }

    // Create new route node
    node *r = node::New();
    r->type = route;
    r->valid = true;

    // Split the treap
    Treap *leftTreap;
    Treap *rightTreap;
    int splitVal = b->data->split(&leftTreap, &rightTreap);

    // Create left base node
    node *leftNode = node::New();
    leftNode->type = normal;
    leftNode->parent = r;
    leftNode->stat = 0;
    leftNode->data = leftTreap;

    // Create right base node
    node *rightNode = node::New();
    rightNode->type = normal;
    rightNode->parent = r;
    rightNode->stat = 0;
    rightNode->data = rightTreap;

    // Add the treaps to the route node
    r->key = splitVal;
    r->left = leftNode;
    r->right = rightNode;

    try_replace(b, r);
}

// Auxilary functions
node *find_base_node(node *n, int i) {
    while (n->type == route) {
        if (i <= n->key) {
            n = n->left.load();
        }
        else {
            n = n->right.load();
        }
    }

    return n;
}

node *find_base_stack(node *n, int i, stack<node *> *s) {
    // Empty the stack
    while (s->size() > 0) {
        s->pop();
    }

    while (n->type == route) {
        s->push(n);

        if (i < n->key) {
            n = n->left.load();
        }
        else {
            n = n->right.load();
        }
    }

    s->push(n);
    return n;
}

node *leftmost_and_stack(node *n, stack<node *> *s) {
    while (n->type == route) {
        s->push(n);
        n = n->left.load();
    }

    s->push(n);
    return n;
}

node *LfcaTree::parent_of(node *n) {
    node *prev_node = nullptr;
    node *curr_node = root.load();

    while (curr_node != n && curr_node->type == route) {
        prev_node = curr_node;
        if (n->key < curr_node->key) {
            curr_node = curr_node->left.load();
        }
        else {
            curr_node = curr_node->right.load();
        }
    }

    // This is copied from the LFCA article. This restricts the function to only finding only the parent of route nodes. It could check if `curr_node` is not `n` instead.
    if (curr_node->type != route) {
        return NOT_FOUND;
    }

    return prev_node;
}
