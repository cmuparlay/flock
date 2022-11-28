#ifndef _LFCA_H
#define _LFCA_H

#include <atomic>
#include <vector>

#include "searchtree.h"
#include "treap.h"
#include "preallocatable.h"

// Constants
#define CONT_CONTRIB 250          // For adaptation
#define LOW_CONT_CONTRIB 1        // ...
#define RANGE_CONTRIB 100         // ...
#define HIGH_CONT 1000            // ...
#define LOW_CONT -1000            // ...
#define NOT_FOUND (node *)1       // Special pointers
#define NOT_SET (vector<int> *)1  // ...
#define PREPARING (node *)0       // Used for join
#define DONE (node *)1            // ...
#define ABORTED (node *)2         // ...

enum contention_info {
    contended,
    uncontened,
    noinfo
};

enum node_type {
    route,
    normal,
    join_main,
    join_neighbor,
    range
};

// Data Structures
struct rs : public Preallocatable<rs> {                                 // Result storage for range queries
    atomic<vector<int> *> result{NOT_SET};  // The result
    atomic<bool> more_than_one_base{false};

    rs *operator=(const rs &other) {
        result.store(other.result.load());
        more_than_one_base.store(other.more_than_one_base.load());

        return this;
    }

    ~rs() {
        // Delete the result if it was set
        vector<int> *result_local = result.load();
        if (result_local != NOT_SET) {
            delete result_local;
        }
    }
};

struct node : public Preallocatable<node> {
    // route_node
    int key{0};                          // Split key
    atomic<node *> left{nullptr};              // < key
    atomic<node *> right{nullptr};             // >= key
    atomic<bool> valid{true};         // Used for join
    atomic<node *> join_id{nullptr};  // ...

    // normal_base
    Treap *data = nullptr;   // Items in the set
    int stat = 0;            // Statistics variable
    node *parent = nullptr;  // Parent node or NULL (root)

    // join_main
    node *neigh1 = nullptr;                      // First (not joined) neighbor base
    atomic<node *> neigh2{PREPARING};  // Joined n... (neighbor?)
    node *gparent = nullptr;                     // Grand parent
    node *otherb= nullptr;                      // Other branch

    // join_neighbor
    node *main_node = nullptr;  // The main node for the join

    // range_base
    int lo = 0;
    int hi = 0;  // Low and high key
    rs *storage = nullptr;

    // node
    node_type type;

    node *operator=(const node &other) {
        key = other.key;
        left.store(other.left.load());
        right.store(other.right.load());
        valid.store(other.valid.load());
        join_id.store(other.join_id.load());

        data = other.data;
        stat = other.stat;
        parent = other.parent;

        neigh1 = other.neigh1;
        neigh2.store(other.neigh2.load());
        gparent = other.gparent;
        otherb = other.otherb;

        main_node = other.main_node;

        lo = other.lo;
        hi = other.hi;
        storage = other.storage;  // Link to the same result storage, so that all nodes in the same range query contain the result set when it is stored.

        type = other.type;

        return this;
    }
};

class LfcaTree : public SearchTree {
private:
    std::atomic<node *> root{nullptr};

    bool do_update(Treap *(*u)(Treap *, int, bool *), int i);
    std::vector<int> all_in_range(int lo, int hi, rs *help_s);
    bool try_replace(node *b, node *new_b);
    node *secure_join(node *b, bool left);
    void complete_join(node *m);
    node *parent_of(node *n);
    void adapt_if_needed(node *b);
    void low_contention_adaptation(node *b);
    void high_contention_adaptation(node *b);
    void help_if_needed(node *n);

public:
    LfcaTree();

    void insert(int val);
    bool remove(int val);
    bool lookup(int val);
    std::vector<int> rangeQuery(int low, int high);
};

#endif /* _LFCA_H */
