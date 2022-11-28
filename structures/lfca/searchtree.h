#ifndef _SEARCHTREE_H
#define _SEARCHTREE_H

#include <vector>

class SearchTree {
public:
    virtual ~SearchTree() { };
    virtual void insert(int val) = 0;
    virtual bool remove(int val) = 0;
    virtual bool lookup(int val) = 0;
    virtual std::vector<int> rangeQuery(int low, int high) = 0;
};

#endif /* _SEARCHTREE_H */
