#ifndef _SEARCHTREE_H
#define _SEARCHTREE_H

#include <vector>

class SearchTree {
public:
    virtual ~SearchTree() { };
    virtual bool insert(long val) = 0;
    virtual bool remove(long val) = 0;
    virtual bool lookup(long val) = 0;
    virtual std::vector<long> rangeQuery(long low, long high) = 0;
};

#endif /* _SEARCHTREE_H */
