#ifndef TREAP_H
#define TREAP_H

#include "preallocatable.h"

#include <algorithm>
#include <ctime>
#include <iterator>
#include <limits>
#include <random>

using namespace std;

#define TREAP_NODES 64

typedef long TreapIndex;
const TreapIndex NullNode = -1;
const TreapIndex ControlNode = TREAP_NODES;  // The extra node allocated beyond the size of the treap

class Treap : public Preallocatable<Treap> {
private:
    struct TreapNode {
        long val;
        long payload;
        long weight;

        TreapIndex parent {NullNode};
        TreapIndex left {NullNode};
        TreapIndex right {NullNode};
    };

    struct TreapTransferInfo {
        bool isLeftChild;
        TreapIndex newParentIndex;
        TreapIndex originalIndex;
    };

    int size {0};
    TreapNode nodes[TREAP_NODES + 1];
    TreapIndex root {NullNode};

    void moveNode(TreapIndex srcIndex, TreapIndex dstIndex);

    TreapIndex createNewNode(long val);
    TreapIndex transferNodesFrom(Treap *other, TreapIndex rootIndex);

    void bstInsert(TreapIndex index);
    TreapIndex bstFind(long val);

    void leftRotate(TreapIndex index);
    void rightRotate(TreapIndex index);

    void moveUp(TreapIndex index);
    void moveDown(TreapIndex index);

    bool insert(long val);
    bool remove(long val);

    long getMedianVal();

public:
    Treap *immutableInsert(long val, bool *success);
    Treap *immutableRemove(long val, bool *success);

    bool contains(long val);

    vector<long> rangeQuery(long min, long max);

    int getSize();
    long getMaxValue();

    static Treap *merge(Treap *left, Treap *right);
    long split(Treap **left, Treap **right);

    void sequentialInsert(long val);
    bool sequentialRemove(long val);

    long getRoot();

    Treap *operator=(const Treap &other);
};

#endif /* TREAP_H */
