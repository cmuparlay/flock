#include <iostream>
#include <limits>
#include <mrlock.h>
#include <stack>

#include "mrlocktree.h"

const long Empty = numeric_limits<long>::min();
const int TreapSplitThreshold = TREAP_NODES;
const int TreapMergeThreshold = TREAP_NODES / 2;

using namespace std;

MrlockTree::MrlockTree() : mrlock(1) {
    // Set up the initial head as a base node
    head = new Node(Empty);
    head->treap = Treap::New();
    head->isRoute = false;

    // Set up the tree lock
    treeLock.Resize(1);
    treeLock.Set(1);
}

MrlockTree::~MrlockTree() {
    if (head != NULL) {
        // Recursively delete all nodes
        stack<Node *> nodeStack;
        nodeStack.push(head);

        while (!nodeStack.empty()) {
            Node *currentNode = nodeStack.top();
            nodeStack.pop();

            // Add this node's children, if they exist
            if (currentNode->left != NULL) {
                nodeStack.push(currentNode->left);
            }
            if (currentNode->right != NULL) {
                nodeStack.push(currentNode->right);
            }

            // Delete this node
            delete currentNode;
        }
    }
}

bool MrlockTree::insert(long val) {
    // Acquire the lock
    ScopedMrLock lock(&mrlock, treeLock);

    Node *temp = head;

    // Search until a base node is found
    while (temp->isRoute) {
        if (temp->val >= val) {
            temp = temp->left;
        }
        else {
            temp = temp->right;
        }
    }

    // Insert the value
    bool success;
    temp->treap = temp->treap->immutableInsert(val, &success);

    // If inserting causes the treap to become too large, split it in two
    if (temp->treap->getSize() >= TreapSplitThreshold) {
        Node *left = new Node(Empty);
        left->isRoute = false;

        Node *right = new Node(Empty);
        right->isRoute = false;

        long splitVal = temp->treap->split(&left->treap, &right->treap);

        temp->val = splitVal;
        temp->isRoute = true;
        temp->left = left;
        temp->right = right;

        temp->treap = nullptr;
    }
    return success;
}

bool MrlockTree::remove(long val) {
    // Acquire the lock
    ScopedMrLock lock(&mrlock, treeLock);

    Node *temp = head;
    Node *tempParent = nullptr;

    // Search until a base node is found
    while (temp->isRoute) {
        if (temp->val >= val) {
            tempParent = temp;
            temp = temp->left;
        }
        else {
            tempParent = temp;
            temp = temp->right;
        }
    }

    // Perform the remove
    bool success;
    temp->treap = temp->treap->immutableRemove(val, &success);

    // Check if a merge is possible. This is when the node has a parent, and the node's sibling is also a base node
    bool mergeIsPossible = tempParent != nullptr && !tempParent->left->isRoute && !tempParent->right->isRoute;

    if (mergeIsPossible) {
        // Check if the two nodes are small enough to be merged
        int combinedSize = tempParent->left->treap->getSize() + tempParent->right->treap->getSize();
        if (combinedSize <= TreapMergeThreshold) {
            tempParent->treap = Treap::merge(tempParent->left->treap, tempParent->right->treap);
            tempParent->isRoute = false;

            delete(tempParent->left);
            tempParent->left = nullptr;

            delete(tempParent->right);
            tempParent->right = nullptr;
        }
    }

    return success;
}

bool MrlockTree::lookup(long val) {
    // Acquire the lock
    ScopedMrLock lock(&mrlock, treeLock);

    Node *temp = head;

    // Search until a base node is found
    while (temp->isRoute) {
        if (temp->val >= val) {
            temp = temp->left;
        }
        else {
            temp = temp->right;
        }
    }

    return temp->treap->contains(val);
}

vector<long> MrlockTree::rangeQuery(long low, long high) {
    // Acquire the lock
    ScopedMrLock lock(&mrlock, treeLock);

    vector<long> result;
    vector<Node *> nodesToCheck;
    nodesToCheck.push_back(head);

    while (!nodesToCheck.empty()) {
        Node *temp = nodesToCheck.back();
        nodesToCheck.pop_back();

        // If popped node is base node, perform range query on treap and add it to result.
        if (!temp->isRoute) {
            vector<long> values = temp->treap->rangeQuery(low, high);
            result.insert(result.end(), values.begin(), values.end());
            continue;
        }

        // Check values to the left if this value is not smaller than the minimum value
        if (temp->val >= low) {
            nodesToCheck.push_back(temp->left);
        }

        // Check values to the right if this value is not larger than the maximum value
        if (temp->val <= high) {
            nodesToCheck.push_back(temp->right);
        }
    }

    return result;
}
