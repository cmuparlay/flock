/**
 * @file treap.cpp
 *
 * An immutable treap that stores integers. The immutable operations are thread safe.
 * Algorithms based on pseudo-code from https://algorithmtutor.com/Data-Structures/Tree/Treaps/
 *
 */

#include "treap.h"

static const int NegInfinity = numeric_limits<int>::min();
static const int PosInfinity = numeric_limits<int>::max();

static thread_local mt19937 randEngine{(unsigned int)time(NULL)};
static uniform_int_distribution<int> weightDist{NegInfinity + 1, PosInfinity - 1};

/**
 * Copies another Treap
 *
 * @param other
 * The treap to copy
 */
Treap *Treap::operator=(const Treap &other) {
    // Copy nodes from other to self
    copy(begin(other.nodes), end(other.nodes), begin(nodes));

    size = other.size;
    root = other.root;

    return this;
}

/**
 * Moves a node from one index to another
 * 
 * @param srcIndex
 * The node index to move
 * 
 * @param dstIndex
 * The new node index to be replaced
 */
void Treap::moveNode(int srcIndex, int dstIndex) {
    if (srcIndex == dstIndex) {
        return;
    }

    TreapIndex leftIndex = nodes[srcIndex].left;
    TreapIndex rightIndex = nodes[srcIndex].right;
    TreapIndex parentIndex = nodes[srcIndex].parent;

    // Transfer the node over
    nodes[dstIndex] = nodes[srcIndex];

    // Fix children pointers
    if (leftIndex != NullNode) {
        nodes[leftIndex].parent = dstIndex;
    }
    if (rightIndex != NullNode) {
        nodes[rightIndex].parent = dstIndex;
    }

    // Fix parent
    if (parentIndex == NullNode) {
        // Update the root to reflect the moved node
        root = dstIndex;
    }
    else {
        // Determine if the source node was the right or left child before being moved
        if (nodes[parentIndex].left == srcIndex) {
            nodes[parentIndex].left = dstIndex;
        }
        else {
            nodes[parentIndex].right = dstIndex;
        }
    }
}

/**
 * Creates a new node and initializes it
 * 
 * @param val
 * The value of the node
 * 
 * @return TreapIndex
 * The index of the created node
 */
TreapIndex Treap::createNewNode(int val) {
    TreapIndex newNodeIndex = size++;
    Treap::TreapNode *newNode = &nodes[newNodeIndex];
    newNode->val = val;
    newNode->weight = weightDist(randEngine);
    newNode->parent = NullNode;
    newNode->left = NullNode;
    newNode->right = NullNode;

    return newNodeIndex;
}

/**
 * Transfers all nodes from another treap to the current treap, starting from a given index.
 * This is different from copying the treap or calling insert with each node, as it preserves the original structure of the transferred nodes.
 * 
 * @param other
 * The treap to transfer from
 * 
 * @param rootIndex
 * The index to begin transferring from
 * 
 * @return TreapIndex
 * The index of the root of the transferred nodes, in the current treap
 */
TreapIndex Treap::transferNodesFrom(Treap *other, TreapIndex rootIndex) {
    if (rootIndex == NullNode) {
        throw invalid_argument("Root node index to transfer from is Null");
    }

    TreapIndex transferRoot;
    vector<TreapTransferInfo> nodesToTransfer;

    // Add the root node to be transferred
    nodesToTransfer.push_back(TreapTransferInfo {
        .isLeftChild = false,
        .newParentIndex = NullNode,
        .originalIndex = rootIndex
    });

    // Begin transferring nodes
    while (!nodesToTransfer.empty()) {
        TreapTransferInfo currentInfo = nodesToTransfer.back();
        nodesToTransfer.pop_back();

        // Insert this node in the next available space
        TreapIndex newIndex = createNewNode(other->nodes[currentInfo.originalIndex].val);

        // Set the parent node if applicable
        if (currentInfo.newParentIndex != NullNode) {
            nodes[newIndex].parent = currentInfo.newParentIndex;

            if (currentInfo.isLeftChild) {
                nodes[currentInfo.newParentIndex].left = newIndex;
            }
            else {
                nodes[currentInfo.newParentIndex].right = newIndex;
            }
        }
        else {
            // This is the first node transferred (the "root"). Save this index.
            transferRoot = newIndex;
        }

        // Add any children of this node to be transferred
        if (other->nodes[currentInfo.originalIndex].left != NullNode) {
            nodesToTransfer.push_back(TreapTransferInfo {
                .isLeftChild = true,
                .newParentIndex = newIndex,
                .originalIndex = other->nodes[currentInfo.originalIndex].left
            });
        }

        if (other->nodes[currentInfo.originalIndex].right != NullNode) {
            nodesToTransfer.push_back(TreapTransferInfo {
                .isLeftChild = false,
                .newParentIndex = newIndex,
                .originalIndex = other->nodes[currentInfo.originalIndex].right
            });
        }
    }

    return transferRoot;
}

/**
 * @brief
 * Performs a right rotation on the target index
 * 
 * @param index
 * The index to perform the rotation on
 */
void Treap::rightRotate(TreapIndex index) {
    TreapIndex parentIndex = nodes[index].parent;
    TreapIndex leftIndex = nodes[index].left;
    TreapIndex leftRightIndex = nodes[leftIndex].right;

    // Move the target down to the right
    nodes[index].parent = leftIndex;
    nodes[leftIndex].right = index;

    // Hook the left node up to the target's old parent (or the root)
    nodes[leftIndex].parent = parentIndex;
    if (parentIndex == NullNode) {
        // This node is the new root
        root = leftIndex;
    }
    else {
        // Determine if the target node was the left or right child of the parent
        if (index == nodes[parentIndex].left) {
            nodes[parentIndex].left = leftIndex;
        }
        else {
            nodes[parentIndex].right = leftIndex;
        }
    }

    // Move any orphaned nodes to the left of the target
    nodes[index].left = leftRightIndex;
    if (leftRightIndex != NullNode) {
        nodes[leftRightIndex].parent = index;
    }
}

/**
 * Performs a left rotation on the target index
 * 
 * @param index
 * The index to perform the rotation on
 */
void Treap::leftRotate(TreapIndex index) {
    TreapIndex parentIndex = nodes[index].parent;
    TreapIndex rightIndex = nodes[index].right;
    TreapIndex rightLeftIndex = nodes[rightIndex].left;

    // Move the target down to the left
    nodes[index].parent = rightIndex;
    nodes[rightIndex].left = index;

    // Hook the right node up to the target's old parent (or the root)
    nodes[rightIndex].parent = parentIndex;
    if (parentIndex == NullNode) {
        // This node is the new root
        root = rightIndex;
    }
    else {
        // Determine if the target node was the left or right child of the parent
        if (index == nodes[parentIndex].left) {
            nodes[parentIndex].left = rightIndex;
        }
        else {
            nodes[parentIndex].right = rightIndex;
        }
    }

    // Move any orphaned nodes to the right of the target
    nodes[index].right = rightLeftIndex;
    if (rightLeftIndex != NullNode) {
        nodes[rightLeftIndex].parent = index;
    }
}

/**
 * Moves a node up in the treap based on its weight
 * 
 * @param index
 * The index of the node to move up
 */
void Treap::moveUp(TreapIndex index) {
    while (true) {
        TreapIndex parentIndex = nodes[index].parent;

        // Stop when the current node becomes the root, or no longer has a smaller weight than its parent
        if (parentIndex == NullNode || nodes[index].weight >= nodes[parentIndex].weight) {
            return;
        }

        // Determine if the current node is the left or right child of the parent
        if (index == nodes[parentIndex].left) {
            rightRotate(parentIndex);
        }
        else {
            leftRotate(parentIndex);
        }
    }
}

/**
 * Moves a node down in the treap so that it becomes a leaf node
 * 
 * @param index
 * The index of the node to move down
 */
void Treap::moveDown(TreapIndex index) {
    while (true) {
        TreapIndex leftIndex = nodes[index].left;
        TreapIndex rightIndex = nodes[index].right;

        if (leftIndex == NullNode && rightIndex == NullNode) {
            // The node is a leaf node
            return;
        }

        if (leftIndex != NullNode && rightIndex != NullNode) {
            // The node has two children. Rotate in the direction of higher priority
            if (nodes[leftIndex].weight < nodes[rightIndex].weight) {
                rightRotate(index);
            }
            else {
                leftRotate(index);
            }
        }
        else if (leftIndex != NullNode) {
            // There is only a left child
            rightRotate(index);
        }
        else {
            // There is only a right child
            leftRotate(index);
        }
    }
}

/**
 * Inserts a node into the treap BST-style, based on its value
 * 
 * @param index
 * The index of the node to insert
 */
void Treap::bstInsert(TreapIndex index) {
    TreapIndex searchIndex = root;
    while (true) {
        if (nodes[searchIndex].val > nodes[index].val) {
            if (nodes[searchIndex].left == NullNode) {
                nodes[searchIndex].left = index;
                nodes[index].parent = searchIndex;
                return;
            }
            else {
                searchIndex = nodes[searchIndex].left;
            }
        }
        else {
            if (nodes[searchIndex].right == NullNode) {
                nodes[searchIndex].right = index;
                nodes[index].parent = searchIndex;
                return;
            }
            else {
                searchIndex = nodes[searchIndex].right;
            }
        }
    }
}

/**
 * Finds a node in the treap using a BST search
 * 
 * @param val
 * The value to find
 *
 * @return TreapIndex
 * The index of the node containing the search value, or NullNode if it could not be found
 */
TreapIndex Treap::bstFind(int val) {
    TreapIndex searchIndex = root;
    while (searchIndex != NullNode) {
        if (nodes[searchIndex].val == val) {
            // The value was found
            return searchIndex;
        }
        else if (nodes[searchIndex].val > val) {
            // The value is smaller than this node
            searchIndex = nodes[searchIndex].left;
        }
        else {
            // The value is greater than this node
            searchIndex = nodes[searchIndex].right;
        }
    }

    // The value could not be found
    return NullNode;
}

/**
 * @brief
 * Inserts a value into the treap, maintining both BST ordering and heap ordering
 * 
 * @param val
 * The value to insert
 */
void Treap::insert(int val) {
    // If the treap is full, new nodes can't be added
    if (size == TREAP_NODES) {
        throw out_of_range("Treap is full");
    }

    // Retrieve the node for this insertion
    TreapIndex newNodeIndex = createNewNode(val);

    if (size == 1) {
        // This is the first node in the treap. Make the new node the root.
        root = newNodeIndex;
        return;
    }

    // Perform BST insertion with the new node
    bstInsert(newNodeIndex);

    // Move the new node up based on its weight
    moveUp(newNodeIndex);
}

/**
 * Removes a value from the treap
 * 
 * @param val
 * The value to remove
 * 
 * @return true
 * If the node was removed
 * 
 * @return false
 * If the node did not exist in the treap
 */
bool Treap::remove(int val) {
    // Search for the target value
    TreapIndex foundIndex = bstFind(val);

    if (foundIndex == NullNode) {
        // The value could not be found
        return false;
    }

    // Move the target node down to a leaf node so it can be removed
    moveDown(foundIndex);

    // Cut off the node from the tree
    TreapIndex parentIndex = nodes[foundIndex].parent;
    if (parentIndex == NullNode) {
        // The root was removed
        root = NullNode;
    }
    else {
        // Determine if the removed node was the right or left child of its parent
        if (nodes[parentIndex].left == foundIndex) {
            nodes[parentIndex].left = NullNode;
        }
        else {
            nodes[parentIndex].right = NullNode;
        }
    }

    // Move the last node in the nodes array to fill the gap created by removing this node
    moveNode(size - 1, foundIndex);

    size--;
    return true;
}

/**
 *
 * Calculates the median value of the treap
 *
 * @return int
 * The median value
 */
int Treap::getMedianVal() {
    // There is no median for an empty Treap
    if (size == 0) {
        throw logic_error("Cannot calculate median of a Treap with no elements");
    }

    vector<int> values;

    // Collect all values
    for (int i = 0; i < size; i++) {
        values.push_back(nodes[i].val);
    }

    // Sort the values
    sort(values.begin(), values.end());

    // Calculate the median
    if (size % 2 == 0) {
        // The median is the average of the two middle values
        return (int)(values.at(size / 2 - 1) / 2.0) + (values.at(size / 2) / 2.0);  // Divide each term first to prevent overflow
    }
    else {
        // The median is the middle value
        return values.at(size / 2);
    }
}

/**
 * Performs an immutable insertion of a value into a copy of the treap
 * 
 * @param val
 * The value to insert
 * 
 * @return Treap*
 * A pointer to a copy of the treap with the value inserted
 */
Treap *Treap::immutableInsert(int val) {
    // Copy the current object
    Treap *newTreap = Treap::New(*this);

    // Insert the value in the copy
    newTreap->insert(val);

    return newTreap;
}

/**
 * Performs an immutable removal of a value from a copy of the treap
 * 
 * @param val
 * The value to remove
 *
 * @param success
 * The location to store whether the remove was a success
 *
 * @return Treap*
 * A pointer to a copy of the treap with the value removed
 */
Treap *Treap::immutableRemove(int val, bool *success) {
    // Copy the current object
    Treap *newTreap = Treap::New(*this);

    // Remove the value from the copy
    *success = newTreap->remove(val);

    return newTreap;
}

/**
 * Determine if a value is stored within the treap
 * 
 * @param val
 * The value to search for
 * 
 * @return true
 * If the value is in the treap
 * 
 * @return false
 * If the value is not in the treap
 */
bool Treap::contains(int val) {
    TreapIndex foundIndex = bstFind(val);

    return foundIndex != NullNode;
}

/**
 * Returns all values between a given min and max, inclusive
 * 
 * @param min
 * The minimum value (inclusive)
 * 
 * @param max
 * The maximum value (inclusive)
 * 
 * @return vector<int>
 * The values in the Treap between the minimum and maximum values
 */
vector<int> Treap::rangeQuery(int min, int max) {
    vector<int> values;

    if (root == NullNode) {
        return values;
    }

    vector<TreapIndex> nodesToCheck;
    nodesToCheck.push_back(root);

    while (!nodesToCheck.empty()) {
        TreapIndex currentIndex = nodesToCheck.back();
        nodesToCheck.pop_back();

        int currentVal = nodes[currentIndex].val;
        int currentLeft = nodes[currentIndex].left;
        int currentRight = nodes[currentIndex].right;

        // Add this value if it is in range
        if (currentVal >= min && currentVal <= max) {
            values.push_back(currentVal);
        }

        // Check values to the left if this value is not smaller than the minimum value
        if (currentVal >= min && currentLeft != NullNode) {
            nodesToCheck.push_back(currentLeft);
        }

        // Check values to the right if this value is not larger than the maximum value
        if (currentVal <= max && currentRight != NullNode) {
            nodesToCheck.push_back(currentRight);
        }
    }

    return values;
}

/**
 * Returns the size of the treap
 * 
 * @return int
 * The size of the treap
 */
int Treap::getSize() {
    return size;
}

/**
 * Get the maximum value stored in this treap
 *
 * @return int
 * The maximum value in the treap
 */
int Treap::getMaxValue() {
    if (size == 0) {
        throw logic_error("Cannot get the maximum value of an empty treap");
    }

    // Find the rightmost node
    TreapIndex tempIndex = root;
    while (nodes[tempIndex].right != NullNode) {
        tempIndex = nodes[tempIndex].right;
    }

    return nodes[tempIndex].val;
}

/**
 * Merges two treaps into a new treap
 * 
 * @param left
 * The left treap to merge. All values must be smaller than those in the right treap.
 * 
 * @param right
 * The right treap to merge. All values must be greater than or equal to those in the left treap.
 * 
 * @return Treap*
 * The merged treap
 */
Treap *Treap::merge(Treap *left, Treap *right) {
    // Validate merge
    int newSize = left->size + right->size;
    if (newSize > TREAP_NODES) {
        throw invalid_argument("Merging these treaps would overflow the new treap. (Sizes: " + to_string(left->size) + ", " + to_string(right->size) + ")");
    }

    Treap *mergedTreap = Treap::New();

    // If there are no elements in the Treaps, just return an empty Treap
    if (newSize == 0) {
        return mergedTreap;
    }

    // If one Treap is empty, copy the non-empty one and return
    if (right->root == NullNode) {
        TreapIndex newRoot = mergedTreap->transferNodesFrom(left, left->root);
        mergedTreap->root = newRoot;
        mergedTreap->nodes[newRoot].parent = NullNode;
        return mergedTreap;
    }
    if (left->root == NullNode) {
        TreapIndex newRoot = mergedTreap->transferNodesFrom(right, right->root);
        mergedTreap->root = newRoot;
        mergedTreap->nodes[newRoot].parent = NullNode;
        return mergedTreap;
    }

    // Copy the two treaps into the new treap
    TreapIndex leftRootIndex = mergedTreap->transferNodesFrom(left, left->root);
    TreapIndex rightRootIndex = mergedTreap->transferNodesFrom(right, right->root);

    // Calculate the average of the two roots
    int leftRootVal = mergedTreap->nodes[leftRootIndex].val;
    int rightRootVal = mergedTreap->nodes[rightRootIndex].val;
    int avgVal = (int)((leftRootVal / 2.0) + (rightRootVal / 2.0));

    // Add a dummy node to join the two treaps
    mergedTreap->nodes[ControlNode].val = avgVal;
    mergedTreap->nodes[ControlNode].weight = NegInfinity;
    mergedTreap->nodes[ControlNode].parent = NullNode;
    mergedTreap->nodes[ControlNode].left = leftRootIndex;
    mergedTreap->nodes[ControlNode].right = rightRootIndex;

    mergedTreap->nodes[leftRootIndex].parent = ControlNode;
    mergedTreap->nodes[rightRootIndex].parent = ControlNode;
    mergedTreap->root = ControlNode;

    // Move the dummy node down to become a leaf node
    mergedTreap->moveDown(ControlNode);

    // Cut off the dummy node
    TreapIndex controlParentIndex = mergedTreap->nodes[ControlNode].parent;
    if (mergedTreap->nodes[controlParentIndex].left == ControlNode) {
        mergedTreap->nodes[controlParentIndex].left = NullNode;
    }
    else {
        mergedTreap->nodes[controlParentIndex].right = NullNode;
    }

    // Return the new merged treap
    return mergedTreap;
}

/**
 * Splits a Treap into two treaps of (on average) equal size
 * 
 * @param left
 * The location to store the left split treap
 * 
 * @param right
 * The location to store the right split treap
 *
 * @returns
 * The value the treap was split at
 */
int Treap::split(Treap **left, Treap **right) {
    if (size == 0) {
        throw logic_error("An empty treap cannot be split");
    }

    *left = Treap::New();
    *right = Treap::New();
    int splitVal = getMedianVal();

    // Copy the current treap so it can be modified (the current treap should not be changed)
    Treap workingTreap(*this);

    // Add a dummy node to split the treap
    workingTreap.nodes[ControlNode].val = splitVal;
    workingTreap.nodes[ControlNode].weight = NegInfinity;
    workingTreap.nodes[ControlNode].parent = NullNode;
    workingTreap.nodes[ControlNode].left = NullNode;
    workingTreap.nodes[ControlNode].right = NullNode;

    // "Insert" the new node into the treap
    workingTreap.bstInsert(ControlNode);

    // Move the new node up so it becomes the root (NegInfinity weight insures this)
    workingTreap.moveUp(ControlNode);

    // Copy the left and right subtree into the split treaps and update the root
    if (workingTreap.nodes[ControlNode].left != NullNode) {
        (*left)->root = (*left)->transferNodesFrom(&workingTreap, workingTreap.nodes[ControlNode].left);
    }

    if (workingTreap.nodes[ControlNode].right != NullNode) {
        (*right)->root = (*right)->transferNodesFrom(&workingTreap, workingTreap.nodes[ControlNode].right);
    }

    return splitVal;
}

/**
 * Inserts an element into the Treap sequentially.
 * 
 * @param val
 * The value to insert
 */
void Treap::sequentialInsert(int val) {
    insert(val);
}

/**
 * Removes an element from the Treap sequentially.
 * 
 * @param val
 * The value to remove
 * 
 * @return true
 * If the value was removed
 * 
 * @return false
 * If the value was not in the Treap
 */
bool Treap::sequentialRemove(int val) {
    return remove(val);
}

/**
 * Returns the value of the root of the Treap
 * 
 * @return int
 * The value of the root of the treap
 */
int Treap::getRoot() {
    return nodes[root].val;
}