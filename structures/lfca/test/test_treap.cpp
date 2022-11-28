#include <gtest/gtest.h>

#include "../treap.h"

#define MAX_TREAPS_NEEDED (TREAP_NODES * 2 + 1)  // FillingAndEmptying

class TreapTest : public ::testing::Test {
protected:
    Treap *treap {nullptr};
    Treap *left {nullptr};
    Treap *right {nullptr};
    Treap *merged {nullptr};

    void SetUp() override {
        Treap::Preallocate(MAX_TREAPS_NEEDED);

        treap = Treap::New();
    }
    void TearDown() override {
        Treap::Deallocate();
    }

    void insertHelper(int val) {
        treap = treap->immutableInsert(val);
    }

    bool removeHelper(int val) {
        bool success;

        treap = treap->immutableRemove(val, &success);

        return success;
    }
};

TEST_F(TreapTest, InsertAndRemove) {
    EXPECT_EQ(0, treap->getSize());

    insertHelper(5);
    EXPECT_EQ(1, treap->getSize());

    insertHelper(3);
    ASSERT_EQ(2, treap->getSize());

    removeHelper(5);
    EXPECT_EQ(1, treap->getSize());

    removeHelper(3);
    EXPECT_EQ(0, treap->getSize());
}

TEST_F(TreapTest, Contains) {
    EXPECT_FALSE(treap->contains(1));
    insertHelper(1);
    ASSERT_TRUE(treap->contains(1));

    EXPECT_FALSE(treap->contains(2));
    insertHelper(2);
    ASSERT_TRUE(treap->contains(2));

    EXPECT_TRUE(treap->contains(1));
    EXPECT_TRUE(removeHelper(1));
    ASSERT_FALSE(treap->contains(1));

    EXPECT_TRUE(treap->contains(2));
    EXPECT_TRUE(removeHelper(2));
    ASSERT_FALSE(treap->contains(2));
}

TEST_F(TreapTest, RemoveNonExisting) {
    EXPECT_FALSE(treap->contains(1));
    EXPECT_FALSE(treap->contains(2));
    EXPECT_EQ(0, treap->getSize());

    insertHelper(1);
    insertHelper(2);

    EXPECT_TRUE(treap->contains(1));
    EXPECT_TRUE(treap->contains(2));
    ASSERT_EQ(2, treap->getSize());

    EXPECT_FALSE(removeHelper(3));
    EXPECT_EQ(2, treap->getSize());
    EXPECT_TRUE(treap->contains(1));
    EXPECT_TRUE(treap->contains(2));
}

TEST_F(TreapTest, FillingToLimit) {
    ASSERT_EQ(treap->getSize(), 0);

    for (int i = 1; i <= TREAP_NODES; i++) {
        insertHelper(i);
        ASSERT_EQ(i, treap->getSize());
        ASSERT_TRUE(treap->contains(i));
    }

    // The treap is now full. New elements can't be added
    bool correctException = false;
    try {
        insertHelper(TREAP_NODES + 1);
    } catch (out_of_range e) {
        correctException = true;
    } catch (...) { }

    EXPECT_TRUE(correctException);
    EXPECT_EQ(TREAP_NODES, treap->getSize());
    EXPECT_FALSE(treap->contains(TREAP_NODES + 1));
}

TEST_F(TreapTest, FillingAndEmptying) {
    ASSERT_EQ(treap->getSize(), 0);

    // Fill the treap
    for (int i = 1; i <= TREAP_NODES; i++) {
        insertHelper(i);
        ASSERT_EQ(i, treap->getSize());
        ASSERT_TRUE(treap->contains(i));
    }

    // Empty the treap
    for (int i = 1; i <= TREAP_NODES; i++) {
        ASSERT_TRUE(removeHelper(i));
        ASSERT_EQ(TREAP_NODES - i, treap->getSize());
        ASSERT_FALSE(treap->contains(i));
    }
}

TEST_F(TreapTest, FullSplit) {
    ASSERT_EQ(treap->getSize(), 0);

    // Fill the treap
    for (int i = 1; i <= TREAP_NODES; i++) {
        insertHelper(i);
        ASSERT_EQ(i, treap->getSize());
        ASSERT_TRUE(treap->contains(i));
    }
    int medianVal = (TREAP_NODES + 1) / 2;

    // Split the treap
    int actualSplit = treap->split(&left, &right);

    // The split is non-deterministic, but there are certain properties that must hold. Test for these

    // Test that all numbers are in one of the two treaps
    for (int i = 1; i <= TREAP_NODES; i++) {
        bool inLeft = left->contains(i);
        bool inRight = right->contains(i);

        // Make sure the number exists
        ASSERT_TRUE(inLeft || inRight);

        // Make sure the number is not in both treaps
        ASSERT_FALSE(inLeft && inRight);
    }

    // Test that all numbers in the left treap are smaller than in the right treap
    int minInt = numeric_limits<int>::min();
    int maxInt = numeric_limits<int>::max();
    int largestInLeft = minInt;
    int smallestInRight = maxInt;
    for (int i = 1; i <= TREAP_NODES; i++) {
        if (left->contains(i) && i > largestInLeft) {
            largestInLeft = i;
        }

        if (right->contains(i) && i < smallestInRight) {
            smallestInRight = i;
        }
    }
    EXPECT_LE(largestInLeft, medianVal);
    EXPECT_LT(medianVal, smallestInRight);

    // Ensure the treap was split at the correct value, which is the median of the Treap's values
    EXPECT_EQ(medianVal, actualSplit);
}

TEST_F(TreapTest, SplitEmpty) {
    ASSERT_EQ(treap->getSize(), 0);

    // Empty treaps can't be split
    bool correctException = false;
    try {
        treap->split(&left, &right);
    } catch (logic_error e) {
        correctException = true;
    } catch (...) { }

    EXPECT_TRUE(correctException);
    ASSERT_EQ(left, nullptr);
    ASSERT_EQ(right, nullptr);
}

TEST_F(TreapTest, MergeFull) {
    left = Treap::New();
    right = Treap::New();

    ASSERT_EQ(0, left->getSize());
    ASSERT_EQ(0, right->getSize());

    int halfSize = TREAP_NODES / 2;

    // Insert half of the nodes into the left, and half into the right
    for (int i = 1; i <= halfSize; i++) {
        left->sequentialInsert(i);
        ASSERT_EQ(i, left->getSize());
        ASSERT_TRUE(left->contains(i));
    }
    for (int i = halfSize + 1; i <= TREAP_NODES; i++) {
        right->sequentialInsert(i);
        ASSERT_EQ(i - halfSize, right->getSize());
        ASSERT_TRUE(right->contains(i));
    }

    // Merge the two halves
    merged = Treap::merge(left, right);

    // Ensure all values make it to the merged Treap
    for (int i = 1; i <= TREAP_NODES; i++) {
        ASSERT_TRUE(merged->contains(i));
    }
    ASSERT_EQ(TREAP_NODES, merged->getSize());
}

TEST_F(TreapTest, MergeEmpty) {
    left = Treap::New();
    right = Treap::New();

    ASSERT_EQ(0, left->getSize());
    ASSERT_EQ(0, right->getSize());

    merged = Treap::merge(left, right);

    ASSERT_EQ(0, merged->getSize());
}

TEST_F(TreapTest, MergeLeftEmpty) {
    left = Treap::New();
    right = Treap::New();

    ASSERT_EQ(0, left->getSize());
    ASSERT_EQ(0, right->getSize());

    right->sequentialInsert(1);

    ASSERT_EQ(1, right->getSize());

    merged = Treap::merge(left, right);

    ASSERT_EQ(1, merged->getSize());
    ASSERT_TRUE(merged->contains(1));
}

TEST_F(TreapTest, MergeRightEmpty) {
    left = Treap::New();
    right = Treap::New();

    ASSERT_EQ(0, left->getSize());
    ASSERT_EQ(0, right->getSize());

    left->sequentialInsert(1);

    ASSERT_EQ(1, left->getSize());

    merged = Treap::merge(left, right);

    ASSERT_EQ(1, merged->getSize());
    ASSERT_TRUE(merged->contains(1));
}