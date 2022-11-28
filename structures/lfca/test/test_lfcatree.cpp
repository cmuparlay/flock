#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "../treap.h"
#include "../lfca.h"

#define NUM_THREADS 8
#define PARALLEL_START 0
#define PARALLEL_END 100000

// These values are estimates, due to the nondeterministic nature of the parallel tests.
#define MAX_TREAPS_NEEDED (2 * 2 * (PARALLEL_END - PARALLEL_START))  // insert and remove for ParallelRemove, with 2 retries per operation
#define MAX_NODES_NEEDED (4 * (PARALLEL_END - PARALLEL_START))
#define MAX_RESULT_SETS_NEEDED 1024  // RangeQueryBulkTest

class LfcaTreeTest : public ::testing::Test {
protected:
    LfcaTree *lfcaTree;

    void SetUp() override {
        Treap::Preallocate(MAX_TREAPS_NEEDED);
        node::Preallocate(MAX_NODES_NEEDED);
        rs::Preallocate(MAX_RESULT_SETS_NEEDED);

        lfcaTree = new LfcaTree();
    }
    void TearDown() override {
        Treap::Deallocate();
        node::Deallocate();
        rs::Deallocate();

        delete lfcaTree;
    }
};

TEST_F(LfcaTreeTest, InsertAndRemoveAndLookup) {
    lfcaTree->insert(1);
    EXPECT_TRUE(lfcaTree->lookup(1));

    lfcaTree->insert(2);
    EXPECT_TRUE(lfcaTree->lookup(2));
    lfcaTree->insert(3);
    EXPECT_TRUE(lfcaTree->lookup(3));
    lfcaTree->insert(4);
    EXPECT_TRUE(lfcaTree->lookup(4));
    lfcaTree->insert(5);
    EXPECT_TRUE(lfcaTree->lookup(5));

    lfcaTree->remove(1);
    EXPECT_FALSE(lfcaTree->lookup(1));
    lfcaTree->remove(2);
    EXPECT_FALSE(lfcaTree->lookup(2));
    lfcaTree->remove(3);
    EXPECT_FALSE(lfcaTree->lookup(3));
    lfcaTree->remove(4);
    EXPECT_FALSE(lfcaTree->lookup(4));
    lfcaTree->remove(5);
    EXPECT_FALSE(lfcaTree->lookup(5));
}

TEST_F(LfcaTreeTest, RangeQuery) {
    for (int i = 1; i <= 9; i++) {
        lfcaTree->insert(i);
    }

    vector<int> expectedQuery = {3, 4, 5, 6, 7, 8, 9};
    vector<int> actualQuery = lfcaTree->rangeQuery(3, 100);
    sort(actualQuery.begin(), actualQuery.end());
    EXPECT_EQ(expectedQuery, actualQuery);

    expectedQuery = {1, 2, 3, 4};
    actualQuery = lfcaTree->rangeQuery(-100, 4);
    sort(actualQuery.begin(), actualQuery.end());
    EXPECT_EQ(expectedQuery, actualQuery);

    expectedQuery = {4, 5, 6};
    actualQuery = lfcaTree->rangeQuery(4, 6);
    sort(actualQuery.begin(), actualQuery.end());
    EXPECT_EQ(expectedQuery, actualQuery);
}

TEST_F(LfcaTreeTest, RangeQueryEmptyTree) {
    vector<int> expectedQuery = { };
    vector<int> actualQuery = lfcaTree->rangeQuery(0, 0);
    EXPECT_EQ(expectedQuery, actualQuery);
}

TEST_F(LfcaTreeTest, SplitAndMergeBulkTest) {
    for (int i = 0; i < 1024; i++) {
        lfcaTree->insert(i);
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }

    for (int i = 0; i < 1024; i++) {
        lfcaTree->remove(i);
        for (int j = i + 1; j < 1024; j++)
        {
            ASSERT_TRUE(lfcaTree->lookup(j));
        }
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, RangeQueryBulkTest) {
    for (int i = 0; i < 1024; i++) {
        lfcaTree->insert(i);
    }

    vector<int> expectedQuery = {};
    vector<int> actualQuery;
    for (int i = 100; i < 1024; i++) {
        expectedQuery.push_back(i);
        actualQuery = lfcaTree->rangeQuery(100, i);
        sort(actualQuery.begin(), actualQuery.end());
        ASSERT_EQ(expectedQuery, actualQuery);
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeFailure) {
    // Fill up the base node
    for (int i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Add a quarter of the nodes for each treap to the left and right side of the split base node
    int oneQuarterOfRange = TREAP_NODES / 4;
    for (int i = -1; i > -oneQuarterOfRange; i--) {
        lfcaTree->insert(i);
    }
    for (int i = TREAP_NODES; i < TREAP_NODES + oneQuarterOfRange; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    int uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    int testVal = 0;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    testVal = TREAP_NODES - 1;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown
}

TEST_F(LfcaTreeTest, LowContentionMergeLeft) {
    // Fill up the base node
    for (int i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    int uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    int testVal = 0;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (int i = 0; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeRight) {
    // Fill up the base node
    for (int i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    int uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    int testVal = TREAP_NODES - 1;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (int i = 0; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeLeftWithRightRoute) {
    // Fill up the base node
    for (int i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Split the right base node again by filling it up with more than it can hold
    for (int i = TREAP_NODES; i < TREAP_NODES * 2; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    int uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    int testVal = 0;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (int i = 0; i < TREAP_NODES * 2; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeRightWithLeftRoute) {
    // Fill up the base node
    for (int i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Split the left base node again by filling it up with more than it can hold
    for (int i = -1; i > -TREAP_NODES; i--) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    int uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    int testVal = TREAP_NODES - 1;
    for (int i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (int i = -TREAP_NODES + 1; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

static void insertThread(LfcaTree *tree, int start, int end, int delta) {
    for (int i = start; i <= end; i += delta) {
        tree->insert(i);
    }
}

static void removeThread(LfcaTree *tree, int start, int end, int delta) {
    for (int i = start; i <= end; i += delta) {
        tree->remove(i);
    }
}

// A very poor, nondeterministic unit test for crude concurrency. Included just for some sanity, but should not be relied on.
TEST_F(LfcaTreeTest, ParallelInsert) {
    vector<thread> threads;

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(insertThread, lfcaTree, PARALLEL_START + i, PARALLEL_END, NUM_THREADS));
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (int i = 0; i <= PARALLEL_END; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}


TEST_F(LfcaTreeTest, ParallelRemove) {
    // Insert all elements
    for (int i = PARALLEL_START; i <= PARALLEL_END; i++) {
        lfcaTree->insert(i);
    }

    vector<thread> threads;

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(removeThread, lfcaTree, PARALLEL_START + i, PARALLEL_END, NUM_THREADS));
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (int i = 0; i <= PARALLEL_END; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, ParallelRemovePartial) {
    // Insert all elements
    for (int i = PARALLEL_START; i <= PARALLEL_END; i++) {
        lfcaTree->insert(i);
    }

    // Only remove the middle 50% of the values from the tree
    int oneQuarterOfRange = (PARALLEL_END - PARALLEL_START) / 4;
    int removeStart = PARALLEL_START + oneQuarterOfRange;
    int removeEnd = PARALLEL_END - oneQuarterOfRange;

    vector<thread> threads;

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(removeThread, lfcaTree, removeStart + i, removeEnd, NUM_THREADS));
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (int i = 0; i < removeStart; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }

    for (int i = removeStart; i <= removeEnd; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }

    for (int i = removeEnd + 1; i <= PARALLEL_END; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}
