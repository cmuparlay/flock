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
    for (long i = 1; i <= 9; i++) {
        lfcaTree->insert(i);
    }

    vector<long> expectedQuery = {3, 4, 5, 6, 7, 8, 9};
    vector<long> actualQuery = lfcaTree->rangeQuery(3, 100);
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
    vector<long> expectedQuery = { };
    vector<long> actualQuery = lfcaTree->rangeQuery(0, 0);
    EXPECT_EQ(expectedQuery, actualQuery);
}

TEST_F(LfcaTreeTest, SplitAndMergeBulkTest) {
    for (long i = 0; i < 1024; i++) {
        lfcaTree->insert(i);
    }

    for (long i = 0; i < 1024; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }

    for (long i = 0; i < 1024; i++) {
        lfcaTree->remove(i);
        for (long j = i + 1; j < 1024; j++)
        {
            ASSERT_TRUE(lfcaTree->lookup(j));
        }
    }

    for (long i = 0; i < 1024; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, RangeQueryBulkTest) {
    for (long i = 0; i < 1024; i++) {
        lfcaTree->insert(i);
    }

    vector<long> expectedQuery = {};
    vector<long> actualQuery;
    for (long i = 100; i < 1024; i++) {
        expectedQuery.push_back(i);
        actualQuery = lfcaTree->rangeQuery(100, i);
        sort(actualQuery.begin(), actualQuery.end());
        ASSERT_EQ(expectedQuery, actualQuery);
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeFailure) {
    // Fill up the base node
    for (long i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Add a quarter of the nodes for each treap to the left and right side of the split base node
    long oneQuarterOfRange = TREAP_NODES / 4;
    for (long i = -1; i > -oneQuarterOfRange; i--) {
        lfcaTree->insert(i);
    }
    for (long i = TREAP_NODES; i < TREAP_NODES + oneQuarterOfRange; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    long uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    long testVal = 0;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    testVal = TREAP_NODES - 1;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown
}

TEST_F(LfcaTreeTest, LowContentionMergeLeft) {
    // Fill up the base node
    for (long i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    long uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    long testVal = 0;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (long i = 0; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeRight) {
    // Fill up the base node
    for (long i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    long uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    long testVal = TREAP_NODES - 1;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (long i = 0; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeLeftWithRightRoute) {
    // Fill up the base node
    for (long i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Split the right base node again by filling it up with more than it can hold
    for (long i = TREAP_NODES; i < TREAP_NODES * 2; i++) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the left base node without conflict
    long uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    long testVal = 0;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (long i = 0; i < TREAP_NODES * 2; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, LowContentionMergeRightWithLeftRoute) {
    // Fill up the base node
    for (long i = 0; i < TREAP_NODES; i++) {
        lfcaTree->insert(i);
    }

    // Split the left base node again by filling it up with more than it can hold
    for (long i = -1; i > -TREAP_NODES; i--) {
        lfcaTree->insert(i);
    }

    // Attempt to force a low contention merge due to a large number of operations on the right base node without conflict
    long uncontendedOpsNeeded = abs(LOW_CONT / LOW_CONT_CONTRIB);
    long testVal = TREAP_NODES - 1;
    for (long i = 0; i < uncontendedOpsNeeded; i++) {
        lfcaTree->remove(testVal);
        lfcaTree->insert(testVal);
    }

    // No exception should be thrown

    // Make sure all values can be found after the merge
    for (long i = -TREAP_NODES + 1; i < TREAP_NODES; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}

static void insertThread(LfcaTree *tree, long start, long end, long delta) {
    for (long i = start; i <= end; i += delta) {
        tree->insert(i);
    }
}

static void removeThread(LfcaTree *tree, long start, long end, long delta) {
    for (long i = start; i <= end; i += delta) {
        tree->remove(i);
    }
}

// A very poor, nondeterministic unit test for crude concurrency. Included just for some sanity, but should not be relied on.
TEST_F(LfcaTreeTest, ParallelInsert) {
    vector<thread> threads;

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(insertThread, lfcaTree, PARALLEL_START + i, PARALLEL_END, NUM_THREADS));
    }

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (long i = 0; i <= PARALLEL_END; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}


TEST_F(LfcaTreeTest, ParallelRemove) {
    // Insert all elements
    for (long i = PARALLEL_START; i <= PARALLEL_END; i++) {
        lfcaTree->insert(i);
    }

    vector<thread> threads;

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(removeThread, lfcaTree, PARALLEL_START + i, PARALLEL_END, NUM_THREADS));
    }

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (long i = 0; i <= PARALLEL_END; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }
}

TEST_F(LfcaTreeTest, ParallelRemovePartial) {
    // Insert all elements
    for (long i = PARALLEL_START; i <= PARALLEL_END; i++) {
        lfcaTree->insert(i);
    }

    // Only remove the middle 50% of the values from the tree
    long oneQuarterOfRange = (PARALLEL_END - PARALLEL_START) / 4;
    long removeStart = PARALLEL_START + oneQuarterOfRange;
    long removeEnd = PARALLEL_END - oneQuarterOfRange;

    vector<thread> threads;

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.push_back(thread(removeThread, lfcaTree, removeStart + i, removeEnd, NUM_THREADS));
    }

    for (long i = 0; i < NUM_THREADS; i++) {
        threads.at(i).join();
    }

    for (long i = 0; i < removeStart; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }

    for (long i = removeStart; i <= removeEnd; i++) {
        ASSERT_FALSE(lfcaTree->lookup(i));
    }

    for (long i = removeEnd + 1; i <= PARALLEL_END; i++) {
        ASSERT_TRUE(lfcaTree->lookup(i));
    }
}
