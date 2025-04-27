#include "btree.h"
#include "gtest/gtest.h"

/*
 * btree_student_test.cc
 *
 * Once you have address all A3: TODOs, you should be able to pass all the tests
 * in this file.
 *
 * CMake should generate an executable called btree_student_test.
 * You can create a GoogleTest run/debug configuration in CLion to run the
 * tests.
 *
 * Clarification:
 * Tables and indexes stored in CapybaraDB are modeled after a Btree instead
 * of a B+tree. This means that key-data pairs can appear in both internal and
 * leaf nodes. You will not need to traverse to the leaf nodes to find the
 * key-data pairs.
 *
 * There are hidden test cases.
 */

/**
 * @brief Helper class to support testing the B-Tree.
 */
class BtreeStudentTest {
 public:
  /**
   * @brief Creates the filename for the database.
   *
   * These filenames are actually determined by the Pager layer
   * However, we need to delete them before each test case
   * or else, the Pager will read from the database and journal file with
   * possibly corrupted data.
   *
   * @param test_name the name of the test
   */
  explicit BtreeStudentTest(std::string test_name) {
    filename_ = "test_" + test_name + ".db";
    journal_filename_ = "test_" + test_name + ".db-journal";
    name_ = test_name;
  }
  void SetUp() {
    // This code runs before each test case.
    std::remove(filename_.c_str());
    std::remove(journal_filename_.c_str());
  }

  static std::vector<std::byte> UnsignedIntToByteVector(u32 unsigned_int) {
    std::vector<std::byte> byte_vector;
    byte_vector.resize(sizeof(unsigned_int));
    std::memcpy(byte_vector.data(), &unsigned_int, sizeof(unsigned_int));
    return byte_vector;
  }

  static u32 FindMaximumBtreeDepth(Btree &btree, PageNumber root_page_number) {
    // Please make sure that there are no BtCursors with write access on the
    // table identified by root_page_number before calling this function.

    // Step 1: Create a read-only BtCursor and move to the first entry in the
    // table
    bool table_is_empty = false;
    std::weak_ptr<BtCursor> p_cursor_weak;
    ResultCode rc;
    btree.BtCursorCreate(root_page_number, false, p_cursor_weak);
    btree.BtreeFirst(p_cursor_weak, table_is_empty);

    // Step 2: If the table is empty, return 1
    if (table_is_empty) {
      return 1;
    }

    // Step 3: Traverse through each node in the Btree and keep track of the
    // maximum depth
    u32 maximum_depth = 1;
    bool already_at_last_entry = false;
    while (!already_at_last_entry) {
      u32 depth;
      rc = btree.BtreeGetNodeDepth(p_cursor_weak, depth);
      EXPECT_EQ(rc, ResultCode::kOk);
      rc = btree.BtreeNext(p_cursor_weak, already_at_last_entry);
      EXPECT_EQ(rc, ResultCode::kOk);
      if (depth > maximum_depth) {
        maximum_depth = depth;
      }
    }

    // Step 4: Close the cursor and return the maximum depth
    rc = btree.BtCursorClose(p_cursor_weak);
    EXPECT_EQ(rc, ResultCode::kOk);
    return maximum_depth;
  }

  std::string GetFilename() { return filename_; }

  // ########### Private Variables ###########
 private:
  std::string name_;
  std::string filename_;
  std::string journal_filename_;
};

TEST(BtreeStudentTest, TestDeleteNonExistentEntry) {
  // This test tests deletion of a key that doesn't exist in the B-tree.
  std::string test_name = "TestDeleteNonExistentEntry";
  PageNumber table_root_page_number = 0;
  std::weak_ptr<BtCursor> p_cursor_weak;
  BtreeStudentTest test(test_name);
  test.SetUp();
  ResultCode rc;

  Btree btree(test.GetFilename(), 10);

  rc = btree.BtreeBeginTrans();
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCreateTable(table_root_page_number);
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtCursorCreate(table_root_page_number, true, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  u32 key_int = 999;
  std::vector<std::byte> key =
      BtreeStudentTest::UnsignedIntToByteVector(key_int);

  int compare_result;
  rc = btree.BtreeMoveTo(p_cursor_weak, key, compare_result);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_NE(compare_result, 0);

  rc = btree.BtreeDelete(p_cursor_weak);
  EXPECT_NE(rc, ResultCode::kOk);  // Deletion of non-existent key should fail

  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);
}

TEST(BtreeStudentTest, CanInsertOneEntry) {
  // This test checks if you can insert one entry into the Btree.

  // Step 1: Create a Btree
  // With an instance of the Btree class, you are able to create multiple
  // tables. You are then allowed to create BtCursors to traverse through the
  // table. For the tests below, you will only be creating a Btree
  std::string test_name = "CanInsertOneEntry";
  PageNumber table_root_page_number = 0;
  std::weak_ptr<BtCursor> p_cursor_weak;
  BtreeStudentTest test = BtreeStudentTest(test_name);
  bool table_is_empty = false;
  test.SetUp();
  ResultCode rc;
  Btree btree(test.GetFilename(), 10);

  // Step 2: Begin transaction
  // Any operations
  rc = btree.BtreeBeginTrans();
  EXPECT_EQ(rc, ResultCode::kOk);

  // Step 3: Create a new table and return the table root page number
  rc = btree.BtreeCreateTable(table_root_page_number);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(table_root_page_number, 3);

  // Step 4: Create a BtCursor to the table
  // By passing the value of true to the second parameter, you create a cursor
  // with write access to the table.
  rc = btree.BtCursorCreate(table_root_page_number, true, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // This is just a sanity check to make sure that the table is empty
  // You move the cursor to the first entry in the table and check if the table
  // is empty
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_TRUE(table_is_empty);

  // Step 5: Insert a key-data pair into the table
  // In reality, the key and data can be of variable lengths.
  // We are using 4 byte unsigned integers throughout the assignment for
  // simplicity.
  u32 key_int = 42;
  u32 data_int = 24;
  std::vector<std::byte> key =
      BtreeStudentTest::UnsignedIntToByteVector(key_int);
  std::vector<std::byte> data =
      BtreeStudentTest::UnsignedIntToByteVector(data_int);
  // BtreeInsert inserts the key-data pair into the table.
  // The cursor should be pointing at where the key-data pair was inserted.
  // If there is already an existing key, the data will be updated.
  rc = btree.BtreeInsert(p_cursor_weak, key, data);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Step 6: Check if the key and data were inserted correctly
  std::vector<std::byte> retrieved_key;
  u32 retrieved_key_size;
  // Use BtreeKeySize to get the size of the key
  // In this case, retrieved_key_size should be 4
  rc = btree.BtreeKeySize(p_cursor_weak, retrieved_key_size);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(retrieved_key_size, sizeof(key_int));
  // Use BtreeKey to get all 4 bytes of the key
  // If third parameter specifies the number of bytes to retrieve
  // In this case, retrieved_key_size is 4, allowing us to retrieve all 4 bytes
  // of the key
  btree.BtreeKey(p_cursor_weak, 0, retrieved_key_size, retrieved_key);
  EXPECT_EQ(retrieved_key, key);

  // Step 7: Check if the data was inserted correctly
  // Same logic as key
  std::vector<std::byte> retrieved_data;
  u32 retrieved_data_size;
  rc = btree.BtreeDataSize(p_cursor_weak, retrieved_data_size);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(retrieved_data_size, sizeof(data_int));
  btree.BtreeData(p_cursor_weak, 0, retrieved_data_size, retrieved_data);
  EXPECT_EQ(retrieved_data, data);

  // Step 8: Close the cursor and commit the transaction
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);
}



TEST(BtreeStudentTest, VisualizeMultiLayerBtree) {
  // Setup
  std::string test_name = "VisualizeMultiLayerBtree";
  PageNumber table_root_page_number = 0;
  std::weak_ptr<BtCursor> p_cursor_weak;
  BtreeStudentTest test = BtreeStudentTest(test_name);
  bool table_is_empty = false;
  test.SetUp();
  ResultCode rc;
  Btree btree(test.GetFilename(), 10);

  // Begin transaction
  rc = btree.BtreeBeginTrans();
  EXPECT_EQ(rc, ResultCode::kOk);

  // Create table
  rc = btree.BtreeCreateTable(table_root_page_number);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Create cursor with write access
  rc = btree.BtCursorCreate(table_root_page_number, true, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Check if the table is empty
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_TRUE(table_is_empty);

  // Visualize the initial empty tree
  // std::cout << "\nInitial empty B-tree structure:" << std::endl;
  // std::cout << Btree::VisualizeBtreeWithExistingCursor(btree, p_cursor_weak) << std::endl;

  // First, insert a sequence of ordered keys with small data to force splits
  // and create at least three levels in the B-tree
  const int NUM_ENTRIES = 100;

  std::cout << "\nInserting " << NUM_ENTRIES << " ordered entries to create multi-layer tree..." << std::endl;

  for (int i = 0; i < NUM_ENTRIES; i++) {
    // Create key and data
    u32 key_value = i * 10; // Use ordered keys: 0, 10, 20, 30...
    std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);
    std::vector<std::byte> data = BtreeStudentTest::UnsignedIntToByteVector(i + 1000);

    // Insert the key-data pair
    rc = btree.BtreeInsert(p_cursor_weak, key, data);
    EXPECT_EQ(rc, ResultCode::kOk);

  }

  // Now insert some entries with large data payloads to force overflow pages
  std::cout << "\nInserting entries with large data payloads..." << std::endl;

  // Create large data (over kMaxLocalPayload size to trigger overflow pages)
  std::vector<std::byte> large_data(600, std::byte(0xAA));

  for (int i = 0; i < 10; i++) {
    u32 key_value = 1000 + i; // Keys: 1000, 1001, 1002...
    std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);

    // Make each large data entry slightly different
    for (size_t j = 0; j < 10; j++) {
      large_data[j] = std::byte(i * 10 + j);
    }

    rc = btree.BtreeInsert(p_cursor_weak, key, large_data);
    EXPECT_EQ(rc, ResultCode::kOk);

    std::cout << "Inserted large entry with key " << key_value
              << " and data size " << large_data.size() << " bytes" << std::endl;
  }

  // Move cursor to first entry for visualization
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_FALSE(table_is_empty);

  // Get and print tree depth


  // Different visualization approaches

  // // 1. Visualize the tree structure with the cursor
  // std::cout << "\nB-Tree structure using cursor visualization:" << std::endl;
  // std::cout << Btree::VisualizeBtreeWithExistingCursor(btree, p_cursor_weak) << std::endl;
  //
  // // 2. Visualize the complete tree (if available)
  // std::cout << "\nDetailed B-Tree structure:" << std::endl;
  // std::cout << Btree::VisualizeBtree(btree, table_root_page_number) << std::endl;

  // 3. Test traversal by visiting each key in order
  std::cout << "\nTraversing the B-Tree in order:" << std::endl;
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_FALSE(table_is_empty);

  int count = 0;
  bool at_end = false;

  while (!at_end && count < 20) { // Just show first 20 entries
    // Get current key
    std::vector<std::byte> key;
    u32 key_size;
    rc = btree.BtreeKeySize(p_cursor_weak, key_size);
    EXPECT_EQ(rc, ResultCode::kOk);
    btree.BtreeKey(p_cursor_weak, 0, key_size, key);

    // Convert key back to u32 for display
    u32 key_value = 0;
    std::memcpy(&key_value, key.data(), sizeof(u32));

    std::cout << "Key: " << key_value;

    // Move to next entry
    rc = btree.BtreeNext(p_cursor_weak, at_end);
    EXPECT_EQ(rc, ResultCode::kOk);

    count++;
    if (!at_end) {
      std::cout << " → ";
    }

    if (count % 5 == 0) {
      std::cout << std::endl;
    }
  }

  if (!at_end) {
    std::cout << "... (more entries)" << std::endl;
  }

  // Clean up
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);
  std::cout << "\nDetailed B-Tree structure:" << std::endl;
  std::cout << Btree::VisualizeBtree(btree, table_root_page_number) << std::endl;
  u32 final_depth = BtreeStudentTest::FindMaximumBtreeDepth(btree, table_root_page_number);
  std::cout << "\nFinal B-tree depth: " << final_depth << std::endl;
}
