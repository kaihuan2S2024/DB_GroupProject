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
    u32 key_value = i; // Use ordered keys: 0, 10, 20, 30...
    std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);
    std::vector<std::byte> data = BtreeStudentTest::UnsignedIntToByteVector(i + 1000);
    // Insert the key-data pair
    rc = btree.BtreeInsert(p_cursor_weak, key, data);
    EXPECT_EQ(rc, ResultCode::kOk);

  }

  // u32 key_value = 35;
  // std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);
  // int compare_result;
  // rc = btree.BtreeMoveTo(p_cursor_weak, key, compare_result);
  // EXPECT_EQ(rc, ResultCode::kOk);
  // EXPECT_EQ(compare_result, 0);

  // rc = btree.BtreeDelete(p_cursor_weak);

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

TEST(BtreeStudentTest, InsertAndVisualizeStringKey) {
  // Setup
  std::string test_name = "InsertAndVisualizeStringKey";
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

  // Insert string keys with data
  std::vector<std::string> string_keys = {
      "apple", "banana", "cherry", "dates", "elderberry",
      "fig", "grape", "honeydew", "kiwis", "lemon"
  };

  std::cout << "\nInserting string keys into B-tree..." << std::endl;

  for (size_t i = 0; i < string_keys.size(); i++) {
    // Convert string to vector<std::byte> using our helper
    std::vector<std::byte> key = Btree::StringToByteVector(string_keys[i]);

    // Create some data associated with each key
    u32 data_value = 1000 + i;
    std::vector<std::byte> data = BtreeStudentTest::UnsignedIntToByteVector(data_value);

    // Insert the key-data pair
    rc = btree.BtreeInsert(p_cursor_weak, key, data);
    EXPECT_EQ(rc, ResultCode::kOk);

    std::cout << "Inserted key: " << string_keys[i] << ", data: " << data_value << std::endl;
  }

  // Test retrieving a specific string key
  std::string test_key = "grape";
  std::vector<std::byte> search_key = Btree::StringToByteVector(test_key);
  int compare_result;

  // Move cursor to the key
  rc = btree.BtreeMoveTo(p_cursor_weak, search_key, compare_result);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(compare_result, 0); // Key should be found

  // Verify the data associated with this key
  std::vector<std::byte> retrieved_data;
  u32 retrieved_data_size;
  rc = btree.BtreeDataSize(p_cursor_weak, retrieved_data_size);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(retrieved_data_size, sizeof(u32));

  btree.BtreeData(p_cursor_weak, 0, retrieved_data_size, retrieved_data);
  u32 retrieved_data_int;
  std::memcpy(&retrieved_data_int, retrieved_data.data(), sizeof(u32));
  EXPECT_EQ(retrieved_data_int, 1000 + 6); // "grape" is at index 6

  // Test moving through the keys
  bool already_at_last_entry = false;
  rc = btree.BtreeNext(p_cursor_weak, already_at_last_entry);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_FALSE(already_at_last_entry);

  // Get the key after "grape" which should be "honeydew"
  std::vector<std::byte> next_key;
  u32 next_key_size;
  rc = btree.BtreeKeySize(p_cursor_weak, next_key_size);
  EXPECT_EQ(rc, ResultCode::kOk);
  btree.BtreeKey(p_cursor_weak, 0, next_key_size, next_key);

  // Convert vector<std::byte> back to string for comparison using our helper
  std::string next_key_str = Btree::ByteVectorToString(next_key);
  EXPECT_EQ(next_key_str, "honeydew");

  // Test deleting a key
  std::string delete_key = "banana";
  std::vector<std::byte> delete_key_bytes = Btree::StringToByteVector(delete_key);
  rc = btree.BtreeMoveTo(p_cursor_weak, delete_key_bytes, compare_result);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_EQ(compare_result, 0); // Key should be found

  rc = btree.BtreeDelete(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Verify key was deleted
  rc = btree.BtreeMoveTo(p_cursor_weak, delete_key_bytes, compare_result);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_NE(compare_result, 0); // Key should not be found

  // Clean up
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);

  // Create another cursor for read-only operations to check the final structure
  rc = btree.BtCursorCreate(table_root_page_number, false, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Test traversing through all the keys in order
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);
  EXPECT_FALSE(table_is_empty);

  std::cout << "\nTraversing all string keys in order:" << std::endl;
  int key_count = 0;
  already_at_last_entry = false;

  while (!already_at_last_entry) {
    std::vector<std::byte> current_key;
    u32 current_key_size;

    rc = btree.BtreeKeySize(p_cursor_weak, current_key_size);
    EXPECT_EQ(rc, ResultCode::kOk);

    btree.BtreeKey(p_cursor_weak, 0, current_key_size, current_key);
    std::string current_key_str = Btree::ByteVectorToString(current_key);

    std::cout << key_count + 1 << ". " << current_key_str << std::endl;

    rc = btree.BtreeNext(p_cursor_weak, already_at_last_entry);
    EXPECT_EQ(rc, ResultCode::kOk);
    key_count++;
  }

  EXPECT_EQ(key_count, string_keys.size() - 1); // One less after deletion

  // Test for consistent ordering
  rc = btree.BtreeFirst(p_cursor_weak, table_is_empty);
  EXPECT_EQ(rc, ResultCode::kOk);

  std::vector<std::string> expected_order = {"apple", "cherry", "dates", "elderberry",
                                            "fig", "grape", "honeydew", "kiwis", "lemon"};

  for (const auto& expected : expected_order) {
    std::vector<std::byte> current_key;
    u32 current_key_size;

    rc = btree.BtreeKeySize(p_cursor_weak, current_key_size);
    EXPECT_EQ(rc, ResultCode::kOk);

    btree.BtreeKey(p_cursor_weak, 0, current_key_size, current_key);
    std::string current_key_str = Btree::ByteVectorToString(current_key);

    EXPECT_EQ(current_key_str, expected);

    rc = btree.BtreeNext(p_cursor_weak, already_at_last_entry);
    EXPECT_EQ(rc, ResultCode::kOk);

    if (expected == expected_order.back()) {
      EXPECT_TRUE(already_at_last_entry);
    } else {
      EXPECT_FALSE(already_at_last_entry);
    }
  }

  // Final cursor close
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Visualize the B-tree with string keys
  std::cout << "\nB-Tree structure with string keys:" << std::endl;
  std::cout << Btree::VisualizeBtree(btree, table_root_page_number) << std::endl;
}

TEST(BtreeStudentTest, TestBtreeSearch) {
  // Setup
  std::string test_name = "TestBtreeSearch";
  PageNumber table_root_page_number = 0;
  std::weak_ptr<BtCursor> p_cursor_weak;
  BtreeStudentTest test = BtreeStudentTest(test_name);
  test.SetUp();
  ResultCode rc;
  Btree btree(test.GetFilename(), 10);

  // Begin transaction and create table
  rc = btree.BtreeBeginTrans();
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCreateTable(table_root_page_number);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Create cursor
  rc = btree.BtCursorCreate(table_root_page_number, true, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Insert several entries with different keys and values
  const int NUM_ENTRIES = 10;
  for (int i = 0; i < NUM_ENTRIES; i++) {
    u32 key_value = i * 10; // Keys: 0, 10, 20, 30...
    u32 data_value = i * 100 + 5; // Data: 5, 105, 205, 305...

    std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);
    std::vector<std::byte> data = BtreeStudentTest::UnsignedIntToByteVector(data_value);

    rc = btree.BtreeInsert(p_cursor_weak, key, data);
    EXPECT_EQ(rc, ResultCode::kOk);
  }

  // Test 1: Search for existing key
  {
    u32 search_key_value = 30; // This should exist
    std::vector<std::byte> search_key = BtreeStudentTest::UnsignedIntToByteVector(search_key_value);
    int result;
    std::vector<std::byte> found_data = btree.BtreeSearch(p_cursor_weak, search_key, result);

    // Verify the result
    EXPECT_EQ(result, 0); // 0 means hit
    EXPECT_FALSE(found_data.empty());

    // Convert the data back to an integer and verify
    u32 retrieved_data_value;
    std::memcpy(&retrieved_data_value, found_data.data(), sizeof(u32));
    EXPECT_EQ(retrieved_data_value, 305); // Should be 3 * 100 + 5
  }

  // Test 2: Search for non-existing key
  {
    u32 search_key_value = 25; // This doesn't exist
    std::vector<std::byte> search_key = BtreeStudentTest::UnsignedIntToByteVector(search_key_value);
    int result;
    std::vector<std::byte> found_data = btree.BtreeSearch(p_cursor_weak, search_key, result);

    // Verify the result
    EXPECT_EQ(result, 1); // 1 means miss
    EXPECT_TRUE(found_data.empty());
  }

  // Test 3: Search at beginning of tree
  {
    u32 search_key_value = 0; // First entry
    std::vector<std::byte> search_key = BtreeStudentTest::UnsignedIntToByteVector(search_key_value);
    int result;
    std::vector<std::byte> found_data = btree.BtreeSearch(p_cursor_weak, search_key, result);

    EXPECT_EQ(result, 0); // 0 means hit
    EXPECT_FALSE(found_data.empty());

    u32 retrieved_data_value;
    std::memcpy(&retrieved_data_value, found_data.data(), sizeof(u32));
    EXPECT_EQ(retrieved_data_value, 5); // Should be 0 * 100 + 5
  }

  // Test 4: Search at end of tree
  {
    u32 search_key_value = 90; // Last entry
    std::vector<std::byte> search_key = BtreeStudentTest::UnsignedIntToByteVector(search_key_value);
    int result;
    std::vector<std::byte> found_data = btree.BtreeSearch(p_cursor_weak, search_key, result);

    EXPECT_EQ(result, 0); // 0 means hit
    EXPECT_FALSE(found_data.empty());

    u32 retrieved_data_value;
    std::memcpy(&retrieved_data_value, found_data.data(), sizeof(u32));
    EXPECT_EQ(retrieved_data_value, 905); // Should be 9 * 100 + 5
  }

  // Test 5: Search after deletion
  {
    // Delete an entry
    u32 delete_key_value = 50;
    std::vector<std::byte> delete_key = BtreeStudentTest::UnsignedIntToByteVector(delete_key_value);
    int compare_result;
    rc = btree.BtreeMoveTo(p_cursor_weak, delete_key, compare_result);
    EXPECT_EQ(rc, ResultCode::kOk);
    EXPECT_EQ(compare_result, 0);

    rc = btree.BtreeDelete(p_cursor_weak);
    EXPECT_EQ(rc, ResultCode::kOk);

    // Now search for the deleted key
    int result;
    std::vector<std::byte> found_data = btree.BtreeSearch(p_cursor_weak, delete_key, result);

    EXPECT_EQ(result, 1); // 1 means miss
    EXPECT_TRUE(found_data.empty());
  }

  // Clean up
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);
}

TEST(BtreeStudentTest, TestBtreeRangeSearch) {
  // Setup
  std::string test_name = "TestBtreeRangeSearch";
  PageNumber table_root_page_number = 0;
  std::weak_ptr<BtCursor> p_cursor_weak;
  BtreeStudentTest test = BtreeStudentTest(test_name);
  test.SetUp();
  ResultCode rc;
  Btree btree(test.GetFilename(), 10);

  // Begin transaction and create table
  rc = btree.BtreeBeginTrans();
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCreateTable(table_root_page_number);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Create cursor
  rc = btree.BtCursorCreate(table_root_page_number, true, p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);

  // Insert a sequence of entries with different keys and values
  const int NUM_ENTRIES = 20;
  for (int i = 0; i < NUM_ENTRIES; i++) {
    u32 key_value = i * 5; // Keys: 0, 5, 10, 15...
    u32 data_value = 1000 + i; // Data: 1000, 1001, 1002...

    std::vector<std::byte> key = BtreeStudentTest::UnsignedIntToByteVector(key_value);
    std::vector<std::byte> data = BtreeStudentTest::UnsignedIntToByteVector(data_value);

    rc = btree.BtreeInsert(p_cursor_weak, key, data);
    EXPECT_EQ(rc, ResultCode::kOk);
  }

  // Test 1: Range search with exact start and end keys
  {
    u32 start_key_value = 15;
    u32 end_key_value = 40;
    std::vector<std::byte> start_key = BtreeStudentTest::UnsignedIntToByteVector(start_key_value);
    std::vector<std::byte> end_key = BtreeStudentTest::UnsignedIntToByteVector(end_key_value);

    int result;
    std::vector<std::vector<std::byte>> range_data =
        btree.BtreeRangeSearch(p_cursor_weak, start_key, end_key, result);

    // Verify the result
    EXPECT_EQ(result, 0); // 0 means hit on start key
    EXPECT_EQ(range_data.size(), 6); // Should find 6 entries: 15, 20, 25, 30, 35, 40

    // Check the first and last entries
    if (!range_data.empty()) {
      u32 first_data_value;
      std::memcpy(&first_data_value, range_data.front().data(), sizeof(u32));
      EXPECT_EQ(first_data_value, 1003); // 15/5 = 3, so 1000 + 3

      u32 last_data_value;
      std::memcpy(&last_data_value, range_data.back().data(), sizeof(u32));
      EXPECT_EQ(last_data_value, 1008); // 40/5 = 8, so 1000 + 8
    }
  }

  // Test 2: Range search with start key not found (but within range)
  {
    u32 start_key_value = 17; // Not in tree
    u32 end_key_value = 30;
    std::vector<std::byte> start_key = BtreeStudentTest::UnsignedIntToByteVector(start_key_value);
    std::vector<std::byte> end_key = BtreeStudentTest::UnsignedIntToByteVector(end_key_value);

    int result;
    std::vector<std::vector<std::byte>> range_data =
        btree.BtreeRangeSearch(p_cursor_weak, start_key, end_key, result);

    // Verify the result
    EXPECT_EQ(result, 1); // 1 means miss on start key
    EXPECT_EQ(range_data.size(), 3); // Should find 3 entries: 20, 25, 30

    // Check the first entry
    if (!range_data.empty()) {
      u32 first_data_value;
      std::memcpy(&first_data_value, range_data.front().data(), sizeof(u32));
      EXPECT_EQ(first_data_value, 1004); // 20/5 = 4, so 1000 + 4
    }
  }

  // Test 4: Range search entire tree
  {
    u32 start_key_value = 0;
    u32 end_key_value = 95; // Last key is 95
    std::vector<std::byte> start_key = BtreeStudentTest::UnsignedIntToByteVector(start_key_value);
    std::vector<std::byte> end_key = BtreeStudentTest::UnsignedIntToByteVector(end_key_value);

    int result;
    std::vector<std::vector<std::byte>> range_data =
        btree.BtreeRangeSearch(p_cursor_weak, start_key, end_key, result);

    EXPECT_EQ(result, 0); // 0 means hit on start key
    EXPECT_EQ(range_data.size(), NUM_ENTRIES); // Should find all entries

    // Check first, middle and last entries
    if (range_data.size() == NUM_ENTRIES) {
      u32 first_data_value, middle_data_value, last_data_value;
      std::memcpy(&first_data_value, range_data.front().data(), sizeof(u32));
      std::memcpy(&middle_data_value, range_data[NUM_ENTRIES/2].data(), sizeof(u32));
      std::memcpy(&last_data_value, range_data.back().data(), sizeof(u32));

      EXPECT_EQ(first_data_value, 1000);
      EXPECT_EQ(middle_data_value, 1000 + NUM_ENTRIES/2);
      EXPECT_EQ(last_data_value, 1000 + NUM_ENTRIES - 1);
    }
  }

  // Test 5: Range search with both keys not in tree
  {
    u32 start_key_value = 22; // Not in tree
    u32 end_key_value = 38; // Not in tree
    std::vector<std::byte> start_key = BtreeStudentTest::UnsignedIntToByteVector(start_key_value);
    std::vector<std::byte> end_key = BtreeStudentTest::UnsignedIntToByteVector(end_key_value);

    int result;
    std::vector<std::vector<std::byte>> range_data =
        btree.BtreeRangeSearch(p_cursor_weak, start_key, end_key, result);

    EXPECT_EQ(result, 1); // 1 means miss on start key
    EXPECT_EQ(range_data.size(), 3); // Should find 3 entries: 25, 30, 35

    // Check the entries
    if (range_data.size() == 3) {
      u32 data_values[3];
      for (int i = 0; i < 3; i++) {
        std::memcpy(&data_values[i], range_data[i].data(), sizeof(u32));
      }

      EXPECT_EQ(data_values[0], 1005); // 25/5 = 5, so 1000 + 5
      EXPECT_EQ(data_values[1], 1006); // 30/5 = 6, so 1000 + 6
      EXPECT_EQ(data_values[2], 1007); // 35/5 = 7, so 1000 + 7
    }
  }

  // Test 6: Range search after deletion
  {
    // Delete a range of entries
    for (int i = 4; i <= 6; i++) { // Delete keys 20, 25, 30
      u32 delete_key_value = i * 5;
      std::vector<std::byte> delete_key = BtreeStudentTest::UnsignedIntToByteVector(delete_key_value);
      int compare_result;
      rc = btree.BtreeMoveTo(p_cursor_weak, delete_key, compare_result);
      EXPECT_EQ(rc, ResultCode::kOk);
      EXPECT_EQ(compare_result, 0);

      rc = btree.BtreeDelete(p_cursor_weak);
      EXPECT_EQ(rc, ResultCode::kOk);
    }

    // Now search for a range that includes the deleted entries
    u32 start_key_value = 15;
    u32 end_key_value = 35;
    std::vector<std::byte> start_key = BtreeStudentTest::UnsignedIntToByteVector(start_key_value);
    std::vector<std::byte> end_key = BtreeStudentTest::UnsignedIntToByteVector(end_key_value);

    int result;
    std::vector<std::vector<std::byte>> range_data =
        btree.BtreeRangeSearch(p_cursor_weak, start_key, end_key, result);

    EXPECT_EQ(result, 0); // 0 means hit on start key
    EXPECT_EQ(range_data.size(), 2); // Should find only 2 entries: 15, 35

    // Check the entries
    if (range_data.size() == 2) {
      u32 first_data_value, second_data_value;
      std::memcpy(&first_data_value, range_data[0].data(), sizeof(u32));
      std::memcpy(&second_data_value, range_data[1].data(), sizeof(u32));

      EXPECT_EQ(first_data_value, 1003); // 15/5 = 3, so 1000 + 3
      EXPECT_EQ(second_data_value, 1007); // 35/5 = 7, so 1000 + 7
    }
  }

  // Clean up
  rc = btree.BtCursorClose(p_cursor_weak);
  EXPECT_EQ(rc, ResultCode::kOk);
  rc = btree.BtreeCommit();
  EXPECT_EQ(rc, ResultCode::kOk);

  std::cout << Btree::VisualizeBtree(btree, table_root_page_number) << std::endl;
}
