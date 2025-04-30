/*
 * btree_bt_cursor_public.cc
 *
 * The file is dedicated to the implementation of the cursor functions.
 * Students will need to implement the 11 TODOS in this file for Assignment 3.
 *
 * Your line(s) code should replace the lines marked with the pattern "// ***"
 *
 */
#include "btree.h"

// --------------------- BtCursor Constructor ---------------------
BtCursor::BtCursor() {
  root_page_number = 0;
  p_page = nullptr;
  cell_index = 0;
  writable = false;
  skip_next = false;
  compare_result = 0;
}

// --------------------- BtCursor Public Functions ---------------------

ResultCode Btree::BtCursorCreate(PageNumber root_page_number, bool writable,
                                 std::weak_ptr<BtCursor> &p_cursor_weak) {

  // Step 1: Check if the cursor can be created, return error if not
  if (writable && has_writable_bt_cursor_) {
    return ResultCode::kError;
  }

  // Step 2: Check if the database is locked, and lock it if not
  ResultCode rc;
  if (!p_first_page_) {
    rc = LockBtree();
    if (rc != ResultCode::kOk) {
      return rc;
    }
  }

  // Step 3: Check if the database is read-only, and return error if user wants
  // a writable cursor
  if (writable && read_only_) {
    return ResultCode::kReadOnly;
  }

  // Step 4: Attempt to get the root page, and return error if failed
  auto bt_cursor = std::make_shared<BtCursor>();
  bt_cursor->root_page_number = root_page_number;
  bt_cursor->p_page = nullptr;
  bt_cursor->cell_index = 0;
  bt_cursor->writable = writable;
  bt_cursor->skip_next = false;

  // Step 5: Check if the root page can be retrieved, and return error if not
  BasePage *p_base_page = nullptr;
  rc = pager_->SqlitePagerGet(root_page_number, &p_base_page,
                              NodePage::CreateDerivedPage);
  if (rc != ResultCode::kOk) {
    goto create_cursor_exception;
  }

  // Step 6: Check if the root page has invalid number of locks, and return
  // error if so
  int num_locks;
  num_locks = lock_count_map_.find(root_page_number) == lock_count_map_.end()
                  ? 0
                  : lock_count_map_[root_page_number];
  if (num_locks < 0 || (num_locks > 0 && writable)) {
    rc = ResultCode::kLocked;
    goto create_cursor_exception;
  }
  num_locks = writable ? -1 : num_locks + 1;
  lock_count_map_[root_page_number] = num_locks;

  // Step 7: Insert the BtCursor into the map
  bt_cursor->p_page = dynamic_cast<NodePage *>(p_base_page);
  bt_cursor_set_.insert(bt_cursor);
  p_cursor_weak = bt_cursor;
  if (writable) {
    has_writable_bt_cursor_ = true;
  }
  return ResultCode::kOk;

create_cursor_exception:

  if (bt_cursor) {
    if (bt_cursor->p_page) {
      pager_->SqlitePagerUnref(bt_cursor->p_page);
    }
  }
  UnlockBtreeIfUnused();
  return rc;
}

ResultCode Btree::BtCursorClose(const std::weak_ptr<BtCursor> &p_cursor_weak) {
  // Step 1: Check if the cursor exists, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  // Step 2: Update the number of locks of the cursor's root page
  int num_locks = lock_count_map_[p_cursor->root_page_number];
  num_locks = num_locks < 0 ? 0 : num_locks - 1;
  lock_count_map_[p_cursor->root_page_number] = num_locks;

  if (p_cursor->writable && has_writable_bt_cursor_) {
    has_writable_bt_cursor_ = false;
  }

  if (p_cursor->p_page) {
    pager_->SqlitePagerUnref(p_cursor->p_page);
  }

  bt_cursor_set_.erase(p_cursor);

  return ResultCode::kOk;
}

ResultCode Btree::BtreeKeySize(const std::weak_ptr<BtCursor> &p_cursor_weak,
                               u32 &key_size) {
  // Step 1: Check if the cursor exists, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  // Step 2: Using information from the BtCursor to get the key size
  auto cursor = *p_cursor;
  NodePage *p_node_page = cursor.p_page;
  if (!p_node_page || cursor.cell_index >= p_node_page->GetNumCells()) {
    key_size = 0;
  } else {
    CellHeaderByteView cell_header =
        p_node_page->GetCellHeaderByteView(cursor.cell_index);
    key_size = cell_header.key_size;
  }
  return ResultCode::kOk;
}

u32 Btree::BtreeKey(const std::weak_ptr<BtCursor> &p_cursor_weak, u32 offset,
                    u32 amount, std::vector<std::byte> &result) {

  // Step 1: Check if the cursor exists, and return 0 if not
  if (p_cursor_weak.expired()) {
    return 0;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return 0;
  }

  // Step 2: Find the key size and return 0 if the key size is 0
  auto cursor = *p_cursor;
  CellHeaderByteView cell_header{};
  result.clear();
  if (amount == 0 || !cursor.p_page ||
      cursor.cell_index >= cursor.p_page->GetNumCells()) {
    return 0;
  }
  cell_header = cursor.p_page->GetCellHeaderByteView(cursor.cell_index);
  if (amount + offset > cell_header.key_size) {
    amount = cell_header.key_size - offset;
    if (amount == 0) {
      return 0;
    }
  }

  // Step 3: Call GetPayload to get the key
  GetPayload(cursor, offset, amount, result);
  return amount;
}

ResultCode Btree::BtreeDataSize(const std::weak_ptr<BtCursor> &p_cursor_weak,
                                u32 &data_size) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  auto cursor = *p_cursor;
  if (!cursor.p_page || cursor.cell_index >= cursor.p_page->GetNumCells()) {
    data_size = 0;
  } else {
    CellHeaderByteView cell_header =
        cursor.p_page->GetCellHeaderByteView(cursor.cell_index);
    data_size = cell_header.data_size;
  }
  return ResultCode::kOk;
}

u32 Btree::BtreeData(const std::weak_ptr<BtCursor> &p_cursor_weak, u32 offset,
                     u32 amount, std::vector<std::byte> &result) {

  if (p_cursor_weak.expired()) {
    return 0;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return 0;
  }

  auto cursor = *p_cursor;
  result.clear();
  if (amount == 0 || !cursor.p_page ||
      cursor.cell_index >= cursor.p_page->GetNumCells()) {
    return 0;
  }
  CellHeaderByteView cell_header =
      cursor.p_page->GetCellHeaderByteView(cursor.cell_index);
  if (amount + offset > cell_header.data_size) {
    amount = cell_header.data_size - offset;
    if (amount == 0) {
      return 0;
    }
  }
  GetPayload(cursor, offset + cell_header.key_size, amount, result);
  return amount;
}

ResultCode Btree::BtreeKeyCompare(const std::weak_ptr<BtCursor> &p_cursor_weak,
                                  std::vector<std::byte> &key, u32 num_ignore,
                                  int &result) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  auto cursor = *p_cursor;
  if (!cursor.p_page || cursor.cell_index >= cursor.p_page->GetNumCells()) {
    return ResultCode::kError;
  }
  CellHeaderByteView cell_header =
      cursor.p_page->GetCellHeaderByteView(cursor.cell_index);
  u32 num_local =
      num_ignore > cell_header.key_size ? 0 : cell_header.key_size - num_ignore;
  u32 key_size = key.size();
  u32 n = key_size < num_local ? key_size : num_local;
  if (n > kMaxLocalPayload) {
    n = kMaxLocalPayload;
  }
  int c;
  CellTracker tracker = cursor.p_page->cell_trackers_[cursor.cell_index];
  if (!tracker.IsCellWrittenIntoImage()) {
    c = std::memcmp(tracker.cell.payload_.data(), key.data(), n);
    if (c == 0 && key.size() != tracker.cell.cell_header_.key_size) {
      c = tracker.cell.cell_header_.key_size < key.size() ? -1 : 1;
    }
    result = c;
    return ResultCode::kOk;
  }
  ImageIndex payload_start_idx = tracker.image_idx + sizeof(CellHeaderByteView);
  if (cell_header.overflow_page == 0) {
    c = std::memcmp(cursor.p_page->p_image_->data() + payload_start_idx,
                    key.data(), n);
    result = c;
    return ResultCode::kOk;
  }
  u32 key_compare_start_idx = 0;
  PageNumber next_page_number = cell_header.overflow_page;
  while (key_size > 0 && num_local > 0) {
    BasePage *p_base_page = nullptr;
    if (next_page_number == 0) {
      return ResultCode::kCorrupt;
    }
    ResultCode rc = pager_->SqlitePagerGet(next_page_number, &p_base_page,
                                           NodePage::CreateDerivedPage);
    if (rc != ResultCode::kOk) {
      return rc;
    }
    auto p_overflow_page = dynamic_cast<OverFreePage *>(p_base_page);
    OverflowPageHeaderByteView overflow_page_header =
        p_overflow_page->GetOverflowPageHeaderByteView();
    next_page_number = overflow_page_header.next_page;
    n = key_size < num_local ? key_size : num_local;
    if (n > kOverflowSize) {
      n = kOverflowSize;
    }
    c = std::memcmp(p_overflow_page->p_image_->data() +
                        sizeof(OverflowPageHeaderByteView),
                    key.data() + key_compare_start_idx, n);
    pager_->SqlitePagerUnref(p_base_page);
    if (c != 0) {
      result = c;
      return ResultCode::kOk;
    }
    key_size -= n;
    num_local -= n;
  }
  if (c == 0) {
    // The original logic is this:  c = num_local - key_size;
    // But to avoid implicit type conversion, we use the following logic
    // instead:
    c = num_local < key_size ? -1 : num_local == key_size ? 0 : 1;
  }
  result = c;
  return ResultCode::kOk;
}

ResultCode Btree::BtreeFirst(const std::weak_ptr<BtCursor> &p_cursor_weak,
                             bool &table_is_empty) {

  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  BtCursor &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }
  ResultCode rc;
  rc = MoveToRoot(cursor);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  if (cursor.p_page->GetNumCells() == 0) {
    table_is_empty = true;
    return ResultCode::kOk;
  }
  table_is_empty = false;
  rc = MoveToLeftmost(cursor);
  cursor.skip_next = false;
  return rc;
}

ResultCode Btree::BtreeLast(const std::weak_ptr<BtCursor> &p_cursor_weak,
                            bool &table_is_empty) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }

  BtCursor &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }
  if (cursor.p_page->GetNumCells() == 0) {
    table_is_empty = true;
    return ResultCode::kOk;
  }
  table_is_empty = false;
  ResultCode rc;
  while (cursor.p_page->GetNodePageHeaderByteView().right_child != 0) {
    rc = MoveToChild(cursor,
                     cursor.p_page->GetNodePageHeaderByteView().right_child);
    if (rc != ResultCode::kOk) {
      return rc;
    }
  }
  cursor.cell_index = cursor.p_page->GetNumCells() - 1;
  cursor.skip_next = false;
  return ResultCode::kOk;
}

/**
 * This function moves the cursor such that it points to an entry near the given
 * key whose value matches that in the input vector key If an exact match is
 * found, the cursor will point to an entry in a leaf node that contains the
 * given key. If not, the cursor will point to an entry that comes either before
 * or after the given key.
 *
 * Here are the values of the reference input &result for different cases:
 * result < 0: The cursor points to an entry that comes before the given key.
 * result = 0: The cursor points to an entry that contains the given key.
 * result > 0: The cursor points to an entry that comes after the given key.
 * @param p_cursor_weak: weak cursor pointer
 * @param key: key of the data
 * @param result: reference parameter
 * @return Result code
 */
// CHAOS: complete but problematic?
ResultCode Btree::BtreeMoveTo(const std::weak_ptr<BtCursor> &p_cursor_weak,
                              std::vector<std::byte> &key, int &result) {
  // Step 1: Check if the cursor exists, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }

  // Step 2: Move the cursor to the root page
  ResultCode rc;
  rc = MoveToRoot(cursor);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  rc = ResultCode::kOk;

  // Outermost while loop will continue for traversing down the tree
  while (rc == ResultCode::kOk) {

    // The variables lower_bound and upper_bound have to be signed integers.
    // This is because upper_bound must be able to become -1
    // to successfully skip the loop when the page is empty.
    int lower_bound = 0;
    int upper_bound = cursor.p_page->cell_trackers_.size() - 1;
    int c = -1;

    // This is a binary search on the cells in the current node
    while (lower_bound <= upper_bound) {
      cursor.cell_index = (lower_bound + upper_bound) / 2;
      rc = BtreeKeyCompare(p_cursor_weak, key, 0, c);
      if (rc != ResultCode::kOk) {
        return rc;
      }
      // found
      if (c == 0) {
        // CHAOS: Need to point the cursor down to leaf node when the key matches
        // force the cursor to the leaf node where it contains
        // it is a leaf
        if (!cursor.p_page->IsInternalNode()) {
          result = c;
          cursor.compare_result = c;
          return ResultCode::kOk;
        }
        // it is NOT a leaf
        lower_bound = cursor.cell_index;
        break;
      }
      if (c < 0) {
        // key on the right
        lower_bound = cursor.cell_index + 1;
      } else {
        // key on the left
        upper_bound = cursor.cell_index - 1;
      }
    }
    PageNumber child_page_number;
    // This is an edge case
    if (lower_bound >= cursor.p_page->cell_trackers_.size()) {
      child_page_number =
          cursor.p_page->GetNodePageHeaderByteView().right_child;
    } else {
      // CHAOS: general case
      CellHeaderByteView cell_header =
          cursor.p_page->GetCellHeaderByteView(lower_bound);
      child_page_number = cell_header.left_child;
    }
    if (child_page_number == 0) {
      result = c;
      cursor.compare_result = c;
      break;
    }
    rc = MoveToChild(cursor, child_page_number);
  }
  return rc;
}

ResultCode Btree::BtreeMoveToWithStop(const std::weak_ptr<BtCursor> &p_cursor_weak,
                              std::vector<std::byte> &key, int &result) {
  // Step 1: Check if the cursor exists, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }

  // Step 2: Move the cursor to the root page
  ResultCode rc;
  rc = MoveToRoot(cursor);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  rc = ResultCode::kOk;

  // Outermost while loop will continue for traversing down the tree
  while (rc == ResultCode::kOk) {

    // The variables lower_bound and upper_bound have to be signed integers.
    // This is because upper_bound must be able to become -1
    // to successfully skip the loop when the page is empty.
    int lower_bound = 0;
    int upper_bound = cursor.p_page->cell_trackers_.size() - 1;
    int c = -1;

    // This is a binary search on the cells in the current node
    while (lower_bound <= upper_bound) {
      cursor.cell_index = (lower_bound + upper_bound) / 2;
      rc = BtreeKeyCompare(p_cursor_weak, key, 0, c);
      if (rc != ResultCode::kOk) {
        return rc;
      }
      // found
      if (c == 0) {
        result = c;
        cursor.compare_result = c;
        return ResultCode::kOk;
      }
      if (c < 0) {
        // key on the right
        lower_bound = cursor.cell_index + 1;
      } else {
        // key on the left
        upper_bound = cursor.cell_index - 1;
      }
    }
    PageNumber child_page_number;
    // This is an edge case
    if (lower_bound >= cursor.p_page->cell_trackers_.size()) {
      child_page_number =
          cursor.p_page->GetNodePageHeaderByteView().right_child;
    } else {
      // CHAOS: general case
      CellHeaderByteView cell_header =
          cursor.p_page->GetCellHeaderByteView(lower_bound);
      child_page_number = cell_header.left_child;
    }
    if (child_page_number == 0) {
      result = c;
      cursor.compare_result = c;
      break;
    }
    rc = MoveToChild(cursor, child_page_number);
  }
  return rc;
}

ResultCode Btree::BtreeNext(const std::weak_ptr<BtCursor> &p_cursor_weak,
                            bool &already_at_last_entry) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }
  if (cursor.skip_next && cursor.cell_index < cursor.p_page->GetNumCells()) {
    cursor.skip_next = false;
    already_at_last_entry = false;
    return ResultCode::kOk;
  }
  cursor.cell_index++;
  ResultCode rc;
  if (cursor.cell_index >= cursor.p_page->GetNumCells()) {
    PageNumber right_child =
        cursor.p_page->GetNodePageHeaderByteView().right_child;
    if (right_child != 0 && cursor.p_page->IsInternalNode()) {
      rc = MoveToChild(cursor, right_child);
      if (rc != ResultCode::kOk) {
        return rc;
      }
      already_at_last_entry = false;
      return ResultCode::kOk;
    }
    do {
      if (cursor.p_page->p_parent_ == nullptr) {
        already_at_last_entry = true;
        return ResultCode::kOk;
      }
      rc = MoveToParent(cursor);
      if (rc != ResultCode::kOk) {
        return rc;
      }
    } while (cursor.cell_index >= cursor.p_page->GetNumCells());
    already_at_last_entry = false;
    return ResultCode::kOk;
  }
  rc = MoveToLeftmost(cursor);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  already_at_last_entry = false;
  return ResultCode::kOk;
}

ResultCode Btree::BtreeLinkedListNext(const std::weak_ptr<BtCursor> &p_cursor_weak,
                            bool &already_at_last_entry) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }

  if (cursor.cell_index < cursor.p_page->GetNumCells()) {
    cursor.cell_index++;
    already_at_last_entry = false;
    return ResultCode::kOk;
  } else {
    PageNumber next_page_number = p_cursor->p_page->GetNodePageHeaderByteView().right_child;
    if (next_page_number == 0) {
      already_at_last_entry = true;
      return ResultCode::kOk;
    } else {
      BasePage *p_base_page = nullptr;
      ResultCode rc = pager_->SqlitePagerGet(next_page_number, &p_base_page, NodePage::CreateDerivedPage);
      if (rc != ResultCode::kOk) {
        return rc;
      }
      cursor.p_page = dynamic_cast<NodePage *>(p_base_page);
      cursor.cell_index = 0;

      already_at_last_entry = false;
      return ResultCode::kOk;
    }
  }
}

ResultCode Btree::BTreePrev(const std::weak_ptr<BtCursor> &p_cursor_weak) {
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }

  PageNumber left_page_number = cursor.p_page->GetCell(cursor.cell_index).cell_header_.left_child;
  BasePage *p_base_page = nullptr;
  ResultCode rc = pager_->SqlitePagerGet(left_page_number, &p_base_page, NodePage::CreateDerivedPage);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  auto *left_child_page = dynamic_cast<NodePage *>(p_base_page);
  if (left_child_page == nullptr) {
    return ResultCode::kError;
  }
  cursor.p_page = left_child_page;
  cursor.cell_index = cursor.p_page->GetNumCells() - 1;
  return ResultCode::kOk;

}

/**
 * BtreeInsert inserts a key-data pair into the Btree and leave the cursor
 * pointing to the new entry. If the key already exists, the old data will be
 * replaced by the new data.
 * @param p_cursor_weak: weak cursor pointer
 * @param key: key of the inserted data
 * @param data: value of the inserted data
 * @return result code
 */
ResultCode Btree::BtreeInsert(const std::weak_ptr<BtCursor> &p_cursor_weak,
                              std::vector<std::byte> &key,
                              std::vector<std::byte> &data) {

  // Step 1: Check if p_cursor is valid for insertion, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }
  if (!in_trans_ || key.size() + data.size() == 0) {
    return ResultCode::kAbort;
  }
  if (!cursor.writable) {
    return ResultCode::kPerm;
  }
  ResultCode rc;

  // Step 2: Move cursor to the page where the key should be inserted
  int local_compare_result;

  // TODO: A3 -> Move the cursor the page and cell index where key should be
  // inserted. There should be a function here in this file that helps you.
  // TODO: Your code here
  // CHAOS: force to leave node
  rc = BtreeMoveTo(p_cursor_weak, key, local_compare_result);

  if (rc != ResultCode::kOk) {
    return rc;
  }
  // ----------------------------------------

  // TODO: A3 -> Call Pager to make sure that the page is writable
  // You can find the function in pager.cc
  // TODO: Your code here

  rc = pager_->SqlitePagerWrite(p_cursor->p_page);

  if (rc != ResultCode::kOk) {
    return rc;
  }
  // ----------------------------------------

  // Step 3: Fill the overflow pages for the new cell if needed
  Cell new_cell(key, data);

  // TODO: A3 -> Call A B-Tree function to fill in the overflow pages for the
  // cell if necessary. There is a function in B-Tree that does it.
  // TODO: Your code here
  // CHAOS: 存疑，应该不用改
  rc = FillInCell(new_cell);

  if (rc != ResultCode::kOk) {
    return rc;
  }
  // ----------------------------------------

  // Step 4: Prepare the page for insertion
  if (local_compare_result == 0) {
    // Case 1: There is a key-data pair inside this page that matches the give key
    //         Clear the cell inside this page so that we can replace it and insert it again
    // CHAOS: complete

    // new_cell.cell_header_.left_child =
    //     cursor.p_page->GetCellHeaderByteView(cursor.cell_index).left_child;

    // TODO: A3 -> Clear the payload inside the overflow page for this cell and
    // drop the cell from the page.
    // There are functions in B-Tree and NodePage for each of these 2 steps. One
    // in btree.cc and one in node_page.cc
    // TODO: Your code here

    rc = ClearCell(*cursor.p_page, cursor.cell_index);

    if (rc != ResultCode::kOk) {
      return rc;
    }

    cursor.p_page->DropCell(cursor.cell_index);


    // ----------------------------------------

  } else if (local_compare_result < 0 && cursor.p_page->GetNumCells() > 0) {
    // Case 2: The key-data pair is not inside this page.
    // We will increase cursor.cell_index so that our cursor will point to the
    // cell we are about to insert
    cursor.cell_index++;
  } else if (p_cursor->p_page->GetNodePageHeaderByteView().right_child != 0) {
    return ResultCode::kError;
  }
  if (!pager_->SqlitePagerIsWritable(p_cursor->p_page)) {
    return ResultCode::kError;
  }

  // Step 5: Insert the cell

  // TODO: A3 -> Insert the new_cell into the page at the cursor's cell index
  // There is a function in node_page.cc that helps you with this insert.
  // TODO: Your code here

  cursor.p_page->InsertCell(new_cell, cursor.cell_index);

  // ----------------------------------------

  // Step 6: Balance the Btree

  // TODO: A3 -> Call Balance function
  // TODO: Your code here

  rc = Balance(cursor.p_page, p_cursor_weak);

  // ----------------------------------------

  return rc;
}

/*
 *  Deletes an entry that p_cursor is pointing to.
 *
 */
/**
 * BtreeDelete deletes an entry that p_cursor is pointing to
 * @param p_cursor_weak: weak cursor pointer
 * @return
 */
ResultCode Btree::BtreeDelete(const std::weak_ptr<BtCursor> &p_cursor_weak) {
  // CHAOS: pcursor weak must be handled to point to the leave node instead of any internal node like btree

  // Step 1: Check if p_cursor is valid for deletion, and return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }
  if (!in_trans_) {
    return ResultCode::kError;
  }
  if (cursor.cell_index >= cursor.p_page->GetNumCells()) {
    return ResultCode::kError;
  }
  if (!cursor.writable) {
    return ResultCode::kPerm;
  }
  ResultCode rc;

  // TODO: A3 -> Check with Pager layer to make sure that the page is writable
  // Before we write anything into a page, we must check with Pager to make sure
  // that the page is writable.
  // TODO: Your code here

  rc = pager_->SqlitePagerWrite(cursor.p_page);

  if (rc != ResultCode::kOk) {
    return rc;
  }
  // ----------------------------------------
  // Save the key value for finding internal cursor in B+ tree
  Cell current_cell_delete = cursor.p_page->GetCell(cursor.cell_index);
  std::vector<std::byte> target_key_value(current_cell_delete.cell_header_.key_size);
  std::memcpy(target_key_value.data(), current_cell_delete.payload_.data(), current_cell_delete.cell_header_.key_size);

  // Step 2: Find the child page number
  // CHAOS: Maybe not necessary for b+ treee
  PageNumber child_page_number = 0;

  // TODO: A3 -> Assign the correct value to child_page_number
  // The value can be found by accessing the cell header of the cell that the
  // cursor is pointing to.
  // The left child page number is stored in tha cell header
  // TODO: Your code here

  child_page_number = cursor.p_page->GetCellHeaderByteView(cursor.cell_index).left_child;

  // ----------------------------------------

  // Step 3: Clear all the overflow pages tied to this cell

  // TODO: A3 -> Clear the overflow pages
  // There should be a function in this file that helps you.
  // TODO: Your code here

  rc = ClearCell(*cursor.p_page, cursor.cell_index);

  if (rc != ResultCode::kOk) {
    return rc;
  }
  // ----------------------------------------

  // Step 4: Handle cases for internal and leaf node deletion
  // CHAOS: Maybe will not be considered for b+ tree
  if (child_page_number != 0) {
    BasePage *p_base_page = nullptr;
    ResultCode rc = pager_->SqlitePagerGet(child_page_number, &p_base_page, NodePage::CreateDerivedPage);
    if (rc != ResultCode::kOk) {
      return rc;
    }
    auto *child_page = dynamic_cast<NodePage *>(p_base_page);
    // Case 1: We are deleting an entry in an internal page
    // You won't need to worry about this part for the assignment.

    // We will need to move some data from the leaf page to the internal page
    std::shared_ptr<BtCursor> p_leaf_cursor = std::make_shared<BtCursor>();
    GetTempCursor(cursor, *p_leaf_cursor);
    std::weak_ptr<BtCursor> p_leaf_cursor_weak = p_leaf_cursor;
    bool already_at_last_entry;
    bt_cursor_set_.insert(p_leaf_cursor);

    if (child_page->IsInternalNode()) {

      // Return early if BtreeNext fails
      rc = BtreeNext(p_leaf_cursor_weak, already_at_last_entry);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      // Return early if SqlitePagerWrite fails
      rc = pager_->SqlitePagerWrite(p_leaf_cursor->p_page);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }

      // Insert a cell, balance it, and then drop the cell
      cursor.p_page->DropCell(cursor.cell_index);
      Cell next_cell = p_leaf_cursor->p_page->GetCell(p_leaf_cursor->cell_index);
      next_cell.cell_header_.left_child = child_page_number;
      p_cursor->p_page->InsertCell(next_cell, p_cursor->cell_index);
      rc = Balance(p_cursor->p_page, p_cursor_weak);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      p_cursor->skip_next = true;
      p_leaf_cursor->p_page->DropCell(p_leaf_cursor->cell_index);
      rc = Balance(p_cursor->p_page, p_cursor_weak);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      ReleaseTempCursor(*p_leaf_cursor);
      bt_cursor_set_.erase(p_leaf_cursor);
    } else {
      rc = BTreePrev(p_leaf_cursor_weak);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      // Return early if SqlitePagerWrite fails
      rc = pager_->SqlitePagerWrite(p_leaf_cursor->p_page);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }

      // Insert a cell, balance it, and then drop the cell
      cursor.p_page->DropCell(cursor.cell_index);
      Cell cell_push_to_parent = p_leaf_cursor->p_page->GetCell(p_leaf_cursor->cell_index);

      std::vector<std::byte> key_value(cell_push_to_parent.cell_header_.key_size);
      std::memcpy(key_value.data(), cell_push_to_parent.payload_.data(), cell_push_to_parent.cell_header_.key_size);
      Cell next_cell = Cell(key_value);

      next_cell.cell_header_.left_child = child_page_number;
      p_cursor->p_page->InsertCell(next_cell, p_cursor->cell_index);
      rc = Balance(p_cursor->p_page, p_cursor_weak);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      // p_cursor->skip_next = true;
      // p_leaf_cursor->p_page->DropCell(p_leaf_cursor->cell_index);
      rc = Balance(p_cursor->p_page, p_cursor_weak);
      if (rc != ResultCode::kOk) {
        bt_cursor_set_.erase(p_leaf_cursor);
        return rc;
      }
      ReleaseTempCursor(*p_leaf_cursor);
      bt_cursor_set_.erase(p_leaf_cursor);
    }
  } else {
    // Case 2: We are deleting an entry in a leaf page

    // TODO: A3 -> Drop the cell from the page
    // Drop the cell from the page that the cursor is pointing to
    // There is a function in NodePage that helps you with this.
    // TODO: Your code here

    cursor.p_page->DropCell(cursor.cell_index);

    // ----------------------------------------

    if (cursor.cell_index >= cursor.p_page->GetNumCells()) {
      if (cursor.p_page->GetNumCells() == 0) {
        cursor.cell_index = 0;
        cursor.skip_next = true;
      } else {
        cursor.cell_index = cursor.p_page->GetNumCells() - 1;
        cursor.skip_next = false;
      }
    } else {
      cursor.skip_next = true;
    }

    // TODO: A3 -> Balance
    // Call the balance function on the page that the cursor is pointing to.
    // TODO: Your code here

    rc = Balance(cursor.p_page, p_cursor_weak);

    // Stop the cursor at the internal node with target key value, if the node still exist after balance
    BtreeMoveToWithStop(p_cursor_weak, target_key_value, cursor.compare_result);

    if (cursor.compare_result == 0) { // HIT
      BtreeDelete(p_cursor_weak);
    }


    // ----------------------------------------
  }
  return rc;
}

// Helper function created for Btree Assignment test
// Returns the depth of the node that the cursor is pointing to by reference
ResultCode
Btree::BtreeGetNodeDepth(const std::weak_ptr<BtCursor> &p_cursor_weak,
                         u32 &depth) {

  // Step 1: Check if p_cursor is valid. Return error if not
  if (p_cursor_weak.expired()) {
    return ResultCode::kError;
  }
  auto p_cursor = p_cursor_weak.lock();
  if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
    return ResultCode::kError;
  }
  auto &cursor = *p_cursor;
  if (!cursor.p_page) {
    return ResultCode::kAbort;
  }

  // Step 2: Find the depth of the node that the cursor is pointing to
  depth = 1;
  NodePage *p_page = cursor.p_page;
  while (p_page->p_parent_) {
    p_page = p_page->p_parent_;
    depth++;
  }
  return ResultCode::kOk;
}

/*
 * Result = 0, Hit
 * Result = 1, Miss
 * Result = -1, Error
 */
std::vector<std::byte> Btree::BtreeSearch(const std::weak_ptr<BtCursor> &p_cursor_weak,
                              std::vector<std::byte> &key, int &result) {
  ResultCode rc;
  rc = BtreeMoveTo(p_cursor_weak, key, result);
  if (rc != ResultCode::kOk) {
    result = -1;
    return {};
  }

  if (result == 0) { // Hit
    if (p_cursor_weak.expired()) {
      result = -1;
      return {};
    }
    auto p_cursor = p_cursor_weak.lock();
    if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
      result = -1;
      return {};
    }
    auto &cursor = *p_cursor;
    if (!cursor.p_page) {
      result = -1;
      return {};
    }

    Cell target_cell = cursor.p_page->GetCell(cursor.cell_index);
    std::vector<std::byte> target_data(target_cell.cell_header_.data_size);
    std::memcpy(target_data.data(), target_cell.payload_.data() + target_cell.cell_header_.key_size, target_cell.cell_header_.data_size);

    result = 0;
    return target_data;

  } else { // Miss
    result = 1;
    return {};
  }

}


/*
 * Result = 0, Hit
 * Result = 1, Miss
 * Result = -1, Error
 */
std::vector<std::vector<std::byte>> Btree::BtreeRangeSearch(const std::weak_ptr<BtCursor> &p_cursor_weak,
                              std::vector<std::byte> &key_start, std::vector<std::byte> &key_end,int &result) {
  ResultCode rc;
  rc = BtreeMoveTo(p_cursor_weak, key_start, result);
  if (rc != ResultCode::kOk) {
    result = -1;
    return {};
  }

  if (result == 0) { // Hit
    if (p_cursor_weak.expired()) {
      result = -1;
      return {};
    }
    auto p_cursor = p_cursor_weak.lock();
    if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
      result = -1;
      return {};
    }
    auto &cursor = *p_cursor;
    if (!cursor.p_page) {
      result = -1;
      return {};
    }

    std::vector<std::vector<std::byte>> target_data_vector;
    bool already_at_last_entry;
    int compare_result;

    // Process entries until we reach the end or exceed key_end
    do {
      // Make sure we have a valid cell before trying to access it
      if (cursor.cell_index < cursor.p_page->GetNumCells()) {
        Cell target_cell = cursor.p_page->GetCell(cursor.cell_index);

        // Only add non-empty data
        if (target_cell.cell_header_.data_size > 0) {
          std::vector<std::byte> target_data(target_cell.cell_header_.data_size);
          std::memcpy(target_data.data(),
                     target_cell.payload_.data() + target_cell.cell_header_.key_size,
                     target_cell.cell_header_.data_size);
          target_data_vector.push_back(target_data);
        }
      }

      // Move to next entry
      rc = BtreeLinkedListNext(p_cursor_weak, already_at_last_entry);
      if (rc != ResultCode::kOk || already_at_last_entry) {
        break; // Exit if we're at the last entry or encounter an error
      }

      // Compare with end key only if we're not at the last entry
      rc = BtreeKeyCompare(p_cursor_weak, key_end, 0, compare_result);
      if (rc != ResultCode::kOk) {
        break;
      }
    } while (compare_result <= 0); // Exit when key > key_end

    result = 0;
    return target_data_vector;

  } else { // Miss
    // We didn't find an exact match for key_start
    if (p_cursor_weak.expired()) {
        result = -1;
        return {};
    }
    auto p_cursor = p_cursor_weak.lock();
    if (bt_cursor_set_.find(p_cursor) == bt_cursor_set_.end()) {
        result = -1;
        return {};
    }
    auto &cursor = *p_cursor;
    if (!cursor.p_page) {
        result = -1;
        return {};
    }

    std::vector<std::vector<std::byte>> target_data_vector;
    bool already_at_last_entry;
    int compare_result;

    // Check if the cursor is pointing to a key less than key_start
    if (result < 0) {
        // Cursor is pointing to a key less than key_start
        // We need to move to the next entry that's >= key_start
        rc = BtreeNext(p_cursor_weak, already_at_last_entry);

        // Error case
        if (rc != ResultCode::kOk) {
            result = -1;
            return {};
        }
        // Last Cell case
        if (already_at_last_entry) {
          result = 1;
          return {};
        }
    }

    // At this point, cursor should be at the first entry >= key_start
    // Now check if this entry is <= key_end
    rc = BtreeKeyCompare(p_cursor_weak, key_end, 0, compare_result);
    if (rc != ResultCode::kOk) {
        result = -1;
        return {};
    }

    // If cursor is pointing to a key greater than key_end, return empty result
    if (compare_result > 0) {
        result = 1;
        return {};
    }

    // Process all entries between key_start and key_end
    do {
        Cell target_cell = cursor.p_page->GetCell(cursor.cell_index);
        std::vector<std::byte> target_data(target_cell.cell_header_.data_size);
        std::memcpy(target_data.data(),
                   target_cell.payload_.data() + target_cell.cell_header_.key_size,
                   target_cell.cell_header_.data_size);
        target_data_vector.push_back(target_data);

        rc = BtreeLinkedListNext(p_cursor_weak, already_at_last_entry);
        if (rc != ResultCode::kOk) {
            result = -1;
            return {};
        }

        // Check if we've gone past key_end
        if (!already_at_last_entry) {
            rc = BtreeKeyCompare(p_cursor_weak, key_end, 0, compare_result);
            if (rc != ResultCode::kOk) {
                result = -1;
                return {};
            }
        }
        // Continue until we reach the end or exceed key_end
    } while (!already_at_last_entry && compare_result <= 0);

    // We did find results, but they weren't exact matches for key_start
    result = 1;
    return target_data_vector;
  }
}