/*
 * balance.cc
 *
 * The file is dedicated to the implementation of the Btree::Balance() function.
 * Students will need to implement the 19 TODOS in this file for Assignment 3.
 *
 * Your line(s) code should replace the lines marked with the pattern "// ***"
 *
 */
#include "btree.h"

/**
 * Structure to hold all the context needed during a balance operation.
 * This avoids passing too many parameters between methods.
 */
struct BalanceContext {
  NodePage *p_parent;
  std::vector<NodePage *> divider_pages;
  std::vector<u16> num_cells_in_divider_pages;
  std::vector<PageNumber> divider_page_numbers;
  std::vector<u16> divider_cell_indexes;
  std::vector<Cell> divider_cells;
  std::vector<Cell> redistributed_cells;
  std::vector<u16> new_divider_cell_indexes;
  u16 cursor_cell_index;
  std::vector<u32> redistributed_cell_sizes;
  u32 num_cells_inserted;
  std::vector<u32> new_combined_cell_sizes;
  std::vector<std::pair<PageNumber, NodePage *>> new_page_number_to_page;
  PageNumber final_right_child;
  int divider_start_cell_idx;
};

ResultCode Btree::BalanceLeafNode(NodePage *p_page, const std::weak_ptr<BtCursor> &p_cursor) {
  ResultCode rc;
  NodePage *p_parent = p_page->p_parent_;
  BasePage *p_base_page = nullptr;
  NodePage *p_extra_unref = nullptr;

  // Step 4: Find the index of the cell in the parent that points to the current page
  int idx = BalanceHelperFindChildIdx(p_page, p_parent);

  // Check for file corruption
  bool discovered_file_corruption = (idx < 0);
  if (discovered_file_corruption) {
    return ResultCode::kCorrupt;
  }

  // Step 5-7: Collect divider pages, cells, and prepare for redistribution
  BalanceContext context;
  rc = InitializeBalanceContext(context, p_page, p_parent, p_cursor, idx, false);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 8-9: Calculate and distribute cell sizes for the new pages
  CalculateNewPageDistribution(context);

  // Step 10: Allocate new pages for the cells
  rc = AllocateNewPages(context, false);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 11: Sort the new pages by page number
  SortNewPagesByNumber(context);

  // Step 12: Insert the cells into the new pages and update the cursor
  rc = RedistributeCells(context, p_cursor, false);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 13: Re-parent the child pages
  ReParentAllPages(context);

  // Step 14: Call Balance on the parent page
  rc = Balance(p_parent, p_cursor);

  // Step 15: Cleanup
balance_cleanup:
  CleanupBalanceOperation(context, p_extra_unref, p_parent, p_cursor);
  return rc;




}

ResultCode Btree::BalanceInternalNode(NodePage *p_page,
                          const std::weak_ptr<BtCursor> &p_cursor) {
  ResultCode rc;
  NodePage *p_parent = p_page->p_parent_;
  BasePage *p_base_page = nullptr;
  NodePage *p_extra_unref = nullptr;

  // Step 4: Find the index of the cell in the parent that points to the current page
  int idx = BalanceHelperFindChildIdx(p_page, p_parent);

  // Check for file corruption
  bool discovered_file_corruption = (idx < 0);
  if (discovered_file_corruption) {
    return ResultCode::kCorrupt;
  }

  // Step 5-7: Collect divider pages, cells, and prepare for redistribution
  BalanceContext context;
  rc = InitializeBalanceContext(context, p_page, p_parent, p_cursor, idx, true);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 8-9: Calculate and distribute cell sizes for the new pages
  CalculateNewPageDistribution(context);

  // Step 10: Allocate new pages for the cells
  rc = AllocateNewPages(context, true);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 11: Sort the new pages by page number
  SortNewPagesByNumber(context);

  // Step 12: Insert the cells into the new pages and update the cursor
  rc = RedistributeCells(context, p_cursor, true);
  if (rc != ResultCode::kOk) {
    goto balance_cleanup;
  }

  // Step 13: Re-parent the child pages
  ReParentAllPages(context);

  // Step 14: Call Balance on the parent page
  rc = Balance(p_parent, p_cursor);

  // Step 15: Cleanup
balance_cleanup:
  CleanupBalanceOperation(context, p_extra_unref, p_parent, p_cursor);
  return rc;
}

/**
 * Balance a B-tree node, ensuring proper distribution of cells.
 * This is the main entry point for the balancing operation.
 *
 * @param p_page: pointer to the current page we are balancing
 * @param p_cursor: cursor attached to the balance operation
 * @return: appropriate result code
 */
ResultCode Btree::Balance(NodePage *p_page,
                          const std::weak_ptr<BtCursor> &p_cursor) {
  // Step 1: Check if the page is writable, if not, return kError.
  if (!pager_->SqlitePagerIsWritable(p_page)) {
    return ResultCode::kError;
  }

  // Step 2: Check if the page needs any balancing at all
  if (IsBalancing(p_page)) {
    p_page->RelinkCellList(); // Make sure the cells are linked properly
    return ResultCode::kOk;
  }

  ResultCode rc;
  NodePage *p_parent = p_page->p_parent_;
  BasePage *p_base_page = nullptr;
  NodePage *p_extra_unref = nullptr;

  // Step 3: Handle the special case of root page
  if (!p_parent) {
    bool return_ok_early = false;
    rc = BalanceHelperHandleRoot(p_page, p_parent, p_cursor, p_extra_unref, return_ok_early);
    if (rc != ResultCode::kOk) {
      return rc;
    }
    if (return_ok_early) {
      return ResultCode::kOk;
    }
  }

  rc = pager_->SqlitePagerWrite(p_parent);
  if (rc != ResultCode::kOk) {
    return rc;
  }

  // Check if Node page is leaf or internal
  if (p_page->IsInternalNode()) {
    rc = BalanceInternalNode(p_page, p_cursor);
  } else {
    rc = BalanceLeafNode(p_page, p_cursor);
  }
  return rc;
}

/**
 * Check if balancing is required for the given page.
 *
 * @param p_page: the page to check
 * @return: true if balancing is not required, false otherwise
 */
bool Btree::IsBalancing(NodePage *p_page) {
  return !p_page->IsOverfull() &&
         p_page->num_free_bytes_ < kPageSize / 2 &&
         p_page->GetNumCells() >= 2;
}

/**
 * Initialize the balance context with all necessary information.
 *
 * @param context: the balance context to initialize
 * @param p_page: the current page being balanced
 * @param p_parent: the parent page
 * @param p_cursor: the cursor attached to the balance operation
 * @param idx: the index of the cell in the parent that points to p_page
 * @return: appropriate result code
 */
ResultCode Btree::InitializeBalanceContext(BalanceContext &context,
                                          NodePage *p_page,
                                          NodePage *p_parent,
                                          const std::weak_ptr<BtCursor> &p_cursor,
                                          int idx, bool isInternal) {
  context.p_parent = p_parent;
  ResultCode rc;
  BasePage *p_base_page = nullptr;
  std::vector<NodePageHeaderByteView> divider_page_headers;

  // Calculate divider start cell index
  int num_cells_in_parent = (int)p_parent->GetNumCells();
  if (idx == num_cells_in_parent) {
    context.divider_start_cell_idx = idx - 2;
  } else {
    context.divider_start_cell_idx = idx - 1;
  }
  if (context.divider_start_cell_idx < 0) {
    context.divider_start_cell_idx = 0;
  }

  // Collect divider pages, cells, and cell indexes
  rc = CollectDividerPages(context, p_parent, divider_page_headers);
  if (rc != ResultCode::kOk) {
    return rc;
  }

  // Track cursor position
  rc = TrackCursorPosition(context, p_cursor, isInternal);
  if (rc != ResultCode::kOk) {
    return rc;
  }

  // Collect all cells for redistribution
  rc = CollectCellsForRedistribution(context, p_parent, divider_page_headers,
                                     isInternal);
  if (rc != ResultCode::kOk) {
    return rc;
  }

  return ResultCode::kOk;
}

/**
 * Collect divider pages from the parent node.
 *
 * @param context: the balance context
 * @param p_parent: the parent page
 * @param divider_page_headers: container for page headers
 * @return: appropriate result code
 */
ResultCode Btree::CollectDividerPages(BalanceContext &context,
                                     NodePage *p_parent,
                                     std::vector<NodePageHeaderByteView> &divider_page_headers) {
  ResultCode rc;
  BasePage *p_base_page = nullptr;
  int num_cells_in_parent = (int)p_parent->GetNumCells();

  for (int i = 0, k = context.divider_start_cell_idx; i < 3; ++i, ++k) {
    if (k < num_cells_in_parent) {
      // Left child case
      context.divider_cell_indexes.push_back(k);
      PageNumber left_child_page_number = p_parent->GetCellHeaderByteView(k).left_child;
      context.divider_page_numbers.push_back(left_child_page_number);
      context.divider_cells.push_back(p_parent->GetCell(k));
    } else if (k == num_cells_in_parent) {
      // Right child case
      PageNumber right_child_page_number = p_parent->GetNodePageHeaderByteView().right_child;
      context.divider_page_numbers.push_back(right_child_page_number);
    } else {
      break;
    }

    // Get the page and store it in divider_pages
    rc = pager_->SqlitePagerGet(context.divider_page_numbers.back(), &p_base_page,
                               NodePage::CreateDerivedPage);
    if (rc != ResultCode::kOk) {
      return rc;
    }

    auto *p_node_page = dynamic_cast<NodePage *>(p_base_page);
    divider_page_headers.push_back(p_node_page->GetNodePageHeaderByteView());
    rc = InitPage(*p_node_page, p_parent);
    if (rc != ResultCode::kOk) {
      return rc;
    }
    context.divider_pages.push_back(p_node_page);
  }

  return ResultCode::kOk;
}

/**
 * Track the cursor's position before redistribution.
 *
 * @param context: the balance context
 * @param p_cursor: the cursor attached to the balance operation
 * @return: appropriate result code
 */
ResultCode Btree::TrackCursorPosition(BalanceContext &context,
                                     const std::weak_ptr<BtCursor> &p_cursor, bool isInternal) {
  context.cursor_cell_index = 0;
  if (p_cursor.expired()) {
    return ResultCode::kOk;
  }

  auto cursor = *p_cursor.lock();
  // Find the cursor's cell index across all divider pages
  for (size_t i = 0; i < context.divider_page_numbers.size(); ++i) {
    if (cursor.p_page == context.divider_pages[i]) {
      context.cursor_cell_index += cursor.cell_index;
      break;
    }
    context.cursor_cell_index += context.divider_pages[i]->GetNumCells();

    // Special case: BalanceHelperHandleRoot could have made p_page into p_parent
    if (i < context.divider_page_numbers.size() - 1 &&
        cursor.p_page == context.p_parent &&
        cursor.cell_index == context.divider_cell_indexes[i]) {
      break;
    }
    if (isInternal) {
      ++context.cursor_cell_index;
    }
  }

  return ResultCode::kOk;
}

/**
 * Collect all cells from divider pages for redistribution.
 *
 * @param context: the balance context
 * @param p_parent: the parent page
 * @param divider_page_headers: container of page headers
 * @return: appropriate result code
 */
ResultCode Btree::CollectCellsForRedistribution(BalanceContext &context,
                                              NodePage *p_parent,
                                              const std::vector<NodePageHeaderByteView> &divider_page_headers,
                                              bool isInternal) {
  ResultCode rc;
  BasePage *p_base_page = nullptr;

  for (size_t i = 0; i < context.divider_page_numbers.size(); ++i) {
    context.num_cells_in_divider_pages.push_back(context.divider_pages[i]->GetNumCells());

    // Collect all cells from the page
    for (size_t j = 0; j < context.divider_pages[i]->GetNumCells(); ++j) {
      context.redistributed_cells.push_back(context.divider_pages[i]->GetCell(j));
      context.redistributed_cell_sizes.push_back(
          context.redistributed_cells.back().GetCellSize());
    }

    // Handle divider cells between pages
    if (i < context.divider_page_numbers.size() - 1) {
      if (isInternal) {
        // add the parent cell to redistributed cell array
        context.redistributed_cells.push_back(context.divider_cells[i]);
        // update the left child ptr of the parent cell
        context.redistributed_cells.back().cell_header_.left_child =
            divider_page_headers[i].right_child;
        // add the parent cell size to redistributed cell size array
        context.redistributed_cell_sizes.push_back(
            context.redistributed_cells.back().GetCellSize());
      }
      // 存疑
      p_parent->DropCell(context.divider_start_cell_idx);
    } else {
      context.final_right_child = divider_page_headers[i].right_child;
    }

    // Free the page after extracting its cells
    p_base_page = context.divider_pages[i];
    auto *p_node_page = dynamic_cast<NodePage *>(p_base_page);
    p_node_page->ZeroPage();

    PageNumber page_number_to_free = context.divider_page_numbers[i];
    rc = FreePage(p_base_page, page_number_to_free, false);
    if (rc != ResultCode::kOk) {
      return rc;
    }
  }

  return ResultCode::kOk;
}

/**
 * Calculate the distribution of cells in the new pages.
 *
 * @param context: the balance context
 */
void Btree::CalculateNewPageDistribution(BalanceContext &context) {
  // Calculate initial distribution
  u32 subtotal = 0;
  for (u32 i = 0; i < context.redistributed_cell_sizes.size(); ++i) {
    u32 cell_size = context.redistributed_cell_sizes[i];
    if (subtotal + cell_size > kUsableSpace) {
      context.new_combined_cell_sizes.push_back(subtotal);
      context.new_divider_cell_indexes.push_back(i);
      assert(context.new_combined_cell_sizes.back() <= kUsableSpace);
      subtotal = cell_size;
    } else {
      subtotal += cell_size;
    }
  }
  context.new_combined_cell_sizes.push_back(subtotal);
  assert(context.new_combined_cell_sizes.back() <= kUsableSpace);
  context.new_divider_cell_indexes.push_back(context.redistributed_cell_sizes.size());

  // Evenly distribute cells across pages
  BalancePageDistribution(context);
}

/**
 * Balance the distribution of cells across pages.
 *
 * @param context: the balance context
 */
void Btree::BalancePageDistribution(BalanceContext &context) {
  // Redistribute cells from front pages to back pages for better balance
  for (u32 i = context.new_combined_cell_sizes.size() - 1; i > 0; --i) {
    while (context.new_combined_cell_sizes[i] < kUsableSpace / 2) {
      context.new_divider_cell_indexes[i - 1] -= 1;
      context.new_combined_cell_sizes[i] +=
          context.redistributed_cell_sizes[context.new_divider_cell_indexes[i - 1]];
      context.new_combined_cell_sizes[i - 1] -=
          context.redistributed_cell_sizes[context.new_divider_cell_indexes[i - 1] - 1];
    }
  }

  assert(context.new_combined_cell_sizes[0] > 0);
}

/**
 * Allocate new pages for the redistributed cells.
 *
 * @param context: the balance context
 * @return: appropriate result code
 */
ResultCode Btree::AllocateNewPages(BalanceContext &context, bool isInternal) {
  ResultCode rc;

  for (u32 i = 0; i < context.new_combined_cell_sizes.size(); ++i) {
    NodePage *p_new_page = nullptr;
    PageNumber new_page_number{};

    rc = AllocatePage(p_new_page, new_page_number);
    if (rc != ResultCode::kOk) {
      return rc;
    }

    p_new_page->ZeroPage();
    p_new_page->is_init_ = true;
    p_new_page->SetNodeType(isInternal);
    context.new_page_number_to_page.emplace_back(new_page_number, p_new_page);
  }

  return ResultCode::kOk;
}

/**
 * Sort new pages by page number for better disk access patterns.
 *
 * @param context: the balance context
 */
void Btree::SortNewPagesByNumber(BalanceContext &context) {
  std::sort(context.new_page_number_to_page.begin(),
            context.new_page_number_to_page.end(),
            [](const auto& a, const auto& b) {
              return a.first < b.first;
            });
}

/**
 * Redistribute cells to the new pages and update the cursor.
 *
 * @param context: the balance context
 * @param p_cursor: the cursor attached to the balance operation
 * @return: appropriate result code
 */
ResultCode Btree::RedistributeCells(BalanceContext &context,
                                   const std::weak_ptr<BtCursor> &p_cursor, bool isInternal) {
  NodePage *p_old_page = nullptr;
  if (!p_cursor.expired()) {
    p_old_page = p_cursor.lock()->p_page;
  }

  context.num_cells_inserted = 0;
  for (u32 i = 0; i < context.new_page_number_to_page.size(); ++i) {
    PageNumber new_page_number = context.new_page_number_to_page[i].first;
    NodePage *p_new_page = context.new_page_number_to_page[i].second;

    // Insert cells into the new page
    ResultCode rc = InsertCellsIntoNewPage(context, p_new_page, p_cursor, i);
    if (rc != ResultCode::kOk) {
      return rc;
    }

    // Insert cells into parent if needed
    if (i < context.new_page_number_to_page.size() - 1) {
      rc = InsertCellsIntoParent(context, p_new_page, new_page_number, p_cursor,
                                 i, isInternal);
      if (rc != ResultCode::kOk) {
        return rc;
      }
    } else { // Handle the last linked list pointer for leaf node only
      if (!isInternal) {
        NodePageHeaderByteView page_header = p_new_page->GetNodePageHeaderByteView();
        page_header.right_child = context.final_right_child;
        p_new_page->SetNodePageHeaderByteView(page_header);
      }
    }
  }

  // Update the right child references
  UpdateRightChildReferences(context);

  // Update cursor if necessary
  if (!p_cursor.expired()) {
    auto cursor = *p_cursor.lock();
    if (context.num_cells_inserted <= context.cursor_cell_index &&
        cursor.p_page == context.p_parent &&
        cursor.cell_index > context.num_cells_in_divider_pages.back()) {
      cursor.cell_index +=
          context.new_page_number_to_page.size() - context.divider_page_numbers.size();
    } else {
      pager_->SqlitePagerRef(p_cursor.lock()->p_page);
      pager_->SqlitePagerUnref(p_old_page);
    }
  }

  return ResultCode::kOk;
}

/**
 * Insert cells into a new page during redistribution.
 *
 * @param context: the balance context
 * @param p_new_page: the new page to insert cells into
 * @param p_cursor: the cursor attached to the balance operation
 * @param page_index: the index of the new page
 * @return: appropriate result code
 */
ResultCode Btree::InsertCellsIntoNewPage(BalanceContext &context,
                                        NodePage *p_new_page,
                                        const std::weak_ptr<BtCursor> &p_cursor,
                                        u32 page_index) {
  while (context.num_cells_inserted < context.new_divider_cell_indexes[page_index]) {
    Cell cell_to_insert = context.redistributed_cells[context.num_cells_inserted];

    // Update the cursor if necessary
    if (context.num_cells_inserted == context.cursor_cell_index && !p_cursor.expired()) {
      p_cursor.lock()->p_page = p_new_page;
      p_cursor.lock()->cell_index = p_new_page->GetNumCells();
    }

    // Insert the cell
    p_new_page->InsertCell(cell_to_insert, p_new_page->GetNumCells());
    context.num_cells_inserted++;
  }

  return ResultCode::kOk;
}

/**
 * Insert cells into the parent page during redistribution.
 *
 * @param context: the balance context
 * @param p_new_page: the new page
 * @param new_page_number: the page number of the new page
 * @param p_cursor: the cursor attached to the balance operation
 * @param page_index: the index of the new page
 * @return: appropriate result code
 */
ResultCode Btree::InsertCellsIntoParent(BalanceContext &context,
                                       NodePage *p_new_page,
                                       PageNumber new_page_number,
                                       const std::weak_ptr<BtCursor> &p_cursor,
                                       u32 page_index,
                                       bool isInternal) {
  Cell cell_to_insert;
  if (isInternal) {
    // Update right child of new page
    NodePageHeaderByteView page_header = p_new_page->GetNodePageHeaderByteView();
    page_header.right_child = context.redistributed_cells[context.num_cells_inserted].cell_header_.left_child;
    p_new_page->SetNodePageHeaderByteView(page_header);
    cell_to_insert = context.redistributed_cells[context.num_cells_inserted];
  } else {
    // CHAOS: handle linked list part at here
    // Add an empty key cell to the parent
    const Cell cell_push_to_parent = context.redistributed_cells[context.num_cells_inserted - 1];
    std::vector<std::byte> key_value(cell_push_to_parent.cell_header_.key_size);
    std::memcpy(key_value.data(), cell_push_to_parent.payload_.data(), cell_push_to_parent.cell_header_.key_size);
    cell_to_insert = Cell(key_value);

    // Handle linked list
    NodePageHeaderByteView page_header = p_new_page->GetNodePageHeaderByteView();
    PageNumber next_page_number = context.new_page_number_to_page[page_index + 1].first;
    page_header.right_child = next_page_number;
    p_new_page->SetNodePageHeaderByteView(page_header);
  }
  cell_to_insert.cell_header_.left_child = new_page_number;

  // Update cursor if necessary
  if (context.num_cells_inserted == context.cursor_cell_index && !p_cursor.expired()) {
    p_cursor.lock()->p_page = context.p_parent;
    p_cursor.lock()->cell_index = context.divider_start_cell_idx;
  }

  // Insert cell into parent
  context.p_parent->InsertCell(cell_to_insert, context.divider_start_cell_idx);

  if (isInternal) {
    context.num_cells_inserted++;
  }
  context.divider_start_cell_idx++;

  return ResultCode::kOk;
}

/**
 * Update right child references after redistribution.
 *
 * @param context: the balance context
 */
void Btree::UpdateRightChildReferences(BalanceContext &context) {
  // Update last page's right child
  NodePageHeaderByteView page_header =
      context.new_page_number_to_page.back().second->GetNodePageHeaderByteView();
  page_header.right_child = context.final_right_child;
  context.new_page_number_to_page.back().second->SetNodePageHeaderByteView(page_header);

  // Update parent's references
  if (context.divider_start_cell_idx == context.p_parent->GetNumCells()) {
    // Update parent's right child
    page_header = context.p_parent->GetNodePageHeaderByteView();
    page_header.right_child = context.new_page_number_to_page.back().first;
    context.p_parent->SetNodePageHeaderByteView(page_header);
  } else {
    // Update left child of next cell
    CellHeaderByteView cell_header =
        context.p_parent->GetCellHeaderByteView(context.divider_start_cell_idx);
    cell_header.left_child = context.new_page_number_to_page.back().first;
    context.p_parent->SetCellHeaderByteView(context.divider_start_cell_idx, cell_header);
  }
}

/**
 * Re-parent all child pages after redistribution.
 *
 * @param context: the balance context
 */
void Btree::ReParentAllPages(BalanceContext &context) {
  // Re-parent child pages on new pages
  for (auto& page_pair : context.new_page_number_to_page) {
    ReParentChildPages(*page_pair.second);
  }

  // Re-parent child pages on parent
  ReParentChildPages(*context.p_parent);
}

/**
 * Clean up resources after balancing.
 *
 * @param context: the balance context
 * @param p_extra_unref: extra page to unreference
 * @param p_parent: the parent page
 * @param p_cursor: the cursor attached to the balance operation
 */
void Btree::CleanupBalanceOperation(BalanceContext &context,
                                   NodePage *p_extra_unref,
                                   NodePage *p_parent,
                                   const std::weak_ptr<BtCursor> &p_cursor) {
  if (p_extra_unref) {
    pager_->SqlitePagerUnref(p_extra_unref);
  }

  for (auto &page : context.divider_pages) {
    pager_->SqlitePagerUnref(page);
  }

  for (auto &page_number_page_pair : context.new_page_number_to_page) {
    pager_->SqlitePagerUnref(page_number_page_pair.second);
  }

  if (!p_cursor.expired() && !p_cursor.lock()->p_page) {
    p_cursor.lock()->p_page = p_parent;
    p_cursor.lock()->cell_index = 0;
  } else {
    pager_->SqlitePagerUnref(p_parent);
  }
}

/**
 * This is a helper function for Btree::Balance(), which helps handle the
 * special case of the root page. Hint: remember that the root page identifies
 * the table or index in the database. For any other page in the table, you can
 * copy its content into memory, free the page, allocate another page, and copy
 * the content back into the new page. But you cannot do this with the root
 * page!
 * @param p_page: pointer to the current page
 * @param p_parent: parent of the current page
 * @param p_cursor: weak reference to the cursor
 * @param p_extra_unref: temporary placeholder for page that needs to be unref'd
 * later
 * @param return_ok_early: early balance termination flag
 * @return: appropriate result code
 */
ResultCode
Btree::BalanceHelperHandleRoot(NodePage *&p_page, NodePage *&p_parent,
                               const std::weak_ptr<BtCursor> &p_cursor,
                               NodePage *&p_extra_unref,
                               bool &return_ok_early) {

  ResultCode rc;
  PageNumber child_page_number;
  NodePage *p_child = nullptr;

  // Step 1: Handle the case where the root page has no cells
  if (p_page->cell_trackers_.empty()) {

    /*
     * 1-1: Handle the case where the root page has a right child
     *
     * There may be cases where a single page needed to redistribute all the
     * cells during the balance process. Eventually, the Balance function will
     * attach that page to the right child of the root page with all the cells
     * in the root page being removed.
     *
     * When this occurs, we have to copy the content of the right child into the
     * root page and free the right child.
     */
    bool root_has_right_child;

    // TODO: A3 -> Assign the correct value to root_has_right_child
    // You can find the right child page number inside the root page's node page
    // header p_page should point to the root page at this point. A function in
    // node_page.cc could help you find the page's node page header. After that
    // you can check if the right child page number is 0. If it is not 0, then
    // it means that the root page has a right child. If it is 0, then it means
    // that the root page has no right child
    // TODO: Your code here

    root_has_right_child = (p_page->GetNodePageHeaderByteView().right_child != 0);

    // --------------------------

    if (root_has_right_child) {
      // Get right child
      BasePage *p_base_page = nullptr;
      child_page_number = p_page->GetNodePageHeaderByteView().right_child;
      rc = pager_->SqlitePagerGet(child_page_number, &p_base_page,
                                  NodePage::CreateDerivedPage);
      if (rc != ResultCode::kOk) {
        return rc;
      }
      p_child = dynamic_cast<NodePage *>(p_base_page);

      // Copy the right child into the root page
      p_child->CopyPage(*p_page);
      p_page->p_parent_ = nullptr;
      ReParentChildPages(*p_page);

      // Free the right child
      auto cursor = p_cursor.lock();
      if (cursor->p_page == p_child) {
        pager_->SqlitePagerUnref(p_child);
        cursor->p_page = p_page;
        pager_->SqlitePagerRef(p_page);
      }
      FreePage(p_base_page, child_page_number, false);
      pager_->SqlitePagerUnref(p_child);
    } else {
      p_page->RelinkCellList();
    }
    return_ok_early = true;
    return ResultCode::kOk;
  }

  /*
   * Step 2: Handle case where the page is not overfull
   *
   * Overfull occurs when a cell cannot be written into the page image because
   * there is not enough space. If every cell has been written into the page
   * image, we can return early.
   */
  bool root_page_is_overfull;

  // TODO: A3 -> Assign the correct value to root_page_is_overfull
  // At this point p_page points to the root page.
  // There should be a function in node_page.cc that can help you determine
  // if the page that p_page points to is overfull
  // TODO: Your code here

  root_page_is_overfull = p_page->IsOverfull();

  // -------------------------

  if (!root_page_is_overfull) {
    p_page->RelinkCellList();
    return_ok_early = true;
    return ResultCode::kOk;
  }

  // At this point, the root page is overfull.
  // Create a child page and copy the root page's content into the child page.
  // Replace the original root page with 0 cells and a right child pointer to
  // the child page. The code inside Balance() after the helper function should
  // split the child page's content properly.
  rc = pager_->SqlitePagerWrite(p_page);
  if (rc != ResultCode::kOk) {
    return rc;
  }
  rc = AllocatePage(p_child, child_page_number);
  if (rc != ResultCode::kOk) {
    return rc;
  }

  p_page->CopyPage(*p_child);
  p_child->p_parent_ = p_page;
  pager_->SqlitePagerRef(p_page);
  p_child->is_overfull_ = true;
  if (p_cursor.lock()->p_page == p_page) {
    pager_->SqlitePagerUnref(p_page);
    p_cursor.lock()->p_page = p_child;
  } else {
    p_extra_unref = p_child;
  }
  p_page->ZeroPage();
  NodePageHeaderByteView header = p_page->GetNodePageHeaderByteView();
  header.right_child = child_page_number;
  p_page->SetNodePageHeaderByteView(header);
  p_page->SetNodeType(true);
  // The next part of the balancing function will redistribute the parent's
  // cells into the child pages We can trick the function into thinking that
  // p_page is a parent page
  p_parent = p_page;
  p_page = p_child;
  return_ok_early = false; // We still need to balance the child page
  return ResultCode::kOk;
}

/**
 * This function is meant to check which cell in the parent (p_parent) points at
 * the current page (p_page)
 * @param p_page: pointer to the current page
 * @param p_parent: pointer to the parent of current page
 * @return index of correct cell or -1 if failed
 */
int Btree::BalanceHelperFindChildIdx(NodePage *p_page, NodePage *p_parent) {
  // Find the index of the p_parent's cells that points to p_page and store it
  // in idx
  int idx = -1;
  PageNumber current_page_number;

  // TODO: A3 -> Assign the correct value to current_page_number
  // The Btree layer normally doesn't know what page number each page has.
  // It is an information only known by the Pager layer.
  // Assign the correct value to current_page_number by calling the function in
  // pager.cc that returns the pager number of a page
  // TODO: Your code here

  current_page_number = pager_->SqlitePagerPageNumber(p_page);

  // -----------------------------

  // Step 1: Find the index of the cell in p_parent that points to p_page
  for (int i = 0; i < p_parent->GetNumCells(); ++i) {
    bool is_left_child = false;

    // TODO: A3 -> Assign the correct value to is_left_child
    // You should get the the cell header inside the parent nodes that points to
    // the current page.
    // If the left child of the cell points to the current page, then
    // is_left_child should be true
    //
    // There should be a function in node_page.cc that allows you to get the
    // cell header of a cell at index i.
    // TODO: Your code here

    is_left_child = (p_parent->GetCellHeaderByteView(i).left_child == current_page_number);


    // -----------------------------

    if (is_left_child) {
      // Found the index of the cell in p_parent that points to p_page
      // Store it in idx and break
      idx = i;
      break;
    }
  }

  // Step 2: Handle the case where the right child of p_parent points to p_page
  // If there is no cell in p_parent that points to p_page, check the p_parent's
  // right child It that right child points to p_page, then set idx to
  // p_parents' number of cells
  if (idx < 0 && p_parent->GetNodePageHeaderByteView().right_child ==
                     current_page_number) {
    idx = (int)p_parent->GetNumCells();
  }

  // Step 3: Return the index
  // In cases where the idx is neither found in the left child of each cell nor
  // the right child of the page, the value of -1 will be returned.
  return idx;
}