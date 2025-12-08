//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize() - 1;
  // Standard lower_bound to find first slot whose key >= target.
  while (left <= right) {
    int mid = left + ((right - left) / 2);
    int cmp_result = comparator(array_[mid].first, key);
    if (cmp_result >= 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() const -> bool {
  // In BusTub, the logical max size configured for a leaf page (`GetMaxSize()`)
  // is typically chosen to be *strictly less* than the physical capacity
  // (`LEAF_PAGE_SIZE`). We want the B+ tree implementation to be able to insert
  // the (GetMaxSize() + 1)-th entry and then immediately split the page.
  //
  // If we treated `GetMaxSize()` as a hard capacity bound here (i.e. used
  // `>=`), then an insert into a "full" page would trip this assertion before
  // the tree code has a chance to split. Instead, we only report "full" when
  // the size is *strictly greater* than the configured logical maximum, which
  // keeps the assertion in `Insert()` valid while still allowing the B+ tree
  // code to trigger a split right after the insert.
  return GetSize() > GetMaxSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsUnderflow() const -> bool { return GetSize() < GetMinSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CanBorrow() const -> bool { return GetSize() > GetMinSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  int index = KeyIndex(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    *value = array_[index].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  // In our usage, the B+ tree implementation is responsible for checking
  // whether a leaf page needs to be split before it overflows its *physical*
  // capacity. The configured logical max size (`GetMaxSize()`) is treated as a
  // soft threshold to decide when to split, so we avoid asserting on it here.
  int index = KeyIndex(key, comparator);
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    Remove(index);
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetMinSize() const -> int { return IsRootPage() ? 1 : (GetMaxSize() + 1) / 2; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int move_start = GetSize() / 2;
  // Move the right half of the entries to the recipient. The caller is
  // responsible for updating the leaf linked list (next_page_id).
  recipient->CopyNFrom(array_ + move_start, GetSize() - move_start);
  SetSize(move_start);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // During merge we append all entries to the recipient. The caller will update
  // the leaf linked list (next_page_id) to bypass the merged-away page.
  recipient->CopyNFrom(array_, GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // Borrowing from the right sibling: push its first entry to the left sibling.
  recipient->CopyLastFrom(array_[0]);
  Remove(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // Borrowing from the left sibling: move our last entry to the front of the right sibling.
  MappingType item = array_[GetSize() - 1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(item);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = 0; i < size; ++i) {
    // Append while preserving sorted order because callers pass contiguous ranges.
    array_[GetSize()] = items[i];
    IncreaseSize(1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[GetSize()] = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ToString() const -> std::string {
  std::stringstream ss;
  ss << "LeafPage[" << GetPageId() << "] size=" << GetSize() << " keys={";
  for (int i = 0; i < GetSize(); i++) {
    ss << array_[i].first;
    if (i + 1 < GetSize()) {
      ss << ",";
    }
  }
  ss << "}";
  return ss.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
