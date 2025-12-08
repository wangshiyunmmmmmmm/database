//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  IndexIterator();
  IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager);
  IndexIterator(const IndexIterator &) = delete;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;
  IndexIterator(IndexIterator &&other) noexcept;
  auto operator=(IndexIterator &&other) noexcept -> IndexIterator &;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  void ReleaseCurrentPage();

  Page *page_{nullptr};
  LeafPage *current_page_{nullptr};
  int current_index_{0};
  BufferPoolManager *buffer_pool_manager_{nullptr};
  MappingType current_item_{};
};

}  // namespace bustub
