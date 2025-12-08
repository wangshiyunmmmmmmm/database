/**
 * index_iterator.cpp
 */
#include <algorithm>
#include <cassert>
#include <stdexcept>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager)
    : page_(page), current_index_(index), buffer_pool_manager_(buffer_pool_manager) {
  if (page_ != nullptr) {
    page_->RLatch();
    current_page_ = reinterpret_cast<LeafPage *>(page_->GetData());
    int size = current_page_->GetSize();
    if (size == 0) {
      current_index_ = 0;
      operator++();
      return;
    }
    if (current_index_ < 0) {
      current_index_ = 0;
    }
    if (current_index_ >= size) {
      current_index_ = size;
      operator++();
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator &&other) noexcept
    : page_(other.page_),
      current_page_(other.current_page_),
      current_index_(other.current_index_),
      buffer_pool_manager_(other.buffer_pool_manager_),
      current_item_(other.current_item_) {
  other.page_ = nullptr;
  other.current_page_ = nullptr;
  other.buffer_pool_manager_ = nullptr;
  other.current_index_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(IndexIterator &&other) noexcept -> IndexIterator & {
  if (this != &other) {
    ReleaseCurrentPage();
    page_ = other.page_;
    current_page_ = other.current_page_;
    current_index_ = other.current_index_;
    buffer_pool_manager_ = other.buffer_pool_manager_;
    current_item_ = other.current_item_;
    other.page_ = nullptr;
    other.current_page_ = nullptr;
    other.buffer_pool_manager_ = nullptr;
    other.current_index_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { ReleaseCurrentPage(); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return current_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw std::out_of_range("Dereferencing end iterator");
  }
  current_item_.first = current_page_->KeyAt(current_index_);
  current_item_.second = current_page_->ValueAt(current_index_);
  return current_item_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  auto *leaf = current_page_;
  current_index_++;
  if (current_index_ < leaf->GetSize()) {
    return *this;
  }
  page_id_t next_page_id = leaf->GetNextPageId();
  ReleaseCurrentPage();
  if (next_page_id == INVALID_PAGE_ID || buffer_pool_manager_ == nullptr) {
    current_index_ = 0;
    return *this;
  }
  page_ = buffer_pool_manager_->FetchPage(next_page_id);
  if (page_ == nullptr) {
    current_page_ = nullptr;
    current_index_ = 0;
    return *this;
  }
  page_->RLatch();
  current_page_ = reinterpret_cast<LeafPage *>(page_->GetData());
  current_index_ = 0;
  if (current_page_->GetSize() == 0) {
    return operator++();
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if ((current_page_ == nullptr) != (itr.current_page_ == nullptr)) {
    return false;
  }
  if (current_page_ == nullptr && itr.current_page_ == nullptr) {
    return true;
  }
  return current_page_->GetPageId() == itr.current_page_->GetPageId() && current_index_ == itr.current_index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::ReleaseCurrentPage() {
  if (page_ != nullptr && buffer_pool_manager_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = nullptr;
  }
  current_page_ = nullptr;
  current_index_ = 0;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
