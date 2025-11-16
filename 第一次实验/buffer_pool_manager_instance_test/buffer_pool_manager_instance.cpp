//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);  // 使用scoped_lock更安全
  
  frame_id_t frame_id;
  
  // 尝试从free_list获取
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 尝试从replacer驱逐
    if (!replacer_->Evict(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }
    
    // 处理被驱逐的页面
    Page *evicted_page = &pages_[frame_id];
    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page->GetPageId(), evicted_page->GetData());
    }
    
    // 确保完全清理旧页面
    page_table_->Remove(evicted_page->GetPageId());
    replacer_->Remove(frame_id);
    
    // 重置页面状态
    evicted_page->ResetMemory();
    evicted_page->page_id_ = INVALID_PAGE_ID;
    evicted_page->is_dirty_ = false;
    evicted_page->pin_count_ = 0;
  }

  // 分配新页面ID
  *page_id = AllocatePage();

  // 初始化新页面
  Page *new_page = &pages_[frame_id];
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;
  // 注意：这里不要调用ResetMemory()，因为我们需要保留分配的内存

  // 建立映射
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  
  frame_id_t frame_id;
  
  // 检查页面是否已在缓冲池
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // 寻找可用frame
  if (!FindAvailableFrame(&frame_id)) {
    return nullptr;
  }

  // 处理frame中可能存在的旧页面
  Page *old_page = &pages_[frame_id];
  if (old_page->GetPageId() != INVALID_PAGE_ID) {
    if (old_page->IsDirty()) {
      disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
    }
    page_table_->Remove(old_page->GetPageId());
    replacer_->Remove(frame_id);
  }

  // 从磁盘读取数据到页面
  Page *new_page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, new_page->GetData());  // 这会填充页面数据
  
  // 设置页面元数据
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;
  // 注意：不要调用ResetMemory()，因为ReadPage已经设置了数据

  // 建立映射
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::FindAvailableFrame(frame_id_t *frame_id) -> bool {
  // 优先从free_list获取
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // 从replacer获取可驱逐的frame
  return replacer_->Evict(frame_id);
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->pin_count_--;

  // 重要：使用逻辑或来更新脏标志，而不是直接赋值
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 1. 检查页面是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];

  // 2. 如果页面是脏的，写入磁盘
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);

  // 遍历所有页面帧
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    Page *page = &pages_[frame_id];

    // 如果页面有效且是脏的，写入磁盘
    if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // 1. 检查页面是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];

  // 2. 检查页面是否被pin
  if (page->GetPinCount() > 0) {
    return false;
  }

  // 3. 如果页面是脏的，先写入磁盘
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }

  // 4. 从页表中移除映射
  page_table_->Remove(page_id);

  // 5. 从replacer中移除
  replacer_->Remove(frame_id);

  // 6. 重置页面状态
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  // 7. 将帧加入空闲列表
  free_list_.push_back(frame_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::DeallocatePage(page_id_t page_id) {
  // 目前简化实现，只是标记页面为无效
  // 在实际系统中，这里会释放磁盘空间
  // 对于P1项目，这个方法可以留空或者只做标记
}

}  // namespace bustub