//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

// 前向声明，提高头文件独立性
class Page;

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * @brief Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (预留用于WAL机制，P1暂不使用)
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

 protected:
  /**
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  /**
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
   *
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  /**
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  /**
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPgImp(page_id_t page_id) -> bool override;

  /**
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override;

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager (负责实际磁盘页面的读写) */
  DiskManager *disk_manager_;
  /** Pointer to the log manager (预留用于WAL机制，P1暂不实现) */
  LogManager *log_manager_;
  /** Page table (维护page_id到frame_id的映射) */
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  /** Replacer (用于选择可驱逐的页面帧) */
  LRUKReplacer *replacer_;
  /** List of free frames (未分配页面的帧ID列表) */
  std::list<frame_id_t> free_list_;
  /** 互斥锁 (保护page_table_、free_list_、replacer_等共享数据结构的线程安全) */
  std::mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   * @note 目前为简化实现，仅标记页面为无效，实际磁盘空间未释放
   */
  void DeallocatePage(page_id_t page_id);

 private:
  /**
   * @brief 查找可用的帧ID
   * @param[out] frame_id 输出参数，存储找到的帧ID
   * @return 成功找到返回true，否则返回false
   * @note 优先从free_list_获取，若无则通过replacer_驱逐页面
   */
  auto FindAvailableFrame(frame_id_t *frame_id) -> bool;
};
}  // namespace bustub