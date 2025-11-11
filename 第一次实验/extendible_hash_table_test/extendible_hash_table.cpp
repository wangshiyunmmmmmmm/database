//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  // 初始化：创建1个空桶，目录指向该桶
  auto initial_bucket = std::make_shared<Bucket>(bucket_size_, 0);
  dir_.push_back(initial_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  // 计算哈希值，并按全局深度取低N位作为目录索引
  std::hash<K> hasher;
  size_t hash_val = hasher(key);
  return hash_val & ((1 << global_depth_) - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);  // 加锁保证线程安全
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  // 检查目录索引有效性
  if (dir_index < 0 || static_cast<size_t>(dir_index) >= dir_.size()) {
    return -1;  // 无效索引返回-1
  }
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // 1. 计算目录索引
  size_t dir_index = IndexOf(key);
  // 2. 获取对应桶并查找键值对
  std::shared_ptr<Bucket> target_bucket = dir_[dir_index];
  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // 1. 计算目录索引
  size_t dir_index = IndexOf(key);
  // 2. 获取对应桶并删除键值对
  std::shared_ptr<Bucket> target_bucket = dir_[dir_index];
  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  size_t dir_index = IndexOf(key);
  std::shared_ptr<Bucket> target_bucket = dir_[dir_index];

  // 循环直到插入成功（可能需要多次拆分）
  while (true) {
    // 尝试插入：若成功则直接返回
    if (target_bucket->Insert(key, value)) {
      return;
    }

    // 插入失败（桶满），执行拆分流程
    SplitBucket(target_bucket, dir_index);

    // 拆分后重新计算索引和目标桶（可能变化）
    dir_index = IndexOf(key);
    target_bucket = dir_[dir_index];
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(std::shared_ptr<Bucket> old_bucket, size_t old_index) {
  // 步骤1：检查是否需要扩展目录（局部深度 == 全局深度）
  if (old_bucket->GetDepth() == global_depth_) {
    // 目录容量翻倍，新指针指向原有桶
    size_t old_dir_size = dir_.size();
    for (size_t i = 0; i < old_dir_size; ++i) {
      dir_.push_back(dir_[i]);
    }
    global_depth_++;  // 全局深度+1
  }

  // 步骤2：创建新桶，局部深度与旧桶一致（后续会+1）
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, old_bucket->GetDepth());
  num_buckets_++;  // 桶总数+1

  // 步骤3：旧桶和新桶的局部深度+1
  old_bucket->IncrementDepth();
  new_bucket->IncrementDepth();

   // 步骤4：重新分配旧桶中的键值对
  auto &items = old_bucket->GetItems();
  auto it = items.begin();
  size_t base_depth = old_bucket->GetDepth() - 1;           // 拆分前的局部深度
  size_t base_index = old_index & ((1 << base_depth) - 1);  // 基索引

  while (it != items.end()) {
    const K &item_key = it->first;
    size_t mask = (1 << old_bucket->GetDepth()) - 1;
    size_t item_hash = std::hash<K>()(item_key);
    size_t item_index = item_hash & mask;

    // 计算新桶的目标索引（删除旧桶目标索引的定义，直接使用逻辑判断）
    size_t new_bucket_target = base_index | (1 << base_depth);

    // 若键属于新桶，则移动到新桶（直接判断是否等于新桶目标）
    if (item_index == new_bucket_target) {
      new_bucket->Insert(item_key, it->second);
      it = items.erase(it);
    } else {
      // 不属于新桶则保留在旧桶（无需显式判断旧桶目标）
      ++it;
    }
  }

  // 步骤5：更新目录指针（将新桶的指针分配到对应位置）
  // 移除未使用的 step 变量，或使用它优化循环
  size_t mask = (1 << old_bucket->GetDepth()) - 1;  // 用 mask 替代 step
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (dir_[i] == old_bucket) {
      size_t i_masked = i & mask;
      size_t new_bucket_target = base_index | (1 << base_depth);

      if (i_masked == new_bucket_target) {
        dir_[i] = new_bucket;
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket 类实现
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t size, int depth) : size_(size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // 遍历链表查找键
  for (const auto &pair : list_) {
    if (pair.first == key) {
      value = pair.second;  // 找到则赋值并返回true
      return true;
    }
  }
  return false;  // 未找到
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // 遍历链表删除键
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);  // 删除找到的键值对
      return true;
    }
  }
  return false;  // 键不存在
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 情况1：键已存在，更新值
  for (auto &pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }

  // 情况2：键不存在，检查桶是否已满
  if (IsFull()) {
    return false;  // 桶满，插入失败
  }

  // 情况3：桶未满，插入新键值对
  list_.emplace_back(key, value);
  return true;
}

// 显式实例化（实验要求支持的类型）
template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub