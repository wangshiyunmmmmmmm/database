//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstdint>
// 添加这一行：定义 INVALID_FRAME_ID
#define INVALID_FRAME_ID (-1)

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : replacer_size_(num_frames), k_(k) {
  BUSTUB_ASSERT(num_frames > 0, "Replacer size must be positive");
  BUSTUB_ASSERT(k >= 1, "K must be at least 1");
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;  // 无可用驱逐帧
  }

  frame_id_t victim_frame = INVALID_FRAME_ID;
  size_t max_backward_distance = 0;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();

  for (const auto &[fid, info] : frame_map_) {
    if (!info.is_evictable_) {
      continue;  // 跳过不可驱逐的帧
    }

    const auto &timestamps = info.timestamps_;
    size_t backward_distance = std::numeric_limits<size_t>::max();

    // 计算后退k-距离
    if (timestamps.size() >= k_) {
      // 取第k个最早的时间戳（列表头部为最早）
      auto kth_ts_it = timestamps.begin();
      std::advance(kth_ts_it, timestamps.size() - k_);
      backward_distance = current_timestamp_ - *kth_ts_it;
    }

    // 选择驱逐目标：优先最大后退距离，相同则选最早访问的
    bool is_better_victim = false;
    if (backward_distance > max_backward_distance) {
      is_better_victim = true;
    } else if (backward_distance == max_backward_distance) {
      auto earliest_ts = timestamps.front();
      if (earliest_ts < earliest_timestamp) {
        is_better_victim = true;
      }
    }

    if (is_better_victim) {
      victim_frame = fid;
      max_backward_distance = backward_distance;
      earliest_timestamp = timestamps.front();
    }
  }

  if (victim_frame == INVALID_FRAME_ID) {
    return false;  // 无符合条件的帧（理论上不会走到这）
  }

  // 执行驱逐：移除帧信息，更新可驱逐数量
  frame_map_.erase(victim_frame);
  curr_size_--;
  *frame_id = victim_frame;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 校验帧ID有效性
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  auto &info = frame_map_[frame_id];
  // 添加当前时间戳，保持列表最多k个元素
  info.timestamps_.push_back(current_timestamp_);
  if (info.timestamps_.size() > k_) {
    info.timestamps_.pop_front();
  }

  current_timestamp_++;  // 时间戳递增
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    // 关键修复：未访问过的帧（不在 frame_map_ 中），直接返回，不创建条目，不更新计数
    return;
  }

  auto &info = it->second;
  if (info.is_evictable_ == set_evictable) {
    return;  // 状态无变化，直接返回
  }

  // 更新可驱逐状态和计数
  info.is_evictable_ = set_evictable;
  curr_size_ += set_evictable ? 1 : -1;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    return;  // 帧不存在，直接返回
  }

  const auto &info = it->second;
  BUSTUB_ASSERT(info.is_evictable_, "Cannot remove non-evictable frame");

  // 移除帧信息，更新计数
  frame_map_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
