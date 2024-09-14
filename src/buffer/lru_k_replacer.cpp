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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // PrintLists();

  if (curr_size_ == 0) {  // replacer中没有能替换页面
    return false;
  }

  bool remove_from_default_list = false;
  bool remove_from_k_list = false;
  if (!default_list_.empty()) {  //有访问小于K次的页
    remove_from_default_list = RemoveNode(frame_id, &default_list_, &default_map_);//替换
  }

  if (!remove_from_default_list && !k_list_.empty()) {  //有访问次数大于K的页面
    remove_from_k_list = RemoveNode(frame_id, &k_list_, &k_map_);
  }

  // LOG_DEBUG("Evicting frame_id: %d, success or not: %d", *frame_id, remove_from_default_list || remove_from_k_list);

  return remove_from_default_list || remove_from_k_list;
}


auto LRUKReplacer::RemoveNode(frame_id_t *frame_id, std::list<std::shared_ptr<LRUKNode>> *lst,
                              std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> *map) -> bool {
  auto r_it = lst->rbegin();  // 按时间从新到旧反着来
  while (r_it != lst->rend()) {
    if ((*r_it)->GetEvictable()) {  // 页面可以替换
      auto node = (*r_it);
      (*frame_id) = node->GetFrameId();
      map->erase((*frame_id));
      lst->erase(std::next(r_it).base());  // 删除页面
      --curr_size_;  // replacer大小

      // delete node;

      return true;
    }
    ++r_it;  
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    return;
  }

  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);
  if (default_iter == default_map_.end() && k_iter == k_map_.end()) {  // 新页面，不在小于k和大于k
    auto node = std::make_shared<LRUKNode>(frame_id, k_);
    node->InsertTimeStamp(current_timestamp_);  // 时间戳
    default_map_[frame_id] = node;  //加入小于K
    default_list_.push_front(node);  // 放在小于K列表前端
  } else {
    if (default_iter != default_map_.end()) {  // 在小于K列表
      auto node = default_map_[frame_id];
      node->InsertTimeStamp(current_timestamp_);  // 时间戳
      

      if (node->GetNumOfReferences() >= k_) {  // 访问次数为K
        node->UpdateKDistance();
        default_list_.remove(node);
        default_map_.erase(frame_id);  // 从小于k列表中删除
        k_map_[frame_id] = node;  // 插入大于K列表
        InsertKNode(node);
      }
      // else {  // push the node to the front
      //   default_list_.push_front(node);
      // }

    } else {  // 在大于K列表
      auto node = k_map_[frame_id];
      node->InsertTimeStamp(current_timestamp_);
      node->UpdateKDistance(); //更新K-distance
      k_list_.remove(node); // 从大于k列表删除
      InsertKNode(node); //重新插入新位置
    }
  }
  ++current_timestamp_;

  // PrintLists();
}

// start the scan from the front of the list
void LRUKReplacer::InsertKNode(std::shared_ptr<LRUKNode> &node) {
  if (k_list_.empty()) { // 大于K列表空
    k_list_.push_back(node); // 直接插入
    return;
  }
  auto it = k_list_.begin();
  size_t k_dist = node->GetKDistance();
  while (it != k_list_.end() && k_dist < (*it)->GetKDistance()) {
    ++it;
  }
  k_list_.insert(it, node); // 按KDistance从小到大排
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);

  if (default_iter == default_map_.end() && k_iter == k_map_.end()) { // 不在大于K、小于K列表
    throw std::runtime_error("Frame_id does not exist"); 
  }

  // 页面在大于K或小于K
  auto node = default_iter != default_map_.end() ? default_map_[frame_id] : k_map_[frame_id];

 

  if (node->GetEvictable() && !set_evictable) { // 可替换设为不可替换
    --curr_size_;
  } else if (!node->GetEvictable() && set_evictable) { // 不可替换设为可替换
    ++curr_size_;
  }

  node->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto default_iter = default_map_.find(frame_id);
  auto k_iter = k_map_.find(frame_id);
  if (default_iter == default_map_.end() && k_iter == k_map_.end()) { // 大于K小于K都不在
    return;
  }

  std::shared_ptr<LRUKNode> node = nullptr;
  if (default_iter != default_map_.end()) { // 在小于K列表
    node = default_map_[frame_id];
    if (!node->GetEvictable()) {
      throw std::runtime_error("Cannot remove");
    }
    default_map_.erase(frame_id);
    default_list_.remove(node); // 删除

    // delete node;
  } else if (k_iter != k_map_.end()) { // 在大于K列表
    node = k_map_[frame_id];
    if (!node->GetEvictable()) {
      throw std::runtime_error("Cannot remove");
    }
    k_map_.erase(frame_id);
    k_list_.remove(node);

    // delete node;
  }


  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::PrintLists() {
  std::string str1 = "\nDefault List: ";
  for (const auto &node : default_list_) {
    if (node->GetEvictable()) {
      // str1 += "{" + std::to_string(node->GetFrameId()) + "}√: " + std::to_string(node->GetKDistance()) +
      // node->GetHistory() + " ";
      str1 += "{" + std::to_string(node->GetFrameId()) + "}√ ";
    } else {
      // str1 += "{" + std::to_string(node->GetFrameId()) + "}x: " + std::to_string(node->GetKDistance()) +
      // node->GetHistory() + " ";
      str1 += "{" + std::to_string(node->GetFrameId()) + "}x ";
    }
  }
  // LOG_DEBUG("Default List: %s", str1.c_str());

  str1 += "\nK List: ";

  // std::string str2;
  for (const auto &node : k_list_) {
    if (node->GetEvictable()) {
      // str2 += "{" + std::to_string(node->GetFrameId()) + "}√: " + std::to_string(node->GetKDistance()) +
      // node->GetHistory() + " ";
      str1 += "{" + std::to_string(node->GetFrameId()) + "}√ ";
    } else {
      str1 += "{" + std::to_string(node->GetFrameId()) + "}x ";
    }
  }
  // LOG_DEBUG("K List: %s\n", str2.c_str());
  LOG_DEBUG("%s", str1.c_str());
}

}  // namespace bustub

