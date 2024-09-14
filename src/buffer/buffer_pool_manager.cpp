//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // 
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i)); // 所有页面标记为可用，加到空闲列表
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ans = nullptr;
  // 有空闲页面或可替换
  if (!free_list_.empty() || replacer_->Size() > 0) {
    frame_id_t frame_id = 0;
    if (!free_list_.empty()) { // 有空闲页面优先
      // 分配新空闲页
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      // 无空闲页，替换出一个帧
      replacer_->Evict(&frame_id); // 替换出一个帧
      // 若有脏数据写回磁盘
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      }
      // 页表中删除
      page_table_.erase(pages_[frame_id].page_id_);
      pages_[frame_id].ResetMemory();
      DeallocatePage(pages_[frame_id].GetPageId());
    }
    // 新页面id
    page_id_t page_new_id = AllocatePage();
    *page_id = page_new_id;
    // 初始化页
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    // 添加到页表
    page_table_[*page_id] = frame_id;
    // 记录访问，设置为不可替换出
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // 指针
    ans = &pages_[frame_id];
  }
  return ans;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ans = nullptr;
  // 检查是否在内存中
  if (page_table_.find(page_id) != page_table_.end()) { // 在内存
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++; //使用+
    replacer_->RecordAccess(frame_id); // 访问记录
    replacer_->SetEvictable(frame_id, false); // 设为不可替换
    ans = &pages_[frame_id]; 
    // 不在buffer
  } else { 
    if (!free_list_.empty() || replacer_->Size() > 0) { // 有空闲/可替换
      frame_id_t frame_id = 0; // 保存页的帧
      if (!free_list_.empty()) { 
        // 有空闲
        frame_id = free_list_.front();
        free_list_.pop_front();
      } else { //替换出
        // 
        replacer_->Evict(&frame_id);
        // 脏数据写回
        if (pages_[frame_id].IsDirty()) {
          disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
        }
        // 从页表中删除替换出的记录
        page_table_.erase(pages_[frame_id].page_id_);
        pages_[frame_id].ResetMemory();
        DeallocatePage(pages_[frame_id].GetPageId());
      }
      // 从磁盘读数据
      disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].is_dirty_ = false;
      // 添加到页表
      page_table_[page_id] = frame_id;
      // 设不可替换
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      ans = &pages_[frame_id];
    }
  }
  return ans;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].pin_count_ <= 0) {
    return false; // 不在内存或没有被使用
  }
  pages_[page_table_[page_id]].pin_count_--; // 使用-
  pages_[page_table_[page_id]].is_dirty_ = (is_dirty || pages_[page_table_[page_id]].is_dirty_); // 更新页面
  if (pages_[page_table_[page_id]].pin_count_ == 0) { // 若没有在使用
    replacer_->SetEvictable(page_table_[page_id], true); //设置为可替换
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { //刷新，写脏数据更新
  std::lock_guard<std::mutex> lock(latch_); 
  if (page_table_.find(page_id) == page_table_.end()) { //不在内存
    return false;
  }
  frame_id_t frame_id = page_table_[page_id]; // 页表中找到页面对应帧
  disk_manager_->WritePage(page_id, pages_[frame_id].data_); // 数据写入磁盘
  pages_[frame_id].is_dirty_ = false; // 已写回
  return true;
}

void BufferPoolManager::FlushAllPages() { // 刷新所有
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { // 删除
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) { // 不在内存，相当于删除
    return true;
  }
  if (pages_[page_table_[page_id]].pin_count_ > 0) { // 正在使用，不能删除
    return false;
  }
  replacer_->Remove(page_table_[page_id]); //移除替换
  free_list_.emplace_back(page_table_[page_id]); //添加到空闲
  // 删除释放
  pages_[page_table_[page_id]].ResetMemory(); 
  pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
  pages_[page_table_[page_id]].is_dirty_ = false;
  pages_[page_table_[page_id]].pin_count_ = 0;
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; } //下一个页面id

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; } // 管理页面的生命周期

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { // 读锁
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { //写锁
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; } // 分配新页面

}  // namespace bustub

