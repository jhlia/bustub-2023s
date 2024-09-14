//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * 获取/设置页面类型
 * 
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }  // 是否为叶子
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }  // 设页面类型

/*
 * 获取/设置页面大小（存储在该页面中的键/值对的数量）
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }  // 当前页面大小
void BPlusTreePage::SetSize(int size) { size_ = size; }  // 设页面大小
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }  // 增页面大小

/*
 * 获取/设置页面的最大大小（容量）
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }  // 获取页面的最大大小
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }  // 设置页面的最大大小

/*
 * 获取最小页面大小的辅助方法
 * 通常，最小页面大小 == 最大页面大小 / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (page_type_ == IndexPageType::INTERNAL_PAGE) {
    return (max_size_ + 1) / 2;  // 内部页
  }
  return max_size_ / 2;  // 叶子页
}

}  // namespace bustub
