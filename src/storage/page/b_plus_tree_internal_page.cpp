//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * 在创建一个新的内部页面之后初始化
 * 包括设定页面类型、设置当前大小以及设置最大页面大小
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t parent_page_id, int max_size) {
  SetMaxSize(max_size);  // 设最大大小
  SetSize(0);  // 设当前大小
  SetPageType(IndexPageType::INTERNAL_PAGE);  // 设页面类型
  SetParentPageId(parent_page_id);  // 设父页面ID
}
/*
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;  // 指定索引处的键
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;  // 设指定索引处的键
}

/*
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // assert(index < GetSize());
  return array_[index].second;  // 指定索引处的值
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  array_[index].first = key;  // 设指定索引处的键
  array_[index].second = value;  // 设指定索引处的值
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // assert(index < GetSize());
  array_[index].second = value;  // 设指定索引处的值
}

/**
 * 基于给定键找到右侧下一个页面 ID（ValueType）
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(KeyType key, const KeyComparator &comparator,
                                               int *child_page_index) const -> ValueType {
  // 搜索的 key可能跟中间节点key相等
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) <= 0;  
  };

  auto res = std::lower_bound(array_, array_ + GetSize(), key, compare_first);
  res = std::prev(res);


  if (child_page_index != nullptr) {
    *child_page_index = std::distance(array_, res);  // 获孩子节点索引
  }
  return res->second;  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {

  int size = GetSize();  // 当前大小

  // 可能作新节点的第一位的
  if (comparator(key, array_[0].first) < 0) {
    std::copy_backward(array_, array_ + size, array_ + size + 1); 
    IncreaseSize(1);  // 增当前大小
    array_[0].first = key;  
    array_[0].second = value; 
    return true;
  }

  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;  
  };

  auto it = std::lower_bound(array_ + 1, array_ + size, key, compare_first);  

  int index = std::distance(array_, it);  

  LOG_DEBUG("Internal page insertion: distance %d", index);

  std::copy_backward(array_ + index, array_ + size, array_ + size + 1);  
  IncreaseSize(1);  // 增当前大小
  array_[index] = std::make_pair(key, value);  

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0; 
  };

  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0 && value == res->second) {
    int dist = std::distance(array_, res);  
    std::copy(array_ + dist + 1, array_ + GetSize(), array_ + dist); 
    IncreaseSize(-1);  // 减当前大小
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(MappingType *array, int size) {
  std::copy(array, array + size, array_ + GetSize());  
  IncreaseSize(size);  // 增当前大小
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftData(int dist) {
  if (dist > 0) {  // 向右移动
    std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + dist);  
  } else if (dist < 0) {  // 左移
    std::copy(array_ - dist, array_ + GetSize(), array_);  
  }
  IncreaseSize(dist);  // 增当前大小
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;  // 设父页面ID
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetParentPageId() -> page_id_t {
  return parent_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_);  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetData() -> MappingType * {
  return array_; 
}


template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
