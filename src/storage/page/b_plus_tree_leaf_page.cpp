//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"


namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * 创建新叶子页后的初始化
 * 包括设置页面类型，将当前大小设置为零，设置下一个页面 ID 和设置最大大小
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t parent_page_id, int max_size) {

  SetMaxSize(max_size);  
  SetSize(0);  
  SetPageType(IndexPageType::LEAF_PAGE);  // 设页面类型
  SetParentPageId(parent_page_id);  // 设父页面 ID
  next_page_id_ = INVALID_PAGE_ID;  // 下一个页面初始化为无效
}

/**
 * 设置/获取下一个页面 ID 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * 用于找到并返回与输入 "index"关联的键
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index < GetSize());
  return array_[index].first;  // 指定索引键
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;  // 指定索引值
}

/**
 * 基于叶节点中的目标查找并返回相应的值
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, ValueType &value, const KeyComparator &comparator,
                                           int *index) const -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;  
  };

  
  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0) {
    value = res->second;  // 设相应值

    if (index != nullptr) {
      std::cout << "Locating the starting index: " << std::distance(array_, res) << std::endl;  
      *index = std::distance(array_, res);  // 设索引
    }

    return true;
  }

  return false;
}


/**
 * 将 <key, value> 对插入叶子页
 * （如果发现重复的键，返回 false）
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;  // 第一个
  };

  int size = GetSize();  // 当前大小
  auto it = std::lower_bound(array_, array_ + size, key, compare_first);  // 找插入位置
  if (it < array_ + size && comparator(key, it->first) == 0) {  // 重复
    LOG_DEBUG("Leaf insert | find the duplicate key %s/%s at index %td", std::to_string(key.ToString()).c_str(),
              std::to_string(it->first.ToString()).c_str(), std::distance(array_, it));  
    return false;
  }

  int index = std::distance(array_, it);  

  std::move_backward(array_ + index, array_ + size, array_ + size + 1);  // 后移

  array_[index].first = key;  // 设键
  array_[index].second = value;  // 设值
  IncreaseSize(1);  // 增当前大小

  std::string str;  // 输出所有
  for (int i = 0; i < GetSize(); ++i) {
    str += ("[" + std::to_string(array_[i].first.ToString()) + "] ");
  }

  return true;
}

auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  if (GetSize() == 0) {
    return false;
  }

  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;  
  };

  auto res = std::lower_bound(array_, array_ + GetSize() - 1, key, compare_first);
  if (comparator(key, res->first) == 0 && value == res->second) {
    // 删除 <key, value>
    int dist = std::distance(array_, res); 
    std::copy(array_ + dist + 1, array_ + GetSize(), array_ + dist);  
    IncreaseSize(-1);  // 减当前大小
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(MappingType *array, int size) {
  std::copy(array, array + size, array_ + GetSize());  // 复制
  IncreaseSize(size);  // 增当前大小
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftData(int dist) {
  if (dist > 0) {  // 右移
    std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + dist);  // 复制
  } else if (dist < 0) {  // 左移
    std::copy(array_ - dist, array_ + GetSize(), array_);  
  }
  IncreaseSize(dist);  // 增当前大小
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPage(page_id_t page_id) { next_page_id_ = page_id; }  

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() -> page_id_t { return next_page_id_; }  

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }  

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetParentPageId() -> page_id_t { return parent_page_id_; } 

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_);  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetData() -> MappingType * { return array_; }  


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  assert(index < GetSize());
  array_[index].first = key;  
  array_[index].second = value;  
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
