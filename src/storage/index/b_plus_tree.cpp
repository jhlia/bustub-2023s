#include <sstream>
#include <string>

#include <cmath>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {

  // 获取头页面并锁定以写入
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  
  // 初始化根页面ID为无效值，表示树为空
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 * B+ 树是否为空
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if(root_page->root_page_id == INVALID_PAGE_ID) return true;
  return false;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * 与输入键对应的值
 * This method is used for point query
 * 查询
 * @return : true means key exists
 * @return 
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  // 为上下文实例声明变量
  // Context ctx;
  // (void)ctx;

  // 加读锁读头页面
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  
  // 如果根页面ID无效,树空
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // b+ tree is empty
    return false;  
  }

  // 读根页面
  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();

  const InternalPage *internal_page = nullptr;
  
  // 查找叶子页
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();

    // 根据键查找下一个页面
    guard = bpm_->FetchPageRead(internal_page->FindValue(key, comparator_));
    page = guard.As<BPlusTreePage>();
  }

  // 找到叶子页面
  const auto *leaf_page = guard.As<LeafPage>();
  ValueType res;

  // 在叶子页查找键
  if (leaf_page->FindValue(key, res, comparator_)) {
    result->push_back(res);  // 将结果值添加到结果向量中
    return true; 
  }

  return false;  
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * 键值对插入
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * 当前树空，新树，根页面ID
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;  // 上下文
  (void)ctx;  
  
  // 加锁头页面写入
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // tree is empty
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // 如果树空，创建根页面
    BasicPageGuard root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    ctx.root_page_id_ = root_page_guard.PageId();
    auto page = root_page_guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);  // 初始化叶子页
    root_page_guard.Drop();  // 释放根页

    // 写入数据到新创页
    WritePageGuard root_page_write_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
    page = root_page_write_guard.AsMut<LeafPage>();  // set is_dirty_ = true;
    page->Insert(key, value, comparator_);

    return true;  // 成功插入
  }

  // 查找叶节点
  WritePageGuard guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  
  InternalPage *internal_page = nullptr;
  while (!page->IsLeafPage()) {
    internal_page = guard.AsMut<InternalPage>();
    ctx.write_set_.emplace_back(std::move(guard));  // 记录当前页面

    page_id_t tmp = internal_page->FindValue(key, comparator_);  // 查找下一个页面ID

    guard = bpm_->FetchPageWrite(tmp);  // 读取下一个页面
    page = guard.AsMut<BPlusTreePage>();  // 更新页面
  }

  // 找叶
  auto leaf_page = guard.AsMut<LeafPage>();
  page_id_t cur_page_id = guard.PageId();
  ctx.write_set_.emplace_back(std::move(guard));  

  // 叶是否有空余空间
  if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
    bool res = leaf_page->Insert(key, value, comparator_);  // 插入
    return res;  // 插入结果
  }

  // 插入后需要分裂叶
  if (leaf_page->Insert(key, value, comparator_)) {
    int min_size = leaf_page->GetMinSize();  // 最小大小
    int cur_size = leaf_page->GetSize();  // 当前大小

    // 创建新的叶子
    page_id_t new_page_id;
    BasicPageGuard new_page_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_page = new_page_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    new_page_guard.Drop();  // 释放

    // BasicPageGuard -> WritePageGuard
    WritePageGuard new_guard = bpm_->FetchPageWrite(new_page_id);  // 锁
    new_page = new_guard.AsMut<LeafPage>();

    // 分裂叶子
    new_page->CopyHalfFrom(leaf_page->GetData(), min_size, cur_size);  // 数据复制到新页面
    KeyType pushed_key = leaf_page->KeyAt(min_size);  

    new_page->SetSize(cur_size - min_size);  // 新页大小
    leaf_page->SetSize(min_size);  // 更新当前叶页大小
    new_page->SetNextPage(leaf_page->GetNextPageId());  
    leaf_page->SetNextPage(new_page_id);  

    // 插入到父页
    InsertInParent(pushed_key, std::move(new_guard), ctx);

    // 清空上下文
    ctx.write_set_.clear();
    ctx.header_page_ = std::nullopt;

    return true;  // 成功插入
  }

  ctx.write_set_.clear();  // 清空上下文
  ctx.header_page_.value().Drop();  // 释放
  ctx.header_page_ = std::nullopt;

  return false; 
}

/**
 * key: the key pushed to the parent node
 * cur_page: the old page
 * new_page: newly created page
 * ctx: keep track of the path
 * 插入到父节点,父节点已满，则需要分裂父节点
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(const KeyType &key, WritePageGuard &&new_page_guard, Context &ctx) {
  // root_page is full, create a new root page
  page_id_t cur_page_id = ctx.write_set_.back().PageId();  // 当前页ID
  if (ctx.IsRootPage(cur_page_id)) {  // 如果当前页面是根页面
    auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    BasicPageGuard new_root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    new_root_page_guard.Drop();  // 释放

    // BasicPageGuard -> WritePageGuard
    WritePageGuard new_root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);

    ctx.root_page_id_ = header_page->root_page_id_;  // 更新根页ID
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(INVALID_PAGE_ID, internal_max_size_);  // 初始化新根页

    // 需要更新孩子的父页ID
    bool is_leaf_page = ctx.write_set_.back().AsMut<BPlusTreePage>()->IsLeafPage();
    if (is_leaf_page) {
      auto first_child_page = ctx.write_set_.back().AsMut<LeafPage>();
      auto second_child_page = new_page_guard.AsMut<LeafPage>();

      first_child_page->SetParentPageId(header_page->root_page_id_);  // 设父页ID
      second_child_page->SetParentPageId(header_page->root_page_id_);
    } else {
      auto first_child_page = ctx.write_set_.back().AsMut<InternalPage>();
      auto second_child_page = new_page_guard.AsMut<InternalPage>();

      first_child_page->SetParentPageId(header_page->root_page_id_);
      second_child_page->SetParentPageId(header_page->root_page_id_);
    }

    // 更新根页
    new_root_page->SetValueAt(0, cur_page_id);  // 设孩子页ID
    new_root_page->SetKeyValueAt(1, key, new_page_guard.PageId());  // 设键值
    new_root_page->IncreaseSize(2);  // 增加根页大小

    ctx.write_set_.pop_back();  // 弹出当前页
    return;  // 结束
  }

  // 先弹出来找父节点
  ctx.write_set_.pop_back();
  WritePageGuard &cur_page_guard = ctx.write_set_.back();  // 获父页
  auto cur_page = cur_page_guard.AsMut<InternalPage>();

  // parent page is not full, just insert it and return
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {  // 如果父页未满
    cur_page->Insert(key, new_page_guard.PageId(), comparator_);  // 直接插新键值
    ctx.write_set_.pop_back();  // 弹出父页
    return;  
  }

  // the parent page is full, split before insertion
  page_id_t new_page_id;  // 新页面ID
  BasicPageGuard new_basic_page_guard = bpm_->NewPageGuarded(&new_page_id);  // 创建新页面
  auto new_page = new_basic_page_guard.AsMut<InternalPage>();
  new_page->Init(cur_page->GetParentPageId(), internal_max_size_);  // 新节点共享父节点
  new_basic_page_guard.Drop();  // 释放

  // BasicPageGuard -> WritePageGuard
  WritePageGuard new_parent_page_guard = bpm_->FetchPageWrite(new_page_id);  // 锁定新父页
  new_page = new_parent_page_guard.AsMut<InternalPage>();

  int min_size = cur_page->GetMinSize();  // 父页最小大小
  int cur_size = cur_page->GetSize();  // 父页当前大小

  KeyType pushed_key = cur_page->KeyAt(min_size);  


  // insert the new key
  auto last_key = cur_page->KeyAt(min_size - 1);  
  bool is_first_case = comparator_(key, pushed_key) > 0;  // 新键 key 是否应该被插入到 pushed_key 之后
  bool is_second_case = comparator_(key, pushed_key) < 0 && comparator_(key, last_key) > 0;  // 新键 key 是否应该被插入到 pushed_key 和 last_key 之间
  if (is_first_case || is_second_case) {  
    new_page->CopyHalfFrom(cur_page->GetData(), min_size, cur_size);  // 数据

    cur_page->SetSize(min_size);  // 更新当前页大小
    new_page->SetSize(cur_size - min_size);  // 更新新页大小

    new_page->Insert(key, new_page_guard.PageId(), comparator_);  // 插入新键
    pushed_key = is_second_case ? key : pushed_key; 
  } else {
    // 新key是被插入 cur_page, new_page 会占多一位
    new_page->CopyHalfFrom(cur_page->GetData(), min_size - 1, cur_size);  // 复制数据

    cur_page->SetSize(min_size - 1);  // 更新当前页大小
    new_page->SetSize(cur_size - min_size + 1);  // 更新新页大小

    cur_page->Insert(key, new_page_guard.PageId(), comparator_);  // 在当前页插入新键

    // pushed_key 更新
    pushed_key = last_key;  
  }

  InsertInParent(pushed_key, std::move(new_parent_page_guard), ctx);  // 递归插
}

INDEX_TEMPLATE_ARGUMENTS  
// 打印单个叶子页或内部页的内容
void BPLUSTREE_TYPE::PrintPage(WritePageGuard &guard, bool is_leaf_page) {
  // 判断当前页面是否为叶子页
  if (is_leaf_page) {
    // 转换为可修改的叶子页
    auto temp_page = guard.AsMut<LeafPage>();
    
    // 当前页 ID
    std::cout << "Leaf Contents (page_id " << guard.PageId() << "): " << std::endl;
    
    for (int i = 0; i < temp_page->GetSize(); i++) {

      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << "}";
      
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  } else {  // 内部页
    // 转换内部页
    auto temp_page = guard.AsMut<InternalPage>();
    
    std::cout << "Internal Contents (page_id " << guard.PageId() << "): " << std::endl;
    
    for (int i = 0; i < temp_page->GetSize(); i++) {
      // 索引对应的键和值
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << ": " << temp_page->ValueAt(i) << "}";
      
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  }
  
  std::cout << std::endl;
  std::cout << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintPage(ReadPageGuard &guard, bool is_leaf_page) {
  // 是否为叶子页
  if (is_leaf_page) {
    // 转换为叶子页
    auto temp_page = guard.As<LeafPage>();
    
    std::cout << "Leaf Contents (page_id " << guard.PageId() << "): " << std::endl;
    
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << "}";
     
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  } else {  // 内部页
    // 转换内部页
    auto temp_page = guard.As<InternalPage>();
    
    std::cout << "Internal Contents (page_id " << guard.PageId() << "): " << std::endl;
    
    for (int i = 0; i < temp_page->GetSize(); i++) {
      std::cout << "index " << i << ": {" << temp_page->KeyAt(i).ToString() << ": " << temp_page->ValueAt(i) << "}";
      if ((i + 1) < temp_page->GetSize()) {
        std::cout << ", ";
      }
    }
  }
 
  std::cout << std::endl;
  std::cout << std::endl;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * 删除输入键相关键值对
 * If current tree is empty, return immediately.
 *
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * 首先找到正确的叶页面作为删除目标，然后从叶页面中删除条目。处理必要的重分配或合并。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;
  (void)ctx;

  // 头页面,锁
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // 树空返回
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // B+ 树是空的, 直接返回
    return;
  }

  // 查找叶页删除
  WritePageGuard guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  InternalPage *internal_page = nullptr;
  std::unordered_map<page_id_t, int> page_id_to_index;  // 存储每个页面ID和索引

  // 找到叶子页
  while (!page->IsLeafPage()) {
    internal_page = guard.AsMut<InternalPage>();
    ctx.write_set_.emplace_back(std::move(guard));

    // 中间节点查找孩子节点的索引，合并或重分配时可以找到兄弟节点
    int child_page_index = -1;
    page_id_t child_page_id = internal_page->FindValue(key, comparator_, &child_page_index);
    page_id_to_index[child_page_id] = child_page_index;  // 孩子节点的索引

    guard = bpm_->FetchPageWrite(child_page_id);  // 读孩子页
    page = guard.AsMut<BPlusTreePage>();
  }

  // 叶子页中查删键值对
  auto leaf_page = guard.AsMut<LeafPage>();
  ValueType res;
  // 找不到键值对，直接返回
  if (!leaf_page->FindValue(key, res, comparator_)) { 
    return;
  }

  ctx.write_set_.emplace_back(std::move(guard));
  DeleteEntry(ctx, key, res, page_id_to_index);  // 从 B+ 树中删除键值对

  ctx.write_set_.clear();  
  ctx.header_page_ = std::nullopt;  // 更新头页
}

// 删除键值对
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(Context &ctx, KeyType key, ValueType val,
                                 std::unordered_map<page_id_t, int> &page_id_to_index) {
  WritePageGuard cur_guard = std::move(ctx.write_set_.back());  // 叶子节点
  ctx.write_set_.pop_back();  // 弹出叶子
  page_id_t cur_leaf_page_id = cur_guard.PageId();  
  auto cur_leaf_page = cur_guard.AsMut<LeafPage>();

  // 删除是否成功
  if (!cur_leaf_page->Delete(key, val, comparator_)) {
    return;  // 失败
  }

  // 叶子节点空且为根节点，更新根节点ID
  if (cur_leaf_page_id == ctx.root_page_id_ && cur_leaf_page->GetSize() == 0) {
    auto header_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = INVALID_PAGE_ID;  // 更新根页无效
    ctx.root_page_id_ = INVALID_PAGE_ID;
    return;  
  

  // 删除节点是根节点有数据，直接返回
  if (cur_leaf_page_id == ctx.root_page_id_) {
    return;
  }


  if (cur_leaf_page->GetSize() >= cur_leaf_page->GetMaxSize()) {
    ctx.write_set_.clear();  
    ctx.header_page_ = std::nullopt;  // 更新
    return;  
  }

 
  page_id_t index_in_parent_page = page_id_to_index[cur_leaf_page_id];  // 叶子节点在父节点索引
  WritePageGuard &parent_guard = ctx.write_set_.back();  // 访问父节点
  auto parent_page = parent_guard.AsMut<InternalPage>();

  // 获兄弟节点
  page_id_t sibling_page_id = -1;  // 初始化兄弟
  bool is_last_entry = index_in_parent_page == parent_page->GetSize() - 1;  // 判断是否最后一个
  if (is_last_entry) {  // 当前叶子节点位于最右，兄弟选择左
    sibling_page_id = parent_page->ValueAt(index_in_parent_page - 1);
  } else {  // 兄弟选择右
    sibling_page_id = parent_page->ValueAt(index_in_parent_page + 1);
  }
  WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);  // 获取兄弟

  // 左右节点
  LeafPage *left_page = nullptr;  
  LeafPage *right_page = nullptr;  
  KeyType up_key;  // 存父节点
  page_id_t up_value;  // 存父节点值
  bool redistribute_toward_right = true;  // 判断是否向右重分配

  if (is_last_entry) {  // 最后
    left_page = sibling_page_guard.AsMut<LeafPage>();  // 获左兄弟
    right_page = cur_leaf_page;  // 当前页右兄弟
    up_key = parent_page->KeyAt(index_in_parent_page);  // 上级节点
    up_value = cur_leaf_page_id;  // 叶子节点
  } else {
    left_page = cur_leaf_page;  // 当前页左兄弟
    right_page = sibling_page_guard.AsMut<LeafPage>();  
    up_key = parent_page->KeyAt(index_in_parent_page + 1);  // 上级
    up_value = sibling_page_guard.PageId();  
    redistribute_toward_right = false;  
  }

  // 右节点向左节点合并
  int left_page_cur_size = left_page->GetSize();
  int right_page_cur_size = right_page->GetSize();
  if (left_page_cur_size + right_page_cur_size < left_page->GetMaxSize()) {
    left_page->Merge(right_page->GetData(), right_page->GetSize());  // 合并左侧与右侧数据

    // 叶子节点更新 next_page_id
    left_page->SetNextPage(right_page->GetNextPageId());

    DeleteInternalEntry(ctx, up_key, up_value, page_id_to_index);  // 从父节点删除信息
    return;  
  }

  // redistribute
  if (redistribute_toward_right) {  
    right_page->ShiftData(1);  // 右移数据
    right_page->SetKeyValueAt(0, left_page->KeyAt(left_page_cur_size - 1), left_page->ValueAt(left_page_cur_size - 1));  // 更新右节点数据
    left_page->IncreaseSize(-1);  // 左节点大小减
    parent_page->SetKeyAt(index_in_parent_page, right_page->KeyAt(0));  // 更新父节点键
  } else {                                                              // 右 => 左
    left_page->IncreaseSize(1);  // 左节点大小增
    left_page->SetKeyValueAt(left_page_cur_size, right_page->KeyAt(0), right_page->ValueAt(0));  // 更新左节点数据
    right_page->ShiftData(-1);  // 右节点数据左移
    parent_page->SetKeyAt(index_in_parent_page + 1, right_page->KeyAt(0));  // 更新父节点键
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * 输入参数为空，首先找到最左边的叶子页面，然后构造索引迭代器
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("B+ tree is empty");
  }

  // 读根页面
  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();
  const InternalPage *internal_page = nullptr;
  
  // 找最左边叶子页
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();
    guard = bpm_->FetchPageRead(internal_page->ValueAt(0));  // 最左
    page = guard.As<BPlusTreePage>();
  }

  // 获最左边叶子页
  const auto *leaf_page = guard.As<LeafPage>();
  MappingType entry = MappingType(leaf_page->KeyAt(0), leaf_page->ValueAt(0));

  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0, entry); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * 输入参数为低键，找到包含该输入键的叶子页面，然后构造索引迭代器
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("B+ tree is empty");
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();
  const InternalPage *internal_page = nullptr;

  // 查对应叶子页
  while (!page->IsLeafPage()) {
    internal_page = guard.As<InternalPage>();
    guard = bpm_->FetchPageRead(internal_page->FindValue(key, comparator_));  // 找下一页
    page = guard.As<BPlusTreePage>();
  }

  // 叶子页
  const auto *leaf_page = guard.As<LeafPage>();
  ValueType res;
  int index = -1;
  
  // 找键,索引
  if (leaf_page->FindValue(key, res, comparator_, &index)) {
    MappingType entry = MappingType(key, res);
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), index, entry);  
  }

  // 没对应 key
  return INDEXITERATOR_TYPE();  
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * 输入参数为空，构造一个表示键/值对在叶节点结束的索引迭代器
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); 
}

/**
 * @return Page id of the root of this tree
 * 返回树的根页面ID
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;  // 返回根页
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();  // 根页ID
  auto guard = bpm->FetchPageBasic(root_page_id);  // 锁根页
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());  // 树结构
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {  // 叶子页
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // 叶子页
    std::cout << "Leaf Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";  
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {  // 内部页
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << "(" << internal->GetSize() << ")" << std::endl;

    // 内部页
    std::cout << "Internal Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";  
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

    // 子节点
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));  
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());  
    }
  }
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";  // 如果树为空，返回空的表示
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());  // 获取可打印的树
  std::ostringstream out_buf;  // 定义输出缓冲区
  p_root.Print(out_buf);  // 打印树

  return out_buf.str();  // 返回字符串表示的树
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);  // 获取根页面
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;  // 可打印树的根

  if (root_page->IsLeafPage()) {  // 如果是叶子页面
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();  // 获取叶子页面的字符串表示
    proot.size_ = proot.keys_.size() + 4;  // 额外的空格

    return proot;  // 返回
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();  // 获取内部页面
  proot.keys_ = internal_page->ToString();  // 获取内部页面的字符串表示
  proot.size_ = 0;  // 初始化大小
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);  // 获取子节点ID
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);  // 递归获取子节点的表示
    proot.size_ += child_node.size_;  // 更新大小
    proot.children_.push_back(child_node);  // 添加子节点
  }

  return proot;  // 返回可打印树
}

// 实例化特定类型的 BPlusTree
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
