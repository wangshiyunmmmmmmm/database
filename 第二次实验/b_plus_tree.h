#pragma once

#include <mutex>
#include <queue>
#include <string>
#include <vector>

// ��������
#include "buffer/buffer_pool_manager.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

/**
 * ����������B+������ģ����
 * @tparam KeyType �����ͣ���int64_t��GenericKey��
 * @tparam ValueType ֵ���ͣ�ͨ��ΪRID��
 * @tparam KeyComparator ���Ƚ��������ڱȽϼ���˳��
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class IndexIterator;

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

enum class Operation { Search, Insert, Delete };

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  /**
   * B+�����캯��
   * @param name �������ƣ�����Header Page��¼��
   * @param buffer_pool_manager ����ع�����ʵ��
   * @param comparator ���Ƚ���
   * @param leaf_max_size Ҷ�ӽڵ����������Ĭ��ֵ����ͷ�ļ���
   * @param internal_max_size �ڲ��ڵ����������Ĭ��ֵ����ͷ�ļ���
   */
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE - 1, int internal_max_size = INTERNAL_PAGE_SIZE - 1);

  /**
   * ��B+���л�ȡָ������Ӧ��ֵ
   * @param key Ҫ���ҵļ�
   * @param result �洢��������������ܶ��RID��Ӧ��ͬ����
   * @param transaction ���������ģ���δʹ�ã�
   * @return �Ƿ��ҵ���
   */
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  /**
   * ��B+���в����ֵ��
   * @param key Ҫ����ļ�
   * @param value Ҫ�����ֵ��RID��
   * @param transaction ���������ģ���δʹ�ã�
   * @return �����Ƿ�ɹ����ظ�������false��
   */
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  /**
   * ��B+����ɾ����ֵ��
   * @param key Ҫɾ���ļ�
   * @param transaction ���������ģ���δʹ�ã�
   * @return �Ƿ�ɹ�ɾ��
   */
  auto Remove(const KeyType &key, Transaction *transaction = nullptr) -> bool;

  /**
   * �����������ƣ����ڵ��ԣ�
   */
  auto GetIndexName() -> std::string { return index_name_; }

  /**
   * ��ӡB+���ṹ�������ã�
   */
  auto Print() -> void;
  void Print(BufferPoolManager *buffer_pool_manager);

  /**
   * ����B+����DOT���ӻ��ļ�
   * @param out �����
   */
  auto DrawBPlusTree(std::ostream &out) -> void;
  void Draw(BufferPoolManager *buffer_pool_manager, const std::string &out_file);

  /**
   * ���ص�ǰ���ĸ߶�
   */
  auto GetHeight() -> int { return height_; }

  /** ��ʼ����β���������ڳ�������ʹ�ã� */
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  /** ���ļ����뿪���Ƴ�B+���еļ�ֵ�� */
  void InsertFromFile(const std::string &file_name, Transaction *transaction);
  void RemoveFromFile(const std::string &file_name, Transaction *transaction);

 private:
  // ����˽�з���
  auto FindLeafPageImpl(const KeyType &key, Transaction *txn, Operation op, bool leftMost) -> Page *;
  /**
   * ���Ұ���ָ������Ҷ��ҳ��, �������� Latch Crabbing ����
   * @param key ���Ҽ�
   * @param txn ��ǰ����
   * @param leftMost �Ƿ��������Ҷ�ӣ����ڷ�Χɨ�裩
   * @param is_write �Ƿ�Ϊд����
   * @return �ҵ���ҳ��ָ�루��Ҫ�Ըý� Unpin��
   */
  auto FindLeafPage(const KeyType &key, Transaction *txn, bool leftMost = false, bool is_write = false) -> Page *;

  /**
   * ��ʼ���������״β���ʱ���ã�
   * @param key ��һ����
   * @param value ��һ��ֵ
   */
  auto StartNewTree(const KeyType &key, const ValueType &value) -> bool;

  /**
   * ��Ҷ��ҳ������ֵ��
   * @param key �����
   * @param value ����ֵ
   * @param txn ��ǰ����
   */
  auto InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *txn) -> bool;

  /**
   * ����Ҷ��ҳ��
   * @param leaf_page �����ѵ�Ҷ��ҳ��
   */
  void SplitLeafPage(LeafPage *leaf_page);

  /**
   * �����ڲ�ҳ��
   * @param internal_page �����ѵ��ڲ�ҳ��
   */
  void SplitInternalPage(InternalPage *internal_page);

  /**
   * �����Ѻ����ҳ����븸�ڵ�
   * @param old_page ԭҳ��
   * @param key ���Ѽ��������������ڵ㣩
   * @param new_page ��ҳ��
   */
  void InsertIntoParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page);
  /** �ڵ��С��С�ڵ���Сʱ�����ϲ���·������ */
  void CoalesceOrRedistribute(BPlusTreePage *node);
  auto AdjustRoot(BPlusTreePage *old_root_node) -> bool;
  /** Ҷ�ӽڵ�������ڵ㣨RedistributeLeafNode��MergeLeafNode�� */
  void RedistributeLeafNode(LeafPage *borrower, LeafPage *lender, InternalPage *parent, int index);
  void MergeLeafNode(LeafPage *left, LeafPage *right, InternalPage *parent, int index);
  /** �ڲ��ڵ�������ڵ㣨ͨ��ģ��ʵ�֣� */
  template <typename N>
  void Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index);
  template <typename N>
  void Redistribute(N *neighbor_node, N *node, InternalPage *parent, int index);
  /**
   * ����Header Page�еĸ�ҳ���¼
   * @param insert_record �Ƿ�Ϊ�½���¼���״β���Ϊ1��
   */
  void UpdateRootPageId(int insert_record = 0);

  // ��Ա����
  std::string index_name_;                   // ��������
  BufferPoolManager *buffer_pool_manager_;   // ����ع�����ʵ��
  page_id_t root_page_id_{INVALID_PAGE_ID};  // ��ҳ��ID
  int height_{0};                            // ��ǰ���߶�
  KeyComparator comparator_;                 // ���Ƚ���
  int leaf_max_size_;                        // Ҷ�ӽڵ��������
  int internal_max_size_;                    // �ڲ��ڵ��������
  // 使用递归互斥量，避免在某些辅助调用路径中同一线程重复加锁导致 EDEADLK。
  std::recursive_mutex latch_;               // �����������������ã�
};

}  // namespace bustub