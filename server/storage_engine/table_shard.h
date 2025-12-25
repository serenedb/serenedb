////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <rocksdb/types.h>

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "basics/errors.h"
#include "basics/read_write_lock.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/identifiers/transaction_id.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "storage_engine/replication_iterator.h"
// consider just forward declaration
#include "utils/coro_helper.h"
#include "utils/merkle_tree.h"
#include "utils/operation_result.h"

#ifdef SDB_CLUSTER
#include "cluster/follower_info.h"
#include "indexes/index.h"
#include "indexes/index_iterator.h"
#endif

namespace vpack {
class Builder;
class Slice;
}  // namespace vpack
namespace sdb {
namespace transaction {
class Methods;
}

class Index;
class IndexesSnapshot;
class FollowerInfo;
class DocumentIterator;

struct OperationOptions;
class Result;

catalog::TableMeta MakeTableMeta(const catalog::Table& c);

class TableShard {
 public:
  static constexpr double kDefaultLockTimeout = 10.0 * 60.0;

  virtual ~TableShard() = default;

  virtual RevisionId revision(transaction::Methods* trx) const { return {}; }

  // Return the number of documents in this collection
  virtual uint64_t numberDocuments(transaction::Methods* trx) const {
    return {};
  }

  virtual uint64_t approxNumberDocuments() const { return 0; }

  auto& GetMeta() const noexcept { return _collection_meta; }
  auto GetId() const noexcept { return _collection_meta.id; }

  virtual void close();

  void drop();

  /// recalculate counts for collection in case of failure, blocking
  virtual yaclib::Future<uint64_t> recalculateCounts();

  /// whether or not the collection contains any documents. this
  /// function is allowed to return true even if there are no documents
  virtual bool hasDocuments();

  /// fetches current index selectivity estimates
  /// if allowUpdate is true, will potentially make a cluster-internal
  /// roundtrip to fetch current values!
  virtual IndexEstMap clusterIndexEstimates(bool allow_updating,
                                            TransactionId tid);

  /// flushes the current index selectivity estimates
  virtual void flushClusterIndexEstimates();

  virtual void prepareIndexes(catalog::Table& logical_collection,
                              vpack::Slice indexes_slice);

  /// determines order of index execution on collection
  struct IndexOrder {
    bool operator()(const std::shared_ptr<Index>& left,
                    const std::shared_ptr<Index>& right) const;
  };

  using IndexContainerType = std::set<std::shared_ptr<Index>, IndexOrder>;
  /// find index by definition
  static std::shared_ptr<Index> findIndex(vpack::Slice info,
                                          const IndexContainerType& indexes);
  /// Find index by definition
  std::shared_ptr<Index> lookupIndex(vpack::Slice info) const;

  /// Find index by iid
  std::shared_ptr<Index> lookupIndex(IndexId idx_id) const;

  /// Find index by name
  std::shared_ptr<Index> lookupIndex(std::string_view idx_name) const;

  /// get list of all indexes. this includes in-progress indexes and thus
  /// should be used with care
  std::vector<std::shared_ptr<Index>> getAllIndexes() const;

  /// get a list of "ready" indexes, that means all indexes which are
  /// not "in progress" anymore
  std::vector<std::shared_ptr<Index>> getReadyIndexes() const;

  /// get a snapshot of all indexes of the collection, with the read
  /// lock on the list of indexes being held while the snapshot is active
  IndexesSnapshot getIndexesSnapshot();

  virtual Index* primaryIndex() const;

  void getIndexesVPack(
    vpack::Builder&,
    const std::function<bool(const Index*, IndexSerialization&)>& filter) const;

  // TODO(gnusi): remove
  void getAllIndexesInternal(vpack::Builder& result) const {
    getIndexesVPack(result, [](const Index*, IndexSerialization& flags) {
      flags = IndexSerialization::Internals;
      return true;
    });
  }

  /// return the figures for a collection
  virtual yaclib::Future<OperationResult> figures(
    bool details, const OperationOptions& options);

  /// create or restore an index
  /// restore utilize specified ID, assume index has to be created
  virtual yaclib::Future<std::shared_ptr<Index>> createIndex(
    catalog::Table& logical_collection, vpack::Slice info, bool restore,
    bool& created,
    std::shared_ptr<std::function<sdb::Result(double)>> = nullptr) {
    SDB_ASSERT(false);
    return {};
  }

  virtual Result dropIndex(const catalog::Table& c, IndexId iid);

#ifdef SDB_CLUSTER
  virtual std::unique_ptr<DocumentIterator> getAllIterator(
    transaction::Methods* trx, ReadOwnWrites read_own_writes) const {
    SDB_ASSERT(false);
    return {};
  }
  virtual std::unique_ptr<DocumentIterator> getAnyIterator(
    transaction::Methods* trx) const {
    SDB_ASSERT(false);
    return {};
  }
#endif

  /// Get an iterator associated with the specified replication batch
  virtual std::unique_ptr<ReplicationIterator> getReplicationIterator(
    ReplicationIterator::Ordering, uint64_t batch_id);

  /// Get an iterator associated with the specified transaction
  virtual std::unique_ptr<ReplicationIterator> getReplicationIterator(
    ReplicationIterator::Ordering, transaction::Methods&);

  virtual void adjustNumberDocuments(transaction::Methods&, int64_t);

  virtual Result truncate(transaction::Methods& trx,
                          const OperationOptions& options,
                          bool& used_range_delete) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  /// compact-data operation
  virtual void compact() {}

  virtual Result lookupKey(transaction::Methods*, std::string_view, RevisionId&,
                           ReadOwnWrites read_own_writes) const {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result lookupKeyForUpdate(transaction::Methods*, std::string_view,
                                    RevisionId&) const {
    return {ERROR_NOT_IMPLEMENTED};
  }

  struct LookupOptions {
    bool read_own_writes = false;
    bool count_bytes = false;
  };

#ifdef SDB_CLUSTER
  virtual Result lookup(transaction::Methods* trx, std::string_view key,
                        const IndexIterator::DocumentCallback& cb,
                        LookupOptions options) const {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result lookup(transaction::Methods* trx, RevisionId token,
                        const IndexIterator::DocumentCallback& cb,
                        LookupOptions options,
                        const StorageSnapshot* snapshot = nullptr) const {
    return {ERROR_NOT_IMPLEMENTED};
  }
#endif

  virtual Result insert(transaction::Methods& trx,
                        const IndexesSnapshot& indexes_snapshot,
                        RevisionId new_revision_id, vpack::Slice new_document,
                        const OperationOptions& options) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result update(transaction::Methods& trx,
                        const IndexesSnapshot& indexes_snapshot,
                        RevisionId previous_revision_id,
                        vpack::Slice previous_document,
                        RevisionId new_revision_id, vpack::Slice new_document,
                        const OperationOptions& options) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result replace(transaction::Methods& trx,
                         const IndexesSnapshot& indexes_snapshot,
                         RevisionId previous_revision_id,
                         vpack::Slice previous_document,
                         RevisionId new_revision_id, vpack::Slice new_document,
                         const OperationOptions& options) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result remove(transaction::Methods& trx,
                        const IndexesSnapshot& indexes_snapshot,
                        RevisionId previous_revision_id,
                        vpack::Slice previous_document,
                        const OperationOptions& options) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual std::unique_ptr<containers::RevisionTree> revisionTree(
    transaction::Methods& trx);
  virtual std::unique_ptr<containers::RevisionTree> revisionTree(
    rocksdb::SequenceNumber trx_seq);
  virtual std::unique_ptr<containers::RevisionTree> computeRevisionTree(
    uint64_t batch_id);

  virtual yaclib::Future<Result> rebuildRevisionTree();

  virtual uint64_t placeRevisionTreeBlocker(TransactionId transaction_id);
  virtual void removeRevisionTreeBlocker(TransactionId transaction_id);

  void setDeleted() noexcept {
    _deleted.store(true, std::memory_order_release);
  }

  bool deleted() const noexcept {
    return _deleted.load(std::memory_order_acquire);
  }

  virtual void freeMemory() noexcept;

  auto& GetFollowers() const {
    SDB_ASSERT(_followers);
    return *_followers;
  }

  explicit TableShard(catalog::TableMeta collection);

 protected:
  // callback that is called directly before the index is dropped.
  // the write-lock on all indexes is still held. not called during
  // reocvery!
  virtual Result duringDropIndex(const catalog::Table& c,
                                 std::shared_ptr<Index> idx);

  // callback that is called directly after the index has been dropped.
  // no locks are held anymore.
  virtual Result afterDropIndex(std::shared_ptr<Index> idx);

  // callback that is called while adding a new index. called under
  // indexes write-lock
  virtual void duringAddIndex(std::shared_ptr<Index> idx);

  /// Inject figures that are specific to StorageEngine
  virtual void figuresSpecific(bool details, vpack::Builder&) {}

  catalog::TableMeta _collection_meta;

  // TODO(gnusi): refactor/move to different place
  std::shared_ptr<FollowerInfo> _followers;

  mutable basics::ReadWriteLock _indexes_lock;
  // current thread owning '_indexes_lock'
  mutable std::atomic<std::thread::id> _indexes_lock_write_owner;
  IndexContainerType _indexes;
  std::atomic_bool _deleted = false;  // TODO(gnusi): remove
};

bool IsLeadingShard(const TableShard& c) noexcept;

}  // namespace sdb
