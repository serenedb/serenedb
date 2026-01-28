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

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/read_write_lock.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/identifiers/transaction_id.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "search/data_store.h"
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

  void UpdateNumRows(int64_t delta) {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }

  catalog::TableStats GetTableStats() const {
    return {.num_rows = _num_rows.load(std::memory_order_relaxed)};
  }

  void GetTableStatsVPack(vpack::Builder& builder) const {
    vpack::WriteTuple(builder, GetTableStats());
  }

  auto GetDataStores() const { return _data_stores; }

  void AddDataStore(ObjectId data_store_id) {
    auto [_, is_new] = _data_stores.insert(data_store_id);
    SDB_ENSURE(is_new, ERROR_INTERNAL,
               "DataStore already exists in TableShard");
  }

  void RemoveDataStore(ObjectId data_store_id) {
    auto num_erased = _data_stores.erase(data_store_id);
    SDB_ENSURE(num_erased == 1, ERROR_INTERNAL,
               "DataStore does not exist in TableShard");
  }

  /// return the figures for a collection
  virtual yaclib::Future<OperationResult> figures(
    bool details, const OperationOptions& options);

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

  explicit TableShard(catalog::TableMeta collection,
                      const catalog::TableStats& stats);

 protected:
  /// Inject figures that are specific to StorageEngine
  virtual void figuresSpecific(bool details, vpack::Builder&) {}

  catalog::TableMeta _collection_meta;

  // TODO(gnusi): refactor/move to different place
  std::shared_ptr<FollowerInfo> _followers;

  std::atomic_bool _deleted = false;  // TODO(gnusi): remove
  containers::FlatHashSet<ObjectId> _data_stores;

  // TODO(codeworse): this probably won't work in case of distributed setup
  std::atomic_uint64_t _num_rows{0};
};

bool IsLeadingShard(const TableShard& c) noexcept;

}  // namespace sdb
