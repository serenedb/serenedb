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

#include "table_shard.h"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>
#include <vpack/slice.h>

#include <algorithm>
#include <atomic>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/read_locker.h"
#include "basics/recursive_locker.h"
#include "basics/result.h"
#include "basics/static_strings.h"
#include "basics/write_locker.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/indexes_snapshot.h"
#include "utils/query_cache.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "indexes/index.h"
#include "transaction/helpers.h"
#endif

namespace sdb {

catalog::TableMeta MakeTableMeta(const catalog::Table& c) {
  return {
    .database = c.GetDatabaseId(),
    .schema = c.GetSchemaId(),
    .id = c.GetId(),
    .plan_id = c.planId(),
    .plan_db = c.planDb(),
    .from = c.from(),
    .to = c.to(),
    .name = std::string{c.GetName()},
  };
}

TableShard::TableShard(catalog::TableMeta collection_meta)
  : _collection_meta{std::move(collection_meta)} {}

/// fetches current index selectivity estimates
/// if allowUpdate is true, will potentially make a cluster-internal roundtrip
/// to fetch current values!
IndexEstMap TableShard::clusterIndexEstimates(bool allow_updating,
                                              TransactionId tid) {
  SDB_THROW(ERROR_INTERNAL,
            "cluster index estimates called for non-cluster collection");
}

/// flushes the current index selectivity estimates
void TableShard::flushClusterIndexEstimates() {
  // default-implementation is a no-op. the operation is only useful for cluster
  // collections
}

void TableShard::prepareIndexes(catalog::Table& logical_collection,
                                vpack::Slice indexes_slice) {
#ifdef SDB_CLUSTER
  if (!indexes_slice.isArray()) {
    // TODO(gnusi): make creation of system indexes explicit
    indexes_slice = vpack::Slice::emptyArraySlice();
  }

  auto& engine =
    SerenedServer::Instance().getFeature<EngineSelectorFeature>().engine();

  std::vector<std::shared_ptr<Index>> indexes;
  {
    // link creation needs read-lock too
    RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
    if (indexes_slice.length() == 0 && _indexes.empty()) {
      engine.fillSystemIndexes(logical_collection, indexes);
    } else {
      engine.prepareIndexes(logical_collection, indexes_slice, indexes);
    }
  }

  RECURSIVE_WRITE_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  SDB_ASSERT(_indexes.empty());

  for (auto& idx : indexes) {
    SDB_ASSERT(idx != nullptr);
    const auto id = idx->id();
    for (const auto& it : _indexes) {
      SDB_ASSERT(it != nullptr);
      if (it->id() == id) {  // index is there twice
        idx.reset();
        break;
      }
    }

    if (idx) {
      _indexes.emplace(idx);
      duringAddIndex(idx);
    }
  }

  if (!_indexes.empty()) {
    auto it = _indexes.cbegin();
    if ((*it)->type() != IndexType::kTypePrimaryIndex ||
        (TableType::Edge == logical_collection.GetTableType() &&
         (_indexes.size() < 3 ||
          ((*++it)->type() != IndexType::kTypeEdgeIndex ||
           (*++it)->type() != IndexType::kTypeEdgeIndex)))) {
      std::string msg = absl::StrCat("got invalid indexes for collection '",
                                     _collection_meta.name, "'");
      SDB_ERROR("xxxxx", Logger::ENGINES, msg);
      SDB_THROW(ERROR_INTERNAL, std::move(msg));
    }
  }
#endif
}

void TableShard::close() {
#ifdef SDB_CLUSTER
  RECURSIVE_WRITE_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  for (auto& it : _indexes) {
    it->unload();
  }
  _indexes.clear();
#endif
}

void TableShard::drop() {
  _deleted.store(true, std::memory_order_release);
  freeMemory();
  try {
    // close collection. this will also invalidate the revisions cache
    close();
  } catch (...) {
    // don't throw from here... dropping should succeed
  }
}

void TableShard::freeMemory() noexcept {}

yaclib::Future<uint64_t> TableShard::recalculateCounts() {
  SDB_THROW(ERROR_NOT_IMPLEMENTED,
            "recalculateCounts not implemented for this engine");
}

bool TableShard::hasDocuments() {
  SDB_THROW(ERROR_NOT_IMPLEMENTED,
            "hasDocuments not implemented for this engine");
}

std::shared_ptr<Index> TableShard::findIndex(
  vpack::Slice info, const IndexContainerType& indexes) {
#ifdef SDB_CLUSTER
  SDB_ASSERT(info.isObject());

  const auto type =
    Index::type(info.get(StaticStrings::kIndexType).stringView());
  const auto name =
    basics::VPackHelper::getString(info, StaticStrings::kIndexName, {});
  for (const auto& idx : indexes) {
    if (idx->type() == type && idx->name() == name &&
        idx->matchesDefinition(info)) {
      return idx;
    }
  }
#endif
  return nullptr;
}

/// Find index by definition
std::shared_ptr<Index> TableShard::lookupIndex(vpack::Slice info) const {
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  return findIndex(info, _indexes);
}

std::shared_ptr<Index> TableShard::lookupIndex(IndexId idx_id) const {
#ifdef SDB_CLUSTER
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  for (const auto& idx : _indexes) {
    if (idx->id() == idx_id) {
      return idx;
    }
  }
#endif
  return nullptr;
}

std::shared_ptr<Index> TableShard::lookupIndex(
  std::string_view idx_name) const {
#ifdef SDB_CLUSTER
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  for (const auto& idx : _indexes) {
    if (idx->name() == idx_name) {
      return idx;
    }
  }
#endif
  return nullptr;
}

std::unique_ptr<containers::RevisionTree> TableShard::revisionTree(
  transaction::Methods& /*trx*/) {
  return nullptr;
}

std::unique_ptr<containers::RevisionTree> TableShard::revisionTree(
  rocksdb::SequenceNumber) {
  return nullptr;
}

std::unique_ptr<containers::RevisionTree> TableShard::computeRevisionTree(
  uint64_t batch_id) {
  return nullptr;
}

yaclib::Future<Result> TableShard::rebuildRevisionTree() {
  return yaclib::MakeFuture<Result>(ERROR_NOT_IMPLEMENTED);
}

uint64_t TableShard::placeRevisionTreeBlocker(TransactionId) {
  SDB_THROW(ERROR_NOT_IMPLEMENTED);
}

void TableShard::removeRevisionTreeBlocker(TransactionId) {
  SDB_THROW(ERROR_NOT_IMPLEMENTED);
}

/// get list of all indexes. this includes in-progress indexes and thus
/// should be used with care
std::vector<std::shared_ptr<Index>> TableShard::getAllIndexes() const {
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  return {_indexes.begin(), _indexes.end()};
}

/// get a list of "ready" indexes, that means all indexes which are
/// not "in progress" anymore
std::vector<std::shared_ptr<Index>> TableShard::getReadyIndexes() const {
  std::vector<std::shared_ptr<Index>> result;
#ifdef SDB_CLUSTER
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  result.reserve(_indexes.size());
  absl::c_copy_if(_indexes, std::back_inserter(result),
                  [](const auto& idx) { return !idx->inProgress(); });
#endif
  return result;
}

/// get a snapshot of all indexes of the collection, with the read
/// lock on the list of indexes being held while the snapshot is active
IndexesSnapshot TableShard::getIndexesSnapshot() {
  std::vector<std::shared_ptr<Index>> indexes;

  // lock list of indexes in collection
  RecursiveReadLocker<basics::ReadWriteLock> locker(
    _indexes_lock, _indexes_lock_write_owner, __FILE__, __LINE__);

  // copy indexes from std::set into flat std::vector
  indexes.reserve(_indexes.size());
  for (const auto& it : _indexes) {
    indexes.emplace_back(it);
  }

  // pass ownership for lock and list of indexes to IndexesSnapshot
  return IndexesSnapshot(std::move(locker), std::move(indexes));
}

Index* TableShard::primaryIndex() const {
#ifdef SDB_CLUSTER
  RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
  for (auto& idx : _indexes) {
    if (idx->type() == kTypePrimaryIndex) {
      SDB_ASSERT(idx->id().isPrimary());
      return idx.get();
    }
  }
#endif
  return nullptr;
}

void TableShard::getIndexesVPack(
  vpack::Builder& result,
  const std::function<bool(const Index*, IndexSerialization&)>& filter) const {
  result.openArray();
#ifdef SDB_CLUSTER
  {
    RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
    for (const auto& idx : _indexes) {
      IndexSerialization flags{};

      if (!filter(idx.get(), flags)) {
        continue;
      }

      idx->toVPack(result, flags);
    }
  }
#endif
  result.close();
}

/// return the figures for a collection
yaclib::Future<OperationResult> TableShard::figures(
  bool details, const OperationOptions& options) {
  auto buffer = std::make_shared<vpack::BufferUInt8>();
  vpack::Builder builder(buffer);

  builder.openObject();

  // add index information
  size_t size_indexes = 0;
  size_t num_indexes = 0;

#ifdef SDB_CLUSTER
  {
    bool seen_edge_index = false;
    RECURSIVE_READ_LOCKER(_indexes_lock, _indexes_lock_write_owner);
    for (const auto& idx : _indexes) {
      // only count an edge index instance
      if (idx->type() != kTypeEdgeIndex || !seen_edge_index) {
        ++num_indexes;
      }
      if (idx->type() == kTypeEdgeIndex) {
        seen_edge_index = true;
      }
      size_indexes += static_cast<size_t>(idx->memory());
    }
  }
#endif

  builder.add("indexes", vpack::Value(vpack::ValueType::Object));
  builder.add("count", num_indexes);
  builder.add("size", size_indexes);
  builder.close();  // indexes

  // add engine-specific figures
  figuresSpecific(details, builder);
  builder.close();
  return yaclib::MakeFuture<OperationResult>(Result(), std::move(buffer),
                                             options);
}

std::unique_ptr<ReplicationIterator> TableShard::getReplicationIterator(
  ReplicationIterator::Ordering, uint64_t batch_id) {
  return nullptr;
}

std::unique_ptr<ReplicationIterator> TableShard::getReplicationIterator(
  ReplicationIterator::Ordering, transaction::Methods&) {
  return nullptr;
}

void TableShard::adjustNumberDocuments(transaction::Methods&, int64_t) {}

bool TableShard::IndexOrder::operator()(
  const std::shared_ptr<Index>& left,
  const std::shared_ptr<Index>& right) const {
#ifdef SDB_CLUSTER
  // Primary index always first (but two primary indexes render comparison
  // invalid but that`s a bug itself)
  SDB_ASSERT(!((left->type() == IndexType::kTypePrimaryIndex) &&
               (right->type() == IndexType::kTypePrimaryIndex)));
  if (left->type() == IndexType::kTypePrimaryIndex) {
    return true;
  } else if (right->type() == IndexType::kTypePrimaryIndex) {
    return false;
  }

  // edge indexes should go right after primary
  if (left->type() != right->type()) {
    if (left->type() == IndexType::kTypeEdgeIndex) {
      return true;
    } else if (right->type() == IndexType::kTypeEdgeIndex) {
      return false;
    }
  }

  // This failpoint allows CRUD tests to trigger reversal
  // of index operations. Hash index placed always AFTER reversable indexes
  // could be broken by unique constraint violation or by intentional failpoint.
  // And this will make possible to deterministically trigger index reversals
  SDB_IF_FAILURE("HashIndexAlwaysLast") {
    if (left->type() != right->type()) {
      if (left->type() == IndexType::kTypeSecondaryIndex) {
        return false;
      } else if (right->type() == IndexType::kTypeSecondaryIndex) {
        return true;
      }
    }
  }

  // indexes which needs no reverse should be done first to minimize
  // need for reversal procedures
  if (left->needsReversal() != right->needsReversal()) {
    return right->needsReversal();
  }
  // use id to make  order of equally-sorted indexes deterministic
  return left->id() < right->id();
#else
  return false;
#endif
}

Result TableShard::dropIndex(const catalog::Table& c, IndexId iid) {
#ifdef SDB_CLUSTER
  if (!iid.isSet() || iid.isPrimary()) {
    return {};
  }

  SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  aql::QueryCache::instance()->invalidate(_collection_meta.schema,
                                          _collection_meta.id);

  Result res = basics::SafeCall([&]() -> Result {
    auto& engine =
      SerenedServer::Instance().getFeature<EngineSelectorFeature>().engine();
    const bool in_recovery = engine.inRecovery();

    std::shared_ptr<Index> to_remove;
    {
      RECURSIVE_WRITE_LOCKER(_indexes_lock, _indexes_lock_write_owner);

      // create a copy of _indexes, in case we need to roll back changes.
      auto copy = _indexes;

      for (auto it = _indexes.begin(); it != _indexes.end(); ++it) {
        if (iid != (*it)->id()) {
          continue;
        }

        to_remove = *it;
        // we have to remove from _indexes already here, because the
        // duringDropIndex may serialize the collection's indexes
        // and look at the _indexes member... and there we need the
        // index to be deleted already.
        _indexes.erase(it);

        if (!in_recovery) {
          Result res = duringDropIndex(c, to_remove);
          if (res.fail()) {
            // callback failed - revert back to copy
            _indexes = std::move(copy);
            return res;
          }
        }

        break;
      }

      if (to_remove == nullptr) {
        // index not found
        return {ERROR_SERVER_INDEX_NOT_FOUND};
      }
    }

    SDB_ASSERT(to_remove != nullptr);

    return afterDropIndex(to_remove);
  });

  return res;
#else
  return {ERROR_NOT_IMPLEMENTED};
#endif
}

// callback that is called directly before the index is dropped.
// the write-lock on all indexes is still held
Result TableShard::duringDropIndex(const catalog::Table& c,
                                   std::shared_ptr<Index> /*idx*/) {
  return {};
}

// callback that is called directly after the index has been dropped.
// no locks are held anymore.
Result TableShard::afterDropIndex(std::shared_ptr<Index> /*idx*/) { return {}; }

// callback that is called while adding a new index
void TableShard::duringAddIndex(std::shared_ptr<Index> /*idx*/) {}

bool IsLeadingShard(const TableShard& c) noexcept {
#ifdef SDB_CLUSTER
  SDB_ASSERT(ServerState::instance()->IsDBServer());
  return c.GetFollowers().getLeader().second.empty();
#else
  return false;
#endif
}

}  // namespace sdb
