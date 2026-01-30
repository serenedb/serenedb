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

TableShard::TableShard(catalog::TableMeta collection_meta,
                       const catalog::TableStats& stats)
  : _collection_meta{std::move(collection_meta)}, _num_rows{stats.num_rows} {}

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

bool IsLeadingShard(const TableShard& c) noexcept {
#ifdef SDB_CLUSTER
  SDB_ASSERT(ServerState::instance()->IsDBServer());
  return c.GetFollowers().getLeader().second.empty();
#else
  return false;
#endif
}

}  // namespace sdb
