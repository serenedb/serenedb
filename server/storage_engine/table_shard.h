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
#include <cstdint>
#include <shared_mutex>
#include <tuple>

#include "basics/result_or.h"
#include "catalog/fwd.h"
#include "catalog/object.h"
#include "catalog/table_options.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb {
namespace transaction {

class Methods;
}

class Index;
class FollowerInfo;
class DocumentIterator;

struct OperationOptions;
class Result;

class TableShard : public catalog::Object {
 public:
  static constexpr double kDefaultLockTimeout = 10.0 * 60.0;

  virtual ~TableShard() = default;
  std::shared_ptr<Object> Clone() const final { return nullptr; }

  ObjectId GetTableId() const noexcept { return GetParentId(); }

  catalog::StorageKind GetStorage() const noexcept { return _storage; }

  auto& GetTableLock() noexcept { return _table_lock; }

  void UpdateNumRows(int64_t delta) noexcept {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }

  catalog::TableStats GetTableStats() const {
    return {.num_rows = _num_rows.load(std::memory_order_relaxed)};
  }

  // Serialize persists (StorageKind, stats) so recovery can construct the right
  // subclass; DeserializeStats reads the stats back and DeserializeStorageKind
  // the leading kind (used by the catalog's shard-construction dispatch).
  void Serialize(duckdb::Serializer& sink) const final;
  static catalog::TableStats DeserializeStats(std::string_view bytes);
  static catalog::StorageKind DeserializeStorageKind(std::string_view bytes);

  // Remove a shard's artifacts.
  // For kRocksDB: rocksdb range delete on the table's key range;
  // For kSearch: removes the iresearch directory + per-shard chunk subtree
  // derived from (db_id, table_id).
  static Result DropArtifacts(catalog::StorageKind kind, ObjectId db_id,
                              ObjectId table_id, ObjectId shard_id,
                              uint64_t size);

  // New table shard ctor
  explicit TableShard(ObjectId table_id, const catalog::TableStats& stats);

  // existed table shard ctor
  explicit TableShard(ObjectId id, ObjectId table_id,
                      const catalog::TableStats& stats);

 protected:
  ObjectId _table_id;
  // TODO(codeworse): this probably won't work in case of distributed setup
  std::atomic_uint64_t _num_rows{0};
  // TODO: remove table lock when we have a proper create index
  // Using std::shared_mutex instead of absl::Mutex because DataSink objects
  // may be destroyed on a different thread than they were created on (e.g.
  // during query cancellation), and absl::Mutex forbids cross-thread unlock.
  std::shared_mutex _table_lock;
  // Set by subclasses in their constructor; default RocksDB matches every
  // existing call site that constructs bare TableShard.
  catalog::StorageKind _storage = catalog::StorageKind::kRocksDB;
};

// Factory for CREATE TABLE / recovery. Returns the right TableShard subclass
// for the requested storage kind. db_id is only consumed by kSearch (it
// determines the iresearch directory path); kRocksDB ignores it.
ResultOr<std::shared_ptr<TableShard>> MakeTableShard(
  catalog::StorageKind kind, ObjectId db_id, ObjectId table_id,
  const catalog::TableStats& stats);

}  // namespace sdb
