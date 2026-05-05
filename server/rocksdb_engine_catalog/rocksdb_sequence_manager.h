////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <memory>
#include <string>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
}  // namespace rocksdb

namespace sdb {

// Per-table monotonic ID allocator. Backed by RocksDB UInt64AddOperator on the
// "sequences" column family: each Reserve() does one atomic Merge(+count)
// followed by one Get() to read the post-merge high-water mark, then returns
// `H - count + 1` so the caller owns the contiguous range [base, base + count).
//
// Concurrency: serialized per-table via an internal mutex. Within a single
// process this is correct (every Get observes our own Merge). Multi-process
// writers against the same DB would race -- not supported here.
class TableSequence {
 public:
  TableSequence(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                ObjectId table_id);

  TableSequence(const TableSequence&) = delete;
  TableSequence& operator=(const TableSequence&) = delete;

  // Reserve `count` consecutive IDs and return the first one (`base`).
  // Caller owns [base, base + count). count must be > 0.
  uint64_t Reserve(uint64_t count);

 private:
  rocksdb::DB* _db;
  rocksdb::ColumnFamilyHandle* _cf;
  std::string _key;       // 8-byte BE table_id
  absl::Mutex _mu;
};

class RocksDBEngineCatalog;

// Owns one TableSequence per table_id. Lazily creates them on first lookup.
class RocksDBSequenceManager {
 public:
  explicit RocksDBSequenceManager(RocksDBEngineCatalog& engine);

  RocksDBSequenceManager(const RocksDBSequenceManager&) = delete;
  RocksDBSequenceManager& operator=(const RocksDBSequenceManager&) = delete;

  // Returns a pointer stable for the lifetime of this manager.
  TableSequence& GetForTable(ObjectId table_id);

 private:
  RocksDBEngineCatalog& _engine;
  rocksdb::ColumnFamilyHandle* _cf;
  absl::Mutex _map_mu;
  absl::flat_hash_map<uint64_t, std::unique_ptr<TableSequence>> _by_table_id
    ABSL_GUARDED_BY(_map_mu);
};

}  // namespace sdb
