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

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>

#include "catalog/object.h"

namespace rocksdb {

class ColumnFamilyHandle;
class DB;

}  // namespace rocksdb
namespace sdb::catalog {

// PG-compatible sequence parameters (CREATE SEQUENCE [...]).
// Values mirror the column-only fields of pg_sequence.
struct SequenceOptions {
  int64_t start_value = 1;
  int64_t increment = 1;
  int64_t min_value = 1;
  int64_t max_value = std::numeric_limits<int64_t>::max();
  int64_t cache_size = 1;
  bool cycle = false;
};

// A first-class catalog object representing a SQL sequence.
//
// Two creation paths feed into this single type:
//   * `CREATE SEQUENCE` (user DDL) -- created by SereneDBSchemaEntry.
//   * Auto-generated PK (table without explicit PRIMARY KEY) -- created
//     alongside the Table in LocalCatalog::CreateTable.
//
// Persistent counter access (next-value-to-emit) is exposed via Reserve /
// Read / Write below. The counter lives in the dedicated `sequences`
// RocksDB column family, keyed by this Sequence's ObjectId.
class Sequence final : public SchemaObject {
 public:
  Sequence(ObjectId database_id, ObjectId schema_id, ObjectId id,
           std::string_view name, SequenceOptions opts);
  ~Sequence();

  static std::shared_ptr<Sequence> ReadInternal(vpack::Slice slice,
                                                ReadContext ctx);

  void WriteInternal(vpack::Builder& b) const final;
  std::shared_ptr<Object> Clone() const final;

  const SequenceOptions& Options() const noexcept { return _options; }
  SequenceOptions& MutableOptions() noexcept { return _options; }

  // Reserve `count` consecutive ticks of the persistent counter. Returns the
  // post-merge high-water mark; the caller owns the range
  //   [high_water - count + 1, high_water]
  // For a freshly-seeded sequence with start=1/increment=1 and count=1, the
  // first call returns 1.
  uint64_t Reserve(uint64_t count);

  // Read the persisted counter without advancing it. Returns 0 if the
  // counter key has never been written.
  uint64_t Read() const;

  // Overwrite the persisted counter (setval / seeding). Atomic Put on the
  // counter key.
  void Write(uint64_t value);

 private:
  // Hot path: `_live` is the source of truth for what's been handed out.
  // `Reserve` does Merge(+count) (WAL durability) then atomic fetch_add (no
  // lock). On first use, `_init` seeds `_live` from the persisted value.
  // `_setval_mu` only guards `Write` (setval) -- the cold path that needs
  // to atomically Put + store-into-atomic against concurrent Reserves.
  mutable std::atomic<uint64_t> _live{0};
  mutable std::once_flag _init;
  mutable absl::Mutex _setval_mu;
  SequenceOptions _options;

  uint64_t LoadFromDb() const;
  void EnsureInitialized() const;
};

}  // namespace sdb::catalog
