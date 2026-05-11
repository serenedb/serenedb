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
#include <string>

#include "catalog/object.h"

namespace rocksdb {

class ColumnFamilyHandle;
class DB;

}  // namespace rocksdb
namespace sdb::catalog {

struct SequenceOptions {
  uint64_t start_value = 1;
  uint64_t increment = 1;
  uint64_t min_value = 1;
  uint64_t max_value = std::numeric_limits<int64_t>::max();
  bool cycle = false;
  uint64_t cache = 1;

  uint64_t Seed() const noexcept { return start_value - increment; }
};

class Table;

class Sequence final : public SchemaObject {
  friend class Table;

 public:
  Sequence(ObjectId database_id, ObjectId schema_id, ObjectId id,
           std::string_view name, SequenceOptions opts,
           ObjectId owner_table_id);

  ~Sequence() = default;

  static std::shared_ptr<Sequence> ReadInternal(vpack::Slice slice,
                                                ReadContext ctx);

  void WriteInternal(vpack::Builder& b) const final;
  std::shared_ptr<Object> Clone() const final;

  const SequenceOptions& Options() const noexcept { return _options; }

  // Set for SERIAL implicit sequences (and the auto-PK Sequence). Wires the
  // sequence into TableDependency::owned_sequences for PG OWNED BY cascade.
  ObjectId GetOwnerTableId() const noexcept { return _owner_table_id; }

  // Hand out [base, base+count-1]; returns base. Merge persists before the
  // atomic increment, so a crash burns the range but never reuses it.
  uint64_t Reserve(uint64_t count);

  // Lock-free variant; caller guarantees Write is never called on this
  // Sequence. Used by the auto-PK path which is invisible to setval.
  uint64_t ReserveWriteUnsafe(uint64_t count);

  uint64_t Read() const;
  void Write(uint64_t value);

 private:
  std::atomic_uint64_t _cnt{0};
  mutable absl::Mutex _cnt_mtx;
  SequenceOptions _options;
  ObjectId _owner_table_id;

  std::atomic_uint64_t _cache_begin{0};
  std::atomic_uint64_t _cache_end{0};

  rocksdb::DB* _db;
  rocksdb::ColumnFamilyHandle* _cf;

  uint64_t LoadFromDb() const;
  uint64_t ReserveCached(uint64_t count);
  uint64_t AdvanceCounter(uint64_t count);
  uint64_t RefillCache(uint64_t count);
};

}  // namespace sdb::catalog
