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
  int64_t start_value = 1;
  int64_t increment = 1;
  int64_t min_value = 1;
  int64_t max_value = std::numeric_limits<int64_t>::max();
  int64_t cache_size = 1;
  bool cycle = false;

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

  // Set when this sequence is implicitly created by a SERIAL column or as
  // the auto-PK for a table without an explicit PK. Wires the sequence into
  // TableDependency::owned_sequences for PG OWNED BY cascade.
  ObjectId GetOwnerTableId() const noexcept { return _owner_table_id; }

  // Hand out [base, base+count-1]; returns base. Persists via Merge before
  // returning, so a crash burns the range but never reuses it.
  // Reader-locked against `Write` (setval) -- multiple Reserves run in
  // parallel; a concurrent Write blocks them for its duration.
  uint64_t Reserve(uint64_t count);

  // Lock-free variant. Caller must guarantee that `Write` is never called
  // for this Sequence (auto-PK sequences are the only such case).
  uint64_t ReserveWriteUnsafe(uint64_t count);

  uint64_t Read() const;
  void Write(uint64_t value);

 private:
  std::atomic<uint64_t> _live{0};
  // ReaderLock for Reserve, writer Lock for Write. ReserveWriteUnsafe
  // does not touch this -- the auto-PK path is not exposed to setval.
  mutable absl::Mutex _setval_mu;
  SequenceOptions _options;
  ObjectId _owner_table_id;

  uint64_t LoadFromDb() const;
};

}  // namespace sdb::catalog
