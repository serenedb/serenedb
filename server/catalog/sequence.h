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
#include "catalog/persistence/sequence.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

using persistence::SequenceOptions;

class Table;

class Sequence final : public Object {
  friend class Table;

 public:
  // opts.name and opts.owner_table_id provide the Object name and the owner
  // table id; no separate parameters needed.
  Sequence(ObjectId schema_id, ObjectId id, SequenceOptions opts);

  ~Sequence() = default;

  static std::shared_ptr<Sequence> Deserialize(duckdb::Deserializer& src,
                                               ReadContext ctx);

  void Serialize(duckdb::Serializer& s) const final;
  std::shared_ptr<Object> Clone() const final;

  const SequenceOptions& Options() const noexcept { return _options; }

  // Set for SERIAL implicit sequences (and the auto-PK Sequence). Wires the
  // sequence into TableDependency::owned_sequences for PG OWNED BY cascade.
  ObjectId GetOwnerTableId() const noexcept {
    return ObjectId{_options.owner_table_id};
  }

  uint64_t Reserve(uint64_t count);

  uint64_t ReserveWriteUnsafe(uint64_t count);

  uint64_t Read() const;
  void Write(uint64_t value);

 private:
  std::atomic_uint64_t _cnt{0};
  mutable absl::Mutex _cnt_mtx;
  // Owns the wire-format state (name, options, owner_table_id) -- see
  // SequenceOptions comment for the reflection-based persistence contract.
  SequenceOptions _options;

  std::atomic_uint64_t _cache_begin{0};
  std::atomic_uint64_t _cache_end{0};

  uint64_t LoadFromDb() const;
  uint64_t ReserveCached(uint64_t count);
  uint64_t AdvanceCounter(uint64_t count);
  uint64_t RefillCache(uint64_t count);
  void Persist(uint64_t value);
};

}  // namespace sdb::catalog
