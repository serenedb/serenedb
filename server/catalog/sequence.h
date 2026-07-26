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
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "catalog/entry.h"
#include "catalog/persistence/sequence.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

using persistence::SequenceOptions;

// The live counter of one sequence, shared by every version of its definition.
// A comment, owner or ACL change writes a new definition, and a nextval running
// against the version it replaced must advance the same counter -- copying the
// value at rewrite time loses that advance and hands the number out twice.
//
// It is not part of the definition and is never serialized: what the catalog
// log holds is the durable horizon, as SetSequence / BumpSequence records.
class SequenceCounter {
 public:
  SequenceCounter(ObjectId id, uint64_t increment, uint64_t cache) noexcept
    : _id{id}, _increment{increment}, _cache{cache} {}

  ObjectId GetId() const noexcept { return _id; }

  void Seed(uint64_t value);
  // Re-reads the durable counter from the catalog store. Boot calls this after
  // replay: the definition is read mid-replay, so the SetSequence entries that
  // follow it in the log are not folded in yet.
  void ReloadDurable();

  // Hand out [base, base+count-1]; returns base. The counter persists
  // before the atomic increment, so a crash burns the range but never
  // reuses it.
  uint64_t Reserve(uint64_t count);

  uint64_t Read() const;
  void Write(uint64_t value);

 private:
  // How far past the handed-out range each persist runs (PG's SEQ_LOG_VALS):
  // values up to the durable horizon are covered by a synced append, so the
  // next kLogAhead fetches are append-free. A crash burns at most the gap.
  static constexpr uint64_t kLogAhead = 32;

  // The horizon in counter units. The counter lives in value space and must
  // stay on the increment lattice (see Write, whose cycle wrap depends on it),
  // so the log-ahead is that many *values*, not that many units -- PG logs
  // SEQ_LOG_VALS increments ahead for the same reason.
  uint64_t LogAhead() const noexcept { return kLogAhead * _increment; }

  uint64_t LoadFromDb() const;
  uint64_t ReserveCached(uint64_t count);
  uint64_t AdvanceCounter(uint64_t count);
  uint64_t RefillCache(uint64_t count);
  // Ensures values up to next_end are durably covered: fast when already
  // durable, waits on a covering in-flight append, or extends the horizon
  // (dropping the lock around the wal append). Requires the counter lock held.
  void CoverDurable(uint64_t next_end) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mtx);

  ObjectId _id;
  uint64_t _increment = 1;
  uint64_t _cache = 1;

  std::atomic_uint64_t _cnt{0};
  mutable absl::Mutex _mtx;
  // Two-stage horizon: extend publishes `_pending` and appends the max-merge
  // BumpSequence record OUTSIDE the lock (so concurrent bumps of one sequence
  // group-commit); the append's return promotes it to `_durable`. Values are
  // handed out only once durably covered.
  uint64_t _durable ABSL_GUARDED_BY(_mtx) = 0;
  uint64_t _pending ABSL_GUARDED_BY(_mtx) = 0;
  // Advance appends in flight; Write drains them so its authoritative
  // SetSequence lands after every advance it raced (wal order == map order).
  uint32_t _appends_in_flight ABSL_GUARDED_BY(_mtx) = 0;
  std::atomic_uint64_t _cache_begin{0};
  std::atomic_uint64_t _cache_end{0};
};

// One sequence, in the form a catalog entry is built from. duckdb's own
// CreateSequenceInfo has no CACHE, no owning table and no stable id, so this
// extends it rather than replacing it: what upstream understands stays where
// upstream looks for it, and the persisted option set adds what it lacks.
//
// The two overlap on START/INCREMENT/MIN/MAX/CYCLE, deliberately: duckdb's are
// signed and are what upstream machinery reads, while the counter is unsigned
// and reads Options(). The constructor is the only writer of either.
//
// Owner and ACL are not here: they travel beside the info and live on the
// entry, because duckdb's CreateInfo has nowhere to put them.
class CreateSequenceInfo final : public duckdb::CreateSequenceInfo {
 public:
  CreateSequenceInfo(ObjectId id, ObjectId schema_id, SequenceOptions opts);

  persistence::SequenceOptions ToData() const;
  void Serialize(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static std::shared_ptr<CreateSequenceInfo> Deserialize(
    duckdb::Deserializer& src, ObjectId id, ObjectId schema_id);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  ObjectId GetSchemaId() const noexcept { return ObjectId{parent_oid}; }
  void SetSchemaId(ObjectId id) noexcept { parent_oid = id.id(); }

  std::string_view GetName() const noexcept {
    return GetSequenceName().GetIdentifierName();
  }

  // The whole option set, and what the record persists: nextval wants the
  // bounds and the increment lattice as a group, and it wants them unsigned.
  const SequenceOptions& Options() const noexcept { return _options; }

  // Set for SERIAL implicit sequences (and the auto-PK sequence): the table the
  // sequence goes down with (PG OWNED BY). Its name still lives in the schema's
  // relation namespace, so this changes what drops it, not where it is found.
  ObjectId GetOwnerTableId() const noexcept {
    return ObjectId{_options.owner_table_id};
  }

  std::string_view Comment() const noexcept { return _options.comment; }

  ObjectId GetParentId() const noexcept { return GetSchemaId(); }

 private:
  SequenceOptions _options;
};

// The counter a sequence's entry carries: seeded from START for a create, and
// from the durable value the catalog log already holds for a replay. Bound onto
// the entry after it is placed; a rewrite inherits its predecessor's instead.
std::shared_ptr<SequenceCounter> NewCounter(ObjectId id,
                                            const SequenceOptions& opts);
std::shared_ptr<SequenceCounter> ReloadedCounter(ObjectId id,
                                                 const SequenceOptions& opts);

}  // namespace sdb::catalog
