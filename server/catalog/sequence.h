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
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "catalog/entry.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

// The option set nextval works in: the bounds and the increment as one lattice,
// unsigned because the counter is. It is a view of the definition, not a second
// copy of it -- START/INCREMENT/MIN/MAX/CYCLE are duckdb's own fields, CACHE
// and the owning table ride the info's tags, and the name and comment are the
// CreateInfo's.
struct SequenceOptions {
  std::string name;
  uint64_t start_value = 1;
  uint64_t increment = 1;
  uint64_t min_value = 1;
  uint64_t max_value = std::numeric_limits<int64_t>::max();
  uint64_t cache = 1;
  uint64_t owner_table_id = 0;
  bool cycle = false;
  std::string comment;

  uint64_t Seed() const noexcept { return start_value - increment; }
};

// The live counter of one sequence, shared by every version of its definition:
// copying the value at rewrite time would lose a concurrent advance and hand a
// number out twice. Never serialized -- the catalog log holds the durable
// horizon, as an authoritative value or a max-merge bump.
class SequenceCounter {
 public:
  SequenceCounter(ObjectId id, uint64_t increment, uint64_t cache) noexcept
    : _id{id}, _increment{increment}, _cache{cache} {}

  ObjectId GetId() const noexcept { return _id; }

  void Seed(uint64_t value);
  // Re-reads the durable counter from the catalog store. Boot calls this after
  // replay: the definition is read mid-replay, so the authoritative entries
  // that follow it in the log are not folded in yet.
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
  // max-merge bump OUTSIDE the lock (so concurrent bumps of one sequence
  // group-commit); the append's return promotes it to `_durable`. Values are
  // handed out only once durably covered.
  uint64_t _durable ABSL_GUARDED_BY(_mtx) = 0;
  uint64_t _pending ABSL_GUARDED_BY(_mtx) = 0;
  // Advance appends in flight; Write drains them so its authoritative
  // the authoritative value lands after every advance it raced (wal order ==
  // map order).
  uint32_t _appends_in_flight ABSL_GUARDED_BY(_mtx) = 0;
  std::atomic_uint64_t _cache_begin{0};
  std::atomic_uint64_t _cache_end{0};
};

// A sequence is duckdb's own duckdb::CreateSequenceInfo; the two things
// upstream has no room for -- CACHE and the table a SERIAL sequence goes down
// with -- ride its tags. Owner and ACL live on the entry.
duckdb::unique_ptr<duckdb::CreateSequenceInfo> MakeSequenceInfo(
  ObjectId id, ObjectId schema_id, const SequenceOptions& opts);

// The option set as the definition states it.
SequenceOptions SequenceOptionsOf(const duckdb::CreateSequenceInfo& info);

// The counter a sequence's entry carries: seeded from START for a create, and
// from the durable value the catalog log already holds for a replay. Bound onto
// the entry after it is placed; a rewrite inherits its predecessor's instead.
std::shared_ptr<SequenceCounter> NewCounter(ObjectId id,
                                            const SequenceOptions& opts);
std::shared_ptr<SequenceCounter> ReloadedCounter(ObjectId id,
                                                 const SequenceOptions& opts);

}  // namespace sdb::catalog
