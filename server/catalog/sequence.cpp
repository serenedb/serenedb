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

#include "catalog/sequence.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/store/store.h"

namespace sdb::catalog {

void SequenceCounter::Seed(uint64_t value) {
  _cnt.store(value, std::memory_order_release);
  {
    absl::MutexLock lock{&_mtx};
    _durable = value;
    _pending = value;
  }
  _cache_begin.store(value + 1, std::memory_order_release);
  _cache_end.store(value, std::memory_order_release);
}

void SequenceCounter::ReloadDurable() { Seed(LoadFromDb()); }

uint64_t SequenceCounter::LoadFromDb() const {
  return GetCatalogStore().GetSequenceValue(_id);
}

void SequenceCounter::CoverDurable(uint64_t next_end) {
  if (next_end <= _durable) {
    return;
  }
  if (next_end > _pending) {
    const auto horizon = next_end + LogAhead();
    _pending = horizon;
    ++_appends_in_flight;
    _mtx.Unlock();
    GetCatalogStore().AdvanceSequenceValue(_id, horizon);
    _mtx.Lock();
    --_appends_in_flight;
    _durable = std::max(_durable, horizon);
    return;
  }
  // A concurrent bump's in-flight append covers this range; wait for its
  // sync instead of writing another record.
  struct Wait {
    const uint64_t* durable;
    uint64_t need;
  };
  Wait wait{&_durable, next_end};
  _mtx.Await(
    absl::Condition(+[](Wait* w) { return *w->durable >= w->need; }, &wait));
}

uint64_t SequenceCounter::ReserveCached(uint64_t count) {
  SDB_ASSERT(_cache > 1);
  auto base = _cache_begin.fetch_add(count, std::memory_order_acq_rel);
  const auto end = _cache_end.load(std::memory_order_acquire);
  if (base + count - 1 <= end) [[likely]] {
    return base;
  }
  return RefillCache(count);
}

uint64_t SequenceCounter::AdvanceCounter(uint64_t count) {
  absl::MutexLock lock{&_mtx};
  const auto base = _cnt.fetch_add(count, std::memory_order_acq_rel);
  CoverDurable(base + count);
  return base + 1;
}

uint64_t SequenceCounter::Reserve(uint64_t count) {
  SDB_ASSERT(count > 0);
  if (_cache > 1) {
    return ReserveCached(count);
  }
  return AdvanceCounter(count);
}

uint64_t SequenceCounter::RefillCache(uint64_t count) {
  absl::MutexLock lock{&_mtx};

  // Another thread may have refilled while we queued for the lock.
  auto end = _cache_end.load(std::memory_order_acquire);
  auto base = _cache_begin.fetch_add(count, std::memory_order_acq_rel);
  if (base + count - 1 <= end) {
    return base;
  }

  uint64_t refill = std::max(count, _cache);
  auto old_cnt = _cnt.fetch_add(refill, std::memory_order_acq_rel);
  // Persist with the lock held: the cache pointers published below must not
  // interleave with another refill, and refills already amortize the append.
  if (const auto new_end = old_cnt + refill; new_end > _durable) {
    const auto horizon = new_end + LogAhead();
    _pending = std::max(_pending, horizon);
    GetCatalogStore().AdvanceSequenceValue(_id, horizon);
    _durable = std::max(_durable, horizon);
  }
  uint64_t new_base = old_cnt + 1;
  _cache_end.store(old_cnt + refill, std::memory_order_release);
  _cache_begin.store(new_base + count, std::memory_order_release);
  return new_base;
}

uint64_t SequenceCounter::Read() const {
  return _cnt.load(std::memory_order_acquire);
}

void SequenceCounter::Write(uint64_t value) {
  absl::MutexLock lock{&_mtx};
  // Drain in-flight advances so the authoritative assign lands after every
  // record it raced (wal order matches the resident map).
  _mtx.Await(absl::Condition(
    +[](uint32_t* in_flight) { return *in_flight == 0; }, &_appends_in_flight));
  // setval is exact: the persisted value is what a restart must report, so
  // no log-ahead here and the horizon collapses back to it.
  GetCatalogStore().PutSequenceValue(_id, value);
  _durable = value;
  _pending = value;
  _cnt.store(value, std::memory_order_release);
  _cache_end.store(value, std::memory_order_release);
  _cache_begin.store(value + 1, std::memory_order_release);
}

CreateSequenceInfo::CreateSequenceInfo(ObjectId id, ObjectId schema_id,
                                       SequenceOptions opts)
  : _options{std::move(opts)} {
  SetId(id);
  SetSchemaId(schema_id);
  SetSequenceName(duckdb::Identifier{_options.name});
  // duckdb's own fields are the copy upstream machinery reads
  // (duckdb_sequences, the entry's ToSQL); ours stay unsigned because the
  // counter is.
  start_value = static_cast<int64_t>(_options.start_value);
  increment = static_cast<int64_t>(_options.increment);
  min_value = static_cast<int64_t>(_options.min_value);
  max_value = static_cast<int64_t>(_options.max_value);
  cycle = _options.cycle;
  usage_count = 0;
  if (!_options.comment.empty()) {
    comment = duckdb::Value(_options.comment);
  }
}

persistence::SequenceOptions CreateSequenceInfo::ToData() const {
  return _options;
}

void CreateSequenceInfo::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, _options);
}

void CreateSequenceInfo::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, _options);
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateSequenceInfo::Copy() const {
  auto copy =
    duckdb::make_uniq<CreateSequenceInfo>(GetId(), GetSchemaId(), _options);
  CopyProperties(*copy);
  return copy;
}

std::shared_ptr<CreateSequenceInfo> CreateSequenceInfo::Deserialize(
  duckdb::Deserializer& src, ObjectId id, ObjectId schema_id) {
  SequenceOptions opts;
  basics::ReadTuple(src, opts);
  return std::make_shared<CreateSequenceInfo>(id, schema_id, std::move(opts));
}

namespace {}  // namespace

std::shared_ptr<SequenceCounter> NewCounter(ObjectId id,
                                            const SequenceOptions& opts) {
  auto counter =
    std::make_shared<SequenceCounter>(id, opts.increment, opts.cache);
  counter->Seed(opts.Seed());
  return counter;
}

std::shared_ptr<SequenceCounter> ReloadedCounter(ObjectId id,
                                                 const SequenceOptions& opts) {
  auto counter =
    std::make_shared<SequenceCounter>(id, opts.increment, opts.cache);
  counter->ReloadDurable();
  return counter;
}

}  // namespace sdb::catalog
