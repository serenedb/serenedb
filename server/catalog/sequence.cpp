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

#include "basics/assert.h"
#include "basics/serializer.h"
#include "catalog/store/store.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {

Sequence::Sequence(ObjectId schema_id, ObjectId id, SequenceOptions opts)
  : Object{opts.perm, schema_id, id, opts.name, ObjectType::Sequence},
    _options{std::move(opts)} {
  auto seed = _options.Seed();
  _cnt.store(seed, std::memory_order_release);
  _durable_horizon = seed;
  _pending_horizon = seed;
  _cache_begin.store(seed + 1, std::memory_order_release);
  _cache_end.store(seed, std::memory_order_release);
}

void Sequence::Serialize(duckdb::Serializer& sink) const {
  auto opts = _options;
  opts.perm = GetPermissions();
  basics::WriteTuple(sink, opts);
}

std::shared_ptr<Sequence> Sequence::Deserialize(duckdb::Deserializer& src,
                                                ReadContext ctx) {
  SequenceOptions opts;
  basics::ReadTuple(src, opts);
  auto seq = std::make_shared<Sequence>(ctx.schema_id, ctx.id, std::move(opts));
  auto persisted = seq->LoadFromDb();
  seq->_cnt.store(persisted, std::memory_order_release);
  {
    absl::MutexLock lock{&seq->_cnt_mtx};
    seq->_durable_horizon = persisted;
    seq->_pending_horizon = persisted;
  }
  seq->_cache_begin.store(persisted + 1, std::memory_order_release);
  seq->_cache_end.store(persisted, std::memory_order_release);
  return seq;
}

std::shared_ptr<Object> Sequence::Clone() const {
  auto opts = _options;
  opts.perm = GetPermissions();
  return std::make_shared<Sequence>(GetParentId(), GetId(), std::move(opts));
}

uint64_t Sequence::LoadFromDb() const {
  auto& store = GetCatalogStore();
  uint64_t value = 0;
  if (store.TryGetBootSequenceValue(GetId(), value)) {
    return value;
  }
  store.GetSequenceValue(GetId(), value);
  return value;
}

void Sequence::CoverDurable(uint64_t next_end) {
  if (next_end <= _durable_horizon) {
    return;
  }
  if (next_end > _pending_horizon) {
    const auto horizon = next_end + kLogAhead;
    _pending_horizon = horizon;
    ++_appends_in_flight;
    _cnt_mtx.Unlock();
    GetCatalogStore().AdvanceSequenceValue(GetId(), horizon);
    _cnt_mtx.Lock();
    --_appends_in_flight;
    _durable_horizon = std::max(_durable_horizon, horizon);
    return;
  }
  // A concurrent bump's in-flight append covers this range; wait for its
  // sync instead of writing another record.
  struct Wait {
    const uint64_t* durable;
    uint64_t need;
  };
  Wait wait{&_durable_horizon, next_end};
  _cnt_mtx.Await(absl::Condition(
    +[](Wait* w) { return *w->durable >= w->need; }, &wait));
}

uint64_t Sequence::ReserveCached(uint64_t count) {
  SDB_ASSERT(_options.cache > 1);
  auto base = _cache_begin.fetch_add(count, std::memory_order_acq_rel);
  const auto end = _cache_end.load(std::memory_order_acquire);
  if (base + count - 1 <= end) [[likely]] {
    return base;
  }
  return RefillCache(count);
}

uint64_t Sequence::AdvanceCounter(uint64_t count) {
  absl::MutexLock lock{&_cnt_mtx};
  const auto base = _cnt.fetch_add(count, std::memory_order_acq_rel);
  CoverDurable(base + count);
  return base + 1;
}

uint64_t Sequence::ReserveWriteUnsafe(uint64_t count) {
  SDB_ASSERT(count > 0);
  if (_options.cache > 1) {
    return ReserveCached(count);
  }
  return AdvanceCounter(count);
}

uint64_t Sequence::Reserve(uint64_t count) {
  SDB_ASSERT(count > 0);
  if (_options.cache > 1) {
    return ReserveCached(count);
  }
  return AdvanceCounter(count);
}

uint64_t Sequence::RefillCache(uint64_t count) {
  absl::MutexLock lock{&_cnt_mtx};

  // Another thread may have refilled while we queued for the lock.
  auto end = _cache_end.load(std::memory_order_acquire);
  auto base = _cache_begin.fetch_add(count, std::memory_order_acq_rel);
  if (base + count - 1 <= end) {
    return base;
  }

  uint64_t refill = std::max(count, _options.cache);
  auto old_cnt = _cnt.fetch_add(refill, std::memory_order_acq_rel);
  // Persist with the lock held: the cache pointers published below must not
  // interleave with another refill, and refills already amortize the append.
  if (const auto new_end = old_cnt + refill; new_end > _durable_horizon) {
    const auto horizon = new_end + kLogAhead;
    _pending_horizon = std::max(_pending_horizon, horizon);
    GetCatalogStore().AdvanceSequenceValue(GetId(), horizon);
    _durable_horizon = std::max(_durable_horizon, horizon);
  }
  uint64_t new_base = old_cnt + 1;
  _cache_end.store(old_cnt + refill, std::memory_order_release);
  _cache_begin.store(new_base + count, std::memory_order_release);
  return new_base;
}

uint64_t Sequence::Read() const { return _cnt.load(std::memory_order_acquire); }

void Sequence::Write(uint64_t value) {
  absl::MutexLock lock{&_cnt_mtx};
  // Drain in-flight advances so the authoritative assign lands after every
  // record it raced (wal order matches the resident map).
  _cnt_mtx.Await(absl::Condition(
    +[](uint32_t* in_flight) { return *in_flight == 0; },
    &_appends_in_flight));
  // setval is exact: the persisted value is what a restart must report, so
  // no log-ahead here and the horizon collapses back to it.
  GetCatalogStore().PutSequenceValue(GetId(), value);
  _durable_horizon = value;
  _pending_horizon = value;
  _cnt.store(value, std::memory_order_release);
  _cache_end.store(value, std::memory_order_release);
  _cache_begin.store(value + 1, std::memory_order_release);
}

}  // namespace sdb::catalog
