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
#include "basics/exceptions.h"
#include "basics/serializer.h"
#include "catalog/store/store.h"

namespace sdb::catalog {

Sequence::Sequence(ObjectId schema_id, ObjectId id, SequenceOptions opts)
  : Object{schema_id, id, opts.name, ObjectType::Sequence},
    _options{std::move(opts)} {
  auto seed = _options.Seed();
  _cnt.store(seed, std::memory_order_release);
  _cache_begin.store(seed + 1, std::memory_order_release);
  _cache_end.store(seed, std::memory_order_release);
}

void Sequence::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, _options);
}

std::shared_ptr<Sequence> Sequence::Deserialize(duckdb::Deserializer& src,
                                                ReadContext ctx) {
  SequenceOptions opts;
  basics::ReadTuple(src, opts);
  auto seq = std::make_shared<Sequence>(ctx.schema_id, ctx.id, std::move(opts));
  auto persisted = seq->LoadFromDb();
  seq->_cnt.store(persisted, std::memory_order_release);
  seq->_cache_begin.store(persisted + 1, std::memory_order_release);
  seq->_cache_end.store(persisted, std::memory_order_release);
  return seq;
}

std::shared_ptr<Object> Sequence::Clone() const {
  return std::make_shared<Sequence>(GetParentId(), GetId(), _options);
}

uint64_t Sequence::LoadFromDb() const {
  auto& store = GetCatalogStore();
  uint64_t value = 0;
  if (store.TryGetBootSequenceValue(GetId(), value)) {
    return value;
  }
  if (auto r = store.GetSequenceValue(GetId(), value); !r.ok()) {
    SDB_THROW(std::move(r));
  }
  return value;
}

void Sequence::Persist(uint64_t value) {
  if (auto r = GetCatalogStore().PutSequenceValue(GetId(), value); !r.ok()) {
    SDB_THROW(std::move(r));
  }
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
  const auto cur = _cnt.load(std::memory_order_acquire);
  Persist(cur + count);
  return _cnt.fetch_add(count, std::memory_order_acq_rel) + 1;
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
  const auto cur = _cnt.load(std::memory_order_acquire);
  Persist(cur + refill);
  auto old_cnt = _cnt.fetch_add(refill, std::memory_order_acq_rel);
  uint64_t new_base = old_cnt + 1;
  _cache_end.store(old_cnt + refill, std::memory_order_release);
  _cache_begin.store(new_base + count, std::memory_order_release);
  return new_base;
}

uint64_t Sequence::Read() const { return _cnt.load(std::memory_order_acquire); }

void Sequence::Write(uint64_t value) {
  absl::MutexLock lock{&_cnt_mtx};
  Persist(value);
  _cnt.store(value, std::memory_order_release);
  _cache_end.store(value, std::memory_order_release);
  _cache_begin.store(value + 1, std::memory_order_release);
}

}  // namespace sdb::catalog
