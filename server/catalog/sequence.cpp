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

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <string>
#include <utility>

#include "basics/assert.h"
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

namespace {

// CACHE and the owning table are the two things duckdb's
// duckdb::CreateSequenceInfo has no field for, so they ride the info's tags --
// definition, like a table's engine, and carried by CreateInfo's own
// serialization.
constexpr std::string_view kCacheTag = "sdb_seq_cache";
constexpr std::string_view kOwnerTableTag = "sdb_seq_owner";

uint64_t ReadTag(const duckdb::InsertionOrderPreservingMap<std::string>& tags,
                 std::string_view key, uint64_t fallback) noexcept {
  const auto it = tags.find(std::string{key});
  if (it == tags.end()) {
    return fallback;
  }
  uint64_t value = 0;
  return absl::SimpleAtoi(it->second, &value) ? value : fallback;
}

}  // namespace

std::shared_ptr<duckdb::CreateSequenceInfo> MakeSequenceInfo(
  ObjectId id, ObjectId schema_id, const SequenceOptions& opts) {
  auto info = std::make_shared<duckdb::CreateSequenceInfo>();
  SetIdentity(*info, id, schema_id);
  info->SetSequenceName(duckdb::Identifier{opts.name});
  info->start_value = static_cast<int64_t>(opts.start_value);
  info->increment = static_cast<int64_t>(opts.increment);
  info->min_value = static_cast<int64_t>(opts.min_value);
  info->max_value = static_cast<int64_t>(opts.max_value);
  info->cycle = opts.cycle;
  info->usage_count = 0;
  if (!opts.comment.empty()) {
    info->comment = duckdb::Value(opts.comment);
  }
  if (opts.cache != 1) {
    info->tags.insert(std::string{kCacheTag}, absl::StrCat(opts.cache));
  }
  if (opts.owner_table_id != 0) {
    info->tags.insert(std::string{kOwnerTableTag},
                      absl::StrCat(opts.owner_table_id));
  }
  return info;
}

SequenceOptions SequenceOptionsOf(const duckdb::CreateSequenceInfo& info) {
  return SequenceOptions{
    .name = std::string{SequenceNameOf(info)},
    .start_value = static_cast<uint64_t>(info.start_value),
    .increment = static_cast<uint64_t>(info.increment),
    .min_value = static_cast<uint64_t>(info.min_value),
    .max_value = static_cast<uint64_t>(info.max_value),
    .cache = ReadTag(info.tags, kCacheTag, 1),
    .owner_table_id = ReadTag(info.tags, kOwnerTableTag, 0),
    .cycle = info.cycle,
    .comment = info.comment.IsNull()
                 ? std::string{}
                 : std::string{duckdb::StringValue::Get(info.comment)},
  };
}

ObjectId SequenceOwnerTableOf(const duckdb::CreateSequenceInfo& info) noexcept {
  return ObjectId{ReadTag(info.tags, kOwnerTableTag, 0)};
}

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
