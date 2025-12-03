////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "query_cache.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <magic_enum/magic_enum.hpp>

#include "app/name_validator.h"
#include "basics/exceptions.h"
#include "basics/read_locker.h"
#include "basics/strings.h"
#include "basics/system-functions.h"
#include "basics/write_locker.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/types.h"
#include "utils/exec_context.h"
#include "vpack/vpack_helper.h"

namespace sdb::aql {
namespace {

// singleton instance of the query cache
static aql::QueryCache gInstance;

// whether or not the cache is enabled
static std::atomic<aql::QueryCacheMode> gMode{
  aql::QueryCacheMode::CacheOnDemand};

// maximum number of results in each per-database cache
static std::atomic<size_t> gMaxResultsCount{128};

// maximum cumulated size of results in each per-database cache
static std::atomic<size_t> gMaxResultsSize{256 * 1024 * 1024};

// maximum size of an individual cache entry
static std::atomic<size_t> gMaxEntrySize{16 * 1024 * 1024};

// whether or not the query cache will return bind vars in its list of
// cached results
// will be set once on startup. cannot be changed at runtime
static bool gShowBindVars = true;

}  // namespace

QueryCacheResultEntry::QueryCacheResultEntry(
  uint64_t hash, const QueryString& query_string,
  const std::shared_ptr<vpack::Builder>& query_result,
  const std::shared_ptr<vpack::Builder>& bind_vars,
  containers::FlatHashMap<ObjectId, std::string>&& data_sources)
  : hash(hash),
    query_string(query_string.data(), query_string.size()),
    query_result(query_result),
    bind_vars(bind_vars),
    data_sources(std::move(data_sources)),
    size(query_string.size()) {
  // add result size
  try {
    if (query_result) {
      size += query_result->size();
      rows = query_result->slice().length();
    }
    if (bind_vars) {
      size += bind_vars->size();
    }
  } catch (...) {
  }
}

double QueryCacheResultEntry::executionTime() const {
  if (!stats) {
    return -1.0;
  }

  try {
    vpack::Slice s = stats->slice();
    if (!s.isObject()) {
      return -1.0;
    }
    s = s.get("stats");
    if (!s.isObject()) {
      return -1.0;
    }
    s = s.get("executionTime");
    if (!s.isNumber()) {
      return -1.0;
    }
    return s.getNumber<double>();
  } catch (...) {
  }

  return -1.0;
}

void QueryCacheResultEntry::toVPack(vpack::Builder& builder) const {
  builder.openObject();

  builder.add("hash", std::to_string(hash));
  builder.add("query", query_string);

  if (gShowBindVars) {
    if (bind_vars && !bind_vars->slice().isNone()) {
      builder.add("bindVars", bind_vars->slice());
    } else {
      builder.add("bindVars", vpack::Slice::emptyObjectSlice());
    }
  }

  builder.add("size", size);
  builder.add("results", rows);
  builder.add("hits", hits.load());

  double execution_time = this->executionTime();

  if (execution_time < 0.0) {
    builder.add("runTime", vpack::Value(vpack::ValueType::Null));
  } else {
    builder.add("runTime", execution_time);
  }

  auto time_string = StringTimeStamp(stamp, false);

  builder.add("started", time_string);

  builder.add("dataSources", vpack::Value(vpack::ValueType::Array));

  // emit all datasource names
  for (const auto& it : data_sources) {
    builder.add(it.second);
  }

  builder.close();

  builder.close();
}

QueryCacheDatabaseEntry::QueryCacheDatabaseEntry() {
  entries_by_hash.reserve(128);
  entries_by_data_source_guid.reserve(16);
}

QueryCacheDatabaseEntry::~QueryCacheDatabaseEntry() {
  entries_by_hash.clear();
  entries_by_data_source_guid.clear();
}

void QueryCacheDatabaseEntry::queriesToVPack(vpack::Builder& builder) const {
  for (const auto& it : entries_by_hash) {
    const QueryCacheResultEntry* entry = it.second.get();
    SDB_ASSERT(entry != nullptr);

    entry->toVPack(builder);
  }
}

std::shared_ptr<QueryCacheResultEntry> QueryCacheDatabaseEntry::lookup(
  uint64_t hash, const QueryString& query_string,
  const std::shared_ptr<vpack::Builder>& bind_vars) const {
  auto it = entries_by_hash.find(hash);

  if (it == entries_by_hash.end()) {
    // not found in cache
    return nullptr;
  }

  // found some result in cache
  auto entry = (*it).second.get();

  if (query_string.size() != entry->query_string.size() ||
      memcmp(query_string.data(), entry->query_string.data(),
             query_string.size()) != 0) {
    // found something, but obviously the result of a different query with the
    // same hash
    return nullptr;
  }

  // compare bind variables
  vpack::Slice entry_bind_vars = vpack::Slice::emptyObjectSlice();
  if (entry->bind_vars != nullptr) {
    entry_bind_vars = entry->bind_vars->slice();
  }
  vpack::ValueLength entry_length = entry_bind_vars.length();

  vpack::Slice lookup_bind_vars = vpack::Slice::emptyObjectSlice();
  if (bind_vars != nullptr) {
    lookup_bind_vars = bind_vars->slice();
  }
  vpack::ValueLength lookup_length = lookup_bind_vars.length();

  if (entry_length > 0 || lookup_length > 0) {
    if (entry_length != lookup_length) {
      // different number of bind variables
      return nullptr;
    }

    // TODO(mbkkt) compare == 0?
    if (!basics::VPackHelper::equals(entry_bind_vars, lookup_bind_vars)) {
      // different bind variables
      return nullptr;
    }
  }

  // all equal -> hit!
  entry->increaseHits();

  // found an entry
  return (*it).second;
}

/// store a query result in the database-specific cache
void QueryCacheDatabaseEntry::store(
  std::shared_ptr<QueryCacheResultEntry>&& entry,
  size_t allowed_max_results_count, size_t allowed_max_results_size) {
  auto* e = entry.get();

  // make room in the cache so the new entry will definitely fit
  enforceMaxResults(allowed_max_results_count - 1,
                    allowed_max_results_size - e->size);

  // insert entry into the cache
  uint64_t hash = e->hash;
  auto result = entries_by_hash.emplace(hash, entry);
  if (!result.second) {
    // remove previous entry
    auto& previous = result.first->second;
    removeDatasources(previous.get());
    unlink(previous.get());

    // update with the new entry
    result.first->second = std::move(entry);
  }

  try {
    for (const auto& it : e->data_sources) {
      auto& ref = entries_by_data_source_guid[it.first];
      ref.first = false;
      ref.second.emplace(hash);
    }
  } catch (...) {
    // rollback

    // remove from data sources
    for (const auto& it : e->data_sources) {
      auto itr2 = entries_by_data_source_guid.find(it.first);

      if (itr2 != entries_by_data_source_guid.end()) {
        itr2->second.second.erase(hash);
      }
    }

    // finally remove entry itself from hash table
    entries_by_hash.erase(hash);
    throw;
  }

  link(e);

  SDB_ASSERT(num_results <= allowed_max_results_count);
  SDB_ASSERT(size_results <= allowed_max_results_size);
  SDB_ASSERT(head != nullptr);
  SDB_ASSERT(tail != nullptr);
  SDB_ASSERT(tail == e);
  SDB_ASSERT(e->next == nullptr);
}

void QueryCacheDatabaseEntry::invalidate(ObjectId object_id) {
  auto itr = entries_by_data_source_guid.find(object_id);

  if (itr == entries_by_data_source_guid.end()) {
    return;
  }

  for (auto& it2 : itr->second.second) {
    auto it3 = entries_by_hash.find(it2);

    if (it3 != entries_by_hash.end()) {
      // remove entry from the linked list
      auto entry = (*it3).second;
      unlink(entry.get());

      // erase it from hash table
      entries_by_hash.erase(it3);
    }
  }

  entries_by_data_source_guid.erase(itr);
}

void QueryCacheDatabaseEntry::enforceMaxResults(size_t num_results,
                                                size_t size_results) {
  while (this->num_results > num_results || this->size_results > size_results) {
    // too many elements. now wipe the first element from the list
    // copy old _head value as unlink() will change it...
    auto head = this->head;
    removeDatasources(head);
    unlink(head);
    auto it = entries_by_hash.find(head->hash);
    SDB_ASSERT(it != entries_by_hash.end());
    entries_by_hash.erase(it);
  }
}

void QueryCacheDatabaseEntry::enforceMaxEntrySize(size_t value) {
  for (auto it = entries_by_hash.begin(); it != entries_by_hash.end();) {
    const auto& entry = (*it).second.get();

    if (entry->size > value) {
      removeDatasources(entry);
      unlink(entry);
      entries_by_hash.erase(it++);
    } else {
      // keep the entry
      ++it;
    }
  }
}

void QueryCacheDatabaseEntry::excludeSystem() {
  for (auto itr = entries_by_data_source_guid.begin();
       itr != entries_by_data_source_guid.end();) {
    if (!itr->second.first) {
      // not a system collection
      ++itr;
    } else {
      for (const auto& hash : itr->second.second) {
        auto it2 = entries_by_hash.find(hash);

        if (it2 != entries_by_hash.end()) {
          auto* entry = (*it2).second.get();
          unlink(entry);
          entries_by_hash.erase(it2);
        }
      }

      entries_by_data_source_guid.erase(itr++);
    }
  }
}

void QueryCacheDatabaseEntry::removeDatasources(
  const QueryCacheResultEntry* e) {
  for (const auto& [id, _] : e->data_sources) {
    auto itr = entries_by_data_source_guid.find(id);

    if (itr != entries_by_data_source_guid.end()) {
      itr->second.second.erase(e->hash);
    }
  }
}

void QueryCacheDatabaseEntry::unlink(QueryCacheResultEntry* e) {
  if (e->prev != nullptr) {
    e->prev->next = e->next;
  }
  if (e->next != nullptr) {
    e->next->prev = e->prev;
  }

  if (head == e) {
    head = e->next;
  }
  if (tail == e) {
    tail = e->prev;
  }

  e->prev = nullptr;
  e->next = nullptr;

  SDB_ASSERT(num_results > 0);
  --num_results;

  SDB_ASSERT(size_results >= e->size);
  size_results -= e->size;
}

void QueryCacheDatabaseEntry::link(QueryCacheResultEntry* e) {
  ++num_results;
  size_results += e->size;

  if (head == nullptr) {
    // list is empty
    SDB_ASSERT(tail == nullptr);
    // set list head and tail to the element
    head = e;
    tail = e;
    return;
  }

  if (tail != nullptr) {
    // adjust list tail
    tail->next = e;
  }

  e->prev = tail;
  tail = e;
}

QueryCache::~QueryCache() {
  for (unsigned int i = 0; i < kNumberOfParts; ++i) {
    invalidate(i);
  }
}

void QueryCache::toVPack(vpack::Builder& builder) const {
  std::lock_guard mutex_locker{_properties_lock};

  builder.openObject();
  builder.add("mode", magic_enum::enum_name(mode()));
  builder.add("maxResults", gMaxResultsCount.load());
  builder.add("maxResultsSize", gMaxResultsSize.load());
  builder.add("maxEntrySize", gMaxEntrySize.load());
  builder.close();
}

QueryCacheProperties QueryCache::properties() const {
  std::lock_guard mutex_locker{_properties_lock};

  return QueryCacheProperties{
    gMode.load(),         gMaxResultsCount.load(), gMaxResultsSize.load(),
    gMaxEntrySize.load(), gShowBindVars,
  };
}

void QueryCache::properties(const QueryCacheProperties& properties) {
  std::lock_guard mutex_locker{_properties_lock};

  setMode(properties.mode);
  setMaxResults(properties.max_results_count, properties.max_results_size);
  setMaxEntrySize(properties.max_entry_size);
  gShowBindVars = properties.show_bind_vars;
}

void QueryCache::properties(vpack::Slice properties) {
  if (!properties.isObject()) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "expecting Object for query cache properties");
  }

  std::lock_guard mutex_locker{_properties_lock};

  auto mode = gMode.load();
  auto max_results_count = gMaxResultsCount.load();
  auto max_results_size = gMaxResultsSize.load();
  auto max_entry_size = gMaxEntrySize.load();

  vpack::Slice v = properties.get("mode");
  if (v.isString()) {
    auto value = magic_enum::enum_cast<QueryCacheMode>(v.stringView());

    if (!value.has_value()) {
      SDB_THROW(ERROR_BAD_PARAMETER, "invalid query cache mode");
    }

    mode = *value;
  }

  v = properties.get("maxResults");
  if (v.isNumber()) {
    int64_t value = v.getNumber<int64_t>();
    if (value <= 0) {
      SDB_THROW(ERROR_BAD_PARAMETER, "invalid value for maxResults");
    }
    max_results_count = v.getNumber<size_t>();
  }

  v = properties.get("maxResultsSize");
  if (v.isNumber()) {
    int64_t value = v.getNumber<int64_t>();
    if (value <= 0) {
      SDB_THROW(ERROR_BAD_PARAMETER, "invalid value for maxResultsSize");
    }
    max_results_size = v.getNumber<size_t>();
  }

  v = properties.get("maxEntrySize");
  if (v.isNumber()) {
    int64_t value = v.getNumber<int64_t>();
    if (value <= 0) {
      SDB_THROW(ERROR_BAD_PARAMETER, "invalid value for maxEntrySize");
    }
    max_entry_size = v.getNumber<size_t>();
  }

  setMode(mode);
  setMaxResults(max_results_count, max_results_size);
  setMaxEntrySize(max_entry_size);
}

bool QueryCache::mayBeActive() const {
  return (mode() != QueryCacheMode::CacheAlwaysOff);
}

QueryCacheMode QueryCache::mode() const {
  return gMode.load(std::memory_order_relaxed);
}

std::shared_ptr<QueryCacheResultEntry> QueryCache::lookup(
  ObjectId database_id, uint64_t hash, const QueryString& query_string,
  const std::shared_ptr<vpack::Builder>& bind_vars) const {
  const auto part = getPart(database_id);
  absl::ReaderMutexLock read_locker{&_entries_lock[part]};

  auto& entry = _entries[part];
  auto it = entry.find(database_id);

  if (it == entry.end()) {
    // no entry found for the requested database
    return nullptr;
  }

  return (*it).second->lookup(hash, query_string, bind_vars);
}

void QueryCache::store(ObjectId database_id,
                       std::shared_ptr<QueryCacheResultEntry> entry) {
  SDB_ASSERT(entry != nullptr);
  auto* e = entry.get();

  if (e->size > gMaxEntrySize.load()) {
    // entry is too big
    return;
  }

  const size_t allowed_max_results_count = gMaxResultsCount.load();
  const size_t allowed_max_results_size = gMaxResultsSize.load();

  if (allowed_max_results_count == 0) {
    // cache has only space for 0 entries...
    return;
  }

  if (e->size > allowed_max_results_size) {
    // entry is too big
    return;
  }

  // set insertion time (wall-clock time, only used for displaying it later)
  e->stamp = utilities::GetMicrotime();

  // get the right part of the cache to store the result in
  const auto part = getPart(database_id);
  absl::WriterMutexLock write_locker{&_entries_lock[part]};
  auto [it, _] = _entries[part].try_emplace(database_id);

  auto& value = it->second;
  if (!value) {
    value = std::make_unique<QueryCacheDatabaseEntry>();
  }
  // store cache entry
  value->store(std::move(entry), allowed_max_results_count,
               allowed_max_results_size);
}

void QueryCache::invalidate(ObjectId database_id,
                            std::span<const ObjectId> data_source_ids) {
  const auto part = getPart(database_id);
  absl::WriterMutexLock write_locker{&_entries_lock[part]};

  auto& entry = _entries[part];
  auto it = entry.find(database_id);

  if (it == entry.end()) {
    return;
  }

  it->second->invalidate(data_source_ids);
}

void QueryCache::invalidate(ObjectId database_id, ObjectId object_id) {
  const auto part = getPart(database_id);
  absl::WriterMutexLock write_locker{&_entries_lock[part]};

  auto& entry = _entries[part];
  auto it = entry.find(database_id);

  if (it == entry.end()) {
    return;
  }

  // invalidate while holding the lock
  it->second->invalidate(object_id);
}

void QueryCache::invalidate(ObjectId database_id) {
  std::unique_ptr<QueryCacheDatabaseEntry> database_query_cache;

  {
    const auto part = getPart(database_id);
    absl::WriterMutexLock write_locker{&_entries_lock[part]};

    auto& entry = _entries[part];
    auto it = entry.find(database_id);

    if (it == entry.end()) {
      return;
    }

    database_query_cache = std::move((*it).second);
    entry.erase(it);
  }

  // delete without holding the lock
  SDB_ASSERT(database_query_cache != nullptr);
}

void QueryCache::invalidate() {
  for (unsigned int i = 0; i < kNumberOfParts; ++i) {
    absl::WriterMutexLock write_locker{&_entries_lock[i]};

    // must invalidate all entries now because disabling the cache will turn off
    // cache invalidation when modifying data. turning on the cache later would
    // then
    // lead to invalid results being returned. this can all be prevented by
    // fully
    // clearing the cache
    invalidate(i);
  }
}

void QueryCache::queriesToVPack(ObjectId database_id,
                                vpack::Builder& builder) const {
  builder.openArray();

  {
    const auto part = getPart(database_id);
    absl::ReaderMutexLock read_locker{&_entries_lock[part]};

    auto& entry = _entries[part];
    auto it = entry.find(database_id);

    if (it != entry.end()) {
      (*it).second->queriesToVPack(builder);
    }
  }

  builder.close();
}

QueryCache* QueryCache::instance() { return &gInstance; }

void QueryCache::enforceMaxResults(size_t num_results, size_t size_results) {
  for (unsigned int i = 0; i < kNumberOfParts; ++i) {
    absl::WriterMutexLock write_locker{&_entries_lock[i]};

    for (auto& it : _entries[i]) {
      it.second->enforceMaxResults(num_results, size_results);
    }
  }
}

void QueryCache::enforceMaxEntrySize(size_t value) {
  for (unsigned int i = 0; i < kNumberOfParts; ++i) {
    absl::WriterMutexLock write_locker{&_entries_lock[i]};

    for (auto& it : _entries[i]) {
      it.second->enforceMaxEntrySize(value);
    }
  }
}

void QueryCache::excludeSystem() {
  for (unsigned int i = 0; i < kNumberOfParts; ++i) {
    absl::WriterMutexLock write_locker{&_entries_lock[i]};

    for (auto& it : _entries[i]) {
      it.second->excludeSystem();
    }
  }
}

unsigned int QueryCache::getPart(ObjectId v) const {
  return static_cast<int>(wyhash64(v.id(), 0xf12345678abcdef) % kNumberOfParts);
}

void QueryCache::invalidate(unsigned int part) { _entries[part].clear(); }

void QueryCache::setMaxResults(size_t num_results, size_t size_results) {
  if (num_results == 0 || size_results == 0) {
    return;
  }

  size_t mr = gMaxResultsCount.load();
  size_t ms = gMaxResultsSize.load();

  if (num_results < mr || size_results < ms) {
    enforceMaxResults(num_results, size_results);
  }

  gMaxResultsCount.store(num_results);
  gMaxResultsSize.store(size_results);
}

void QueryCache::setMaxEntrySize(size_t value) {
  if (value == 0) {
    return;
  }

  size_t v = gMaxEntrySize.load();

  if (value < v) {
    enforceMaxEntrySize(value);
  }

  gMaxEntrySize.store(value);
}

void QueryCache::setMode(QueryCacheMode value) {
  if (value == mode()) {
    // actually no mode change
    return;
  }

  invalidate();

  gMode.store(value, std::memory_order_release);
}

}  // namespace sdb::aql
