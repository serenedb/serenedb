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

#pragma once

#include <absl/time/time.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "basics/async_utils.hpp"
#include "basics/containers/flat_hash_map.h"
#include "catalog/function.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/types.h"
#include "rest_server/database_path_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/search_db_wal.h"

namespace sdb {

struct IndexFactory;

namespace search {

class SearchThreadPools;
class ResourceMutex;

enum class ThreadGroup : uint8_t {
  Refresh = 0,
  Compaction,
};

class SearchEngine;
SearchEngine& GetSearchEngine();

class SearchEngine final {
 public:
  inline static SearchEngine* gInstance = nullptr;
  static SearchEngine& instance() noexcept { return *gInstance; }

  SearchEngine();
  ~SearchEngine();

  void start();
  void stop();

  std::tuple<size_t, size_t, size_t> stats(ThreadGroup id) const;
  std::pair<size_t, size_t> limits(ThreadGroup id) const;
  bool Queue(ThreadGroup id, absl::Duration delay,
             absl::AnyInvocable<void()>&& fn);

  std::filesystem::path GetPersistedPath(ObjectId database_id) const;

  // The database's self-contained search WAL, lazily created on first use. ONE
  // per database, shared by all of its search shards, so a transaction touching
  // several search tables commits atomically.
  SearchDbWal& GetDbWal(ObjectId database_id);

 private:
  DatabasePathFeature& _dir_feature;
  // Per-database central WALs (see GetDbWal). Guarded by _db_wals_mu.
  std::mutex _db_wals_mu;
  containers::FlatHashMap<ObjectId, std::unique_ptr<SearchDbWal>> _db_wals;
  std::shared_ptr<SearchThreadPools> _thread_pools;
  uint32_t _compaction_threads{0};
  uint32_t _refresh_threads{0};
};

}  // namespace search
}  // namespace sdb
