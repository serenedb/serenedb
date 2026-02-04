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
#include <basics/async_utils.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/resource_manager.hpp"
#include "catalog/function.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/types.h"
#include "metrics/fwd.h"
#include "rest_server/serened.h"
#include "storage_engine/engine_feature.h"

namespace sdb {
struct IndexFactory;

namespace search {

class SearchThreadPools;
class ResourceMutex;

enum class ThreadGroup : uint8_t {
  Commit = 0,
  Consolidation,
};

void CleanupDatabase(ObjectId database_id);

SearchEngine& GetSearchEngine();

class SearchEngine final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Search"; }

  explicit SearchEngine(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void start() final;
  void stop() final;
  void unprepare() final;
  void beginShutdown() final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;

  std::tuple<size_t, size_t, size_t> stats(ThreadGroup id) const;
  std::pair<size_t, size_t> limits(ThreadGroup id) const;
  bool Queue(ThreadGroup id, absl::Duration delay,
             absl::AnyInvocable<void()>&& fn);
  void trackOutOfSyncLink() noexcept;
  void untrackOutOfSyncLink() noexcept;

  bool failQueriesOnOutOfSync() const noexcept;

#ifdef SDB_GTEST
  void setDefaultParallelism(uint32_t v) noexcept { _default_parallelism = v; }
#endif

  std::filesystem::path GetPersistedPath(ObjectId database_id) const;

 private:
  DatabasePathFeature& _dir_feature;

  std::shared_ptr<SearchThreadPools> _thread_pools;

  bool _fail_queries_on_out_of_sync{false};

  std::vector<std::string> _skip_recovery_items;

  metrics::Gauge<uint64_t>& _out_of_sync_links;

  uint32_t _consolidation_threads{0};
  uint32_t _commit_threads{0};
  uint32_t _search_execution_threads_limit{0};
  uint32_t _default_parallelism{1};
};

}  // namespace search

}  // namespace sdb
