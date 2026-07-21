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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>
#include <yaclib/async/future.hpp>

#include "absl/synchronization/mutex.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "rest_server/database_path_feature.h"
#include "search/search_db_wal.h"

namespace sdb {
namespace search {

class InvertedIndexStorage;
class SearchTable;

class SearchEngine;
SearchEngine& GetSearchEngine();

class SearchEngine final {
 public:
  inline static SearchEngine* gInstance = nullptr;

  // Process-wide cap on concurrent compactions, the only hard ceiling on
  // in-flight merges. Cores-derived (Lucene maxThreadCount): max(1, min(4,
  // cores/2)). background_threads is auto-floored above this with headroom for
  // refresh + cleanup + drop bursts (see background_scheduler.cpp).
  static int MaxConcurrentCompactions() noexcept;

  SearchEngine();
  ~SearchEngine();

  void start();
  void stop();

  std::filesystem::path GetPersistedPath(ObjectId database_id) const;

  // The database's self-contained search WAL, lazily created on first use. ONE
  // per database, shared by all of its search shards, so a transaction touching
  // several search tables commits atomically.
  SearchDbWal& GetDbWal(ObjectId database_id);

  // Launch the per-target refresh + compaction loops, registering their Futures
  // so stop() can join them. Templated on the storage type
  // (InvertedIndexStorage or SearchTable); instantiated for both in the .cpp.
  template<class Storage>
  void StartTasks(const std::shared_ptr<Storage>& storage);

  // Loops poll this so they bail out of long-running cycles promptly.
  bool IsStopping() const noexcept {
    return _stopping.load(std::memory_order_acquire);
  }

  // Signal the loops to stop without joining. Called before network.stop()
  // tears down the IoPool: once the pool is gone Delay() completes instantly,
  // so the loops must already see the stop flag to break instead of spinning.
  void RequestStop() noexcept {
    _stopping.store(true, std::memory_order_release);
  }

  // Reserve / release one of the MaxConcurrentCompactions() slots. A fan-out
  // sub-task holds a slot only while CompactUnsafe runs.
  bool TryAcquireCompaction() noexcept {
    const int cap = MaxConcurrentCompactions();
    auto cur = _running_compactions.load(std::memory_order_relaxed);
    while (cur < cap) {
      if (_running_compactions.compare_exchange_weak(
            cur, cur + 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }
  void ReleaseCompaction() noexcept {
    _running_compactions.fetch_sub(1, std::memory_order_release);
  }

  // Free global slots right now. The coordinator throttles merge size when this
  // is low (occupancy backpressure) so the pool always drains.
  int FreeCompactionSlots() const noexcept {
    const int cur = _running_compactions.load(std::memory_order_acquire);
    return std::max(0, MaxConcurrentCompactions() - cur);
  }

 private:
  DatabasePathFeature& _dir_feature;
  // Per-database central WALs (see GetDbWal). Guarded by _db_wals_mu.
  absl::Mutex _db_wals_mu;
  containers::FlatHashMap<ObjectId, std::unique_ptr<SearchDbWal>> _db_wals;
  std::atomic<bool> _stopping{false};
  std::atomic<int> _running_compactions{0};
  absl::Mutex _loops_mutex;
  std::vector<yaclib::Future<>> _loops ABSL_GUARDED_BY(_loops_mutex);
};

}  // namespace search
}  // namespace sdb
