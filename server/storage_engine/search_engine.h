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
#include <yaclib/algo/wait_group.hpp>

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
  static int MaxConcurrentMerges() noexcept;

  // Total workers, summed over every ANN graph build in flight, that may run
  // graph inserts at once -- each build's own calling thread included.
  static int MaxAnnBuildWorkers() noexcept;

  // Ceiling on what any ONE build may take out of that total.
  static int MaxAnnWorkersPerBuild() noexcept;

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
    const int cap = MaxConcurrentMerges();
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
    // ANN build workers are deliberately not counted: they draw on the ANN
    // build budget, not on merge slots, so charging them here would understate
    // free merge capacity and throttle merge size for no reason.
    const int cur = _running_compactions.load(std::memory_order_acquire);
    return std::max(0, MaxConcurrentCompactions() - cur);
  }

  // Workers for the parallel phase of ONE ANN graph build, drawn from a budget
  // of their own rather than the merge slots. Workers share their build's
  // output allocation, so they cost CPU but not memory -- which is why this is
  // budgeted apart from the merge gate, whose job is to bound memory.
  //
  // Sizing it out of MaxConcurrentCompactions() (= --background_threads - 1)
  // was the single largest cause of the HNSW build-time gap: a foreground
  // CREATE INDEX got cores/4 - 1 workers, where qdrant gives its builder
  // min(8, cores).
  //
  // `want` is the total the build could use, ITS OWN THREAD INCLUDED, and the
  // return is what it may run with. Never blocks, and never returns less than
  // 1: the thread that entered the build is already running, so refusing it
  // would buy nothing. Counting that thread is the point -- CREATE INDEX is a
  // ParallelSink and flushes one segment per sink thread, so N concurrent
  // builds must settle at N workers in total instead of each fanning out to
  // the full budget and oversubscribing the machine several times over.
  // Held only for the parallel phase, never for the whole merge.
  int AcquireAnnWorkers(int want) noexcept {
    const int cap = MaxAnnBuildWorkers();
    // Two ceilings, and the per-build one is the load-bearing half. Without it
    // the first build to arrive takes the entire budget and the next one runs
    // on its own thread alone: measured as 16 workers against 1 for a
    // two-segment flush, i.e. 4M inserts single-threaded. qdrant avoids this
    // the same way -- cpu_budget is cores - 1 while each build asks for only
    // thread_count_for_hnsw(cores), so two segments settle at 8 and 7.
    const int ceiling = std::clamp(want, 1, MaxAnnWorkersPerBuild());
    auto cur = _running_ann_workers.load(std::memory_order_relaxed);
    for (;;) {
      const int grant = std::clamp(cap - cur, 1, ceiling);
      if (_running_ann_workers.compare_exchange_weak(
            cur, cur + grant, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return grant;
      }
    }
  }
  void ReleaseAnnWorkers(int n) noexcept {
    if (n > 0) {
      _running_ann_workers.fetch_sub(n, std::memory_order_release);
    }
  }

 private:
  DatabasePathFeature& _dir_feature;
  // Per-database central WALs (see GetDbWal). Guarded by _db_wals_mu.
  absl::Mutex _db_wals_mu;
  containers::FlatHashMap<ObjectId, std::unique_ptr<SearchDbWal>> _db_wals;
  std::atomic<bool> _stopping{false};
  std::atomic<int> _running_compactions{0};
  std::atomic<int> _running_ann_workers{0};
  // Live loop futures plus one baseline token held for the engine's lifetime:
  // loops come and go with CREATE/DROP, and a transient zero would complete the
  // group for good. stop() Done()s the token, then Waits.
  yaclib::WaitGroup<> _loops{1};
};

}  // namespace search
}  // namespace sdb
