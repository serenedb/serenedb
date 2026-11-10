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

#include "task.h"

#include <absl/algorithm/container.h>
#include <absl/base/thread_annotations.h>
#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <exception>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <vector>
#include <yaclib/async/make.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/util/result.hpp>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/transaction.h"
#include "scheduler/background_scheduler.h"
#include "search/store.h"
#include "server/utils/lifecycle.h"
#include "server/utils/metrics.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

using Clock = BackgroundScheduler::clock;

constexpr Clock::duration kPoll = std::chrono::milliseconds{100};
// Upper bound for the empty-compaction backoff: don't hammer compaction while
// the index is idle, but resume promptly once work appears.
constexpr Clock::duration kMaxCompactionBackoff = std::chrono::seconds{30};
constexpr double kBusySingletonRemovals = 0.2;
constexpr double kBusyLargestShare = 0.5;

Clock::duration ToDuration(size_t msec) noexcept {
  return std::chrono::milliseconds{msec};
}

bool ShouldStop() noexcept {
  return lifecycle::IsStopping() || GetSearchEngine().IsStopping();
}

// Background compaction policy (the TieredMergePolicy analog), built per
// target from its WITH-configured settings. `small` reduces the merge byte
// budget when the global slot gate is nearly full so the pool keeps draining
// under occupancy backpressure.
irs::CompactionPolicy MakeTierPolicy(const TasksSettings& settings, bool small,
                                     bool idle) {
  irs::index_utils::CompactionTier tier;
  tier.max_segments = settings.compaction_max_segments;
  tier.max_segments_bytes = settings.compaction_max_segments_bytes;
  tier.floor_segment_bytes = settings.compaction_floor_segment_bytes;
  if (small) {
    tier.max_segments_bytes =
      std::max(2 * tier.floor_segment_bytes, tier.max_segments_bytes / 2);
  }
  if (!idle) {
    tier.min_singleton_removals = kBusySingletonRemovals;
    tier.max_largest_share = kBusyLargestShare;
  }
  return irs::index_utils::MakePolicy(tier);
}

auto DoCompaction(std::shared_ptr<Store> idx, irs::CompactionPolicy policy,
                  SearchEngine& engine) -> yaclib::Future<bool> {
  absl::Cleanup release_slot = [&engine] { engine.ReleaseCompaction(); };
  SDB_IF_FAILURE("SearchCompactionTask::lockInvertedIndexStorage") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  SDB_IF_FAILURE("SearchCompactionTask::compactUnsafe") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  // Pin the merge's field options for its whole lifetime. A target found
  // already dropped has nothing to merge.
  const auto options = idx->CompactionOptions();
  if (!options) {
    co_return false;
  }
  metrics::Scoped guard{metrics::Gauge::CompactionActive};

  const auto progress = [] { return !ShouldStop(); };

  bool empty_compaction = false;
  auto [res, time_ms] = co_await idx->CompactUnsafeAsync(
    ClaimMaintenance(), policy, progress, empty_compaction, options.get(),
    &AnnBuildEnv());
  if (res.ok()) {
    SDB_TRACE(SEARCH, "successful compaction of Search index '", idx->GetId(),
              "', took: ", time_ms, "ms");
  } else if (absl::IsUnavailable(res)) {
    SDB_TRACE(SEARCH, "compaction of Search index '", idx->GetId(),
              "' stopped by a backup");
  } else {
    SDB_WARN(SEARCH, "error after running for ", time_ms,
             "ms while compacting Search index '", idx->GetId(),
             "': ", res.message());
  }
  co_return !empty_compaction;
}

// Fan out one CompactUnsafe per currently-free global slot. Each merge is
// independent: the tier policy + the writer's _compacting exclusion set hand
// every concurrent call a disjoint candidate window, so concurrency is bounded
// only by the slot gate; the smaller policy
// when the gate is full keeps the pool draining. noexcept on purpose: a slot is
// acquired here and released only by its child, so if Run/push_back threw (OOM)
// the slot would leak into the gate count -- fail fast instead.
std::vector<yaclib::FutureOn<bool>> LaunchCompactionFanout(
  BackgroundScheduler& s, SearchEngine& engine,
  const std::weak_ptr<Store>& weak, bool idle) noexcept {
  std::vector<yaclib::FutureOn<bool>> runs;
  while (!ShouldStop() && !engine.IsBackupActive() &&
         engine.TryAcquireCompaction()) {
    const bool small = engine.FreeCompactionSlots() == 0;
    runs.push_back(
      s.Run([&engine, weak, small, idle]() -> yaclib::Future<bool> {
        if (ShouldStop() || engine.IsBackupActive()) {
          engine.ReleaseCompaction();
          return yaclib::MakeFuture(false);
        }
        auto idx = weak.lock();
        if (!idx) {
          engine.ReleaseCompaction();
          return yaclib::MakeFuture(false);
        }
        SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }
        SDB_WAIT_ON_FAILURE("pause_compaction_run");
        const auto& settings = idx->GetTasksSettings();
        if (settings.compaction_interval_msec == 0) {
          engine.ReleaseCompaction();
          return yaclib::MakeFuture(false);
        }
        auto policy = MakeTierPolicy(settings, small, idle);
        return DoCompaction(std::move(idx), std::move(policy), engine);
      }));
  }
  return runs;
}

struct Target {
  std::weak_ptr<Store> store;
  size_t compaction_interval = 0;
  Clock::time_point compaction_at;
  Clock::duration backoff{};
  uint64_t generation = 0;
  Tick last_write = 0;
  bool cascade = false;
  bool compacting = false;
};

struct Targets {
  absl::Mutex mutex;
  std::vector<std::shared_ptr<Target>> all ABSL_GUARDED_BY(mutex);
  std::atomic<bool> settle_log{false};
};

Targets& GetTargets() {
  static Targets targets;
  return targets;
}

bool SettleIfIdle(Target& target) {
  const auto store = target.store.lock();
  if (!store) {
    return false;
  }
  const auto write = store->LastWrite();
  const bool idle = write == target.last_write;
  if (idle && store->Settle()) {
    GetTargets().settle_log.store(true, std::memory_order_release);
  }
  target.last_write = write;
  return idle;
}

yaclib::Future<> Compact(std::shared_ptr<Target> target) {
  auto& s = BackgroundScheduler::instance();
  auto& engine = GetSearchEngine();
  co_await yaclib::On(s.executor());
  bool produced = false;
  try {
    if (!ShouldStop()) {
      const bool idle = SettleIfIdle(*target);
      auto runs = LaunchCompactionFanout(s, engine, target->store, idle);
      if (!runs.empty()) {
        co_await yaclib::Await(runs.begin(), runs.end());
        for (auto& run : runs) {
          const auto r = std::move(run).Touch();
          produced |= r.State() == yaclib::ResultState::Value && r.Value();
        }
      }
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    SDB_TRACE(SEARCH, "background compaction stopped");
  } catch (const std::exception& e) {
    SDB_WARN(SEARCH, "background compaction failed: ",
             duckdb::ErrorData{e}.RawMessage());
  }
  if (produced && !engine.IsBackupActive()) {
    if (const auto live = target->store.lock()) {
      live->RequestCleanup();
    }
  }
  absl::MutexLock lock{&GetTargets().mutex};
  const auto every = ToDuration(target->compaction_interval);
  if (produced) {
    target->backoff = {};
    target->cascade = true;
  } else {
    target->backoff = std::min(
      kMaxCompactionBackoff,
      target->backoff == Clock::duration::zero() ? every : target->backoff * 2);
  }
  target->compaction_at = Clock::now() + every + target->backoff;
  target->compacting = false;
  co_return {};
}

struct Released {
  Tick tick = 0;
  std::weak_ptr<Store> store;
};

struct ReleasedQueue {
  absl::Mutex mutex;
  std::deque<Released> pending ABSL_GUARDED_BY(mutex);
  std::atomic<Tick> next{kAllApplied};
};

ReleasedQueue& GetReleased() {
  static ReleasedQueue queue;
  return queue;
}

}  // namespace

void DoCleanup(Store& idx) {
  metrics::Scoped guard{metrics::Gauge::CleanupActive};
  auto [res, time_ms] = idx.CleanupUnsafe();
  if (res.ok()) {
    SDB_TRACE(SEARCH, "successful cleanup of Search index '", idx.GetId(),
              "', took: ", time_ms, "ms");
  } else {
    SDB_WARN(SEARCH, "error after running for ", time_ms,
             "ms while cleaning up Search index '", idx.GetId(),
             "': ", res.message());
  }
}

void AddMaintenanceTarget(const std::shared_ptr<Store>& store) {
  auto target = std::make_shared<Target>();
  const auto& settings = store->GetTasksSettings();
  const auto now = Clock::now();
  target->store = store;
  target->compaction_interval = settings.compaction_interval_msec;
  target->compaction_at = now + ToDuration(target->compaction_interval);
  target->generation = store->CompactionGeneration();
  auto& targets = GetTargets();
  absl::MutexLock lock{&targets.mutex};
  targets.all.push_back(std::move(target));
}

yaclib::Future<> MaintenanceLoop(yaclib::WaitGroup<>& jobs) {
  auto& s = BackgroundScheduler::instance();
  auto& targets = GetTargets();
  duckdb::transaction_t settled_commit = 0;
  try {
    while (!ShouldStop()) {
      const auto now = Clock::now();
      auto next = now + kPoll;
      std::vector<std::shared_ptr<Target>> compact;
      bool settle_log = false;
      {
        absl::MutexLock lock{&targets.mutex};
        std::erase_if(targets.all, [](const auto& target) {
          return target->store.expired();
        });
        settle_log = targets.settle_log.load(std::memory_order_acquire) &&
                     absl::c_none_of(targets.all, [](const auto& target) {
                       const auto store = target->store.lock();
                       return store && store->Unsettled();
                     });
        for (const auto& target : targets.all) {
          const auto store = target->store.lock();
          if (!store) {
            continue;
          }
          const auto& settings = store->GetTasksSettings();
          const size_t compaction_interval = settings.compaction_interval_msec;
          if (compaction_interval != target->compaction_interval) {
            target->compaction_interval = compaction_interval;
            target->backoff = {};
            target->cascade = false;
            target->compaction_at = now + ToDuration(compaction_interval);
          }
          if (compaction_interval != 0 && !target->compacting) {
            const auto generation = store->CompactionGeneration();
            if (target->cascade || generation != target->generation ||
                now >= target->compaction_at) {
              target->generation = generation;
              target->cascade = false;
              target->compacting = true;
              compact.push_back(target);
            } else {
              next = std::min(next, target->compaction_at);
            }
          }
        }
      }
      for (auto& target : compact) {
        jobs.Consume(Compact(std::move(target)));
      }
      if (settle_log) {
        const auto commit = catalog::Transactions::Get().LastCommit();
        if (commit == settled_commit) {
          targets.settle_log.store(false, std::memory_order_release);
          catalog::Transactions::Get().SettleLog();
        }
        settled_commit = commit;
      }
      co_await s.Delay(std::clamp<Clock::duration>(
        next - Clock::now(), std::chrono::milliseconds{1}, kPoll));
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    SDB_TRACE(SEARCH, "maintenance loop stopped");
  } catch (const std::exception& ex) {
    SDB_ERROR(SEARCH, "maintenance loop terminated by exception: ",
              duckdb::ErrorData{ex}.RawMessage());
  }
  co_return {};
}

void QueueReleaseCleanup(const std::shared_ptr<Store>& store, Tick released) {
  auto& queue = GetReleased();
  absl::MutexLock lock{&queue.mutex};
  const auto at = absl::c_upper_bound(
    queue.pending, released,
    [](Tick tick, const Released& pending) { return tick < pending.tick; });
  queue.pending.insert(at, {.tick = released, .store = store});
  queue.next.store(queue.pending.front().tick, std::memory_order_release);
  catalog::Transactions::Get().GarbageQueued();
}

bool ReleasedPending() noexcept {
  return GetReleased().next.load(std::memory_order_acquire) != kAllApplied;
}

std::vector<std::shared_ptr<Store>> LiveStores() {
  std::vector<std::shared_ptr<Store>> stores;
  auto& targets = GetTargets();
  absl::MutexLock lock{&targets.mutex};
  stores.reserve(targets.all.size());
  for (const auto& target : targets.all) {
    if (auto store = target->store.lock(); store && store->Opened()) {
      stores.push_back(std::move(store));
    }
  }
  return stores;
}

void PruneVersions(std::span<const Tick> running, Tick published,
                   std::span<const StoreSnapshots> stores) {
  for (const auto& store : LiveStores()) {
    const auto* own = FindSnapshots(stores, store->GetId());
    store->PruneVersions(own ? std::span<const Tick>{own->running} : running,
                         published);
  }
}

bool FreeVersions() {
  bool remaining = false;
  for (const auto& store : LiveStores()) {
    remaining = store->FreeVersions() || remaining;
  }
  return remaining;
}

void CleanupReleased(Tick oldest, std::span<const StoreSnapshots> stores) {
  auto latest = oldest;
  for (const auto& own : stores) {
    latest = std::max(latest, own.oldest);
  }
  auto& queue = GetReleased();
  if (latest < queue.next.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::shared_ptr<Store>> due;
  {
    absl::MutexLock lock{&queue.mutex};
    for (auto it = queue.pending.begin();
         it != queue.pending.end() && it->tick <= latest;) {
      auto store = it->store.lock();
      const auto* own = store ? FindSnapshots(stores, store->GetId()) : nullptr;
      if (it->tick > (own ? std::max(oldest, own->oldest) : oldest)) {
        ++it;
        continue;
      }
      if (store && !absl::c_linear_search(due, store)) {
        due.push_back(std::move(store));
      }
      it = queue.pending.erase(it);
    }
    queue.next.store(
      queue.pending.empty() ? kAllApplied : queue.pending.front().tick,
      std::memory_order_release);
  }
  for (const auto& store : due) {
    store->RequestCleanup();
  }
}

void RequestCleanups() noexcept {
  auto& targets = GetTargets();
  absl::MutexLock lock{&targets.mutex};
  for (const auto& target : targets.all) {
    if (const auto store = target->store.lock(); store && store->Opened()) {
      store->RequestCleanup();
    }
  }
}

}  // namespace sdb::search
