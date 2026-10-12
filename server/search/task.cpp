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

#include <absl/time/time.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <exception>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <vector>
#include <yaclib/async/run.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/util/result.hpp>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/inverted_store_index.h"
#include "scheduler/background_scheduler.h"
#include "search/compaction/merge_policies.h"
#include "search/compaction/merge_scheduler.h"
#include "search/compaction/pool_merge_executor.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "server/utils/lifecycle.h"
#include "server/utils/metrics.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

using Clock = BackgroundScheduler::clock;

// Poll cadence when the relevant interval is disabled (== 0): keep the loop
// alive and re-reading settings so a later ALTER that sets a non-zero interval
// takes effect without relaunching the coroutine.
constexpr Clock::duration kDisabledPoll = std::chrono::seconds{1};
// Upper bound for the empty-compaction backoff: don't hammer compaction while
// the index is idle, but resume promptly once work appears.
constexpr Clock::duration kMaxCompactionBackoff = std::chrono::seconds{30};
// Granularity for waking on a NudgeCompaction during the backoff wait: the
// coordinator wakes within one slice of a refresh that produced segments.
constexpr Clock::duration kNudgePollSlice = std::chrono::milliseconds{100};
// On RefreshResult::NoChanges lengthen the next refresh delay toward this many
// extra multiples of the configured interval (~x1.5 per idle tick) to avoid
// fsync churn on an idle index; Done resets to the configured interval.
constexpr int kMaxRefreshStretch = 4;
// Run cleanup once stale pressure (raised by non-empty compactions) crosses
// this, independent of the periodic cleanup step.
constexpr uint32_t kStalePressureCleanup = 4;

Clock::duration ToDuration(size_t msec) noexcept {
  return std::chrono::milliseconds{msec};
}

bool ShouldStop() noexcept {
  return lifecycle::IsStopping() || GetSearchEngine().IsStopping();
}

template<class Storage>
void DoRefresh(Storage& idx, bool run_cleanup, RefreshResult& code) {
  SDB_IF_FAILURE("SearchRefreshTask::lockInvertedIndexStorage") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  SDB_IF_FAILURE("SearchRefreshTask::commitUnsafe") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  {
    metrics::Scoped guard{metrics::Gauge::RefreshActive};
    code = RefreshResult::Undefined;
    auto [res, time_ms] = idx.RefreshUnsafe(/*wait=*/false, nullptr, code);
    if (res.ok()) {
      SDB_TRACE(SEARCH, "successful sync of Search index '", idx.GetId(),
                "', took: ", time_ms, "ms");
    } else {
      SDB_WARN(SEARCH, "error after running for ", time_ms,
               "ms while refreshing Search index '", idx.GetId(),
               "': ", res.message());
    }
  }
  if (!run_cleanup) {
    return;
  }
  SDB_IF_FAILURE("SearchRefreshTask::cleanupUnsafe") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
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

bool AcquireMergeHelper() noexcept {
  return GetSearchEngine().TryAcquireCompaction();
}

void ReleaseMergeHelper() noexcept { GetSearchEngine().ReleaseCompaction(); }

template<class Storage>
class StorageMergeJob final : public MergeJob {
 public:
  StorageMergeJob(std::shared_ptr<Storage> storage,
                  irs::MergeTicket ticket) noexcept
    : _storage{std::move(storage)}, _ticket{std::move(ticket)} {}

  irs::CompactionView Candidates() const noexcept final {
    return _ticket.Candidates();
  }

  yaclib::Future<MergeOutcome> Run(const std::atomic_bool& stop,
                                   size_t helpers) final {
    SDB_IF_FAILURE("SearchCompactionTask::lockInvertedIndexStorage") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    SDB_IF_FAILURE("SearchCompactionTask::compactUnsafe") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }
    SDB_WAIT_ON_FAILURE("pause_compaction_run");
    SDB_PARK_ONCE_ON_FAILURE("park_compaction_run");
    if (_storage->Retired() || _ticket.Revoked() ||
        _storage->GetTasksSettings().compaction_interval_msec == 0) {
      co_return MergeOutcome::Cancelled;
    }
    // Pin the merge's field options for its whole lifetime (storage-specific,
    // see PinCompactionOptions). A target found already dropped has nothing to
    // merge.
    auto opts = PinCompactionOptions(*_storage);
    if (!opts.alive) {
      co_return MergeOutcome::Cancelled;
    }
    metrics::Scoped guard{metrics::Gauge::CompactionActive};
    const irs::MergeWriter::FlushProgress progress = [&stop] {
      return !stop.load(std::memory_order_relaxed);
    };
    PoolMergeExecutor executor{BackgroundScheduler::instance().executor(),
                               helpers, AcquireMergeHelper, ReleaseMergeHelper};
    bool empty_compaction = false;
    irs::CompactionResult outcome;
    auto [res, time_ms] = co_await _storage->RunMergeAsync(
      std::move(_ticket), progress, empty_compaction, opts.field_options,
      &AnnBuildEnv(), MergeOrigin::Background, &outcome, &executor);
    if (!res.ok()) {
      SDB_DEBUG(SEARCH, "error after running for ", time_ms,
                "ms while compacting Search index '", _storage->GetId(),
                "': ", res.message());
      co_return MergeOutcome::Failed;
    }
    SDB_TRACE(SEARCH, "compaction of Search index '", _storage->GetId(),
              "' took ", time_ms, "ms");
    const auto merged = OutcomeOf(outcome);
    if (Produced(merged)) {
      _storage->BumpStalePressure();
    }
    co_return merged;
  }

 private:
  std::shared_ptr<Storage> _storage;
  irs::MergeTicket _ticket;
};

template<class Storage>
size_t PlanMerges(const std::shared_ptr<Storage>& idx, MergeScheduler& merges,
                  const std::shared_ptr<MergeScheduler::Relation>& relation) {
  const auto wanted = merges.Wanted(*relation);
  const auto blocked = merges.Blocked(*relation);
  const auto policy = MakeTierPolicy(idx->GetTasksSettings());
  auto reservation = idx->ReserveMerges(
    [&](const irs::IndexReader& index, const irs::CompactingSegments& reserved,
        irs::MergeGroups& groups) {
      irs::CompactingSegments taken = reserved;
      for (const auto& name : blocked) {
        taken.emplace(name);
      }
      for (size_t i = 0; i != wanted; ++i) {
        irs::Compaction candidates;
        policy(candidates, index, taken);
        if (candidates.empty()) {
          break;
        }
        for (const auto* candidate : candidates) {
          taken.emplace(candidate->Meta().name);
        }
        groups.emplace_back(std::move(candidates));
      }
    });
  for (auto& ticket : reservation.tickets) {
    merges.Enqueue(relation, std::make_unique<StorageMergeJob<Storage>>(
                               idx, std::move(ticket)));
  }
  return reservation.tickets.size();
}

// Wait up to `total` for the timer OR a NudgeCompaction. OneShotEvent is a poor
// fit (one-shot, non-thread-safe Reset, lost-wakeup across iterations with many
// producers), so we poll the per-index generation in bounded slices -- the
// fallback the design explicitly allows. Returns early on a nudge or on stop.
template<class Storage>
yaclib::Future<> WaitForCompactionTrigger(BackgroundScheduler& s,
                                          std::weak_ptr<Storage> weak,
                                          uint64_t base_gen,
                                          Clock::duration interval,
                                          Clock::duration total) {
  auto remaining = total;
  while (remaining > Clock::duration::zero()) {
    const auto slice = std::min(remaining, kNudgePollSlice);
    co_await s.Delay(slice);
    if (ShouldStop()) {
      break;
    }
    auto idx = weak.lock();
    if (!idx || idx->Retired()) {
      break;
    }
    const bool nudged =
      idx->CompactionGeneration() != base_gen ||
      ToDuration(idx->GetTasksSettings().compaction_interval_msec) != interval;
    idx.reset();
    if (nudged) {
      break;
    }
    remaining -= slice;
  }
  co_return {};
}

// One tick's verdict for the interval loops' failure-stretch accounting.
enum class LoopTick {
  kProgress,  // reset the stretch
  kIdle,      // grow the stretch
  kNeutral,   // keep the stretch
};

// The scaffolding RefreshLoop and ReindexLoop share: lock-or-exit,
// interval+stretch delay (kDisabledPoll while the interval is 0), the stop
// gates, the executor hop and the stretch accounting. `tick` runs on the
// executor and locks the target itself (so a long tick does not have to pin
// the storage).
template<class Storage, class GetIntervalMs, class Tick>
yaclib::Future<> IntervalLoop(std::weak_ptr<Storage> weak,
                              std::string_view name,
                              GetIntervalMs get_interval_ms, Tick tick) {
  auto& s = BackgroundScheduler::instance();
  int stretch = 0;
  try {
    for (;;) {
      auto idx = weak.lock();
      if (!idx) {
        SDB_TRACE(SEARCH, "maintenance target is deleted, ending ", name,
                  " loop");
        break;
      }
      const auto interval = ToDuration(get_interval_ms(*idx));
      const bool enabled = interval > Clock::duration::zero();
      idx.reset();

      bool changed = false;
      auto remaining = enabled ? interval + interval * stretch : kDisabledPoll;
      while (remaining > Clock::duration::zero() && !changed) {
        const auto slice = std::min(remaining, kDisabledPoll);
        co_await s.Delay(slice);
        if (ShouldStop()) {
          break;
        }
        auto live = weak.lock();
        changed = !live || ToDuration(get_interval_ms(*live)) != interval;
        remaining -= slice;
      }
      if (ShouldStop()) {
        break;
      }
      if (changed || !enabled) {
        stretch = 0;
        continue;
      }
      if (weak.expired()) {
        break;
      }

      SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }

      co_await yaclib::On(s.executor());
      if (ShouldStop()) {
        break;
      }
      switch (tick(weak)) {
        case LoopTick::kProgress:
          stretch = 0;
          break;
        case LoopTick::kIdle:
          stretch = std::min(kMaxRefreshStretch,
                             stretch == 0 ? 1 : stretch + stretch / 2 + 1);
          break;
        case LoopTick::kNeutral:
          break;
      }
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    // The executor was stopped (serened shutting down) while the loop awaited a
    // timer -- a clean termination signal, not an error.
    SDB_TRACE(SEARCH, name, " loop stopped");
  } catch (const std::exception& ex) {
    SDB_ERROR(SEARCH, name, " loop terminated by exception: ", ex.what());
  }
  co_return {};
}

}  // namespace

template<class Storage>
yaclib::Future<> RefreshLoop(std::weak_ptr<Storage> weak) {
  return IntervalLoop(
    std::move(weak), "refresh",
    [](Storage& idx) -> size_t {
      return idx.GetTasksSettings().refresh_interval_msec;
    },
    [cleanup_count = size_t{0}](const std::weak_ptr<Storage>& target) mutable {
      auto idx = target.lock();
      if (!idx) {
        return LoopTick::kNeutral;
      }
      const size_t cleanup_step = idx->GetTasksSettings().cleanup_interval_step;
      const bool stale = idx->StalePressure() >= kStalePressureCleanup;
      const bool periodic = cleanup_step && ++cleanup_count >= cleanup_step;
      const bool run_cleanup = stale || periodic;
      if (run_cleanup) {
        cleanup_count = 0;
        idx->ClearStalePressure();
      }
      RefreshResult code = RefreshResult::Undefined;
      DoRefresh(*idx, run_cleanup, code);
      switch (code) {
        case RefreshResult::Done:
          idx->NudgeCompaction();
          return LoopTick::kProgress;
        case RefreshResult::NoChanges:
          return LoopTick::kIdle;
        default:
          return LoopTick::kNeutral;
      }
    });
}

template<class Storage>
yaclib::Future<> CompactionCoordinator(std::shared_ptr<Storage> storage) {
  auto& s = BackgroundScheduler::instance();
  auto& merges = GetSearchEngine().Merges();
  const std::weak_ptr<Storage> weak = storage;
  const auto relation = merges.AddRelation(storage->GetId(), [weak] {
    if (auto live = weak.lock()) {
      live->NudgeCompaction();
    }
  });
  storage.reset();
  auto backoff = Clock::duration::zero();
  auto last_interval = Clock::duration::zero();
  bool triggered = false;
  try {
    for (;;) {
      auto idx = weak.lock();
      if (!idx || idx->Retired()) {
        SDB_TRACE(SEARCH,
                  "maintenance target is deleted, ending compaction loop");
        break;
      }
      const auto interval =
        ToDuration(idx->GetTasksSettings().compaction_interval_msec);
      const auto base_gen = idx->CompactionGeneration();
      if (interval != last_interval) {
        last_interval = interval;
        backoff = Clock::duration::zero();
      }

      if (interval == Clock::duration::zero()) {
        idx.reset();
        merges.CancelQueued(*relation);
        triggered = false;
        co_await s.Delay(kDisabledPoll);
        if (ShouldStop()) {
          break;
        }
        continue;
      }

      if (triggered) {
        if (PlanMerges(idx, merges, relation) != 0) {
          backoff = Clock::duration::zero();
        } else {
          backoff = backoff == Clock::duration::zero() ? interval : backoff * 2;
          backoff = std::min(backoff, kMaxCompactionBackoff);
        }
      }
      idx.reset();

      co_await WaitForCompactionTrigger(s, weak, base_gen, interval,
                                        interval + backoff);
      if (ShouldStop()) {
        break;
      }
      co_await yaclib::On(s.executor());
      triggered = true;
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    // The executor was stopped (serened shutting down) while the loop awaited a
    // timer -- a clean termination signal, not an error.
    SDB_TRACE(SEARCH, "compaction loop stopped");
  } catch (const std::exception& ex) {
    SDB_ERROR(SEARCH, "compaction loop terminated by exception: ", ex.what());
  }
  merges.CancelQueued(*relation);
  merges.StopRunning(*relation);
  co_return {};
}

template yaclib::Future<> RefreshLoop(std::weak_ptr<InvertedIndexStorage>);
template yaclib::Future<> CompactionCoordinator(
  std::shared_ptr<InvertedIndexStorage>);
template yaclib::Future<> RefreshLoop(std::weak_ptr<SearchTable>);
template yaclib::Future<> CompactionCoordinator(std::shared_ptr<SearchTable>);

namespace {

// Installed once by the connector (RegisterServerExtensions) during the
// single-threaded boot, strictly before any storage starts its loops.
ReindexRunner g_reindex_runner;

}  // namespace

void SetReindexRunner(ReindexRunner runner) {
  g_reindex_runner = std::move(runner);
}

yaclib::Future<> ReindexLoop(std::weak_ptr<InvertedIndexStorage> weak) {
  return IntervalLoop(
    std::move(weak), "reindex",
    [](InvertedIndexStorage& idx) -> size_t {
      return idx.GetTasksSettings().reindex_interval_msec;
    },
    [](const std::weak_ptr<InvertedIndexStorage>& target) {
      if (!g_reindex_runner) {
        return LoopTick::kNeutral;
      }
      duckdb::idx_t database_id;
      duckdb::idx_t id;
      {
        // The runner resolves the index by id through the catalog: don't pin
        // the storage across a potentially long tick.
        auto idx = target.lock();
        if (!idx) {
          return LoopTick::kNeutral;
        }
        database_id = idx->GetDatabaseId();
        id = idx->GetId();
      }
      const auto did_work = g_reindex_runner(database_id, id);
      if (!did_work.ok()) {
        SDB_WARN(SEARCH, "periodic reindex of Search index '", id,
                 "' failed: ", did_work.status().message());
        return LoopTick::kIdle;
      }
      return *did_work ? LoopTick::kProgress : LoopTick::kIdle;
    });
}

}  // namespace sdb::search
