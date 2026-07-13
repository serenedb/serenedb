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

#include <absl/cleanup/cleanup.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iresearch/utils/index_utils.hpp>
#include <memory>
#include <vector>
#include <yaclib/async/run.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/util/result.hpp>

#include "basics/lifecycle.h"
#include "basics/log.h"
#include "basics/metrics.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "scheduler/background_scheduler.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
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

// Default background compaction policy (the TieredMergePolicy analog) -- this
// is what fixes today's dead compaction: the coordinator owns the policy
// instead of the never-assigned catalog setting that left the loop inert.
// `Small` reduces the merge byte budget when the global slot gate is nearly
// full so the pool keeps draining under occupancy backpressure. VACUUM keeps
// CompactionCount{SIZE_MAX}.
const irs::CompactionPolicy& TierPolicyNormal() {
  static const irs::CompactionPolicy kPolicy =
    irs::index_utils::MakePolicy(irs::index_utils::CompactionTier{});
  return kPolicy;
}

const irs::CompactionPolicy& TierPolicySmall() {
  static const irs::CompactionPolicy kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionTier{.max_segments_bytes = size_t{512} << 20});
  return kPolicy;
}

// Per-storage source of the merge's field options, pinned for the merge's
// lifetime -- the only part of the background loops that differs by storage
// type (see the PinCompactionOptions overloads below).
struct CompactionOptions {
  bool alive = false;
  // Pins the catalog object that owns `field_options` for the whole merge.
  std::shared_ptr<const void> keepalive;
  const irs::IndexFieldOptions* field_options = nullptr;
};

CompactionOptions PinCompactionOptions(InvertedIndexStorage& idx) {
  // A fresh catalog snapshot keeps THIS DDL view alive for the merge, so a
  // concurrent DROP cannot dangle the index it encodes the new segment against.
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto index = snapshot->GetObject<catalog::InvertedIndex>(idx.GetId());
  if (!index) {
    return {};
  }
  const irs::IndexFieldOptions* options = index.get();
  return {
    .alive = true, .keepalive = std::move(index), .field_options = options};
}

CompactionOptions PinCompactionOptions(SearchTable& /*table*/) {
  // No per-field config yet -- the merge uses the writer's baseline encoding.
  return {.alive = true, .keepalive = nullptr, .field_options = nullptr};
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
      SDB_TRACE(SEARCH, "successful sync of Search index '", idx.GetId().id(),
                "', took: ", time_ms, "ms");
    } else {
      SDB_WARN(SEARCH, "error after running for ", time_ms,
               "ms while refreshing Search index '", idx.GetId().id(),
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
    SDB_TRACE(SEARCH, "successful cleanup of Search index '", idx.GetId().id(),
              "', took: ", time_ms, "ms");
  } else {
    SDB_WARN(SEARCH, "error after running for ", time_ms,
             "ms while cleaning up Search index '", idx.GetId().id(),
             "': ", res.message());
  }
}

template<class Storage>
bool DoCompaction(Storage& idx, const irs::CompactionPolicy& policy) {
  SDB_IF_FAILURE("SearchCompactionTask::lockInvertedIndexStorage") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  SDB_IF_FAILURE("SearchCompactionTask::compactUnsafe") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  // Pin the merge's field options for its whole lifetime (storage-specific, see
  // PinCompactionOptions). A target found already dropped has nothing to merge.
  auto opts = PinCompactionOptions(idx);
  if (!opts.alive) {
    return false;
  }
  metrics::Scoped guard{metrics::Gauge::CompactionActive};
  bool empty_compaction = false;
  auto [res, time_ms] = idx.CompactUnsafe(
    policy, [] { return !ShouldStop(); }, empty_compaction, opts.field_options);
  if (res.ok()) {
    SDB_TRACE(SEARCH, "successful compaction of Search index '",
              idx.GetId().id(), "', took: ", time_ms, "ms");
  } else {
    SDB_DEBUG(SEARCH, "error after running for ", time_ms,
              "ms while compacting Search index '", idx.GetId().id(),
              "': ", res.message());
  }
  return !empty_compaction;
}

// Fan out one CompactUnsafe per currently-free global slot. Each merge is
// independent: the tier policy + the writer's _compacting exclusion set hand
// every concurrent call a disjoint candidate window (overlap -> Fail -> no
// work), so concurrency is bounded only by the slot gate; the smaller policy
// when the gate is full keeps the pool draining. noexcept on purpose: a slot is
// acquired here and released only by its child, so if Run/push_back threw (OOM)
// the slot would leak into the gate count -- fail fast instead.
template<class Storage>
std::vector<yaclib::FutureOn<bool>> LaunchCompactionFanout(
  BackgroundScheduler& s, SearchEngine& engine,
  const std::weak_ptr<Storage>& weak) noexcept {
  std::vector<yaclib::FutureOn<bool>> runs;
  while (engine.TryAcquireCompaction()) {
    const auto& policy = engine.FreeCompactionSlots() == 0 ? TierPolicySmall()
                                                           : TierPolicyNormal();
    runs.push_back(s.Run([&, weak] {
      absl::Cleanup release = [&] { engine.ReleaseCompaction(); };
      if (ShouldStop()) {
        return false;
      }
      auto idx = weak.lock();
      if (!idx) {
        return false;
      }
      SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }
      return DoCompaction(*idx, policy);
    }));
  }
  return runs;
}

// Wait up to `total` for the timer OR a NudgeCompaction. OneShotEvent is a poor
// fit (one-shot, non-thread-safe Reset, lost-wakeup across iterations with many
// producers), so we poll the per-index generation in bounded slices -- the
// fallback the design explicitly allows. Returns early on a nudge or on stop.
template<class Storage>
yaclib::Future<> WaitForCompactionTrigger(BackgroundScheduler& s,
                                          std::weak_ptr<Storage> weak,
                                          uint64_t base_gen,
                                          Clock::duration total) {
  auto remaining = total;
  while (remaining > Clock::duration::zero()) {
    const auto slice = std::min(remaining, kNudgePollSlice);
    co_await s.Delay(slice);
    if (ShouldStop()) {
      break;
    }
    auto idx = weak.lock();
    if (!idx) {
      break;
    }
    const bool nudged = idx->CompactionGeneration() != base_gen;
    idx.reset();
    if (nudged) {
      break;
    }
    remaining -= slice;
  }
  co_return {};
}

}  // namespace

template<class Storage>
yaclib::Future<> RefreshLoop(std::weak_ptr<Storage> weak) {
  auto& s = BackgroundScheduler::instance();
  size_t cleanup_count = 0;
  int stretch = 0;
  try {
    for (;;) {
      auto idx = weak.lock();
      if (!idx) {
        SDB_TRACE(SEARCH, "maintenance target is deleted, ending refresh loop");
        break;
      }
      const auto& settings = idx->GetTasksSettings();
      const auto interval = ToDuration(settings.refresh_interval_msec);
      const auto cleanup_step = settings.cleanup_interval_step;
      const bool enabled = interval > Clock::duration::zero();
      idx.reset();

      const auto delay =
        enabled ? interval + interval * stretch : kDisabledPoll;
      co_await s.Delay(delay);
      if (ShouldStop()) {
        break;
      }
      if (!enabled) {
        continue;
      }

      idx = weak.lock();
      if (!idx) {
        break;
      }

      SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }

      const bool stale = idx->StalePressure() >= kStalePressureCleanup;
      const bool periodic = cleanup_step && ++cleanup_count >= cleanup_step;
      const bool run_cleanup = stale || periodic;
      if (run_cleanup) {
        cleanup_count = 0;
        idx->ClearStalePressure();
      }

      co_await yaclib::On(s.executor());
      if (ShouldStop()) {
        break;
      }
      RefreshResult code = RefreshResult::Undefined;
      DoRefresh(*idx, run_cleanup, code);

      switch (code) {
        case RefreshResult::Done:
          stretch = 0;
          idx->NudgeCompaction();
          break;
        case RefreshResult::NoChanges:
          stretch = std::min(kMaxRefreshStretch,
                             stretch == 0 ? 1 : stretch + stretch / 2 + 1);
          break;
        default:
          break;
      }
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    // The executor was stopped (serened shutting down) while the loop awaited a
    // timer -- a clean termination signal, not an error.
    SDB_TRACE(SEARCH, "refresh loop stopped");
  } catch (const std::exception& ex) {
    SDB_ERROR(SEARCH, "refresh loop terminated by exception: ", ex.what());
  }
  co_return {};
}

template<class Storage>
yaclib::Future<> CompactionCoordinator(std::weak_ptr<Storage> weak) {
  auto& s = BackgroundScheduler::instance();
  auto& engine = GetSearchEngine();
  auto backoff = Clock::duration::zero();
  bool cascade = false;
  try {
    for (;;) {
      auto idx = weak.lock();
      if (!idx) {
        SDB_TRACE(SEARCH,
                  "maintenance target is deleted, ending compaction loop");
        break;
      }
      const auto& settings = idx->GetTasksSettings();
      const auto interval = ToDuration(settings.compaction_interval_msec);
      const bool enabled = interval > Clock::duration::zero();
      const auto base_gen = idx->CompactionGeneration();
      idx.reset();

      if (!enabled) {
        cascade = false;
        co_await s.Delay(kDisabledPoll);
        if (ShouldStop()) {
          break;
        }
        continue;
      }

      // A productive tick re-evaluates immediately (Lucene MERGE_FINISHED
      // cascade); otherwise wait the interval+backoff or a refresh nudge.
      if (!cascade) {
        co_await WaitForCompactionTrigger(s, weak, base_gen,
                                          interval + backoff);
        if (ShouldStop()) {
          break;
        }
      }
      cascade = false;

      auto runs = LaunchCompactionFanout(s, engine, weak);
      if (runs.empty()) {
        // Gate full: every slot is busy elsewhere; wait for one to free up.
        backoff = backoff == Clock::duration::zero() ? interval : backoff * 2;
        backoff = std::min(backoff, kMaxCompactionBackoff);
        continue;
      }

      // Join the fan-out before looping/returning so no detached child touches
      // the storage after this coroutine completes.
      co_await yaclib::Await(runs.begin(), runs.end());

      bool produced = false;
      for (auto& run : runs) {
        const auto r = std::move(run).Touch();
        produced |= r.State() == yaclib::ResultState::Value && r.Value();
      }
      if (produced) {
        // Re-evaluate at once and raise stale pressure so the refresh loop's
        // cleanup tail reclaims the now-unreferenced merge inputs.
        backoff = Clock::duration::zero();
        cascade = true;
        if (auto live = weak.lock()) {
          live->BumpStalePressure();
        }
      } else {
        backoff = backoff == Clock::duration::zero() ? interval : backoff * 2;
        backoff = std::min(backoff, kMaxCompactionBackoff);
      }
    }
  } catch (const yaclib::ResultError<yaclib::StopError>&) {
    // The executor was stopped (serened shutting down) while the loop awaited a
    // timer -- a clean termination signal, not an error.
    SDB_TRACE(SEARCH, "compaction loop stopped");
  } catch (const std::exception& ex) {
    SDB_ERROR(SEARCH, "compaction loop terminated by exception: ", ex.what());
  }
  co_return {};
}

template yaclib::Future<> RefreshLoop(std::weak_ptr<InvertedIndexStorage>);
template yaclib::Future<> CompactionCoordinator(
  std::weak_ptr<InvertedIndexStorage>);
template yaclib::Future<> RefreshLoop(std::weak_ptr<SearchTable>);
template yaclib::Future<> CompactionCoordinator(std::weak_ptr<SearchTable>);

}  // namespace sdb::search
