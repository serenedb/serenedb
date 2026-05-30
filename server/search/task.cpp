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

#include <atomic>
#include <exception>

#include "basics/assert.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
#include "search/inverted_index_shard.h"

namespace sdb::search {

void RefreshTask::Finalize(
  std::shared_ptr<search::InvertedIndexShard> inverted_index_shard,
  CommitResult res) {
  static constexpr size_t kMaxNonEmptyCommits = 10;
  static constexpr size_t kMaxPendingCompactions = 3;

  if (res != CommitResult::NoChanges) {
    _state->pending_commits.fetch_add(1, std::memory_order_release);

    if (res == CommitResult::Done) {
      _state->noop_commit_count.store(0, std::memory_order_release);
      _state->noop_compaction_count.store(0, std::memory_order_release);

      if (_state->pending_compactions.load(std::memory_order_acquire) <
            kMaxPendingCompactions &&
          _state->non_empty_commits.fetch_add(1, std::memory_order_acq_rel) >=
            kMaxNonEmptyCommits) {
        inverted_index_shard->ScheduleCompaction(_compaction_interval_msec);
        _state->non_empty_commits.store(0, std::memory_order_release);
      }
    }
    ScheduleContinue(std::move(inverted_index_shard), _refresh_interval_msec);
  } else {
    _state->non_empty_commits.store(0, std::memory_order_release);
    _state->noop_commit_count.fetch_add(1, std::memory_order_release);
    for (auto count = _state->pending_commits.load(std::memory_order_acquire);
         count < 1;) {
      if (_state->pending_commits.compare_exchange_weak(
            count, 1, std::memory_order_acq_rel)) {
        ScheduleContinue(std::move(inverted_index_shard),
                         _refresh_interval_msec);
        break;
      }
    }
  }
}

void RefreshTask::operator()() {
  SDB_TRACE(SEARCH, "RefreshTask started");
  const char run_id = 0;
  auto data = _inverted_index_shard.lock();
  absl::Cleanup set_promise = [promise = std::move(_promise)]() mutable {
    SDB_ASSERT(promise.Valid());
    std::move(promise).Set();
  };
  SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }
  if (!data) {
    SDB_TRACE(SEARCH, "InvertedIndexShard ", _id, " is deleted");
    return;
  }
  auto id = data->GetId();
  _state->pending_commits.fetch_sub(1, std::memory_order_release);

  auto code = CommitResult::Undefined;
  absl::Cleanup reschedule = [&code, data, this]() noexcept {
    try {
      Finalize(std::move(data), code);
    } catch (const std::exception& ex) {
      SDB_ERROR(SEARCH, "failed to call finalize: ", ex.what());
    }
  };

  // reload RuntimeState
  {
    SDB_IF_FAILURE("SearchRefreshTask::lockInvertedIndexShard") {
      SDB_THROW(sdb::ERROR_DEBUG);
    }
    absl::ReaderMutexLock lock{data->GetMutex()};
    auto& settings = data->GetTasksSettings();

    _refresh_interval_msec = absl::Milliseconds(settings.refresh_interval_msec);
    _compaction_interval_msec =
      absl::Milliseconds(settings.compaction_interval_msec);
    _cleanup_interval_step = settings.cleanup_interval_step;
  }

  // TODO(phase5-follow-up): Currently `refresh_interval_ms` defaults to 0
  // (not set on CREATE INDEX by our DuckDB path), which disables background
  // sync entirely -- inserts never become searchable. A synchronous
  // CommitWait() caller still expects a commit to happen, so always commit
  // when `_wait` is set even if background scheduling is off.
  if (absl::ZeroDuration() == _refresh_interval_msec && !_wait) {
    std::move(reschedule).Cancel();
    SDB_DEBUG(SEARCH, "sync is disabled for the index '", id.id(), "', runId '",
              size_t(&run_id), "'");
    return;
  }

  SDB_IF_FAILURE("SearchRefreshTask::commitUnsafe") {
    SDB_THROW(sdb::ERROR_DEBUG);
  }
  auto [res, timeMs] = data->CommitUnsafe(_wait, nullptr, code);

  if (res.ok()) {
    SDB_TRACE(SEARCH, "successful sync of Search index '", id.id(),
              "', run id '", size_t(&run_id), "', took: ", timeMs, "ms");
  } else {
    SDB_WARN(SEARCH, "error after running for ", timeMs,
             "ms while committing Search index '", id.id(), "', run id '",
             size_t(&run_id), "': ", res.errorNumber(), " ",
             res.errorMessage());
  }
  if (_cleanup_interval_step &&
      ++_cleanup_interval_count >= _cleanup_interval_step) {  // if enabled
    _cleanup_interval_count = 0;
    SDB_IF_FAILURE("SearchRefreshTask::cleanupUnsafe") {
      SDB_THROW(sdb::ERROR_DEBUG);
    }

    auto [res, timeMs] = data->CleanupUnsafe();

    if (res.ok()) {
      SDB_TRACE(SEARCH, "successful cleanup of Search index '", id.id(),
                "', run id '", size_t(&run_id), "', took: ", timeMs, "ms");
    } else {
      SDB_WARN(SEARCH, "error after running for ", timeMs,
               "ms while cleaning up Search index '", id.id(), "', run id '",
               size_t(&run_id), "': ", res.errorNumber(), " ",
               res.errorMessage());
    }
  }
}

void CompactionTask::operator()() {
  SDB_TRACE(SEARCH, "CompactionTask started");
  const char run_id = 0;
  auto data = _inverted_index_shard.lock();
  absl::Cleanup set_promise = [promise = std::move(_promise)]() mutable {
    std::move(promise).Set();
  };
  SDB_IF_FAILURE("slow_search_task") { absl::SleepFor(absl::Seconds(5)); }
  if (!data) {
    SDB_WARN(SEARCH, "CompactionTask: inverted index shard is deleted");
    return;
  }
  auto id = data->GetId();
  _state->pending_compactions.fetch_sub(1, std::memory_order_release);

  absl::Cleanup reschedule = [this, data]() mutable noexcept {
    try {
      for (auto count =
             _state->pending_compactions.load(std::memory_order_acquire);
           count < 1;) {
        if (_state->pending_compactions.compare_exchange_weak(
              count, count + 1, std::memory_order_acq_rel)) {
          ScheduleContinue(std::move(data), _compaction_interval_msec);
          break;
        }
      }
    } catch (const std::exception& ex) {
      SDB_ERROR(SEARCH, "failed to reschedule: ", ex.what());
    }
  };

  // reload RuntimeState
  {
    SDB_IF_FAILURE("SearchCompactionTask::lockInvertedIndexShard") {
      SDB_THROW(sdb::ERROR_DEBUG);
    }

    absl::ReaderMutexLock lock{data->GetMutex()};
    auto& settings = data->GetTasksSettings();

    _compaction_policy = settings.compaction_policy;
    _compaction_interval_msec =
      absl::Milliseconds(settings.compaction_interval_msec);
  }
  if (absl::ZeroDuration() == _compaction_interval_msec ||
      !_compaction_policy) {
    std::move(reschedule).Cancel();

    SDB_DEBUG(SEARCH, "compaction is disabled for the index '", id.id(),
              "', runId '", size_t(&run_id), "'");
    return;
  }
  static constexpr size_t kMaxNoopCommits = 10;
  static constexpr size_t kMaxNoopCompactions = 10;
  if (_state->noop_commit_count.load(std::memory_order_acquire) <
        kMaxNoopCommits &&
      _state->noop_compaction_count.load(std::memory_order_acquire) <
        kMaxNoopCompactions) {
    _state->pending_compactions.fetch_add(1, std::memory_order_release);
    ScheduleContinue(std::move(data), _compaction_interval_msec);
    std::move(reschedule).Cancel();
  }
  SDB_IF_FAILURE("SearchCompactionTask::compactUnsafe") {
    SDB_THROW(sdb::ERROR_DEBUG);
  }

  bool empty_compaction = false;
  const auto [res, timeMs] =
    data->CompactUnsafe(_compaction_policy, _progress, empty_compaction);

  if (res.ok()) {
    if (empty_compaction) {
      _state->noop_compaction_count.fetch_add(1, std::memory_order_release);
    } else {
      _state->noop_compaction_count.store(0, std::memory_order_release);
    }
    SDB_TRACE(SEARCH, "successful compaction of Search index '", id.id(),
              "', run id '", size_t(&run_id), "', took: ", timeMs, "ms");
  } else {
    SDB_DEBUG(SEARCH, "error after running for ", timeMs,
              "ms while compacting Search index '", id.id(), "', run id '",
              size_t(&run_id), "': ", res.errorNumber(), " ",
              res.errorMessage());
  }
}

}  // namespace sdb::search
