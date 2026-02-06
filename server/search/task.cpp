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

void CommitTask::Finalize(
  std::shared_ptr<search::InvertedIndexShard> inverted_index_shard,
  CommitResult res) {
  static constexpr size_t kMaxNonEmptyCommits = 10;
  static constexpr size_t kMaxPendingConsolidations = 3;
  bool is_deleted = inverted_index_shard->IsDeleted();

  if (res != CommitResult::NoChanges) {
    _state->pending_commits.fetch_add(1, std::memory_order_release);
    if (!is_deleted) {
      ScheduleContinue(_commit_interval_msec);
    }

    if (res == CommitResult::Done) {
      _state->noop_commit_count.store(0, std::memory_order_release);
      _state->noop_consolidation_count.store(0, std::memory_order_release);

      if (_state->pending_consolidations.load(std::memory_order_acquire) <
            kMaxPendingConsolidations &&
          _state->non_empty_commits.fetch_add(1, std::memory_order_acq_rel) >=
            kMaxNonEmptyCommits) {
        if (!is_deleted) {
          inverted_index_shard->ScheduleConsolidation(
            _consolidation_interval_msec);
        }
        _state->non_empty_commits.store(0, std::memory_order_release);
      }
    }
  } else {
    _state->non_empty_commits.store(0, std::memory_order_release);
    _state->noop_commit_count.fetch_add(1, std::memory_order_release);
    if (is_deleted) {
      return;
    }
    for (auto count = _state->pending_commits.load(std::memory_order_acquire);
         count < 1;) {
      if (_state->pending_commits.compare_exchange_weak(
            count, 1, std::memory_order_acq_rel)) {
        ScheduleContinue(_commit_interval_msec);
        break;
      }
    }
  }
}

void CommitTask::operator()() {
  SDB_TRACE("xxxxx", Logger::SEARCH, "CommitTask started");
  const char run_id = 0;
  auto data = _inverted_index_shard.lock();
  absl::Cleanup set_promise = [promise = std::move(_promise)]() mutable {
    std::move(promise).Set();
  };
  if (!data) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "InvertedIndexShard ", _id,
              " is deleted");
    return;
  }
  auto id = data->GetId();
  _state->pending_commits.fetch_sub(1, std::memory_order_release);

  auto code = CommitResult::Undefined;
  absl::Cleanup reschedule = [&code, data, this]() noexcept {
    try {
      Finalize(std::move(data), code);
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::SEARCH,
                "failed to call finalize: ", ex.what());
    }
  };

  // reload RuntimeState
  {
    SDB_IF_FAILURE("SearchCommitTask::lockInvertedIndexShard") {
      SDB_THROW(ERROR_DEBUG);
    }
    absl::ReaderMutexLock lock{data->GetMutex()};
    auto& settings = data->GetTasksSettings();

    _commit_interval_msec = absl::Milliseconds(settings.commit_interval_msec);
    _consolidation_interval_msec =
      absl::Milliseconds(settings.consolidation_interval_msec);
    _cleanup_interval_step = settings.cleanup_interval_step;
  }

  if (absl::ZeroDuration() == _commit_interval_msec) {
    std::move(reschedule).Cancel();
    SDB_DEBUG("xxxxx", Logger::SEARCH, "sync is disabled for the index '",
              id.id(), "', runId '", size_t(&run_id), "'");
    return;
  }

  SDB_IF_FAILURE("SearchCommitTask::commitUnsafe") { SDB_THROW(ERROR_DEBUG); }
  auto [res, timeMs] = data->CommitUnsafe(false, nullptr, code);

  if (res.ok()) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "successful sync of Search index '",
              id.id(), "', run id '", size_t(&run_id), "', took: ", timeMs,
              "ms");
  } else {
    SDB_WARN("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
             "ms while committing Search index '", id.id(), "', run id '",
             size_t(&run_id), "': ", res.errorNumber(), " ",
             res.errorMessage());
  }
  if (_cleanup_interval_step &&
      ++_cleanup_interval_count >= _cleanup_interval_step) {  // if enabled
    _cleanup_interval_count = 0;
    SDB_IF_FAILURE("SearchCommitTask::cleanupUnsafe") {
      SDB_THROW(ERROR_DEBUG);
    }

    auto [res, timeMs] = data->CleanupUnsafe();

    if (res.ok()) {
      SDB_TRACE("xxxxx", Logger::SEARCH, "successful cleanup of Search index '",
                id.id(), "', run id '", size_t(&run_id), "', took: ", timeMs,
                "ms");
    } else {
      SDB_WARN("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
               "ms while cleaning up Search index '", id.id(), "', run id '",
               size_t(&run_id), "': ", res.errorNumber(), " ",
               res.errorMessage());
    }
  }
}

void ConsolidationTask::operator()() {
  SDB_TRACE("xxxxx", Logger::SEARCH, "ConsolidationTask started");
  const char run_id = 0;
  auto data = _inverted_index_shard.lock();
  absl::Cleanup set_promise = [promise = std::move(_promise)]() mutable {
    std::move(promise).Set();
  };
  if (!data) {
    SDB_WARN("xxxxx", Logger::SEARCH,
             "ConsolidationTask: inverted index shard is deleted");
    return;
  }
  auto id = data->GetId();
  _state->pending_consolidations.fetch_sub(1, std::memory_order_release);
  bool is_deleted = data->IsDeleted();

  absl::Cleanup reschedule = [this, is_deleted]() noexcept {
    try {
      if (is_deleted) {
        return;
      }
      for (auto count =
             _state->pending_consolidations.load(std::memory_order_acquire);
           count < 1;) {
        if (_state->pending_consolidations.compare_exchange_weak(
              count, count + 1, std::memory_order_acq_rel)) {
          ScheduleContinue(_consolidation_interval_msec);
          break;
        }
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::SEARCH, "failed to reschedule: ", ex.what());
    }
  };

  // reload RuntimeState
  {
    SDB_IF_FAILURE("SearchConsolidationTask::lockInvertedIndexShard") {
      SDB_THROW(ERROR_DEBUG);
    }

    absl::ReaderMutexLock lock{data->GetMutex()};
    auto& settings = data->GetTasksSettings();

    _consolidation_policy = settings.consolidation_policy;
    _consolidation_interval_msec =
      absl::Milliseconds(settings.consolidation_interval_msec);
  }
  if (absl::ZeroDuration() == _consolidation_interval_msec ||
      !_consolidation_policy) {
    std::move(reschedule).Cancel();

    SDB_DEBUG("xxxxx", Logger::SEARCH,
              "consolidation is disabled for the index '", id.id(),
              "', runId '", size_t(&run_id), "'");
    return;
  }
  constexpr size_t kMaxNoopCommits = 10;
  constexpr size_t kMaxNoopConsolidations = 10;
  if (_state->noop_commit_count.load(std::memory_order_acquire) <
        kMaxNoopCommits &&
      _state->noop_consolidation_count.load(std::memory_order_acquire) <
        kMaxNoopConsolidations &&
      !is_deleted) {
    _state->pending_consolidations.fetch_add(1, std::memory_order_release);
    ScheduleContinue(_consolidation_interval_msec);
  }
  SDB_IF_FAILURE("SearchConsolidationTask::consolidateUnsafe") {
    SDB_THROW(ERROR_DEBUG);
  }

  bool empty_consolidation = false;
  const auto [res, timeMs] = data->ConsolidateUnsafe(
    _consolidation_policy, _progress, empty_consolidation);

  if (res.ok()) {
    if (empty_consolidation) {
      _state->noop_consolidation_count.fetch_add(1, std::memory_order_release);
    } else {
      _state->noop_consolidation_count.store(0, std::memory_order_release);
    }
    SDB_TRACE("xxxxx", Logger::SEARCH,
              "successful consolidation of Search index '", id.id(),
              "', run id '", size_t(&run_id), "', took: ", timeMs, "ms");
  } else {
    SDB_DEBUG("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
              "ms while consolidating Search index '", id.id(), "', run id '",
              size_t(&run_id), "': ", res.errorNumber(), " ",
              res.errorMessage());
  }
}

}  // namespace sdb::search
