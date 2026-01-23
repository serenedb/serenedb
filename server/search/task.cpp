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

#include "basics/logger/logger.h"
#include "search/data_store.h"

namespace sdb::search {
void CommitTask::Finalize(DataStore& data_store, CommitResult& commit_res) {
  constexpr size_t kMaxNonEmptyCommits = 10;
  constexpr size_t kMaxPendingConsolidations = 3;

  if (commit_res != CommitResult::NoChanges) {
    _state->pending_commits.fetch_add(1, std::memory_order_release);
    Schedule(_commit_interval_msec);

    if (commit_res == CommitResult::Done) {
      _state->noop_commit_count.store(0, std::memory_order_release);
      _state->noop_consolidation_count.store(0, std::memory_order_release);

      if (_state->pending_consolidations.load(std::memory_order_acquire) <
            kMaxPendingConsolidations &&
          _state->non_empty_commits.fetch_add(1, std::memory_order_acq_rel) >=
            kMaxNonEmptyCommits) {
        _data_store->ScheduleConsolidation(_consolidation_interval_msec);
        _state->non_empty_commits.store(0, std::memory_order_release);
      }
    }
  } else {
    _state->non_empty_commits.store(0, std::memory_order_release);
    _state->noop_commit_count.fetch_add(1, std::memory_order_release);

    for (auto count = _state->pending_commits.load(std::memory_order_acquire);
         count < 1;) {
      if (_state->pending_commits.compare_exchange_weak(
            count, 1, std::memory_order_acq_rel)) {
        Schedule(_commit_interval_msec);
        break;
      }
    }
  }
}
void CommitTask::operator()() {
  const char run_id = 0;
  _state->pending_commits.fetch_add(1, std::memory_order_release);
  auto commit_res = CommitResult::Undefined;
  absl::Cleanup reschedule = [&commit_res, this] noexcept {
    try {
      Finalize(*_data_store, commit_res);
    } catch (const std::exception& exp) {
      SDB_ERROR("xxxxx", Logger::SEARCH,
                "failed to call finalize: ", exp.what());
    }
  };
  {
    SDB_IF_FAILURE("SearchCommitTask::lockDataStore") {
      SDB_THROW(ERROR_DEBUG);
    }

    absl::ReaderMutexLock lock{&_data_store->GetMutex()};
    auto& meta = _data_store->GetMeta();

    _commit_interval_msec = absl::Milliseconds(meta.commit_interval_msec);
    _consolidation_interval_msec =
      absl::Milliseconds(meta.consolidation_interval_msec);
    _cleanup_interval_step = meta.cleanup_interval_step;
  }

  if (absl::ZeroDuration() == _commit_interval_msec) {
    std::move(reschedule).Cancel();
    SDB_DEBUG("xxxxx", Logger::SEARCH, "sync is disabled for the index '",
              _id.id(), "', runId '", size_t(&run_id), "'");
    return;
  }

  SDB_IF_FAILURE("SearchCommitTask::commitUnsafe") { SDB_THROW(ERROR_DEBUG); }
  auto [res, timeMs] = _data_store->CommitUnsafe(false, nullptr, commit_res);

  if (res.ok()) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "successful sync of Search index '",
              _id.id(), "', run id '", size_t(&run_id), "', took: ", timeMs,
              "ms");
  } else {
    SDB_WARN("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
             "ms while committing Search index '", _data_store->GetId(),
             "', run id '", size_t(&run_id), "': ", res.errorNumber(), " ",
             res.errorMessage());
  }
  if (_cleanup_interval_step &&
      ++_cleanup_interval_count >= _cleanup_interval_step) {
    _cleanup_interval_count = 0;
    SDB_IF_FAILURE("SearchCommitTask::cleanupUnsafe") {
      SDB_THROW(ERROR_DEBUG);
    }

    auto [res, timeMs] = _data_store->CleanupUnsafe();

    if (res.ok()) {
      SDB_TRACE("xxxxx", Logger::SEARCH, "successful cleanup of Search index '",
                _id.id(), "', run id '", size_t(&run_id), "', took: ", timeMs,
                "ms");
    } else {
      SDB_WARN("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
               "ms while cleaning up Search index '", _id.id(), "', run id '",
               size_t(&run_id), "': ", res.errorNumber(), " ",
               res.errorMessage());
    }
  }
}

void ConsolidationTask::operator()() {
  const char run_id = 0;
  _state->pending_consolidations.fetch_sub(1, std::memory_order_release);

  absl::Cleanup reschedule = [this]() noexcept {
    try {
      for (auto count =
             _state->pending_consolidations.load(std::memory_order_acquire);
           count < 1;) {
        if (_state->pending_consolidations.compare_exchange_weak(
              count, count + 1, std::memory_order_acq_rel)) {
          Schedule(_consolidation_interval_msec);
          break;
        }
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::SEARCH, "failed to reschedule: ", ex.what());
    }
  };

  {
    SDB_IF_FAILURE("SearchConsolidationTask::lockDataStore") {
      SDB_THROW(ERROR_DEBUG);
    }

    absl::ReaderMutexLock lock{&_data_store->GetMutex()};
    auto& meta = _data_store->GetMeta();

    _consolidation_policy = meta.consolidation_policy;
    _consolidation_interval_msec =
      absl::Milliseconds(meta.consolidation_interval_msec);
  }
  if (absl::ZeroDuration() == _consolidation_interval_msec ||
      !_consolidation_policy.policy()) {
    std::move(reschedule).Cancel();

    SDB_DEBUG("xxxxx", Logger::SEARCH,
              "consolidation is disabled for the index '", _id.id(),
              "', runId '", size_t(&run_id), "'");
    return;
  }
  constexpr size_t kMaxNoopCommits = 10;
  constexpr size_t kMaxNoopConsolidations = 10;
  if (_state->noop_commit_count.load(std::memory_order_acquire) <
        kMaxNoopCommits &&
      _state->noop_consolidation_count.load(std::memory_order_acquire) <
        kMaxNoopConsolidations) {
    _state->pending_consolidations.fetch_add(1, std::memory_order_release);
    Schedule(_consolidation_interval_msec);
  }
  SDB_IF_FAILURE("SearchConsolidationTask::consolidateUnsafe") {
    SDB_THROW(ERROR_DEBUG);
  }

  bool empty_consolidation = false;
  const auto [res, timeMs] = _data_store->ConsolidateUnsafe(
    _consolidation_policy, _progress, empty_consolidation);

  if (res.ok()) {
    if (empty_consolidation) {
      _state->noop_consolidation_count.fetch_add(1, std::memory_order_release);
    } else {
      _state->noop_consolidation_count.store(0, std::memory_order_release);
    }
    SDB_TRACE("xxxxx", Logger::SEARCH,
              "successful consolidation of Search index '",
              _data_store->GetId(), "', run id '", size_t(&run_id),
              "', took: ", timeMs, "ms");
  } else {
    SDB_DEBUG("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
              "ms while consolidating Search index '", _data_store->GetId(),
              "', run id '", size_t(&run_id), "': ", res.errorNumber(), " ",
              res.errorMessage());
  }
}
}  // namespace sdb::search
