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
#include "search/data_store.h"

namespace sdb::search {
void CommitTask::Finalize(CommitResult res) {
  constexpr size_t kMaxNonEmptyCommits = 10;
  // constexpr size_t kMaxPendingConsolidations = 3;

  switch (res) {
    case CommitResult::Done: {
      _state->pending_commits.fetch_sub(1, std::memory_order_release);
      auto non_empty_commits =
        _state->non_empty_commits.fetch_add(1, std::memory_order_acq_rel);
      if (non_empty_commits == kMaxNonEmptyCommits) {
        _transaction.GetDataStore()->ScheduleConsolidation(
          _consolidation_interval_msec);
        _state->non_empty_commits.store(0, std::memory_order_release);
      }
      break;
    }
    case CommitResult::InProgress:
      break;
    case CommitResult::NoChanges:
      _state->noop_commit_count.fetch_add(1, std::memory_order_release);
      break;
    case CommitResult::Undefined:
      SDB_UNREACHABLE();
  }
}

void CommitTask::operator()() {
  const char run_id = 0;
  const auto& data_store = _transaction.GetDataStore();
  _state->pending_commits.fetch_add(1, std::memory_order_release);
  auto commit_res = CommitResult::Undefined;
  absl::Cleanup reschedule = [commit_res, this] noexcept {
    try {
      Finalize(commit_res);
    } catch (const std::exception& exp) {
      SDB_ERROR("xxxxx", Logger::SEARCH,
                "failed to call finalize: ", exp.what());
    }
  };
  // TODO(codeworse): Commit via threadpool
  auto [res, timeMs] = std::move(_transaction).Commit();

  if (res.ok()) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "successful sync of Search index '",
              _id.id(), "', run id '", size_t(&run_id), "', took: ", timeMs,
              "ms");
  } else {
    SDB_WARN("xxxxx", Logger::SEARCH, "error after running for ", timeMs,
             "ms while committing Search index '", data_store->GetId(),
             "', run id '", size_t(&run_id), "': ", res.errorNumber(), " ",
             res.errorMessage());
  }
}

void ConsolidationTask::operator()() {
  const char run_id = 0;
  _state->pending_consolidations.fetch_sub(1, std::memory_order_release);

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
