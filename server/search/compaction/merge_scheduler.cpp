////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "search/compaction/merge_scheduler.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <exception>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/submit.hpp>

#include "server/utils/metrics.h"

namespace sdb::search {
namespace {

constexpr uint64_t kSegmentCost = uint64_t{1} << 20;
constexpr uint64_t kSmallBytes = uint64_t{64} << 20;
constexpr double kDispatchesPerRank = 8;
constexpr uint64_t kMaxBackoffRounds = 300;

}  // namespace

struct MergeScheduler::Entry {
  uint64_t id = 0;
  std::shared_ptr<Relation> relation;
  std::unique_ptr<MergeJob> job;
  MergeEstimate estimate;
  std::vector<std::string> inputs;
  uint64_t queued_after = 0;
  size_t helpers = 0;
  std::atomic_bool stop{false};
};

MergeEstimate EstimateMerge(irs::CompactionView candidates) noexcept {
  MergeEstimate estimate;
  for (const auto* candidate : candidates) {
    estimate.rewrite_bytes += irs::LiveBytes(candidate->Meta());
  }
  const auto segments = candidates.size();
  const auto removed = segments > 1 ? segments - 1 : 1;
  estimate.score =
    static_cast<double>(estimate.rewrite_bytes + kSegmentCost * segments) /
    static_cast<double>(removed);
  estimate.lane =
    estimate.rewrite_bytes < kSmallBytes ? MergeLane::Small : MergeLane::Big;
  return estimate;
}

MergeScheduler::MergeScheduler(yaclib::IExecutor& executor, size_t threads)
  : _executor{executor}, _max_threads{threads} {}

MergeScheduler::~MergeScheduler() { Stop(); }

size_t MergeScheduler::BigThreads() const noexcept {
  return _max_threads > 1 ? _max_threads - 1 : 1;
}

size_t MergeScheduler::RelationCap() const noexcept {
  return std::max<size_t>(1, _max_threads / 2);
}

size_t MergeScheduler::GrantBigThreads(size_t wanted) {
  if (_big >= BigThreads() || _threads >= _max_threads) {
    return 0;
  }
  const auto granted =
    std::min({wanted, BigThreads() - _big, _max_threads - _threads});
  _big += granted;
  _threads += granted;
  return granted;
}

void MergeScheduler::Drop(Queue& dropped) {
  for (const auto& entry : dropped) {
    --entry->relation->queued;
    _registry.Drop(entry->id);
  }
  UpdateGauges();
}

template<typename Pred>
void MergeScheduler::DropIf(Queue& dropped, Pred pred) {
  for (auto& entry : _queue) {
    if (pred(*entry)) {
      dropped.emplace_back(std::move(entry));
    }
  }
  std::erase(_queue, nullptr);
  Drop(dropped);
}

template<typename Pred>
void MergeScheduler::StopIf(Pred pred) {
  for (const auto& [raw, entry] : _running) {
    if (pred(*entry)) {
      entry->stop.store(true, std::memory_order_relaxed);
    }
  }
}

void MergeScheduler::UpdateGauges() const {
  metrics::Set(metrics::Gauge::CompactionPending,
               static_cast<int64_t>(_queue.size()));
  metrics::Set(metrics::Gauge::CompactionThreads,
               static_cast<int64_t>(_threads));
}

bool MergeScheduler::NoneRunning() const { return _running.empty(); }

bool MergeScheduler::NoneRunningFor(const Relation* relation) {
  return relation->running == 0;
}

bool MergeScheduler::Admits(const Entry& entry) const {
  if (entry.estimate.lane == MergeLane::Small) {
    return _threads < _max_threads || _small == 0;
  }
  return _big < BigThreads() && _threads < _max_threads &&
         entry.relation->running_big < RelationCap();
}

size_t MergeScheduler::Wanted(const Relation& relation) const {
  absl::MutexLock lock{&_mutex};
  if (_stopping || relation.paused != 0) {
    return 0;
  }
  const auto queued_big = static_cast<size_t>(
    absl::c_count_if(_queue, [&relation](const auto& entry) {
      return entry->relation.get() == &relation &&
             entry->estimate.lane == MergeLane::Big;
    }));
  const auto room = RelationCap() > relation.running_big
                      ? RelationCap() - relation.running_big
                      : 0;
  const auto blocked = queued_big > room ? queued_big - room : 0;
  const auto round = RoundSize();
  const auto used = relation.queued + relation.running - blocked;
  return used < round ? round - used : 0;
}

std::vector<std::string> MergeScheduler::StartRound(Relation& relation) {
  absl::MutexLock lock{&_mutex};
  const auto round = ++relation.round;
  absl::erase_if(relation.blocked, [round](const auto& blocked) {
    return blocked.second < round;
  });
  std::vector<std::string> names;
  names.reserve(relation.blocked.size());
  for (const auto& [name, _] : relation.blocked) {
    names.emplace_back(name);
  }
  return names;
}

void MergeScheduler::Enqueue(const std::shared_ptr<Relation>& relation,
                             std::vector<std::unique_ptr<MergeJob>> jobs) {
  if (jobs.empty()) {
    return;
  }
  Queue entries;
  std::vector<irs::MergeStats> inputs;
  entries.reserve(jobs.size());
  inputs.reserve(jobs.size());
  for (auto& job : jobs) {
    auto& entry = entries.emplace_back(std::make_unique<Entry>());
    entry->relation = relation;
    const auto candidates = job->Candidates();
    entry->estimate = EstimateMerge(candidates);
    SDB_IF_FAILURE("compaction_lane_big") {
      entry->estimate.lane = MergeLane::Big;
    }
    entry->inputs = inputs.emplace_back(DescribeInputs(candidates)).inputs;
    entry->job = std::move(job);
  }
  std::vector<Entry*> started;
  Queue stale;
  {
    absl::MutexLock lock{&_mutex};
    if (!_stopping && relation->paused == 0) {
      for (size_t i = 0; i != entries.size(); ++i) {
        auto& entry = entries[i];
        entry->id = _registry.Queue(relation->id, MergeOrigin::Background,
                                    std::move(inputs[i]));
        entry->queued_after = _dispatched;
        _queue.emplace_back(std::move(entry));
        ++relation->queued;
      }
    }
    Dispatch(started, stale);
    UpdateGauges();
  }
  entries.clear();
  Launch(started, stale);
}

void MergeScheduler::CancelQueued(const Relation& relation) {
  Queue dropped;
  absl::MutexLock lock{&_mutex};
  DropIf(dropped,
         [&](const Entry& entry) { return entry.relation.get() == &relation; });
}

void MergeScheduler::StopRunning(const Relation& relation) {
  absl::MutexLock lock{&_mutex};
  StopIf([&](const Entry& entry) { return entry.relation.get() == &relation; });
}

MergeScheduler::Lease MergeScheduler::TakeOver(
  const std::shared_ptr<Relation>& relation) {
  Queue dropped;
  bool running = false;
  {
    absl::MutexLock lock{&_mutex};
    ++relation->paused;
    const auto of_relation = [&](const Entry& entry) {
      return entry.relation == relation;
    };
    DropIf(dropped, of_relation);
    StopIf(of_relation);
    running = !NoneRunningFor(relation.get());
  }
  if (running) {
    duckdb::TaskScheduler::BlockingScope blocking;
    absl::MutexLock lock{
      &_mutex, absl::Condition(&NoneRunningFor,
                               static_cast<const Relation*>(relation.get()))};
  }
  return Lease{relation.get(), Resume{.scheduler = this}};
}

void MergeScheduler::Resume::operator()(Relation* relation) const noexcept {
  {
    absl::MutexLock lock{&scheduler->_mutex};
    --relation->paused;
  }
  relation->NudgeCompaction();
}

void MergeScheduler::Interrupt() noexcept {
  Queue dropped;
  absl::MutexLock lock{&_mutex};
  _stopping = true;
  dropped.swap(_queue);
  Drop(dropped);
  StopIf([](const Entry&) { return true; });
}

void MergeScheduler::Stop() {
  Interrupt();
  absl::MutexLock lock{&_mutex};
  _mutex.Await(absl::Condition(this, &MergeScheduler::NoneRunning));
}

bool MergeScheduler::TryAcquireThread() noexcept {
  absl::MutexLock lock{&_mutex};
  if (_stopping || GrantBigThreads(1) == 0) {
    return false;
  }
  UpdateGauges();
  return true;
}

void MergeScheduler::ReleaseThread() noexcept {
  std::vector<Entry*> started;
  Queue stale;
  {
    absl::MutexLock lock{&_mutex};
    SDB_ASSERT(_big != 0 && _threads != 0);
    --_big;
    --_threads;
    Dispatch(started, stale);
    UpdateGauges();
  }
  Launch(started, stale);
}

uint64_t MergeScheduler::StartForeground(uint64_t relation_id,
                                         MergeOrigin origin,
                                         irs::MergeStats inputs) {
  absl::MutexLock lock{&_mutex};
  const auto id = _registry.Queue(relation_id, origin, std::move(inputs));
  _registry.Dispatch(id);
  return id;
}

void MergeScheduler::FinishForeground(uint64_t id, MergeOutcome outcome,
                                      irs::MergeStats stats,
                                      std::string error) {
  const auto history_size = CompactHistorySize();
  absl::MutexLock lock{&_mutex};
  _registry.Finish(id, outcome, std::move(stats), std::move(error),
                   history_size);
}

std::vector<MergeRecord> MergeScheduler::Active(uint64_t relation_id) const {
  absl::ReaderMutexLock lock{&_mutex};
  return _registry.Active(relation_id);
}

std::vector<MergeRecord> MergeScheduler::History(uint64_t relation_id) {
  const auto history_size = CompactHistorySize();
  absl::MutexLock lock{&_mutex};
  return _registry.History(relation_id, history_size);
}

void MergeScheduler::Dispatch(std::vector<Entry*>& started, Queue& stale) {
  if (_stopping) {
    return;
  }
  SDB_IF_FAILURE("hold_merge_dispatch") { return; }
  DropIf(stale, [](const Entry& entry) { return entry.job->Stale(); });
  for (;;) {
    auto best = _queue.end();
    double best_rank = 0;
    for (auto it = _queue.begin(); it != _queue.end(); ++it) {
      const auto& entry = **it;
      if (!Admits(entry)) {
        continue;
      }
      const auto age = static_cast<double>(_dispatched - entry.queued_after);
      const auto rank = std::log2(std::max(1.0, entry.estimate.score)) -
                        age / kDispatchesPerRank;
      if (best == _queue.end() || rank < best_rank) {
        best = it;
        best_rank = rank;
      }
    }
    if (best == _queue.end()) {
      break;
    }
    started.reserve(started.size() + 1);
    _running.reserve(_running.size() + 1);
    auto entry = std::move(*best);
    _queue.erase(best);
    auto& relation = *entry->relation;
    --relation.queued;
    ++relation.running;
    ++_threads;
    if (entry->estimate.lane == MergeLane::Small) {
      ++_small;
    } else {
      ++_big;
      ++relation.running_big;
    }
    ++_dispatched;
    _registry.Dispatch(entry->id);
    auto* raw = entry.get();
    _running.emplace(raw, std::move(entry));
    started.emplace_back(raw);
  }
  for (auto* entry : started) {
    if (entry->estimate.lane == MergeLane::Big) {
      entry->helpers = GrantBigThreads(_max_threads / 2);
    }
  }
}

void MergeScheduler::Launch(std::span<Entry* const> started,
                            const Queue& stale) {
  for (const auto& entry : stale) {
    entry->relation->NudgeCompaction();
  }
  for (auto* entry : started) {
    absl::Cleanup dropped = [this, entry] {
      Finish(*entry, MergeResult{
                       .outcome = {.error = irs::CompactionError::Cancelled}});
    };
    yaclib::Submit(_executor,
                   [this, entry, dropped = std::move(dropped)]() mutable {
                     std::move(dropped).Cancel();
                     Execute(*entry).Detach();
                   });
  }
}

yaclib::Future<> MergeScheduler::Execute(Entry& entry) {
  MergeResult result;
  try {
    result = co_await entry.job->Run(entry.stop, entry.helpers);
  } catch (const std::exception& e) {
    result.error = e.what();
    SDB_WARN(SEARCH, "background merge of relation ", entry.relation->id,
             " failed: ", result.error);
  } catch (...) {
    SDB_WARN(SEARCH, "background merge of relation ", entry.relation->id,
             " failed");
  }
  Finish(entry, std::move(result));
  co_return {};
}

void MergeScheduler::Finish(Entry& raw, MergeResult result) noexcept {
  const auto outcome = OutcomeOf(result.outcome);
  const auto history_size = CompactHistorySize();
  std::unique_ptr<Entry> entry;
  std::vector<Entry*> started;
  Queue stale;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _running.find(&raw);
    SDB_ASSERT(it != _running.end());
    entry = std::move(it->second);
    _running.erase(it);
    auto& relation = *entry->relation;
    --relation.running;
    _threads -= 1 + entry->helpers;
    if (entry->estimate.lane == MergeLane::Small) {
      --_small;
    } else {
      _big -= 1 + entry->helpers;
      --relation.running_big;
    }
    if (Produced(outcome)) {
      relation.failures = 0;
    } else if (outcome == MergeOutcome::Failed) {
      ++relation.failures;
      const auto shift = std::min<uint32_t>(relation.failures - 1, 20);
      const auto last =
        relation.round + std::min(kMaxBackoffRounds, uint64_t{1} << shift);
      for (auto& name : entry->inputs) {
        relation.blocked.insert_or_assign(std::move(name), last);
      }
    }
    if (outcome == MergeOutcome::Cancelled && result.outcome.size == 0) {
      _registry.Drop(entry->id);
    } else {
      _registry.Finish(entry->id, outcome, std::move(result.outcome.stats),
                       std::move(result.error), history_size);
    }
    Dispatch(started, stale);
    UpdateGauges();
  }
  const auto relation = entry->relation;
  entry.reset();
  if (outcome != MergeOutcome::Cancelled) {
    relation->NudgeCompaction();
  }
  Launch(started, stale);
}

}  // namespace sdb::search
