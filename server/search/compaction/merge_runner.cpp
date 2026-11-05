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

#include "search/compaction/merge_runner.h"

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
#include <limits>
#include <numeric>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/submit.hpp>

#include "server/utils/metrics.h"

namespace sdb::search {
namespace {

constexpr uint64_t kLastSlotBytes = uint64_t{64} << 20;
constexpr uint64_t kCappedSlotsTopBytes = uint64_t{4} << 30;
constexpr uint64_t kMaxBackoffRounds = 300;

}  // namespace

struct MergeRunner::Entry {
  uint64_t id = 0;
  std::shared_ptr<Relation> relation;
  std::unique_ptr<MergeJob> job;
  std::vector<std::string> inputs;
  uint64_t bytes = 0;
  std::atomic_bool stop{false};
};

uint64_t MergeBytes(irs::CompactionView candidates) noexcept {
  uint64_t bytes = 0;
  for (const auto* candidate : candidates) {
    bytes += irs::LiveBytes(candidate->Meta());
  }
  return bytes;
}

uint64_t MergeSizeCap(size_t slots, size_t merges) noexcept {
  SDB_ASSERT(merges < slots);
  const auto free = slots - merges;
  const auto capped = std::min(std::max<size_t>(2, slots / 2), slots - 1);
  if (free > capped) {
    return std::numeric_limits<uint64_t>::max();
  }
  const auto span =
    std::log2(static_cast<double>(kCappedSlotsTopBytes / kLastSlotBytes));
  return static_cast<uint64_t>(static_cast<double>(kLastSlotBytes) *
                               std::exp2(span * static_cast<double>(free - 1) /
                                         static_cast<double>(capped)));
}

MergeRunner::MergeRunner(yaclib::IExecutor& executor, size_t slots)
  : _executor{executor}, _slots{std::max<size_t>(2, slots)} {}

MergeRunner::~MergeRunner() { Stop(); }

bool MergeRunner::Admits(uint64_t bytes) const {
  SDB_IF_FAILURE("hold_merge_start") { return false; }
  return _merges < _slots && bytes <= MergeSizeCap(_slots, _merges);
}

void MergeRunner::Hold() { ++_merges; }

MergeRunner::Waiting MergeRunner::Release() {
  SDB_ASSERT(_merges != 0);
  --_merges;
  return TakeWaiting();
}

bool MergeRunner::NoneRunning() const { return _running.empty(); }

bool MergeRunner::NoneRunningFor(const Relation* relation) {
  return relation->state.running == 0;
}

void MergeRunner::Wait(const std::shared_ptr<Relation>& relation) {
  if (!std::exchange(StateOf(*relation).waiting, true)) {
    _waiting.emplace_back(relation);
  }
}

void MergeRunner::Unwait(const Relation& relation) {
  if (!StateOf(relation).waiting) {
    return;
  }
  std::erase_if(_waiting, [&](const std::shared_ptr<Relation>& waiting)
                            ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
                              if (waiting.get() != &relation) {
                                return false;
                              }
                              StateOf(*waiting).waiting = false;
                              return true;
                            });
}

MergeRunner::Waiting MergeRunner::TakeWaiting() {
  for (const auto& relation : _waiting) {
    StateOf(*relation).waiting = false;
  }
  return std::exchange(_waiting, {});
}

template<typename Pred>
void MergeRunner::StopIf(Pred pred) {
  for (const auto& [raw, entry] : _running) {
    if (pred(*entry)) {
      entry->stop.store(true, std::memory_order_relaxed);
    }
  }
}

void MergeRunner::UpdateGauges() const {
  metrics::Set(metrics::Gauge::CompactionPending,
               static_cast<int64_t>(_waiting.size()));
  metrics::Set(metrics::Gauge::CompactionThreads,
               static_cast<int64_t>(_merges));
}

size_t MergeRunner::Wanted(const Relation& relation) const {
  absl::MutexLock lock{&_mutex};
  const auto& state = StateOf(relation);
  if (_stopping || state.paused != 0) {
    return 0;
  }
  return std::sub_sat(MergesPerRound(), state.running);
}

std::vector<std::string> MergeRunner::StartRound(Relation& relation) {
  absl::MutexLock lock{&_mutex};
  auto& state = StateOf(relation);
  const auto round = ++state.round;
  absl::erase_if(state.blocked, [round](const auto& blocked) {
    return blocked.second < round;
  });
  std::vector<std::string> names;
  names.reserve(state.blocked.size());
  for (const auto& [name, _] : state.blocked) {
    names.emplace_back(name);
  }
  return names;
}

size_t MergeRunner::Start(const std::shared_ptr<Relation>& relation,
                          std::vector<std::unique_ptr<MergeJob>> jobs) {
  std::vector<std::unique_ptr<Entry>> entries;
  std::vector<irs::MergeStats> inputs;
  entries.reserve(jobs.size());
  inputs.reserve(jobs.size());
  for (auto& job : jobs) {
    auto& entry = entries.emplace_back(std::make_unique<Entry>());
    entry->relation = relation;
    const auto candidates = job->Candidates();
    entry->bytes = MergeBytes(candidates);
    SDB_IF_FAILURE("compaction_big_merge") {
      entry->bytes = std::numeric_limits<uint64_t>::max();
    }
    entry->inputs = inputs.emplace_back(DescribeInputs(candidates)).inputs;
    entry->job = std::move(job);
  }
  std::vector<Entry*> started;
  {
    absl::MutexLock lock{&_mutex};
    auto& state = StateOf(*relation);
    if (!_stopping && state.paused == 0) {
      started.reserve(entries.size());
      _running.reserve(_running.size() + entries.size());
      for (size_t i = 0; i != entries.size(); ++i) {
        auto& entry = entries[i];
        if (!Admits(entry->bytes)) {
          continue;
        }
        Hold();
        ++state.running;
        entry->id = _registry.Queue(relation->id, MergeOrigin::Background,
                                    std::move(inputs[i]));
        _registry.Dispatch(entry->id);
        auto* raw = entry.get();
        _running.emplace(raw, std::move(entry));
        started.emplace_back(raw);
      }
      if (started.size() == entries.size()) {
        Unwait(*relation);
      } else {
        Wait(relation);
      }
    }
    UpdateGauges();
  }
  entries.clear();
  Launch(started);
  return started.size();
}

void MergeRunner::Forget(const Relation& relation) {
  absl::MutexLock lock{&_mutex};
  Unwait(relation);
  UpdateGauges();
}

void MergeRunner::StopRunning(const Relation& relation) {
  absl::MutexLock lock{&_mutex};
  StopIf([&](const Entry& entry) { return entry.relation.get() == &relation; });
}

MergeRunner::Lease MergeRunner::TakeOver(
  const std::shared_ptr<Relation>& relation) {
  bool running = false;
  {
    absl::MutexLock lock{&_mutex};
    ++StateOf(*relation).paused;
    Unwait(*relation);
    StopIf([&](const Entry& entry) { return entry.relation == relation; });
    running = !NoneRunningFor(relation.get());
    UpdateGauges();
  }
  if (running) {
    duckdb::TaskScheduler::BlockingScope blocking;
    absl::MutexLock lock{
      &_mutex, absl::Condition(&NoneRunningFor,
                               static_cast<const Relation*>(relation.get()))};
  }
  return Lease{relation.get(), Resume{.runner = this}};
}

void MergeRunner::Resume::operator()(Relation* relation) const noexcept {
  {
    absl::MutexLock lock{&runner->_mutex};
    --runner->StateOf(*relation).paused;
  }
  relation->NudgeCompaction();
}

void MergeRunner::Interrupt() noexcept {
  absl::MutexLock lock{&_mutex};
  _stopping = true;
  TakeWaiting();
  StopIf([](const Entry&) { return true; });
  UpdateGauges();
}

void MergeRunner::Stop() {
  Interrupt();
  absl::MutexLock lock{&_mutex};
  _mutex.Await(absl::Condition(this, &MergeRunner::NoneRunning));
}

bool MergeRunner::TryHoldBig() noexcept {
  absl::MutexLock lock{&_mutex};
  if (_stopping || !Admits(std::numeric_limits<uint64_t>::max())) {
    return false;
  }
  Hold();
  UpdateGauges();
  return true;
}

void MergeRunner::ReleaseBig() noexcept {
  Waiting waiting;
  {
    absl::MutexLock lock{&_mutex};
    waiting = Release();
    UpdateGauges();
  }
  for (const auto& relation : waiting) {
    relation->NudgeCompaction();
  }
}

uint64_t MergeRunner::StartForeground(uint64_t relation_id, MergeOrigin origin,
                                      irs::MergeStats inputs) {
  absl::MutexLock lock{&_mutex};
  const auto id = _registry.Queue(relation_id, origin, std::move(inputs));
  _registry.Dispatch(id);
  return id;
}

void MergeRunner::FinishForeground(uint64_t id, MergeOutcome outcome,
                                   irs::MergeStats stats, std::string error) {
  const auto history_size = CompactHistorySize();
  absl::MutexLock lock{&_mutex};
  _registry.Finish(id, outcome, std::move(stats), std::move(error),
                   history_size);
}

std::vector<MergeRecord> MergeRunner::Active(uint64_t relation_id) const {
  absl::ReaderMutexLock lock{&_mutex};
  return _registry.Active(relation_id);
}

std::vector<MergeRecord> MergeRunner::History(uint64_t relation_id) {
  const auto history_size = CompactHistorySize();
  absl::MutexLock lock{&_mutex};
  return _registry.History(relation_id, history_size);
}

void MergeRunner::Launch(std::span<Entry* const> started) {
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

yaclib::Future<> MergeRunner::Execute(Entry& entry) {
  MergeResult result;
  try {
    result = co_await entry.job->Run(entry.stop);
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

void MergeRunner::Finish(Entry& raw, MergeResult result) noexcept {
  const auto outcome = OutcomeOf(result.outcome);
  const auto history_size = CompactHistorySize();
  std::unique_ptr<Entry> entry;
  Waiting waiting;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _running.find(&raw);
    SDB_ASSERT(it != _running.end());
    entry = std::move(it->second);
    _running.erase(it);
    auto& state = StateOf(*entry->relation);
    --state.running;
    waiting = Release();
    if (Produced(outcome)) {
      state.failures = 0;
    } else if (outcome == MergeOutcome::Failed) {
      ++state.failures;
      const auto shift = std::min<uint32_t>(state.failures - 1, 20);
      const auto last =
        state.round + std::min(kMaxBackoffRounds, uint64_t{1} << shift);
      for (auto& name : entry->inputs) {
        state.blocked.insert_or_assign(std::move(name), last);
      }
    }
    if (outcome == MergeOutcome::Cancelled && result.outcome.size == 0) {
      _registry.Drop(entry->id);
    } else {
      _registry.Finish(entry->id, outcome, std::move(result.outcome.stats),
                       std::move(result.error), history_size);
    }
    UpdateGauges();
  }
  const auto relation = entry->relation;
  entry.reset();
  if (outcome != MergeOutcome::Cancelled) {
    relation->NudgeCompaction();
  }
  for (const auto& other : waiting) {
    other->NudgeCompaction();
  }
}

}  // namespace sdb::search
