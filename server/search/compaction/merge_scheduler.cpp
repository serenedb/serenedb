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

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cmath>
#include <exception>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
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

}  // namespace

struct MergeScheduler::Entry {
  uint64_t id = 0;
  std::shared_ptr<Relation> relation;
  std::unique_ptr<MergeJob> job;
  MergeEstimate estimate;
  std::vector<std::string> inputs;
  absl::Time queued;
  size_t helpers = 0;
  std::atomic_bool stop{false};
};

class MergeScheduler::Start {
 public:
  Start(MergeScheduler& scheduler, Entry& entry) noexcept
    : _scheduler{&scheduler}, _entry{&entry} {}

  Start(Start&& other) noexcept
    : _scheduler{other._scheduler},
      _entry{std::exchange(other._entry, nullptr)} {}
  Start& operator=(Start&&) = delete;

  ~Start() {
    if (_entry != nullptr) {
      _scheduler->Finish(*_entry, MergeOutcome::Cancelled, {});
    }
  }

  void operator()() {
    _scheduler->Execute(*_entry).Detach();
    _entry = nullptr;
  }

 private:
  MergeScheduler* _scheduler;
  Entry* _entry;
};

MergeEstimate EstimateMerge(irs::CompactionView candidates,
                            uint64_t small_bytes) noexcept {
  MergeEstimate estimate{.segments = candidates.size()};
  long double rewrite = 0;
  for (const auto* candidate : candidates) {
    const auto& meta = candidate->Meta();
    estimate.bytes += meta.byte_size;
    if (meta.docs_count != 0) {
      rewrite += static_cast<long double>(meta.byte_size) *
                 static_cast<long double>(meta.live_docs_count) /
                 static_cast<long double>(meta.docs_count);
    }
  }
  estimate.rewrite_bytes = static_cast<uint64_t>(rewrite);
  const auto removed = estimate.segments > 1 ? estimate.segments - 1 : 1;
  estimate.score = static_cast<double>(estimate.rewrite_bytes +
                                       kSegmentCost * estimate.segments) /
                   static_cast<double>(removed);
  estimate.lane =
    estimate.rewrite_bytes < small_bytes ? MergeLane::Small : MergeLane::Big;
  return estimate;
}

MergeScheduler::MergeScheduler(yaclib::IExecutor& executor, Options options)
  : _executor{executor},
    _options{std::move(options)},
    _registry{_options.registry ? *_options.registry
                                : MergeRegistry::Instance()} {}

MergeScheduler::~MergeScheduler() { Stop(); }

size_t MergeScheduler::BigThreads() const noexcept {
  return _options.threads > 1 ? _options.threads - 1 : 1;
}

size_t MergeScheduler::RelationCap() const noexcept {
  return std::max<size_t>(1, _options.threads / 2);
}

absl::Time MergeScheduler::Now() const {
  return _options.now ? _options.now() : absl::Now();
}

void MergeScheduler::Drop(Queue& dropped) {
  for (const auto& entry : dropped) {
    --entry->relation->_queued;
    _registry.Drop(entry->id);
  }
  UpdateGauges();
}

void MergeScheduler::UpdateGauges() const {
  metrics::Set(metrics::Gauge::CompactionPending,
               static_cast<int64_t>(_queue.size()));
  metrics::Set(metrics::Gauge::CompactionThreads,
               static_cast<int64_t>(_threads));
}

bool MergeScheduler::NoneRunning() const { return _running.empty(); }

bool MergeScheduler::NoneRunningFor(const Relation* relation) {
  return relation->_running == 0;
}

bool MergeScheduler::Admits(const Entry& entry) const {
  if (entry.estimate.lane == MergeLane::Small) {
    return _threads < _options.threads || _small == 0;
  }
  return _big < BigThreads() && _threads < _options.threads &&
         entry.relation->_running_big < RelationCap();
}

size_t MergeScheduler::Wanted(const Relation& relation) const {
  absl::MutexLock lock{&_mutex};
  if (_stopping || relation._paused != 0) {
    return 0;
  }
  const auto round = RoundSize();
  const auto used = relation._queued + relation._running;
  return used < round ? round - used : 0;
}

std::vector<std::string> MergeScheduler::Blocked(Relation& relation) const {
  absl::MutexLock lock{&_mutex};
  const auto now = Now();
  absl::erase_if(relation._blocked,
                 [now](const auto& blocked) { return blocked.second <= now; });
  std::vector<std::string> names;
  names.reserve(relation._blocked.size());
  for (const auto& [name, until] : relation._blocked) {
    names.emplace_back(name);
  }
  return names;
}

size_t MergeScheduler::FreeThreads() const {
  absl::MutexLock lock{&_mutex};
  return _threads < _options.threads ? _options.threads - _threads : 0;
}

void MergeScheduler::Enqueue(const std::shared_ptr<Relation>& relation,
                             std::unique_ptr<MergeJob> job) {
  auto entry = std::make_unique<Entry>();
  entry->relation = relation;
  const auto candidates = job->Candidates();
  entry->estimate = EstimateMerge(candidates, _options.small_bytes);
  SDB_IF_FAILURE("compaction_lane_big") {
    entry->estimate.lane = MergeLane::Big;
  }
  entry->inputs.reserve(candidates.size());
  for (const auto* candidate : candidates) {
    entry->inputs.emplace_back(candidate->Meta().name);
  }
  entry->job = std::move(job);
  {
    absl::MutexLock lock{&_mutex};
    if (!_stopping && relation->_paused == 0) {
      entry->id = _registry.Queue(relation->Id(), candidates, entry->estimate);
      entry->queued = Now();
      _queue.emplace_back(std::move(entry));
      ++relation->_queued;
      UpdateGauges();
    }
  }
  entry.reset();
  Pump();
}

void MergeScheduler::CancelQueued(const Relation& relation) {
  Queue dropped;
  absl::MutexLock lock{&_mutex};
  dropped.reserve(_queue.size());
  for (auto it = _queue.begin(); it != _queue.end();) {
    if ((*it)->relation.get() != &relation) {
      ++it;
      continue;
    }
    dropped.emplace_back(std::move(*it));
    it = _queue.erase(it);
  }
  Drop(dropped);
}

void MergeScheduler::StopRunning(const Relation& relation) {
  absl::MutexLock lock{&_mutex};
  for (const auto& [raw, entry] : _running) {
    if (entry->relation.get() == &relation) {
      entry->stop.store(true, std::memory_order_relaxed);
    }
  }
}

MergeScheduler::Lease MergeScheduler::TakeOver(
  const std::shared_ptr<Relation>& relation) {
  Queue dropped;
  {
    absl::MutexLock lock{&_mutex};
    ++relation->_paused;
    dropped.reserve(relation->_queued);
    for (auto it = _queue.begin(); it != _queue.end();) {
      if ((*it)->relation != relation) {
        ++it;
        continue;
      }
      dropped.emplace_back(std::move(*it));
      it = _queue.erase(it);
    }
    Drop(dropped);
    for (const auto& [raw, entry] : _running) {
      if (entry->relation == relation) {
        entry->stop.store(true, std::memory_order_relaxed);
      }
    }
    _mutex.Await(absl::Condition(&NoneRunningFor,
                                 static_cast<const Relation*>(relation.get())));
  }
  return Lease{*this, relation};
}

MergeScheduler::Lease::Lease(Lease&& other) noexcept
  : _scheduler{std::exchange(other._scheduler, nullptr)},
    _relation{std::move(other._relation)} {}

MergeScheduler::Lease& MergeScheduler::Lease::operator=(
  Lease&& other) noexcept {
  if (this != &other) {
    Release();
    _scheduler = std::exchange(other._scheduler, nullptr);
    _relation = std::move(other._relation);
  }
  return *this;
}

MergeScheduler::Lease::~Lease() { Release(); }

void MergeScheduler::Lease::Release() noexcept {
  if (_scheduler == nullptr) {
    return;
  }
  {
    absl::MutexLock lock{&_scheduler->_mutex};
    --_relation->_paused;
  }
  if (_relation->_nudge) {
    _relation->_nudge();
  }
  _scheduler = nullptr;
  _relation.reset();
}

void MergeScheduler::Interrupt() noexcept {
  Queue dropped;
  absl::MutexLock lock{&_mutex};
  _stopping = true;
  dropped.swap(_queue);
  Drop(dropped);
  for (auto& [raw, entry] : _running) {
    entry->stop.store(true, std::memory_order_relaxed);
  }
}

void MergeScheduler::Stop() {
  Interrupt();
  absl::MutexLock lock{&_mutex};
  _mutex.Await(absl::Condition(this, &MergeScheduler::NoneRunning));
}

bool MergeScheduler::TryAcquireHelper() noexcept {
  absl::MutexLock lock{&_mutex};
  if (_stopping || _big >= BigThreads() || _threads >= _options.threads) {
    return false;
  }
  ++_big;
  ++_threads;
  UpdateGauges();
  return true;
}

void MergeScheduler::ReleaseHelper() noexcept {
  {
    absl::MutexLock lock{&_mutex};
    SDB_ASSERT(_big != 0 && _threads != 0);
    --_big;
    --_threads;
    UpdateGauges();
  }
  Pump();
}

void MergeScheduler::Pump() {
  std::vector<Entry*> started;
  {
    absl::MutexLock lock{&_mutex};
    if (_stopping) {
      return;
    }
    SDB_IF_FAILURE("hold_merge_dispatch") { return; }
    const auto now = Now();
    const auto aging = absl::ToDoubleSeconds(_options.aging);
    for (;;) {
      auto best = _queue.end();
      double best_rank = 0;
      for (auto it = _queue.begin(); it != _queue.end(); ++it) {
        const auto& entry = **it;
        if (!Admits(entry)) {
          continue;
        }
        const auto rank = std::log2(std::max(1.0, entry.estimate.score)) -
                          absl::ToDoubleSeconds(now - entry.queued) / aging;
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
      --relation._queued;
      ++relation._running;
      ++_threads;
      if (entry->estimate.lane == MergeLane::Small) {
        ++_small;
      } else {
        ++_big;
        ++relation._running_big;
        entry->helpers = _options.helpers_per_merge;
      }
      _registry.Dispatch(entry->id, 1 + entry->helpers);
      auto* raw = entry.get();
      _running.emplace(raw, std::move(entry));
      started.emplace_back(raw);
    }
    UpdateGauges();
  }
  for (auto* entry : started) {
    yaclib::Submit(_executor, Start{*this, *entry});
  }
}

yaclib::Future<> MergeScheduler::Execute(Entry& entry) {
  auto outcome = MergeOutcome::Failed;
  std::string error;
  try {
    outcome = co_await entry.job->Run(entry.id, entry.stop, entry.helpers);
  } catch (const std::exception& e) {
    error = e.what();
    SDB_WARN(SEARCH, "background merge of relation ", entry.relation->Id(),
             " failed: ", error);
  } catch (...) {
    SDB_WARN(SEARCH, "background merge of relation ", entry.relation->Id(),
             " failed");
  }
  Finish(entry, outcome, std::move(error));
  co_return {};
}

void MergeScheduler::Finish(Entry& raw, MergeOutcome outcome,
                            std::string error) noexcept {
  std::unique_ptr<Entry> entry;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _running.find(&raw);
    SDB_ASSERT(it != _running.end());
    entry = std::move(it->second);
    _running.erase(it);
    auto& relation = *entry->relation;
    --relation._running;
    --_threads;
    if (entry->estimate.lane == MergeLane::Small) {
      --_small;
    } else {
      --_big;
      --relation._running_big;
    }
    if (Produced(outcome)) {
      relation._failures = 0;
    } else if (outcome == MergeOutcome::Failed) {
      ++relation._failures;
      const auto shift = std::min<uint32_t>(relation._failures - 1, 20);
      const auto until =
        Now() + std::min(_options.max_backoff,
                         _options.backoff * (int64_t{1} << shift));
      for (auto& name : entry->inputs) {
        relation._blocked.insert_or_assign(std::move(name), until);
      }
    }
    UpdateGauges();
  }
  if (outcome == MergeOutcome::Cancelled) {
    _registry.Drop(entry->id);
  } else {
    _registry.Finish(entry->id, outcome, {}, std::move(error));
  }
  const auto relation = entry->relation;
  entry.reset();
  if (outcome != MergeOutcome::Cancelled && relation->_nudge) {
    relation->_nudge();
  }
  Pump();
}

}  // namespace sdb::search
