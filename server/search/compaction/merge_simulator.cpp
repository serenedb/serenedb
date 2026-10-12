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

#include "search/compaction/merge_simulator.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <functional>
#include <iresearch/index/composite_reader_impl.hpp>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <tuple>
#include <utility>
#include <yaclib/async/contract.hpp>
#include <yaclib/exe/inline.hpp>

#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_scheduler.h"

namespace sdb::search {
namespace {

struct Segment {
  std::string name;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  std::optional<int64_t> rowid_min;
  std::optional<int64_t> rowid_max;
};

uint32_t DocsOf(uint64_t docs) noexcept {
  return static_cast<uint32_t>(std::min<uint64_t>(docs, UINT32_MAX));
}

class SimulatedIndex final
  : public irs::CompositeReaderImpl<std::vector<SimulatedSegment>> {
 public:
  explicit SimulatedIndex(std::span<const Segment> segments)
    : SimulatedIndex{Init{segments}} {}

 private:
  struct Init {
    explicit Init(std::span<const Segment> segments) {
      readers.reserve(segments.size());
      for (const auto& segment : segments) {
        const auto& reader = readers.emplace_back(
          irs::SegmentInfo{
            .name = segment.name,
            .docs_count = DocsOf(segment.docs),
            .live_docs_count = DocsOf(segment.live_docs),
            .byte_size = segment.bytes,
          },
          segment.rowid_min.transform([&](int64_t first) {
            return irs::SegmentRange{.first = first,
                                     .last = *segment.rowid_max};
          }));
        docs_count += reader.docs_count();
        live_docs_count += reader.live_docs_count();
      }
    }

    std::vector<SimulatedSegment> readers;
    uint64_t docs_count = 0;
    uint64_t live_docs_count = 0;
  };

  explicit SimulatedIndex(Init&& init) noexcept
    : irs::CompositeReaderImpl<std::vector<SimulatedSegment>>{
        std::move(init.readers), init.live_docs_count, init.docs_count} {}
};

struct Relation {
  uint64_t id = 0;
  std::shared_ptr<MergeScheduler::Relation> handle;
  std::vector<Segment> segments;
  std::set<std::string, std::less<>> reserved;
  uint64_t next_segment = 1;
  bool nudged = false;
};

class Simulation;

class SimulatedJob final : public MergeJob {
 public:
  SimulatedJob(Simulation& simulation, Relation& relation,
               irs::CompactionView candidates);
  ~SimulatedJob() final;

  irs::CompactionView Candidates() const noexcept final { return _view; }

  yaclib::Future<MergeOutcome> Run(uint64_t id, const std::atomic_bool& stop,
                                   size_t helpers) final;

  void Finish();
  void Cancel();

 private:
  bool Merges(std::string_view name) const noexcept;
  void ReleaseHelpers();

  Simulation& _simulation;
  Relation& _relation;
  std::vector<SimulatedSegment> _segments;
  irs::Compaction _view;
  MergeEstimate _estimate;
  absl::Time _queued;
  absl::Time _started;
  size_t _helpers = 0;
  yaclib::Promise<MergeOutcome> _promise;
};

class Simulation {
 public:
  Simulation(const MergeSimulation& options,
             const std::function<void()>& check_interrupt);
  ~Simulation();

  std::vector<SimulatedEvent> Run(std::vector<SimulatedFlush> trace);

  const MergeSimulation& Options() const noexcept { return _options; }
  MergeScheduler& Scheduler() noexcept { return *_scheduler; }
  absl::Time Now() const noexcept { return _now; }
  absl::Duration Since(absl::Time time) const noexcept {
    return time - _origin;
  }

  void Started(SimulatedJob& job, absl::Time finish);
  void Finished() noexcept { --_running; }
  void Record(const Relation& relation, SimulatedEvent event);

 private:
  struct Pending {
    absl::Time finish;
    uint64_t sequence = 0;
    SimulatedJob* job = nullptr;

    bool operator>(const Pending& other) const noexcept {
      return std::tie(finish, sequence) >
             std::tie(other.finish, other.sequence);
    }
  };

  Relation& RelationOf(uint64_t id);
  void Flush(const SimulatedFlush& flush);
  void FinishNext();
  void Plan(Relation& relation);

  const MergeSimulation& _options;
  const std::function<void()>& _check_interrupt;
  absl::Time _origin;
  absl::Time _now;
  MergeRegistry _registry;
  std::map<uint64_t, Relation> _relations;
  std::unique_ptr<MergeScheduler> _scheduler;
  std::priority_queue<Pending, std::vector<Pending>, std::greater<>> _pending;
  uint64_t _sequence = 0;
  size_t _queued = 0;
  size_t _running = 0;
  std::vector<SimulatedEvent> _events;
};

SimulatedJob::SimulatedJob(Simulation& simulation, Relation& relation,
                           irs::CompactionView candidates)
  : _simulation{simulation}, _relation{relation}, _queued{simulation.Now()} {
  _segments.reserve(candidates.size());
  _view.reserve(candidates.size());
  for (const auto* candidate : candidates) {
    _relation.reserved.emplace(candidate->Meta().name);
    _view.emplace_back(&_segments.emplace_back(
      static_cast<const SimulatedSegment&>(*candidate)));
  }
  _estimate = EstimateMerge(_view, simulation.Options().small_bytes);
}

SimulatedJob::~SimulatedJob() {
  for (const auto& segment : _segments) {
    _relation.reserved.erase(segment.Meta().name);
  }
}

bool SimulatedJob::Merges(std::string_view name) const noexcept {
  return std::ranges::any_of(_segments, [&](const SimulatedSegment& segment) {
    return segment.Meta().name == name;
  });
}

void SimulatedJob::ReleaseHelpers() {
  for (; _helpers != 0; --_helpers) {
    _simulation.Scheduler().ReleaseHelper();
  }
}

yaclib::Future<MergeOutcome> SimulatedJob::Run(uint64_t,
                                               const std::atomic_bool&,
                                               size_t helpers) {
  auto& scheduler = _simulation.Scheduler();
  while (_helpers != helpers && scheduler.TryAcquireHelper()) {
    ++_helpers;
  }
  _started = _simulation.Now();
  const auto threads = static_cast<double>(1 + _helpers);
  const auto seconds = static_cast<double>(_estimate.rewrite_bytes) /
                       (_simulation.Options().bytes_per_second * threads);
  _simulation.Started(*this, _started + absl::Seconds(seconds));
  auto [future, promise] = yaclib::MakeContract<MergeOutcome>();
  _promise = std::move(promise);
  return std::move(future);
}

void SimulatedJob::Finish() {
  SimulatedEvent event{
    .type = SegmentEvent::Merge,
    .segment = absl::StrCat("_", _relation.next_segment++),
    .queued = _simulation.Since(_queued),
    .started = _simulation.Since(_started),
    .threads = 1 + _helpers,
    .score = _estimate.score,
    .lane = _estimate.lane,
  };
  ReleaseHelpers();
  event.inputs.reserve(_segments.size());
  for (const auto& segment : _segments) {
    event.inputs.emplace_back(segment.Meta().name);
  }
  std::erase_if(_relation.segments, [&](const Segment& segment) {
    if (!Merges(segment.name)) {
      return false;
    }
    event.docs += segment.live_docs;
    event.input_bytes += segment.bytes;
    if (segment.rowid_min) {
      event.rowid_min = std::min(event.rowid_min.value_or(*segment.rowid_min),
                                 *segment.rowid_min);
      event.rowid_max = std::max(event.rowid_max.value_or(*segment.rowid_max),
                                 *segment.rowid_max);
    }
    return true;
  });
  event.live_docs = event.docs;
  event.bytes = _estimate.rewrite_bytes;
  _relation.segments.emplace_back(Segment{
    .name = event.segment,
    .docs = event.docs,
    .live_docs = event.live_docs,
    .bytes = event.bytes,
    .rowid_min = event.rowid_min,
    .rowid_max = event.rowid_max,
  });
  _simulation.Record(_relation, std::move(event));
  _simulation.Finished();
  auto promise = std::move(_promise);
  std::move(promise).Set(MergeOutcome::Merged);
}

void SimulatedJob::Cancel() {
  ReleaseHelpers();
  _simulation.Finished();
  auto promise = std::move(_promise);
  std::move(promise).Set(MergeOutcome::Cancelled);
}

Simulation::Simulation(const MergeSimulation& options,
                       const std::function<void()>& check_interrupt)
  : _options{options}, _check_interrupt{check_interrupt} {
  _scheduler = std::make_unique<MergeScheduler>(
    yaclib::MakeInline(), MergeScheduler::Options{
                            .threads = options.threads,
                            .helpers_per_merge = options.helpers_per_merge,
                            .small_bytes = options.small_bytes,
                            .aging = options.aging,
                            .now = [this] { return _now; },
                            .registry = &_registry,
                            .gauges = false,
                          });
}

Simulation::~Simulation() {
  _scheduler->Interrupt();
  while (!_pending.empty()) {
    auto* job = _pending.top().job;
    _pending.pop();
    job->Cancel();
  }
}

Relation& Simulation::RelationOf(uint64_t id) {
  auto [it, inserted] = _relations.try_emplace(id);
  auto& relation = it->second;
  if (inserted) {
    relation.id = id;
    relation.handle = std::make_shared<MergeScheduler::Relation>(
      id, [&relation] { relation.nudged = true; });
  }
  return relation;
}

void Simulation::Started(SimulatedJob& job, absl::Time finish) {
  _pending.push(
    Pending{.finish = finish, .sequence = _sequence++, .job = &job});
  --_queued;
  ++_running;
}

void Simulation::Record(const Relation& relation, SimulatedEvent event) {
  event.at = Since(_now);
  event.relation_id = relation.id;
  event.segments = relation.segments.size();
  long double covered = 0;
  std::optional<int64_t> low;
  std::optional<int64_t> high;
  for (const auto& segment : relation.segments) {
    event.segment_bytes += segment.bytes;
    if (!segment.rowid_min) {
      continue;
    }
    covered += static_cast<long double>(*segment.rowid_max) -
               static_cast<long double>(*segment.rowid_min) + 1;
    low = std::min(low.value_or(*segment.rowid_min), *segment.rowid_min);
    high = std::max(high.value_or(*segment.rowid_max), *segment.rowid_max);
  }
  if (low) {
    event.overlap =
      static_cast<double>(covered / (static_cast<long double>(*high) -
                                     static_cast<long double>(*low) + 1));
  }
  _events.emplace_back(std::move(event));
}

void Simulation::Flush(const SimulatedFlush& flush) {
  _now = flush.time;
  auto& relation = RelationOf(flush.relation_id);
  const auto& segment = relation.segments.emplace_back(Segment{
    .name = absl::StrCat("_", relation.next_segment++),
    .docs = flush.docs,
    .live_docs = flush.live_docs,
    .bytes = flush.bytes,
    .rowid_min = flush.rowid_min,
    .rowid_max = flush.rowid_max,
  });
  Record(relation, SimulatedEvent{
                     .type = SegmentEvent::Flush,
                     .segment = segment.name,
                     .docs = segment.docs,
                     .live_docs = segment.live_docs,
                     .bytes = segment.bytes,
                     .rowid_min = segment.rowid_min,
                     .rowid_max = segment.rowid_max,
                   });
  relation.nudged = true;
}

void Simulation::FinishNext() {
  const auto next = _pending.top();
  _pending.pop();
  _now = next.finish;
  next.job->Finish();
}

void Simulation::Plan(Relation& relation) {
  const auto wanted = _scheduler->Wanted(*relation.handle);
  const auto blocked = _scheduler->Blocked(*relation.handle);
  const SimulatedIndex index{relation.segments};
  irs::CompactingSegments taken{relation.reserved.begin(),
                                relation.reserved.end()};
  taken.insert(blocked.begin(), blocked.end());
  const auto groups =
    PickMerges(_options.policy, index, std::move(taken), wanted);
  for (const auto& group : groups) {
    ++_queued;
    _scheduler->Enqueue(relation.handle,
                        std::make_unique<SimulatedJob>(*this, relation, group));
  }
}

std::vector<SimulatedEvent> Simulation::Run(std::vector<SimulatedFlush> trace) {
  if (trace.empty()) {
    return {};
  }
  std::ranges::stable_sort(
    trace, [](const SimulatedFlush& lhs, const SimulatedFlush& rhs) {
      return std::tie(lhs.time, lhs.relation_id) <
             std::tie(rhs.time, rhs.relation_id);
    });
  _origin = trace.front().time;
  _now = _origin;
  size_t next = 0;
  while (next != trace.size() || !_pending.empty()) {
    if (_check_interrupt) {
      _check_interrupt();
    }
    const auto first = _events.size();
    if (_pending.empty() ||
        (next != trace.size() && trace[next].time <= _pending.top().finish)) {
      Flush(trace[next++]);
    } else {
      FinishNext();
    }
    for (auto& [id, relation] : _relations) {
      if (std::exchange(relation.nudged, false)) {
        Plan(relation);
      }
    }
    for (auto i = first; i != _events.size(); ++i) {
      _events[i].merges_queued = _queued;
      _events[i].merges_running = _running;
    }
  }
  return std::move(_events);
}

}  // namespace

std::vector<SimulatedEvent> SimulateMerges(
  std::vector<SimulatedFlush> trace, const MergeSimulation& options,
  const std::function<void()>& check_interrupt) {
  Simulation simulation{options, check_interrupt};
  return simulation.Run(std::move(trace));
}

}  // namespace sdb::search
