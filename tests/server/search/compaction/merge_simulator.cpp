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

#include "merge_simulator.h"

#include <absl/algorithm/container.h>
#include <absl/container/btree_set.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <functional>
#include <iresearch/index/composite_reader_impl.hpp>
#include <map>
#include <memory>
#include <queue>
#include <tuple>
#include <utility>
#include <yaclib/async/contract.hpp>
#include <yaclib/exe/inline.hpp>

#include "search/compaction/merge_scheduler.h"

namespace sdb::search {
namespace {

struct Segment {
  std::string name;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
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
        const auto& reader = readers.emplace_back(irs::SegmentInfo{
          .name = segment.name,
          .docs_count = DocsOf(segment.docs),
          .live_docs_count = DocsOf(segment.live_docs),
          .byte_size = segment.bytes,
        });
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
  absl::btree_set<std::string> reserved;
  uint64_t next_segment = 1;
  uint64_t planned_generation = 0;
};

class Simulation;

class SimulatedJob final : public MergeJob {
 public:
  SimulatedJob(Simulation& simulation, Relation& relation,
               irs::CompactionView candidates);
  ~SimulatedJob() final;

  irs::CompactionView Candidates() const noexcept final { return _view; }

  bool Stale() const noexcept final { return false; }

  yaclib::Future<MergeResult> Run(const std::atomic_bool& stop,
                                  size_t helpers) final;

  void Finish();
  void Cancel();

 private:
  bool Merges(std::string_view name) const noexcept;

  Simulation& _simulation;
  Relation& _relation;
  std::vector<SimulatedSegment> _segments;
  irs::Compaction _view;
  MergeEstimate _estimate;
  absl::Time _queued;
  absl::Time _started;
  size_t _helpers = 0;
  yaclib::Promise<MergeResult> _promise;
};

class Simulation {
 public:
  explicit Simulation(const MergeSimulation& options);
  ~Simulation();

  std::vector<SimulatedEvent> Run(std::vector<SimulatedFlush> trace);

  const MergeSimulation& Options() const noexcept { return _options; }
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
  absl::Time _origin;
  absl::Time _now;
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
  _estimate = EstimateMerge(_view);
}

SimulatedJob::~SimulatedJob() {
  for (const auto& segment : _segments) {
    _relation.reserved.erase(segment.Meta().name);
  }
}

bool SimulatedJob::Merges(std::string_view name) const noexcept {
  return absl::c_any_of(_segments, [&](const SimulatedSegment& segment) {
    return segment.Meta().name == name;
  });
}

yaclib::Future<MergeResult> SimulatedJob::Run(const std::atomic_bool&,
                                              size_t helpers) {
  _helpers = helpers;
  _started = _simulation.Now();
  const auto threads = static_cast<double>(1 + _helpers);
  const auto seconds = static_cast<double>(_estimate.rewrite_bytes) /
                       (_simulation.Options().bytes_per_second * threads);
  _simulation.Started(*this, _started + absl::Seconds(seconds));
  auto [future, promise] = yaclib::MakeContract<MergeResult>();
  _promise = std::move(promise);
  return std::move(future);
}

void SimulatedJob::Finish() {
  SimulatedEvent event{
    .type = SimulatedEventType::Merge,
    .segment = absl::StrCat("_", _relation.next_segment++),
    .queued = _simulation.Since(_queued),
    .started = _simulation.Since(_started),
    .threads = 1 + _helpers,
    .score = _estimate.score,
    .lane = _estimate.lane,
  };
  event.inputs.reserve(_segments.size());
  for (const auto& segment : _segments) {
    event.inputs.emplace_back(segment.Meta().name);
  }
  const auto place = absl::c_find_if(_relation.segments,
                                     [&](const Segment& segment) {
                                       return Merges(segment.name);
                                     }) -
                     _relation.segments.begin();
  std::erase_if(_relation.segments, [&](const Segment& segment) {
    if (!Merges(segment.name)) {
      return false;
    }
    event.docs += segment.live_docs;
    event.input_bytes += segment.bytes;
    return true;
  });
  event.live_docs = event.docs;
  event.bytes = _estimate.rewrite_bytes;
  _relation.segments.insert(_relation.segments.begin() + place,
                            Segment{
                              .name = event.segment,
                              .docs = event.docs,
                              .live_docs = event.live_docs,
                              .bytes = event.bytes,
                            });
  _simulation.Record(_relation, std::move(event));
  _simulation.Finished();
  const auto merged = _segments.size();
  auto promise = std::move(_promise);
  std::move(promise).Set(MergeResult{
    .outcome = {.size = merged, .error = irs::CompactionError::Ok}});
}

void SimulatedJob::Cancel() {
  _simulation.Finished();
  auto promise = std::move(_promise);
  std::move(promise).Set(
    MergeResult{.outcome = {.error = irs::CompactionError::Cancelled}});
}

Simulation::Simulation(const MergeSimulation& options) : _options{options} {
  _scheduler =
    std::make_unique<MergeScheduler>(yaclib::MakeInline(), options.threads);
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
    relation.handle = std::make_shared<MergeScheduler::Relation>(id);
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
  for (const auto& segment : relation.segments) {
    event.segment_bytes += segment.bytes;
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
  });
  Record(relation, SimulatedEvent{
                     .type = SimulatedEventType::Flush,
                     .segment = segment.name,
                     .docs = segment.docs,
                     .live_docs = segment.live_docs,
                     .bytes = segment.bytes,
                   });
  relation.handle->NudgeCompaction();
}

void Simulation::FinishNext() {
  const auto next = _pending.top();
  _pending.pop();
  _now = next.finish;
  next.job->Finish();
}

void Simulation::Plan(Relation& relation) {
  const auto wanted = _scheduler->Wanted(*relation.handle);
  const auto blocked = _scheduler->StartRound(*relation.handle);
  if (wanted == 0) {
    return;
  }
  const SimulatedIndex index{relation.segments};
  irs::SegmentNames taken{relation.reserved.begin(), relation.reserved.end()};
  taken.insert(blocked.begin(), blocked.end());
  const auto groups =
    _options.pick(index, irs::CompactingSegments{taken}, wanted);
  std::vector<std::unique_ptr<MergeJob>> jobs;
  jobs.reserve(groups.size());
  for (const auto& group : groups) {
    jobs.emplace_back(std::make_unique<SimulatedJob>(*this, relation, group));
  }
  _queued += jobs.size();
  _scheduler->Enqueue(relation.handle, std::move(jobs));
}

std::vector<SimulatedEvent> Simulation::Run(std::vector<SimulatedFlush> trace) {
  if (trace.empty()) {
    return {};
  }
  absl::c_stable_sort(trace,
                      [](const SimulatedFlush& lhs, const SimulatedFlush& rhs) {
                        return std::tie(lhs.time, lhs.relation_id) <
                               std::tie(rhs.time, rhs.relation_id);
                      });
  _origin = trace.front().time;
  _now = _origin;
  size_t next = 0;
  while (next != trace.size() || !_pending.empty()) {
    const auto first = _events.size();
    if (_pending.empty() ||
        (next != trace.size() && trace[next].time <= _pending.top().finish)) {
      Flush(trace[next++]);
    } else {
      FinishNext();
    }
    for (auto& [id, relation] : _relations) {
      const auto generation = relation.handle->CompactionGeneration();
      if (std::exchange(relation.planned_generation, generation) !=
          generation) {
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

std::vector<SimulatedEvent> SimulateMerges(std::vector<SimulatedFlush> trace,
                                           const MergeSimulation& options) {
  Simulation simulation{options};
  return simulation.Run(std::move(trace));
}

}  // namespace sdb::search
