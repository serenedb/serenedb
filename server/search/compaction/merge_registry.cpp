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

#include "search/compaction/merge_registry.h"

#include <absl/algorithm/container.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <iresearch/index/index_meta.hpp>
#include <iterator>
#include <utility>

#include "search/compaction/merge_planner.h"
#include "server/utils/metrics.h"

namespace sdb::search {
namespace {

int64_t NowMicros() noexcept { return absl::ToUnixMicros(absl::Now()); }

struct Inputs {
  irs::MergeStats stats;
  irs::SegmentRange rowids;
};

Inputs InputsOf(irs::CompactionView candidates) {
  Inputs inputs;
  inputs.stats.inputs.reserve(candidates.size());
  for (const auto* candidate : candidates) {
    const auto segment = DescribeSegment(*candidate);
    inputs.stats.inputs.emplace_back(segment.name);
    inputs.stats.input_docs += segment.docs;
    inputs.stats.input_live_docs += segment.live_docs;
    inputs.stats.input_bytes += segment.bytes;
    inputs.rowids = irs::Union(inputs.rowids, segment.rowids);
  }
  return inputs;
}

}  // namespace

MergeOutcome OutcomeOf(const irs::CompactionResult& result) noexcept {
  switch (result.error) {
    case irs::CompactionError::Fail:
      return MergeOutcome::Failed;
    case irs::CompactionError::Cancelled:
      return MergeOutcome::Cancelled;
    case irs::CompactionError::Busy:
      return MergeOutcome::Busy;
    case irs::CompactionError::Pending:
      return MergeOutcome::Deferred;
    case irs::CompactionError::Ok:
      return result.size == 0 ? MergeOutcome::Nothing : MergeOutcome::Merged;
  }
  return MergeOutcome::Failed;
}

bool Produced(MergeOutcome outcome) noexcept {
  return outcome == MergeOutcome::Merged || outcome == MergeOutcome::Deferred;
}

MergeRegistry& MergeRegistry::Instance() {
  static MergeRegistry gInstance;
  return gInstance;
}

uint64_t MergeRecord::EstimatedBytes() const noexcept {
  return static_cast<uint64_t>(
    irs::LiveBytes(stats.input_bytes, stats.input_live_docs, stats.input_docs));
}

uint64_t MergeRegistry::Queue(uint64_t relation_id,
                              irs::CompactionView candidates,
                              const MergeEstimate& estimate) {
  auto inputs = InputsOf(candidates);
  absl::MutexLock lock{&_mutex};
  const auto id = _next_id++;
  _active.emplace(id, MergeRecord{
                        .id = id,
                        .relation_id = relation_id,
                        .queued_us = NowMicros(),
                        .lane = estimate.lane,
                        .score = estimate.score,
                        .stats = std::move(inputs.stats),
                        .rowids = inputs.rowids,
                      });
  return id;
}

void MergeRegistry::RecordFlushes(
  uint64_t relation_id, std::span<const irs::SubReader* const> segments) {
  const auto now = NowMicros();
  std::vector<MergeRecord> records;
  records.reserve(segments.size());
  for (const auto* segment : segments) {
    auto described = DescribeSegment(*segment);
    auto& record = records.emplace_back(MergeRecord{
      .relation_id = relation_id,
      .event = SegmentEvent::Flush,
      .finished_us = now,
      .output_live_docs = described.live_docs,
      .rowids = described.rowids,
    });
    record.stats.output = std::move(described.name);
    record.stats.output_docs = described.docs;
    record.stats.output_bytes = described.bytes;
  }
  absl::MutexLock lock{&_mutex};
  for (auto& record : records) {
    record.id = _next_id++;
    if (_flushes.size() == kFlushHistoryCapacity) {
      _flushes.pop_front();
    }
    _flushes.emplace_back(std::move(record));
  }
}

void MergeRegistry::Dispatch(uint64_t id, size_t threads) {
  absl::MutexLock lock{&_mutex};
  if (auto it = _active.find(id); it != _active.end()) {
    it->second.started_us = NowMicros();
    it->second.threads = threads;
  }
}

void MergeRegistry::Drop(uint64_t id) {
  absl::MutexLock lock{&_mutex};
  _active.erase(id);
}

uint64_t MergeRegistry::Start(uint64_t relation_id, MergeOrigin origin) {
  absl::MutexLock lock{&_mutex};
  const auto id = _next_id++;
  _active.emplace(id, MergeRecord{
                        .id = id,
                        .relation_id = relation_id,
                        .origin = origin,
                        .started_us = NowMicros(),
                      });
  return id;
}

void MergeRegistry::SetInputs(uint64_t id, irs::CompactionView candidates) {
  auto inputs = InputsOf(candidates);
  absl::MutexLock lock{&_mutex};
  if (auto it = _active.find(id); it != _active.end()) {
    it->second.stats = std::move(inputs.stats);
    it->second.rowids = inputs.rowids;
  }
}

void MergeRegistry::Finish(uint64_t id, MergeOutcome outcome,
                           irs::MergeStats stats, std::string error) {
  absl::MutexLock lock{&_mutex};
  auto it = _active.find(id);
  if (it == _active.end()) {
    return;
  }
  auto record = std::move(it->second);
  _active.erase(it);
  if (outcome == MergeOutcome::Nothing && error.empty()) {
    return;
  }
  if (Produced(outcome)) {
    metrics::Add(metrics::Gauge::CompactionWrittenBytes,
                 static_cast<int64_t>(stats.output_bytes));
  }
  record.outcome = outcome;
  record.finished_us = NowMicros();
  if (!stats.inputs.empty() || stats.profile.TotalNanos() != 0) {
    record.stats = std::move(stats);
  }
  record.output_live_docs = record.stats.output_docs;
  record.error = std::move(error);
  if (_history.size() == kHistoryCapacity) {
    _history.pop_front();
  }
  _history.emplace_back(std::move(record));
}

template<typename Pred>
std::vector<MergeRecord> MergeRegistry::Active(Pred pred) const {
  std::vector<MergeRecord> records;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& [_, record] : _active) {
      if (pred(record)) {
        records.emplace_back(record);
      }
    }
  }
  absl::c_sort(records, [](const MergeRecord& lhs, const MergeRecord& rhs) {
    return lhs.id < rhs.id;
  });
  return records;
}

template<typename Pred>
std::vector<MergeRecord> MergeRegistry::History(Pred pred) const {
  std::vector<MergeRecord> records;
  {
    absl::MutexLock lock{&_mutex};
    absl::c_copy_if(_history, std::back_inserter(records), pred);
    absl::c_copy_if(_flushes, std::back_inserter(records), pred);
  }
  absl::c_stable_sort(records,
                      [](const MergeRecord& lhs, const MergeRecord& rhs) {
                        return lhs.finished_us < rhs.finished_us;
                      });
  return records;
}

std::vector<MergeRecord> MergeRegistry::Active() const {
  return Active([](const MergeRecord&) { return true; });
}

std::vector<MergeRecord> MergeRegistry::Active(uint64_t relation_id) const {
  return Active([relation_id](const MergeRecord& record) {
    return record.relation_id == relation_id;
  });
}

std::vector<MergeRecord> MergeRegistry::History() const {
  return History([](const MergeRecord&) { return true; });
}

std::vector<MergeRecord> MergeRegistry::History(uint64_t relation_id) const {
  return History([relation_id](const MergeRecord& record) {
    return record.relation_id == relation_id;
  });
}

MergeRecorder::~MergeRecorder() {
  if (_id != 0 && !_finished) {
    MergeRegistry::Instance().Finish(_id, MergeOutcome::Failed, {}, {});
  }
}

void MergeRecorder::Start() {
  if (_id == 0) {
    _id = MergeRegistry::Instance().Start(_relation_id, _origin);
  }
}

irs::CompactionPolicy MergeRecorder::Wrap(const irs::CompactionPolicy& policy) {
  return
    [this, &policy](irs::Compaction& candidates, const irs::IndexReader& index,
                    const irs::CompactingSegments& compacting) {
      policy(candidates, index, compacting);
      Begin(candidates);
    };
}

void MergeRecorder::Begin(irs::CompactionView candidates) {
  if (candidates.empty()) {
    return;
  }
  Start();
  MergeRegistry::Instance().SetInputs(_id, candidates);
}

void MergeRecorder::Finish(irs::CompactionResult result) {
  _finished = true;
  if (_id == 0) {
    return;
  }
  MergeRegistry::Instance().Finish(_id, OutcomeOf(result),
                                   std::move(result.stats), {});
}

void MergeRecorder::Fail(std::string error) {
  _finished = true;
  Start();
  MergeRegistry::Instance().Finish(_id, MergeOutcome::Failed, {},
                                   std::move(error));
}

}  // namespace sdb::search
