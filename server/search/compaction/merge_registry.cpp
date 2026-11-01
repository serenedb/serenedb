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
  RowIdRange rowids;
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
    inputs.rowids = Union(inputs.rowids, segment.rowids);
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

uint64_t MergeRegistry::Queue(uint64_t relation_id, MergeOrigin origin,
                              irs::CompactionView candidates) {
  auto inputs = InputsOf(candidates);
  absl::MutexLock lock{&_mutex};
  const auto id = _next_id++;
  _active.emplace(
    id, MergeRecord{
          .id = id,
          .relation_id = relation_id,
          .origin = origin,
          .queued_us = origin == MergeOrigin::Background ? NowMicros() : 0,
          .stats = std::move(inputs.stats),
          .rowids = inputs.rowids,
        });
  return id;
}

void MergeRegistry::Dispatch(uint64_t id) {
  absl::MutexLock lock{&_mutex};
  if (auto it = _active.find(id); it != _active.end()) {
    it->second.started_us = NowMicros();
  }
}

void MergeRegistry::Drop(uint64_t id) {
  absl::MutexLock lock{&_mutex};
  _active.erase(id);
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
  record.error = std::move(error);
  if (_history.size() == kHistoryCapacity) {
    _history.pop_front();
  }
  _history.emplace_back(std::move(record));
}

std::vector<MergeRecord> MergeRegistry::Active(uint64_t relation_id) const {
  std::vector<MergeRecord> records;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& [_, record] : _active) {
      if (record.relation_id == relation_id) {
        records.emplace_back(record);
      }
    }
  }
  absl::c_sort(records, [](const MergeRecord& lhs, const MergeRecord& rhs) {
    return lhs.id < rhs.id;
  });
  return records;
}

std::vector<MergeRecord> MergeRegistry::History(uint64_t relation_id) const {
  std::vector<MergeRecord> records;
  absl::MutexLock lock{&_mutex};
  absl::c_copy_if(_history, std::back_inserter(records),
                  [relation_id](const MergeRecord& record) {
                    return record.relation_id == relation_id;
                  });
  return records;
}

}  // namespace sdb::search
