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

#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <utility>

namespace sdb::search {
namespace {

int64_t NowMicros() noexcept { return absl::ToUnixMicros(absl::Now()); }

}  // namespace

std::string_view MergeOriginName(MergeOrigin origin) noexcept {
  switch (origin) {
    case MergeOrigin::Background:
      return "background";
    case MergeOrigin::Vacuum:
      return "vacuum";
    case MergeOrigin::Sql:
      return "sql";
  }
  return "unknown";
}

std::string_view MergeOutcomeName(MergeOutcome outcome) noexcept {
  switch (outcome) {
    case MergeOutcome::Running:
      return "running";
    case MergeOutcome::Merged:
      return "merged";
    case MergeOutcome::Deferred:
      return "deferred";
    case MergeOutcome::Nothing:
      return "nothing";
    case MergeOutcome::Busy:
      return "busy";
    case MergeOutcome::Failed:
      return "failed";
    case MergeOutcome::Cancelled:
      return "cancelled";
  }
  return "unknown";
}

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

MergeRegistry& MergeRegistry::Instance() {
  static MergeRegistry gInstance;
  return gInstance;
}

uint64_t MergeRegistry::Start(uint64_t relation_id, MergeOrigin origin) {
  absl::MutexLock lock{&_mutex};
  const auto id = _next_id++;
  _running.emplace(id, MergeRecord{
                         .id = id,
                         .relation_id = relation_id,
                         .origin = origin,
                         .started_us = NowMicros(),
                       });
  return id;
}

void MergeRegistry::SetInputs(uint64_t id, const irs::Compaction& candidates) {
  irs::MergeStats inputs;
  inputs.inputs.reserve(candidates.size());
  for (const auto* candidate : candidates) {
    inputs.inputs.emplace_back(candidate->Meta().name);
    inputs.input_docs += candidate->docs_count();
    inputs.input_live_docs += candidate->live_docs_count();
    inputs.input_bytes += candidate->Meta().byte_size;
  }
  absl::MutexLock lock{&_mutex};
  if (auto it = _running.find(id); it != _running.end()) {
    it->second.stats = std::move(inputs);
  }
}

void MergeRegistry::Finish(uint64_t id, MergeOutcome outcome,
                           irs::MergeStats stats, std::string error) {
  absl::MutexLock lock{&_mutex};
  auto it = _running.find(id);
  if (it == _running.end()) {
    return;
  }
  auto record = std::move(it->second);
  _running.erase(it);
  if (outcome == MergeOutcome::Nothing && error.empty()) {
    return;
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

std::vector<MergeRecord> MergeRegistry::Running() const {
  absl::MutexLock lock{&_mutex};
  std::vector<MergeRecord> records;
  records.reserve(_running.size());
  for (const auto& [_, record] : _running) {
    records.emplace_back(record);
  }
  std::sort(records.begin(), records.end(),
            [](const MergeRecord& lhs, const MergeRecord& rhs) {
              return lhs.id < rhs.id;
            });
  return records;
}

std::vector<MergeRecord> MergeRegistry::History() const {
  absl::MutexLock lock{&_mutex};
  return {_history.begin(), _history.end()};
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
      if (candidates.empty()) {
        return;
      }
      Start();
      MergeRegistry::Instance().SetInputs(_id, candidates);
    };
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
