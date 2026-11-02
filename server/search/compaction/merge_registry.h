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

#pragma once

#include <absl/container/btree_map.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_profile.hpp>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::search {

inline constexpr std::string_view kMergeStepColumns[] = {
  "prepare_ms", "norms_ms",    "columns_ms", "columns_commit_ms",
  "ann_ms",     "postings_ms", "commit_ms",
};
static_assert(std::size(kMergeStepColumns) == irs::kMergeStepCount);

enum class MergeOrigin : uint8_t {
  Background = 0,
  Vacuum,
  Sql,
};

enum class MergeOutcome : uint8_t {
  Running = 0,
  Merged,
  Deferred,
  Nothing,
  Busy,
  Failed,
  Cancelled,
};

MergeOutcome OutcomeOf(const irs::CompactionResult& result) noexcept;

bool Produced(MergeOutcome outcome) noexcept;

irs::MergeStats DescribeInputs(irs::CompactionView candidates);

size_t CompactHistorySize();

struct MergeRecord {
  uint64_t id = 0;
  uint64_t relation_id = 0;
  MergeOrigin origin = MergeOrigin::Background;
  MergeOutcome outcome = MergeOutcome::Running;
  int64_t queued_us = 0;
  int64_t started_us = 0;
  int64_t finished_us = 0;
  irs::MergeStats stats;
  std::string error;

  bool Queued() const noexcept { return started_us == 0; }
  uint64_t EstimatedBytes() const noexcept;
};

class MergeRegistry {
 public:
  static constexpr size_t kDefaultHistorySize = 256;

  uint64_t Queue(uint64_t relation_id, MergeOrigin origin,
                 irs::MergeStats inputs);
  void Dispatch(uint64_t id);
  void Drop(uint64_t id);
  void Finish(uint64_t id, MergeOutcome outcome, irs::MergeStats stats,
              std::string error, size_t history_size);

  std::vector<MergeRecord> Active(uint64_t relation_id) const;
  std::vector<MergeRecord> History(uint64_t relation_id, size_t history_size);

 private:
  void TrimHistory(size_t size);

  uint64_t _next_id = 1;
  absl::btree_map<uint64_t, MergeRecord> _active;
  std::deque<MergeRecord> _history;
};

}  // namespace sdb::search
