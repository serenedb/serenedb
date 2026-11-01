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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <deque>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <string>
#include <vector>

#include "search/compaction/rowid_range.h"

namespace sdb::search {

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

enum class MergeLane : uint8_t {
  Small = 0,
  Big,
};

struct MergeEstimate {
  uint64_t rewrite_bytes = 0;
  double score = 0;
  MergeLane lane = MergeLane::Big;
};

struct MergeRecord {
  uint64_t id = 0;
  uint64_t relation_id = 0;
  MergeOrigin origin = MergeOrigin::Background;
  MergeOutcome outcome = MergeOutcome::Running;
  int64_t queued_us = 0;
  int64_t started_us = 0;
  int64_t finished_us = 0;
  irs::MergeStats stats;
  uint64_t output_live_docs = 0;
  RowIdRange rowids;
  std::string error;

  bool Queued() const noexcept { return started_us == 0; }
  uint64_t EstimatedBytes() const noexcept;
};

class MergeRegistry {
 public:
  static constexpr size_t kHistoryCapacity = 256;

  static MergeRegistry& Instance();

  uint64_t Queue(uint64_t relation_id, irs::CompactionView candidates);
  void Dispatch(uint64_t id);
  void Drop(uint64_t id);
  uint64_t Start(uint64_t relation_id, MergeOrigin origin);
  void SetInputs(uint64_t id, irs::CompactionView candidates);
  void Finish(uint64_t id, MergeOutcome outcome, irs::MergeStats stats,
              std::string error);

  std::vector<MergeRecord> Active(uint64_t relation_id) const;
  std::vector<MergeRecord> History(uint64_t relation_id) const;

 private:
  mutable absl::Mutex _mutex;
  uint64_t _next_id ABSL_GUARDED_BY(_mutex) = 1;
  irs::containers::NodeHashMap<uint64_t, MergeRecord> _active
    ABSL_GUARDED_BY(_mutex);
  std::deque<MergeRecord> _history ABSL_GUARDED_BY(_mutex);
};

class MergeRecorder {
 public:
  MergeRecorder(uint64_t relation_id, MergeOrigin origin) noexcept
    : _relation_id{relation_id}, _origin{origin} {}
  MergeRecorder(const MergeRecorder&) = delete;
  MergeRecorder& operator=(const MergeRecorder&) = delete;
  ~MergeRecorder();

  void Begin(irs::CompactionView candidates);
  void Finish(irs::CompactionResult result);
  void Fail(std::string error);

 private:
  void Start();

  uint64_t _relation_id;
  MergeOrigin _origin;
  uint64_t _id = 0;
  bool _finished = false;
};

}  // namespace sdb::search
