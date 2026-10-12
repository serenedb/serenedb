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

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <deque>
#include <iresearch/index/index_writer.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::search {

enum class MergeOrigin : uint8_t {
  Background = 0,
  Vacuum,
  Sql,
};

std::string_view MergeOriginName(MergeOrigin origin) noexcept;

enum class MergeOutcome : uint8_t {
  Running = 0,
  Merged,
  Deferred,
  Nothing,
  Busy,
  Failed,
  Cancelled,
};

inline constexpr size_t kMergeOutcomeCount =
  static_cast<size_t>(MergeOutcome::Cancelled) + 1;

std::string_view MergeOutcomeName(MergeOutcome outcome) noexcept;

MergeOutcome OutcomeOf(const irs::CompactionResult& result) noexcept;

bool Produced(MergeOutcome outcome) noexcept;

struct MergeRecord {
  uint64_t id = 0;
  uint64_t relation_id = 0;
  MergeOrigin origin = MergeOrigin::Background;
  MergeOutcome outcome = MergeOutcome::Running;
  int64_t started_us = 0;
  int64_t finished_us = 0;
  irs::MergeStats stats{};
  std::string error{};
};

class MergeRegistry {
 public:
  static constexpr size_t kHistoryCapacity = 256;

  static MergeRegistry& Instance();

  uint64_t Start(uint64_t relation_id, MergeOrigin origin);
  void SetInputs(uint64_t id, irs::CompactionView candidates);
  void Finish(uint64_t id, MergeOutcome outcome, irs::MergeStats stats,
              std::string error);

  std::vector<MergeRecord> Running() const;
  std::vector<MergeRecord> History() const;

 private:
  mutable absl::Mutex _mutex;
  uint64_t _next_id = 1;
  absl::flat_hash_map<uint64_t, MergeRecord> _running;
  std::deque<MergeRecord> _history;
};

class MergeRecorder {
 public:
  MergeRecorder(uint64_t relation_id, MergeOrigin origin) noexcept
    : _relation_id{relation_id}, _origin{origin} {}
  MergeRecorder(const MergeRecorder&) = delete;
  MergeRecorder& operator=(const MergeRecorder&) = delete;
  ~MergeRecorder();

  irs::CompactionPolicy Wrap(const irs::CompactionPolicy& policy);
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
