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

#include <cstddef>
#include <cstdint>
#include <duckdb/execution/physical_operator_states.hpp>
#include <exception>
#include <limits>
#include <vector>

#include "iresearch/index/merge/merge_plan.hpp"

namespace irs {

class MergeSchedule {
 public:
  static constexpr size_t kNoUnit = std::numeric_limits<size_t>::max();
  static constexpr size_t kFinished = kNoUnit - 1;

  explicit MergeSchedule(MergePlan& plan);

  MergeSchedule(const MergeSchedule&) = delete;
  MergeSchedule& operator=(const MergeSchedule&) = delete;

  size_t Claim(size_t threads);

  bool Run(size_t unit);

  void PartDone() { Wake(); }

  void RangeDone();

  void SetUnits(duckdb::GlobalSourceState* units);

  bool Result();

 private:
  void FailLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  bool ReleasePostings(bool ok);

  void Wake();

  MergePlan& _plan;
  std::vector<uint64_t> _rank;
  std::vector<std::vector<size_t>> _dependents;
  absl::Mutex _mutex;
  std::vector<size_t> _waiting ABSL_GUARDED_BY(_mutex);
  std::vector<size_t> _ready ABSL_GUARDED_BY(_mutex);
  size_t _left ABSL_GUARDED_BY(_mutex) = 0;
  size_t _running ABSL_GUARDED_BY(_mutex) = 0;
  size_t _held ABSL_GUARDED_BY(_mutex) = kNoUnit;
  size_t _ranges_left ABSL_GUARDED_BY(_mutex) = 0;
  bool _failed ABSL_GUARDED_BY(_mutex) = false;
  std::exception_ptr _error ABSL_GUARDED_BY(_mutex);
  duckdb::GlobalSourceState* _units ABSL_GUARDED_BY(_mutex) = nullptr;
};

}  // namespace irs
