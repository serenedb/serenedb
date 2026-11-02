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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/vector.hpp>
#include <duckdb/parallel/interrupt.hpp>
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

  size_t Claim(size_t threads, const duckdb::InterruptState& interrupt);

  bool Run(size_t unit);

  void PartDone();

  void Stop();

  bool Result();

 private:
  void FailLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_tasks.lock);

  duckdb::vector<duckdb::InterruptState> TakeWoken()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_tasks.lock);

  MergePlan& _plan;
  std::vector<uint64_t> _rank;
  std::vector<std::vector<size_t>> _dependents;
  duckdb::StateWithBlockableTasks _tasks;
  std::vector<size_t> _waiting ABSL_GUARDED_BY(_tasks.lock);
  std::vector<size_t> _ready ABSL_GUARDED_BY(_tasks.lock);
  size_t _left ABSL_GUARDED_BY(_tasks.lock) = 0;
  size_t _running ABSL_GUARDED_BY(_tasks.lock) = 0;
  size_t _parts_left ABSL_GUARDED_BY(_tasks.lock) = 0;
  size_t _held ABSL_GUARDED_BY(_tasks.lock) = kNoUnit;
  bool _failed ABSL_GUARDED_BY(_tasks.lock) = false;
  std::exception_ptr _error ABSL_GUARDED_BY(_tasks.lock);
};

}  // namespace irs
