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

#include <absl/synchronization/mutex.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace irs {

class MergePlan;

class MergeSchedule {
 public:
  static constexpr size_t kNoUnit = std::numeric_limits<size_t>::max();

  explicit MergeSchedule(const MergePlan& plan);

  size_t Claim();

  void Hold(size_t unit);

  void Release(size_t unit);

  void Fail();

  size_t Running() const;

  void Finish(size_t unit, bool ok);

  bool Done() const;

  bool Ok() const;

 private:
  mutable absl::Mutex _mutex;
  std::vector<uint64_t> _rank;
  std::vector<std::vector<size_t>> _dependents;
  std::vector<size_t> _waiting;
  std::vector<size_t> _ready;
  size_t _left = 0;
  size_t _running = 0;
  bool _failed = false;
};

}  // namespace irs
