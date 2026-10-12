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

#include "iresearch/index/merge/merge_schedule.hpp"

#include <algorithm>

#include "iresearch/index/merge/merge_plan.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

MergeSchedule::MergeSchedule(const MergePlan& plan) {
  const auto units = plan.Units();
  _rank.resize(units.size());
  _dependents.resize(units.size());
  _waiting.resize(units.size());
  _left = units.size();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto deps = plan.Dependencies(unit);
    _waiting[unit] = deps.size();
    for (const auto dep : deps) {
      SDB_ASSERT(dep < unit);
      _dependents[dep].push_back(unit);
    }
  }
  for (size_t unit = units.size(); unit-- != 0;) {
    uint64_t after = 0;
    for (const auto next : _dependents[unit]) {
      after = std::max(after, _rank[next]);
    }
    _rank[unit] = units[unit].cost + after;
    if (_waiting[unit] == 0) {
      _ready.push_back(unit);
    }
  }
}

std::optional<size_t> MergeSchedule::Claim() {
  std::lock_guard lock{_mutex};
  if (_failed || _ready.empty()) {
    return std::nullopt;
  }
  const auto best =
    std::ranges::max_element(_ready, [&](size_t lhs, size_t rhs) {
      return _rank[lhs] != _rank[rhs] ? _rank[lhs] < _rank[rhs] : lhs > rhs;
    });
  const auto unit = *best;
  *best = _ready.back();
  _ready.pop_back();
  ++_running;
  return unit;
}

size_t MergeSchedule::Ready() const {
  std::lock_guard lock{_mutex};
  return _failed ? 0 : _ready.size();
}

void MergeSchedule::Finish(size_t unit, bool ok) {
  std::lock_guard lock{_mutex};
  SDB_ASSERT(_running != 0);
  --_running;
  --_left;
  if (!ok) {
    _failed = true;
    _ready.clear();
    return;
  }
  if (_failed) {
    return;
  }
  for (const auto next : _dependents[unit]) {
    if (--_waiting[next] == 0) {
      _ready.push_back(next);
    }
  }
}

bool MergeSchedule::Done() const {
  std::lock_guard lock{_mutex};
  return _failed ? _running == 0 : _left == 0;
}

bool MergeSchedule::Ok() const {
  std::lock_guard lock{_mutex};
  return !_failed;
}

}  // namespace irs
