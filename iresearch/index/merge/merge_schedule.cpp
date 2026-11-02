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

#include <absl/algorithm/container.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>

#include <algorithm>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/mutex.hpp>
#include <span>
#include <utility>

#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;

void Wake(std::span<const duckdb::InterruptState> tasks) {
  for (const auto& task : tasks) {
    task.Callback();
  }
}

}  // namespace

MergeSchedule::MergeSchedule(MergePlan& plan) : _plan{plan} {
  const auto units = plan.Units();
  _rank.resize(units.size());
  _dependents.resize(units.size());
  _waiting.resize(units.size());
  _left = units.size();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto& deps = units[unit].deps;
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

size_t MergeSchedule::Claim(const duckdb::InterruptState& interrupt) {
  Guard guard{_tasks.lock};
  if (_failed ? _running == 0 : _left == 0) {
    return kFinished;
  }
  if (_ready.empty()) {
    return _tasks.BlockTask(interrupt) ? kNoUnit : kFinished;
  }
  auto best = absl::c_max_element(_ready, [&](size_t lhs, size_t rhs) {
    return _rank[lhs] != _rank[rhs] ? _rank[lhs] < _rank[rhs] : lhs > rhs;
  });
  SDB_IF_FAILURE("irs::MergeSchedule::Shuffle") {
    best = _ready.begin() +
           absl::Uniform<ptrdiff_t>(absl::BitGen{}, 0, std::ssize(_ready));
  }
  const auto unit = *best;
  *best = _ready.back();
  _ready.pop_back();
  ++_running;
  return unit;
}

bool MergeSchedule::Run(size_t unit) {
  bool ok = false;
  std::exception_ptr failure;
  try {
    ok = _plan.RunUnit(unit);
  } catch (...) {
    failure = std::current_exception();
  }
  if (!ok) {
    _plan.Abort();
  }
  duckdb::vector<duckdb::InterruptState> woken;
  {
    Guard guard{_tasks.lock};
    if (failure && !_error) {
      _error = std::move(failure);
    }
    SDB_ASSERT(_running != 0);
    --_running;
    --_left;
    if (!ok) {
      _failed = true;
      _ready.clear();
    } else if (!_failed) {
      for (const auto next : _dependents[unit]) {
        if (--_waiting[next] == 0) {
          _ready.push_back(next);
        }
      }
    }
    if (_failed || _left == 0 || !_ready.empty()) {
      woken = _tasks.TakeBlockedTasks();
    }
  }
  Wake(woken);
  return ok;
}

void MergeSchedule::Stop() {
  duckdb::vector<duckdb::InterruptState> woken;
  {
    Guard guard{_tasks.lock};
    _tasks.PreventBlocking();
    woken = _tasks.TakeBlockedTasks();
  }
  Wake(woken);
}

bool MergeSchedule::Result() {
  _plan.RecordWall();
  std::exception_ptr error;
  bool ok = false;
  {
    Guard guard{_tasks.lock};
    error = _error;
    ok = !_failed;
  }
  if (error) {
    std::rethrow_exception(error);
  }
  return ok;
}

}  // namespace irs
