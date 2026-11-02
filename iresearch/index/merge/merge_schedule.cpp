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
#include <utility>

#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

MergeSchedule::MergeSchedule(MergePlan& plan) : _plan{plan} {
  const auto units = plan.Units();
  const bool hold_postings = plan.PostingsPending();
  absl::MutexLock lock{&_mutex};
  _rank.resize(units.size());
  _dependents.resize(units.size());
  _waiting.resize(units.size());
  _left = units.size();
  _ranges_left = plan.TermRanges().size();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto& deps = units[unit].deps;
    _waiting[unit] = deps.size();
    for (const auto dep : deps) {
      SDB_ASSERT(dep < unit);
      _dependents[dep].push_back(unit);
    }
    if (hold_postings && _held == kNoUnit &&
        units[unit].kind == MergeUnitKind::Postings) {
      _held = unit;
      ++_waiting[unit];
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

size_t MergeSchedule::Claim(size_t threads) {
  absl::MutexLock lock{&_mutex};
  if (_failed ? _running == 0 : _left == 0) {
    return kFinished;
  }
  if (_ready.empty()) {
    return kNoUnit;
  }
  if (_held != kNoUnit) {
    size_t left = 0;
    for (const auto& range : _plan.TermRanges()) {
      left += range->PartsLeft();
    }
    if (_running + std::min(left, threads - 1) >= threads) {
      return kNoUnit;
    }
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

void MergeSchedule::FailLocked() {
  _failed = true;
  _ready.clear();
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
  absl::MutexLock lock{&_mutex};
  if (failure && !_error) {
    _error = std::move(failure);
  }
  SDB_ASSERT(_running != 0);
  --_running;
  --_left;
  if (!ok) {
    FailLocked();
    return false;
  }
  if (_failed) {
    return true;
  }
  for (const auto next : _dependents[unit]) {
    if (--_waiting[next] == 0) {
      _ready.push_back(next);
    }
  }
  return true;
}

void MergeSchedule::RangeDone() {
  bool last = false;
  {
    absl::MutexLock lock{&_mutex};
    SDB_ASSERT(_ranges_left != 0);
    last = --_ranges_left == 0;
  }
  if (last && !ReleasePostings(_plan.FinishPostings())) {
    return;
  }
  Wake();
}

bool MergeSchedule::ReleasePostings(bool ok) {
  absl::MutexLock lock{&_mutex};
  if (_held == kNoUnit) {
    return false;
  }
  const auto unit = std::exchange(_held, kNoUnit);
  if (!ok) {
    _plan.Abort();
    FailLocked();
    return true;
  }
  SDB_ASSERT(_waiting[unit] != 0);
  if (--_waiting[unit] == 0 && !_failed) {
    _ready.push_back(unit);
  }
  return true;
}

void MergeSchedule::SetUnits(duckdb::GlobalSourceState* units) {
  absl::MutexLock lock{&_mutex};
  _units = units;
}

void MergeSchedule::Wake() {
  duckdb::GlobalSourceState* units = nullptr;
  {
    absl::MutexLock lock{&_mutex};
    units = _units;
  }
  if (!units) {
    return;
  }
  duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{units->lock};
  units->UnblockTasks();
}

bool MergeSchedule::Result() {
  _plan.RecordWall();
  std::exception_ptr error;
  bool ok = false;
  {
    absl::MutexLock lock{&_mutex};
    error = _error;
    ok = !_failed;
  }
  if (error) {
    std::rethrow_exception(error);
  }
  return ok;
}

}  // namespace irs
