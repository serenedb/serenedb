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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <mutex>
#include <optional>
#include <utility>

namespace sdb::search {

struct MergeUnitRun {
  MergeUnitRun(irs::MergePlan& plan,
               std::chrono::steady_clock::time_point begin,
               bool hold_postings = false)
    : plan{plan}, schedule{plan}, begin{begin} {
    if (!hold_postings) {
      return;
    }
    const auto units = plan.Units();
    for (size_t unit = 0; unit != units.size(); ++unit) {
      if (units[unit].kind == irs::MergeUnitKind::Postings) {
        schedule.Hold(unit);
        held = unit;
        postings.store(true, std::memory_order_release);
        return;
      }
    }
  }

  bool MayClaim(size_t threads) const {
    if (!postings.load(std::memory_order_acquire)) {
      return true;
    }
    size_t left = 0;
    for (const auto& range : plan.TermRanges()) {
      left += range->PartsLeft();
    }
    return schedule.Running() + std::min(left, threads - 1) < threads;
  }

  void Wake() {
    std::lock_guard lock{mutex};
    if (waker) {
      waker();
    }
  }

  void ReleasePostings(bool ok) {
    std::lock_guard lock{mutex};
    if (!held) {
      return;
    }
    postings.store(false, std::memory_order_release);
    const auto unit = *std::exchange(held, std::nullopt);
    if (ok) {
      schedule.Release(unit);
    } else {
      schedule.Fail();
    }
    if (waker) {
      waker();
    }
  }

  void SetWaker(std::function<void()> callback) {
    std::lock_guard lock{mutex};
    waker = std::move(callback);
  }

  void Run(size_t unit) {
    bool ok = false;
    std::exception_ptr failure;
    try {
      ok = plan.RunUnit(unit);
    } catch (...) {
      failure = std::current_exception();
    }
    std::lock_guard lock{mutex};
    if (failure && !error) {
      error = std::move(failure);
    }
    schedule.Finish(unit, ok);
  }

  bool Finish() {
    plan.SetWallNanos(static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - begin)
        .count()));
    if (error) {
      std::rethrow_exception(error);
    }
    return schedule.Ok();
  }

  irs::MergePlan& plan;
  irs::MergeSchedule schedule;
  std::chrono::steady_clock::time_point begin;
  absl::Mutex mutex;
  std::exception_ptr error;
  std::optional<size_t> held;
  std::atomic<bool> postings{false};
  std::function<void()> waker;
};

}  // namespace sdb::search
