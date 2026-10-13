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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <mutex>
#include <optional>

namespace sdb::search {

struct MergeUnitRun {
  explicit MergeUnitRun(irs::MergePlan& plan)
    : plan{plan}, schedule{plan}, begin{std::chrono::steady_clock::now()} {}

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
    wake.notify_all();
  }

  std::optional<size_t> Next() {
    std::optional<size_t> unit;
    std::unique_lock lock{mutex};
    wake.wait(lock, [&] {
      return schedule.Done() || (unit = schedule.Claim()).has_value();
    });
    return unit;
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
  std::mutex mutex;
  std::condition_variable wake;
  std::exception_ptr error;
};

}  // namespace sdb::search
