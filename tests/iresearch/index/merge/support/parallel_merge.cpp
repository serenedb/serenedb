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

#include "index/merge/support/parallel_merge.hpp"

#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <exception>
#include <functional>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <numeric>
#include <optional>
#include <random>
#include <thread>
#include <vector>

namespace tests::merge {
namespace {

bool RunUnits(irs::MergePlan& plan, size_t threads,
              const std::function<bool()>& done,
              const std::function<std::optional<size_t>()>& claim,
              const std::function<void(size_t, bool)>& finish) {
  absl::Mutex mutex;
  std::exception_ptr error;
  size_t finished = 0;
  auto work = [&] {
    while (true) {
      std::optional<size_t> unit;
      {
        absl::MutexLock lock{&mutex};
        for (unit = claim(); !unit && !done(); unit = claim()) {
          const auto seen = finished;
          const auto changed = [&] { return finished != seen; };
          mutex.Await(absl::Condition{&changed});
        }
      }
      if (!unit) {
        return;
      }
      bool ok = false;
      try {
        ok = plan.RunUnit(*unit);
      } catch (...) {
        absl::MutexLock lock{&mutex};
        if (!error) {
          error = std::current_exception();
        }
      }
      absl::MutexLock lock{&mutex};
      finish(*unit, ok);
      ++finished;
    }
  };
  std::vector<std::thread> helpers;
  for (size_t i = 1; i < threads; ++i) {
    helpers.emplace_back(work);
  }
  work();
  for (auto& helper : helpers) {
    helper.join();
  }
  if (error) {
    std::rethrow_exception(error);
  }
  return true;
}

bool RunPostings(irs::MergePlan& plan, size_t threads,
                 std::optional<uint64_t> seed) {
  if (!plan.PostingsPending()) {
    return true;
  }
  for (const auto& range : plan.TermRanges()) {
    std::vector<size_t> parts(range->PartCount());
    std::iota(parts.begin(), parts.end(), size_t{0});
    if (seed) {
      std::shuffle(parts.begin(), parts.end(), std::mt19937_64{*seed});
    }
    std::atomic<size_t> next{0};
    std::atomic<bool> ok{true};
    absl::Mutex mutex;
    std::exception_ptr error;
    auto work = [&] {
      for (size_t at; (at = next++) < parts.size();) {
        try {
          if (!range->MergePart(parts[at])) {
            ok = false;
          }
        } catch (...) {
          absl::MutexLock guard{&mutex};
          if (!error) {
            error = std::current_exception();
          }
          ok = false;
        }
      }
    };
    std::vector<std::thread> helpers;
    for (size_t i = 1; i < threads; ++i) {
      helpers.emplace_back(work);
    }
    work();
    for (auto& helper : helpers) {
      helper.join();
    }
    if (error) {
      std::rethrow_exception(error);
    }
    if (!ok) {
      return false;
    }
  }
  return plan.FinishPostings();
}

}  // namespace

bool ThreadedMergeExecutor::Run(irs::MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
    return false;
  }
  if (!RunPostings(plan, _threads, std::nullopt)) {
    return false;
  }
  irs::MergeSchedule schedule{plan};
  RunUnits(
    plan, _threads, [&] { return schedule.Done(); },
    [&] { return schedule.Claim(); },
    [&](size_t unit, bool ok) { schedule.Finish(unit, ok); });
  return schedule.Ok();
}

bool ShuffledMergeExecutor::Run(irs::MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
    return false;
  }
  if (!RunPostings(plan, _threads, _seed)) {
    return false;
  }
  const auto units = plan.Units();
  std::vector<size_t> waiting(units.size());
  std::vector<std::vector<size_t>> dependents(units.size());
  std::vector<size_t> ready;
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto deps = plan.Dependencies(unit);
    waiting[unit] = deps.size();
    for (const auto dep : deps) {
      dependents[dep].push_back(unit);
    }
    if (deps.empty()) {
      ready.push_back(unit);
    }
  }
  std::mt19937_64 rng{_seed};
  size_t left = units.size();
  size_t running = 0;
  bool failed = false;
  RunUnits(
    plan, _threads, [&] { return failed ? running == 0 : left == 0; },
    [&]() -> std::optional<size_t> {
      if (failed || ready.empty()) {
        return std::nullopt;
      }
      const auto at = rng() % ready.size();
      const auto unit = ready[at];
      ready[at] = ready.back();
      ready.pop_back();
      ++running;
      return unit;
    },
    [&](size_t unit, bool ok) {
      --running;
      --left;
      if (!ok) {
        failed = true;
        ready.clear();
        return;
      }
      if (failed) {
        return;
      }
      for (const auto next : dependents[unit]) {
        if (--waiting[next] == 0) {
          ready.push_back(next);
        }
      }
    });
  return !failed;
}

}  // namespace tests::merge
