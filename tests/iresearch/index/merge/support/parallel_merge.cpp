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

#include <condition_variable>
#include <exception>
#include <functional>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <vector>

namespace tests::merge {
namespace {

bool RunUnits(irs::MergePlan& plan, size_t threads,
              const std::function<std::optional<size_t>()>& claim,
              const std::function<void(size_t, bool)>& finish,
              const std::function<bool()>& done) {
  std::mutex mutex;
  std::condition_variable wake;
  std::exception_ptr error;
  auto work = [&] {
    std::unique_lock lock{mutex};
    while (true) {
      std::optional<size_t> unit;
      wake.wait(lock, [&] { return done() || (unit = claim()).has_value(); });
      if (!unit) {
        return;
      }
      lock.unlock();
      bool ok = false;
      try {
        ok = plan.RunUnit(*unit);
      } catch (...) {
        std::lock_guard guard{mutex};
        if (!error) {
          error = std::current_exception();
        }
      }
      lock.lock();
      finish(*unit, ok);
      wake.notify_all();
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

}  // namespace

bool ThreadedMergeExecutor::Run(irs::MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
    return false;
  }
  irs::MergeSchedule schedule{plan};
  RunUnits(
    plan, _threads, [&] { return schedule.Claim(); },
    [&](size_t unit, bool ok) { schedule.Finish(unit, ok); },
    [&] { return schedule.Done(); });
  return schedule.Ok();
}

bool ShuffledMergeExecutor::Run(irs::MergePlan& plan) {
  if (plan.OrderPending() && !plan.RunOrder()) {
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
    plan, _threads,
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
    },
    [&] { return failed ? running == 0 : left == 0; });
  return !failed;
}

}  // namespace tests::merge
