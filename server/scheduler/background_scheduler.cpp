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

#include "scheduler/background_scheduler.h"

#include <absl/flags/flag.h>

#include <algorithm>
#include <memory>
#include <yaclib/async/contract.hpp>

#include "basics/asio_ns.h"
#include "basics/number_of_cores.h"
#include "network/io_context.h"
#include "network/server.h"

ABSL_FLAG(uint64_t, background_threads, 0,
          "Number of background worker threads (drop / cleanup / maintenance "
          "tasks; later object-store prefetch). 0 = auto-detect.");

ABSL_FLAG(uint64_t, ann_build_threads, 0,
          "Workers one ANN (HNSW/IVF) graph build may use, including the "
          "calling thread. Drawn from a pool of its own, not from "
          "--background_threads. 0 = auto-detect from core count.");

namespace sdb {

std::uint64_t BackgroundScheduler::AnnBuildThreads() noexcept {
  const auto configured = absl::GetFlag(FLAGS_ann_build_threads);
  if (configured != 0) {
    return configured;
  }
  const auto cores = static_cast<std::uint64_t>(CountLogicalCores());
  if (cores <= 48) {
    return std::max<std::uint64_t>(1, std::min<std::uint64_t>(8, cores));
  }
  return cores <= 64 ? 12 : 16;
}

std::uint64_t BackgroundScheduler::AnnBuildBudget() noexcept {
  return std::max<std::uint64_t>(
    1, static_cast<std::uint64_t>(CountLogicalCores()));
}

BackgroundScheduler::BackgroundScheduler()
  : _threads(absl::GetFlag(FLAGS_background_threads)),
    _ann_threads(AnnBuildThreads()) {
  // Pool size = max(logical_cores / 4, 2): floor 2 on small boxes, scaling at
  // quarter-rate on big ones. The compaction gate (max concurrent CPU-heavy
  // merges, in SearchEngine) derives from this as pool - 1: merges may use all
  // but one thread, on which the light refresh / cleanup / drop tasks
  // interleave. Quarter-rate keeps merges from contending with the cpu query
  // pool (= all logical cores).
  if (_threads == 0) {
    _threads = std::max<std::uint64_t>(2, CountLogicalCores() / 4);
  }
  absl::SetFlag(&FLAGS_background_threads, _threads);
  absl::SetFlag(&FLAGS_ann_build_threads, _ann_threads);
  gInstance = this;
}

BackgroundScheduler::~BackgroundScheduler() { gInstance = nullptr; }

void BackgroundScheduler::start() {
  _pool = yaclib::MakeFairThreadPool(_threads);
  // Sized by the whole ANN budget, not one build's cap: several builds can be
  // in flight at once and their helpers all land here. One less than the
  // budget, because each build's own calling thread is worker 0 and runs a
  // share itself.
  _ann_pool = yaclib::MakeFairThreadPool(
    std::max<std::uint64_t>(1, AnnBuildBudget() - 1));
}

void BackgroundScheduler::stop() {
  if (_ann_pool) {
    _ann_pool->SoftStop();
    _ann_pool->Wait();
    _ann_pool = nullptr;
  }
  if (_pool) {
    _pool->SoftStop();
    _pool->Wait();
    _pool = nullptr;
  }
}

yaclib::Future<> BackgroundScheduler::Delay(clock::duration d) {
  auto [f, p] = yaclib::MakeContract<>();
  if (d <= clock::duration::zero()) {
    std::move(p).Set();
    return std::move(f);
  }
  auto* pool = Server::instance().IoPool();
  if (pool == nullptr) {
    // No io worker to host the timer. At boot the pool is still coming up, so
    // park -- completing here would turn the caller's backoff loop into a spin
    // (drops scheduled from tombstones run before StartIoPool). After
    // CancelDelays the pool is gone for good and completing now is the point:
    // it is how a loop gets to look at its stop flag.
    absl::MutexLock lock{&_delays_mutex};
    if (!_delays_open) {
      _parked.push_back(std::move(p));
      return std::move(f);
    }
    std::move(p).Set();
    return std::move(f);
  }
  auto& ctx = pool->Next().Context();
  auto timer = std::make_shared<asio_ns::steady_timer>(ctx, d);
  // Arm and register under one lock so a concurrent CancelDelays() either sees
  // the not-yet-armed timer (and this call completes immediately) or an armed,
  // registered one it can cancel -- an unregistered armed timer would sleep out
  // its full duration.
  absl::MutexLock lock{&_delays_mutex};
  if (IsStopping()) {
    std::move(p).Set();
    return std::move(f);
  }
  timer->async_wait(
    [this, timer, p = std::move(p)](const asio_ns::error_code&) mutable {
      {
        absl::MutexLock lock{&_delays_mutex};
        _delays.erase(timer);
      }
      // Runs on an io thread: trivial promise-set only, no background work
      // here.
      std::move(p).Set();
    });
  _delays.insert(std::move(timer));
  return std::move(f);
}

void BackgroundScheduler::OpenDelays() {
  std::vector<yaclib::Promise<>> parked;
  {
    absl::MutexLock lock{&_delays_mutex};
    _delays_open = true;
    parked.swap(_parked);
  }
  // Outside the lock: setting a promise resumes its coroutine inline, and that
  // continuation calls straight back into Delay.
  for (auto& p : parked) {
    std::move(p).Set();
  }
}

void BackgroundScheduler::CancelDelays() {
  absl::flat_hash_set<std::shared_ptr<asio_ns::steady_timer>> delays;
  std::vector<yaclib::Promise<>> parked;
  {
    absl::MutexLock lock{&_delays_mutex};
    _delays_cancelled.store(true, std::memory_order_release);
    // A shutdown before OpenDelays (startup threw, or Ctrl-C during boot)
    // still has to release the parked waiters, or their loops never resume to
    // see the stop flag and their frames leak.
    _delays_open = true;
    delays.swap(_delays);
    parked.swap(_parked);
  }
  for (auto& p : parked) {
    std::move(p).Set();
  }
  for (const auto& timer : delays) {
    // cancel() must be serialized with the timer's completion handler: post it
    // onto the io thread that owns the timer.
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

}  // namespace sdb
