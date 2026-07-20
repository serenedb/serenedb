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

namespace sdb {

BackgroundScheduler::BackgroundScheduler()
  : _threads(absl::GetFlag(FLAGS_background_threads)) {
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
  gInstance = this;
}

BackgroundScheduler::~BackgroundScheduler() { gInstance = nullptr; }

void BackgroundScheduler::start() {
  _pool = yaclib::MakeFairThreadPool(_threads);
}

void BackgroundScheduler::stop() {
  if (_pool) {
    _pool->SoftStop();
    _pool->Wait();
    _pool = nullptr;
  }
}

yaclib::Future<> BackgroundScheduler::Delay(clock::duration d) {
  auto [f, p] = yaclib::MakeContract<>();
  auto* pool = Server::instance().IoPool();
  if (pool == nullptr || d <= clock::duration::zero()) {
    // No io workers to host the timer (no endpoints / not started / shutdown):
    // skip the backoff rather than block a background thread.
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
  if (_delays_cancelled) {
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

void BackgroundScheduler::CancelDelays() {
  absl::flat_hash_set<std::shared_ptr<asio_ns::steady_timer>> delays;
  {
    absl::MutexLock lock{&_delays_mutex};
    _delays_cancelled = true;
    delays.swap(_delays);
  }
  for (const auto& timer : delays) {
    // cancel() must be serialized with the timer's completion handler: post it
    // onto the io thread that owns the timer.
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

}  // namespace sdb
