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

#include <algorithm>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <yaclib/async/contract.hpp>

#include "network/io_context.h"
#include "network/server.h"
#include "server/utils/asio_ns.h"
#include "server/utils/number_of_cores.h"

namespace sdb {

std::uint64_t BackgroundScheduler::AnnBuildBudget() noexcept {
  return std::max<std::uint64_t>(
    1, static_cast<std::uint64_t>(CountLogicalCores()));
}

BackgroundScheduler::BackgroundScheduler() { gInstance = this; }

BackgroundScheduler::~BackgroundScheduler() { gInstance = nullptr; }

void BackgroundScheduler::start() {
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(
    irs::DuckDBEngine::Instance().instance());
  _executor = std::make_unique<DuckExecutor>(
    scheduler, duckdb::SchedulerClass::BACKGROUND);
}

void BackgroundScheduler::stop() {
  if (_executor) {
    _executor->Drain();
  }
}

yaclib::Future<> BackgroundScheduler::Delay(clock::duration d) {
  auto [f, p] = yaclib::MakeContract<>();
  if (d <= clock::duration::zero()) {
    std::move(p).Set();
    return std::move(f);
  }
  // Arm and register under one lock so a concurrent CancelDelays() either sees
  // the not-yet-armed timer (and this call completes immediately) or an armed,
  // registered one it can cancel -- an unregistered armed timer would sleep out
  // its full duration.
  absl::MutexLock lock{&_delays_mutex};
  if (IsStopping()) {
    std::move(p).Set();
    return std::move(f);
  }
  if (!_delays_open) {
    _parked.push_back(std::move(p));
    return std::move(f);
  }
  auto* pool = Server::instance().IoPool();
  SDB_ASSERT(pool);
  auto timer =
    std::make_shared<asio_ns::steady_timer>(pool->Next().Context(), d);
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

void BackgroundScheduler::RunAt(duckdb::timestamp_t at,
                                std::function<void()> task) {
  const std::chrono::microseconds delay{
    at.value - duckdb::Timestamp::GetCurrentTimestamp().value};
  Delay(delay).Detach(*_executor, [this, task = std::move(task)] {
    if (!IsStopping()) {
      task();
    }
  });
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
