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

#include "runtime/runtime.h"

#include <absl/flags/flag.h>

#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <yaclib/async/contract.hpp>

#include "server/utils/number_of_cores.h"

ABSL_FLAG(uint64_t, io_threads, 0,
          "IO threads for HTTP and pg-wire connections and for timers (0 = one "
          "per physical core).");

namespace sdb {

RuntimeConfig ReadRuntimeConfig() {
  auto threads = static_cast<std::uint32_t>(absl::GetFlag(FLAGS_io_threads));
  if (threads == 0) {
    // One independent single-threaded asio reactor per PHYSICAL core: the
    // reactors are round-robin pinned with no shared queue (no contention, so
    // no ceiling) and idle ones just park in epoll_wait, but hyper-thread
    // siblings add nothing to epoll-bound reactors -- so physical, not logical,
    // cores.
    threads = static_cast<std::uint32_t>(CountPhysicalCores());
    absl::SetFlag(&FLAGS_io_threads, threads);
  }
  return {.io_threads = threads};
}

Runtime::Runtime(const RuntimeConfig& config)
  : _io{config.io_threads},
    _background{duckdb::TaskScheduler::GetScheduler(
                  irs::DuckDBEngine::Instance().instance()),
                duckdb::SchedulerClass::BACKGROUND} {
  _io.Start();
  gCurrent = this;
}

Runtime::~Runtime() {
  RequestStop();
  _background.Drain();
  _io.Stop();
  gCurrent = nullptr;
}

yaclib::Future<> Runtime::Delay(clock::duration d) {
  auto [f, p] = yaclib::MakeContract<>();
  if (d <= clock::duration::zero()) {
    std::move(p).Set();
    return std::move(f);
  }
  // Arm and register under one lock so a concurrent RequestStop() either sees
  // the not-yet-armed timer (and this call completes immediately) or an armed,
  // registered one it can cancel -- an unregistered armed timer would sleep out
  // its full duration.
  absl::MutexLock lock{&_delays_mutex};
  if (IsStopping()) {
    std::move(p).Set();
    return std::move(f);
  }
  auto timer = std::make_shared<asio_ns::steady_timer>(_io.Next().Context(), d);
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

void Runtime::RunAt(duckdb::timestamp_t at, std::function<void()> task) {
  const std::chrono::microseconds delay{
    at.value - duckdb::Timestamp::GetCurrentTimestamp().value};
  Delay(delay).Detach(_background, [this, task = std::move(task)] {
    if (!IsStopping()) {
      task();
    }
  });
}

void Runtime::RequestStop() noexcept {
  _stop.request_stop();
  absl::flat_hash_set<std::shared_ptr<asio_ns::steady_timer>> delays;
  {
    absl::MutexLock lock{&_delays_mutex};
    delays.swap(_delays);
  }
  for (const auto& timer : delays) {
    // cancel() must be serialized with the timer's completion handler: post it
    // onto the io thread that owns the timer.
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

}  // namespace sdb
