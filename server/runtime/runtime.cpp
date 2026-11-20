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
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <yaclib/async/contract.hpp>

#include "server/utils/number_of_cores.h"

ABSL_FLAG(uint64_t, io_threads, 0,
          "IO threads for HTTP and pg-wire connections and for timers (0 = one "
          "per physical core).");

namespace sdb {

std::uint32_t ReadIoThreads() {
  auto threads = static_cast<std::uint32_t>(absl::GetFlag(FLAGS_io_threads));
  if (threads == 0) {
    threads = static_cast<std::uint32_t>(CountPhysicalCores());
    absl::SetFlag(&FLAGS_io_threads, threads);
  }
  return threads;
}

Runtime::Runtime(std::uint32_t io_threads)
  : _io{io_threads},
    _background{duckdb::TaskScheduler::GetScheduler(
                  irs::DuckDBEngine::Instance().instance()),
                duckdb::SchedulerClass::BACKGROUND} {
  _io.Start();
  gInstance = this;
}

Runtime::~Runtime() {
  RequestStop();
  _io.Stop();
  _background.Drain();
  gInstance = nullptr;
}

yaclib::Future<> Runtime::Delay(std::chrono::steady_clock::duration duration) {
  auto [future, promise] = yaclib::MakeContract<>();
  if (duration <= std::chrono::steady_clock::duration::zero()) {
    std::move(promise).Set();
    return std::move(future);
  }
  absl::MutexLock lock{&_mutex};
  if (IsStopping()) {
    std::move(promise).Set();
    return std::move(future);
  }
  auto timer =
    std::make_shared<asio_ns::steady_timer>(_io.Next().Context(), duration);
  timer->async_wait([this, timer, promise = std::move(promise)](
                      const asio_ns::error_code&) mutable {
    {
      absl::MutexLock lock{&_mutex};
      _delays.erase(timer);
    }
    std::move(promise).Set();
  });
  _delays.insert(std::move(timer));
  return std::move(future);
}

void Runtime::RequestStop() noexcept {
  irs::containers::FlatHashSet<std::shared_ptr<asio_ns::steady_timer>> delays;
  {
    absl::MutexLock lock{&_mutex};
    _stopping.store(true, std::memory_order_release);
    delays.swap(_delays);
  }
  for (const auto& timer : delays) {
    asio_ns::post(timer->get_executor(), [timer] { timer->cancel(); });
  }
}

}  // namespace sdb
