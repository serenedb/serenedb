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

#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <chrono>
#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <exception>
#include <functional>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <stop_token>
#include <string_view>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/exe/executor.hpp>

#include "network/io_context.h"
#include "runtime/duck_executor.h"
#include "server/utils/asio_ns.h"

namespace sdb {

struct RuntimeConfig {
  std::uint32_t io_threads = 1;
};

RuntimeConfig ReadRuntimeConfig();

class Runtime final {
 public:
  using clock = std::chrono::steady_clock;

  static Runtime* Current() noexcept { return gCurrent; }
  static Runtime& Get() noexcept { return *gCurrent; }

  explicit Runtime(const RuntimeConfig& config);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  network::IoThreadPool& IoPool() noexcept { return _io; }
  yaclib::IExecutor& Background() noexcept { return _background; }

  // Run func on the background pool; co_awaitable. A func returning a
  // yaclib::Future is flattened (Future unwrap), so callers co_await the inner
  // value.
  template<typename Func>
  auto Run(Func&& func) {
    return yaclib::Run(_background, std::forward<Func>(func));
  }

  // Completes after `d` (best-effort; immediate once RequestStop() has run).
  yaclib::Future<> Delay(clock::duration d);
  void RunAt(duckdb::timestamp_t at, std::function<void()> task);

  // Shutdown: wake every armed Delay and complete future ones immediately.
  // Stop flags are only checked when a sleeper wakes, so without this a loop
  // parked on a long timer holds up its join for the timer's remainder.
  void RequestStop() noexcept;
  std::stop_token StopToken() const noexcept { return _stop.get_token(); }
  // True once RequestStop() has run. From here on Delay never waits, so a
  // retry loop that keeps going burns a core instead of backing off: every
  // such loop must poll this and bail out.
  bool IsStopping() const noexcept { return _stop.stop_requested(); }

  void DrainBackground() noexcept { _background.Drain(); }

 private:
  inline static Runtime* gCurrent = nullptr;

  network::IoThreadPool _io;
  DuckExecutor _background;
  std::stop_source _stop;
  absl::Mutex _delays_mutex;
  absl::flat_hash_set<std::shared_ptr<asio_ns::steady_timer>> _delays;
};

using StopCallback = std::stop_callback<std::function<void()>>;

template<typename Fn>
void StopLogged(std::string_view what, Fn&& fn) noexcept {
  try {
    std::forward<Fn>(fn)();
  } catch (const std::exception& ex) {
    SDB_ERROR(GENERAL, "exception stopping ", what, ": ",
              duckdb::ErrorData{ex}.RawMessage());
  } catch (...) {
    SDB_ERROR(GENERAL, "unknown exception stopping ", what);
  }
}

}  // namespace sdb
