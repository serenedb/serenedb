////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <atomic>
#include <functional>
#include <magic_enum/magic_enum.hpp>
#include <string_view>
#include <vector>

#include "basics/assert.h"

namespace sdb::app {

// Lifecycle state-machine + signal-driven wait loop. RunServer drives
// each feature's start/stop directly; AppServer no longer owns or
// iterates features. What's left:
//   - state() and setState() for diagnostics (CrashHandler reporter)
//   - wait() that blocks until SIGTERM/SIGINT or explicit beginShutdown
//   - isStopping() for callers that still ask "is the process winding down"
class AppServer {
 public:
  enum class State : int {
    Uninitialized = 0,
    InStart,
    InWait,
    InStop,
    Stopped,
    Aborted,
  };

  class ProgressHandler {
   public:
    std::function<void(State)> state;
  };

  inline static AppServer* gInstance = nullptr;
  static AppServer& Instance() noexcept {
    SDB_ASSERT(gInstance);
    return *gInstance;
  }

  AppServer();
  ~AppServer();

  AppServer(const AppServer&) = delete;
  AppServer& operator=(const AppServer&) = delete;

  bool isStopping() const noexcept;

  // Prod the wait loop into returning. Idempotent. Triggered from the
  // signal handler / Ctrl-C path; RunServer then drives the LIFO of
  // stop() calls. State stays InWait until RunServer flips it to
  // InStop.
  void beginShutdown() noexcept;

  State state() const noexcept {
    return _state.load(std::memory_order_acquire);
  }

  void setState(State new_state) noexcept {
    _state.store(new_state, std::memory_order_release);
    for (auto& r : _progress_reports) {
      if (r.state) {
        r.state(new_state);
      }
    }
  }

  void addReporter(ProgressHandler reporter) {
    _progress_reports.emplace_back(std::move(reporter));
  }

  void parseOptions(int argc, char* argv[]);

  // Block until beginShutdown() or SIGTERM/SIGINT.
  void wait();

 private:
  std::atomic<State> _state{State::Uninitialized};

  struct {
    absl::CondVar cv;
    absl::Mutex mutex;
  } _shutdown_condition;

  std::vector<ProgressHandler> _progress_reports;
  bool _abort_waiting = false;
};

}  // namespace sdb::app
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::app::AppServer::State>(
  sdb::app::AppServer::State state) noexcept {
  using State = sdb::app::AppServer::State;
  switch (state) {
    case State::Uninitialized:
      return "uninitialized";
    case State::InStart:
      return "in start";
    case State::InWait:
      return "in wait";
    case State::InStop:
      return "in stop";
    case State::Stopped:
      return "in stopped";
    case State::Aborted:
      return "in aborted";
  }
  return invalid_tag;
}

}  // namespace magic_enum
