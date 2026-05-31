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

#include <atomic>
#include <functional>
#include <magic_enum/magic_enum.hpp>
#include <string_view>

#include "basics/assert.h"

namespace sdb::app {

// Lifecycle state-machine + signal-driven wait loop. RunServer drives
// each feature's start/stop directly; AppServer no longer owns or
// iterates features. What's left:
//   - state() and setState() for diagnostics (CrashHandler reporter)
//   - wait() that blocks until SIGTERM/SIGINT (polls lifecycle::IsStopping)
class AppServer {
 public:
  enum class State : int {
    InStart,
    InWait,
    InStop,
    Stopped,
  };

  static AppServer& Instance() noexcept {
    SDB_ASSERT(gInstance);
    return *gInstance;
  }

  AppServer() {
    SDB_ASSERT(gInstance == nullptr, "AppServer is a singleton");
    gInstance = this;
  }
  ~AppServer() { gInstance = nullptr; }

  AppServer(const AppServer&) = delete;
  AppServer& operator=(const AppServer&) = delete;

  State state() const noexcept {
    return _state.load(std::memory_order_acquire);
  }

  void setState(State new_state) noexcept {
    _state.store(new_state, std::memory_order_release);
    if (_state_hook) {
      _state_hook(new_state);
    }
  }

  // Install a single state-change observer. Today's only caller wires
  // CrashHandler::SetState() so /tmp/crash bundles record where in the
  // lifecycle we were when something blew up. setState() invokes the
  // hook synchronously; the hook MUST be noexcept-equivalent.
  void setStateHook(std::function<void(State)> hook) noexcept {
    _state_hook = std::move(hook);
  }

  void parseOptions(int argc, char* argv[]);

  // Block until lifecycle::IsStopping() flips (signal handler).
  void wait();

 private:
  inline static AppServer* gInstance = nullptr;

  std::atomic<State> _state{State::InStart};
  std::function<void(State)> _state_hook;
};

}  // namespace sdb::app
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::app::AppServer::State>(
  sdb::app::AppServer::State state) noexcept {
  using State = sdb::app::AppServer::State;
  switch (state) {
    case State::InStart:
      return "in start";
    case State::InWait:
      return "in wait";
    case State::InStop:
      return "in stop";
    case State::Stopped:
      return "in stopped";
  }
  return invalid_tag;
}

}  // namespace magic_enum
