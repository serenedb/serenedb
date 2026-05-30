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
// each feature's validateOptions/prepare/start/stop/unprepare directly;
// AppServer no longer owns or iterates features. What's left:
//   - state() and setState() for diagnostics (CrashHandler reporter)
//   - wait() that blocks until SIGTERM/SIGINT or explicit beginShutdown
//   - isStopping() for callers that still ask "is the process winding down"
class AppServer {
 public:
  enum class State : int {
    Uninitialized = 0,
    InValidateOptions,
    InPrepare,
    InStart,
    InWait,
    InShutdown,
    InStop,
    InUnprepare,
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

  explicit AppServer(const char* binary_path);
  ~AppServer();

  AppServer(const AppServer&) = delete;
  AppServer& operator=(const AppServer&) = delete;

  bool isStopping() const noexcept;

  // Set state == InShutdown (and prod the wait loop). Idempotent.
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

  const char* getBinaryPath() const noexcept { return _binary_path; }

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
  const char* _binary_path;
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
    case State::InValidateOptions:
      return "in validate options";
    case State::InPrepare:
      return "in prepare";
    case State::InStart:
      return "in start";
    case State::InWait:
      return "in wait";
    case State::InShutdown:
      return "in beginShutdown";
    case State::InStop:
      return "in stop";
    case State::InUnprepare:
      return "in unprepare";
    case State::Stopped:
      return "in stopped";
    case State::Aborted:
      return "in aborted";
  }
  return invalid_tag;
}

}  // namespace magic_enum
