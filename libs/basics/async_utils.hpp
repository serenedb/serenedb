////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>

#include <queue>
#include <thread>

#include "basics/empty.hpp"

namespace irs::async_utils {

template<bool UseDelay = true>
class ThreadPool {
 public:
  using Clock = std::chrono::steady_clock;
  using Func = absl::AnyInvocable<void()>;

  ThreadPool() = default;
  explicit ThreadPool(size_t threads);
  ~ThreadPool() { stop(true); }

  void start(size_t threads);
  bool run(Func&& fn, absl::Duration delay = {});
  void stop(bool skip_pending = false) noexcept;  // always a blocking call
  size_t tasks_active() const {
    absl::MutexLock lock{&_m};
    return _state / 2;
  }
  size_t tasks_pending() const {
    absl::MutexLock lock{&_m};
    return _tasks.size();
  }
  size_t threads() const {
    absl::MutexLock lock{&_m};
    return _threads.size();
  }
  // 1st - tasks active(), 2nd - tasks pending(), 3rd - threads()
  std::tuple<size_t, size_t, size_t> stats() const {
    absl::MutexLock lock{&_m};
    return {_state / 2, _tasks.size(), _threads.size()};
  }

 private:
  struct Task {
    explicit Task(absl::Time at, Func&& fn) : at{at}, fn{std::move(fn)} {}

    absl::Time at;
    Func fn;

    bool operator<(const Task& rhs) const noexcept { return rhs.at < at; }
  };

  void Work();

  bool WasStop() const noexcept { return _state % 2 != 0; }

  std::vector<std::thread> _threads;
  mutable absl::Mutex _m;
  [[no_unique_address]] irs::utils::Need<UseDelay, absl::CondVar> _cv;
  std::conditional_t<UseDelay, std::priority_queue<Task>, std::queue<Func>>
    _tasks;
  // stop flag and active tasks counter
  uint64_t _state = 0;
};

}  // namespace irs::async_utils
