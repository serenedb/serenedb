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

#include "basics/async_utils.hpp"

#include <absl/strings/str_cat.h>

#include "basics/assert.h"
#include "basics/log.h"

using namespace std::chrono_literals;

namespace irs::async_utils {

template<bool UseDelay>
ThreadPool<UseDelay>::ThreadPool(size_t threads,
                                 std::basic_string_view<Char> name) {
  start(threads, name);
}

template<bool UseDelay>
void ThreadPool<UseDelay>::start(size_t threads,
                                 std::basic_string_view<Char> name) {
  absl::MutexLock lock{&_m};
  SDB_ASSERT(_threads.empty());
  _threads.reserve(threads);
  for (size_t i = 0; i != threads; ++i) {
    _threads.emplace_back([this, name] {
      if (!name.empty()) {
        SDB_ASSERT(std::char_traits<Char>::length(name.data()) == name.size());
        SetThreadName(name.data());
      }
      Work();
    });
  }
}

template<bool UseDelay>
bool ThreadPool<UseDelay>::run(Func&& fn, absl::Duration delay) {
  SDB_ASSERT(fn);
  if constexpr (UseDelay) {
    auto at = absl::Now() + delay;
    absl::MutexLock lock{&_m};
    if (WasStop()) {
      return false;
    }
    _tasks.emplace(at, std::move(fn));
    _cv.notify_one();
  } else {
    absl::MutexLock lock{&_m};
    if (WasStop()) {
      return false;
    }
    _tasks.push(std::move(fn));
  }
  return true;
}

template<bool UseDelay>
void ThreadPool<UseDelay>::stop(bool skip_pending) noexcept {
  absl::ReleasableMutexLock lock{&_m};
  if (skip_pending) {
    _tasks = decltype(_tasks){};
  }
  if (WasStop()) {
    return;
  }
  _state |= 1;
  auto threads = std::move(_threads);
  if constexpr (UseDelay) {
    _cv.notify_all();
  }
  lock.Release();
  for (auto& t : threads) {
    t.join();
  }
}

template<bool UseDelay>
void ThreadPool<UseDelay>::Work() {
  absl::MutexLock lock{&_m};
  while (true) {
    while (!_tasks.empty()) {
      Func fn;
      if constexpr (UseDelay) {
        auto& top = _tasks.top();
        if (top.at > absl::Now()) {
          _cv.WaitWithDeadline(&_m, top.at);
          continue;
        }
        fn = std::move(const_cast<Func&>(top.fn));
      } else {
        fn = std::move(_tasks.front());
      }
      _tasks.pop();
      _state += 2;
      _m.unlock();
      try {
        fn();
      } catch (...) {
      }
      _m.lock();
      _state -= 2;
    }
    if (WasStop()) {
      return;
    }
    if constexpr (UseDelay) {
      _cv.Wait(&_m);
    } else {
      auto cond = [&] { return !_tasks.empty() || WasStop(); };
      _m.Await(absl::Condition{&cond});
    }
  }
}

template class ThreadPool<true>;
template class ThreadPool<false>;

}  // namespace irs::async_utils
