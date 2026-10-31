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

#include <absl/cleanup/cleanup.h>

#include <atomic>
#include <cstdint>
#include <duckdb/parallel/task_scheduler.hpp>
#include <utility>

namespace irs {

class BlockingOnce {
 public:
  template<typename F>
  void Call(F&& f) {
    auto state = _state.load(std::memory_order_acquire);
    while (state != kDone) {
      if (state == kIdle && _state.compare_exchange_strong(
                              state, kRunning, std::memory_order_acq_rel)) {
        absl::Cleanup reset = [&] {
          _state.store(kIdle, std::memory_order_release);
          _state.notify_all();
        };
        std::forward<F>(f)();
        std::move(reset).Cancel();
        _state.store(kDone, std::memory_order_release);
        _state.notify_all();
        return;
      }
      if (state == kRunning) {
        duckdb::TaskScheduler::BlockingScope blocking;
        _state.wait(kRunning, std::memory_order_acquire);
        state = _state.load(std::memory_order_acquire);
      }
    }
  }

 private:
  static constexpr uint8_t kIdle = 0;
  static constexpr uint8_t kRunning = 1;
  static constexpr uint8_t kDone = 2;

  std::atomic_uint8_t _state{kIdle};
};

}  // namespace irs
