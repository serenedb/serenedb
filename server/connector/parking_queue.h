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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <deque>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace sdb::connector {

enum class TakeResult : uint8_t { Item, End, Parked };

template<typename T>
class ParkingQueue {
 public:
  void Push(T item) {
    std::lock_guard lock{_mu};
    _ready.push_back(std::move(item));
  }

  void Close() {
    std::lock_guard lock{_mu};
    _closed = true;
  }

  TakeResult Take(duckdb::TableFunctionInput& input, T& out,
                  absl::FunctionRef<std::optional<T>()> produce) {
    std::unique_lock lock{_mu};
    if (!_ready.empty()) {
      out = std::move(_ready.front());
      _ready.pop_front();
      return TakeResult::Item;
    }
    if (_closed) {
      return TakeResult::End;
    }
    if (_producing) {
      if (!input.interrupt_state ||
          input.results_execution_mode !=
            duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR) {
        return TakeResult::End;
      }
      _parked.push_back(*input.interrupt_state);
      input.async_result = duckdb::AsyncResultType::BLOCKED;
      return TakeResult::Parked;
    }
    _producing = true;
    lock.unlock();
    for (;;) {
      std::optional<T> item;
      try {
        item = produce();
      } catch (...) {
        StopProducing(false);
        throw;
      }
      if (!item) {
        StopProducing(true);
        return TakeResult::End;
      }
      lock.lock();
      if (_parked.empty()) {
        // No free tasks to handle item
        _producing = false;
        out = std::move(*item);
        return TakeResult::Item;
      }
      _ready.push_back(std::move(*item));
      auto waiter = std::move(_parked.back());
      _parked.pop_back();
      lock.unlock();
      waiter.Callback();
    }
  }

 private:
  void StopProducing(bool closed) {
    std::vector<duckdb::InterruptState> parked;
    {
      std::lock_guard lock{_mu};
      _producing = false;
      _closed = _closed || closed;
      parked.swap(_parked);
    }
    for (const auto& state : parked) {
      state.Callback();
    }
  }

  std::mutex _mu;
  std::deque<T> _ready;
  std::vector<duckdb::InterruptState> _parked;
  bool _producing = false;
  bool _closed = false;
};

}  // namespace sdb::connector
