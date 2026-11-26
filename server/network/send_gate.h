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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <duckdb/execution/operator/persistent/copy_output_gate.hpp>
#include <duckdb/parallel/interrupt.hpp>

#include "network/cpu_resumer.h"

namespace sdb::network {

inline constexpr size_t kSendHighWater = 256 * 1024;

class SendGate final : public duckdb::CopyOutputGate,
                       private duckdb::StateWithBlockableTasks {
 public:
  SendGate(const std::atomic<size_t>& written, CpuResumer& task)
    : _written{written}, _task{task} {}

  void Wrote(size_t committed, size_t largest_write) {
    _largest_write.store(largest_write, std::memory_order_relaxed);
    _committed.store(committed, std::memory_order_relaxed);
  }

  bool BlockSink(const duckdb::InterruptState& state) final {
    const auto committed = _committed.load(std::memory_order_relaxed);
    const auto high_water = std::max(
      kSendHighWater, 2 * _largest_write.load(std::memory_order_relaxed));
    if (committed <= _written.load(std::memory_order_acquire) + high_water) {
      return false;
    }
    {
      duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{lock};
      if (!BlockTask(state)) {
        return false;
      }
      _release = std::max(_release, committed - high_water);
      _blocked.store(true, std::memory_order_release);
    }
    _task.RequestRun();
    return true;
  }

  bool Blocked() const { return _blocked.load(std::memory_order_acquire); }

  bool Release(bool closed) {
    duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{lock};
    if (closed) {
      PreventBlocking();
    } else if (_written.load(std::memory_order_acquire) < _release) {
      return false;
    }
    UnblockTasks();
    _blocked.store(false, std::memory_order_relaxed);
    return true;
  }

 private:
  const std::atomic<size_t>& _written;
  CpuResumer& _task;
  std::atomic<size_t> _committed{0};
  std::atomic<size_t> _largest_write{0};
  std::atomic<bool> _blocked{false};
  size_t _release = 0;
};

}  // namespace sdb::network
