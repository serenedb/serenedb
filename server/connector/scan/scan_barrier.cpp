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

#include <chrono>
#include <duckdb/common/mutex.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <iresearch/utils/assert.hpp>
#include <thread>

#include "connector/scan/scan_state.h"
#include "utils/number_of_cores.h"

namespace sdb::connector {
namespace {

std::atomic_int64_t gParkCost{0};

int64_t Now() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

void CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield");
#endif
}

bool CanSpin() {
  static const bool kCanSpin = CountLogicalCores() > 1;
  return kCanSpin;
}

}  // namespace

void ScanBarrier::Release(duckdb::TableFunctionInput& input) {
  _released_at.store(Now(), std::memory_order_relaxed);
  if (input.blockable) {
    duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{
      input.blockable->lock};
    _released.store(true, std::memory_order_release);
    input.blockable->UnblockTasks();
  } else {
    _released.store(true, std::memory_order_release);
  }
  SDB_ASSERT(!_notification.HasBeenNotified());
  _notification.Notify();
}

bool ScanBarrier::Park(duckdb::TableFunctionInput& input) {
  if (CanSpin()) {
    const auto budget = gParkCost.load(std::memory_order_relaxed);
    const auto start = Now();
    for (uint32_t i = 1; !Released(); ++i) {
      CpuRelax();
      if (i % 64 == 0) {
        if (Now() - start >= budget) {
          break;
        }
        std::this_thread::yield();
      }
    }
  }
  if (Released()) {
    return false;
  }
  if (input.blockable && input.interrupt_state &&
      input.results_execution_mode ==
        duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR) {
    duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{
      input.blockable->lock};
    if (Released()) {
      return false;
    }
    if (input.blockable->BlockTask(*input.interrupt_state)) {
      input.async_result = duckdb::AsyncResultType::BLOCKED;
      return true;
    }
  }
  return false;
}

void ScanBarrier::Resume() const noexcept {
  const auto cost = Now() - _released_at.load(std::memory_order_relaxed);
  const auto last = gParkCost.load(std::memory_order_relaxed);
  gParkCost.store(last == 0 ? cost : last + (cost - last) / 8,
                  std::memory_order_relaxed);
}

void ScanBarrier::Wait() {
  if (Released()) {
    return;
  }
  _notification.WaitForNotification();
}

}  // namespace sdb::connector
