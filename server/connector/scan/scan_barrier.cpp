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

#include <duckdb/parallel/interrupt.hpp>
#include <duckdb/parallel/task_scheduler.hpp>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

bool ScanBarrier::Finish(State state) noexcept {
  auto open = State::Open;
  if (!_state.compare_exchange_strong(open, state, std::memory_order_acq_rel)) {
    return false;
  }
  _state.notify_all();
  return true;
}

void ScanBarrier::Release(duckdb::TableFunctionInput& input) {
  if (!input.blockable) {
    Finish(State::Released);
    return;
  }
  duckdb::vector<duckdb::InterruptState> parked;
  {
    duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{
      input.blockable->lock};
    if (!Finish(State::Released)) {
      return;
    }
    parked = input.blockable->TakeBlockedTasks();
  }
  for (const auto& task : parked) {
    task.Callback();
  }
}

void ScanBarrier::Abandon() noexcept { Finish(State::Failed); }

bool ScanBarrier::Park(duckdb::TableFunctionInput& input) {
  if (!input.blockable || !input.interrupt_state ||
      input.results_execution_mode !=
        duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR) {
    return false;
  }
  duckdb::annotated_lock_guard<duckdb::annotated_mutex> guard{
    input.blockable->lock};
  if (Released() || !input.blockable->BlockTask(*input.interrupt_state)) {
    return false;
  }
  input.async_result = duckdb::AsyncResultType::BLOCKED;
  return true;
}

void ScanBarrier::Wait() const {
  if (Released()) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  for (auto state = _state.load(std::memory_order_acquire);
       state == State::Open; state = _state.load(std::memory_order_acquire)) {
    _state.wait(State::Open, std::memory_order_acquire);
  }
}

}  // namespace sdb::connector
