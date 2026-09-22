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

#include <duckdb/common/mutex.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <iresearch/utils/assert.hpp>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

void ScanBarrier::Release(duckdb::TableFunctionInput& input) {
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

void ScanBarrier::Wait() {
  if (Released()) {
    return;
  }
  _notification.WaitForNotification();
}

}  // namespace sdb::connector
