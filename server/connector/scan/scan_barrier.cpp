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

#include "connector/scan/scan_state.h"

namespace sdb::connector {

void ScanBarrier::Finish(bool failed) noexcept {
  if (_finished.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  _failed.store(failed, std::memory_order_release);
  _notification.Notify();
}

void ScanBarrier::Wait() {
  if (!Released()) {
    _notification.WaitForNotification();
  }
}

}  // namespace sdb::connector
