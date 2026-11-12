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

void ScanBarrier::Finish(State state) noexcept {
  auto open = State::Open;
  if (_state.compare_exchange_strong(open, state, std::memory_order_acq_rel)) {
    _state.notify_all();
  }
}

void ScanBarrier::Wait() const noexcept {
  for (auto state = _state.load(std::memory_order_acquire);
       state == State::Open; state = _state.load(std::memory_order_acquire)) {
    _state.wait(State::Open, std::memory_order_acquire);
  }
}

}  // namespace sdb::connector
