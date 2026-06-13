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

#include "catalog/server_state.h"

namespace sdb {

ServerState* ServerState::instance() noexcept {
  static ServerState gInstance;
  return &gInstance;
}

ServerState::Mode ServerState::GetMode() const noexcept {
  return _mode.load(std::memory_order_acquire);
}

ServerState::Mode ServerState::SetMode(Mode value) noexcept {
  return _mode.exchange(value, std::memory_order_acq_rel);
}

bool ServerState::IsStartupOrMaintenance() const noexcept {
  Mode value = GetMode();
  return value == Mode::Startup || value == Mode::Maintenance;
}

bool ServerState::ReadOnly() const noexcept {
  return _read_only.load(std::memory_order_acquire);
}

bool ServerState::SetReadOnly(ReadOnlyMode mode) noexcept {
  return _read_only.exchange(mode == ReadOnlyMode::ApiTrue,
                             std::memory_order_acq_rel);
}

}  // namespace sdb
