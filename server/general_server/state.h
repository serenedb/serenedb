////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>
#include <cstdint>

namespace sdb {

class ServerState {
 public:
  enum class ReadOnlyMode : uint8_t {
    ApiTrue,  // Set from outside via API
    ApiFalse,
  };

  enum class Mode : uint8_t {
    Default = 0,
    Startup = 1,
    /// reject almost all requests
    Maintenance = 2,

    Invalid = 255,  // this mode is used to indicate shutdown
  };

  static ServerState* instance() noexcept;

  Mode GetMode() const noexcept;

  /// sets server mode, returns previously held value
  /// (performs atomic read-modify-write operation)
  Mode SetMode(Mode mode) noexcept;

  bool IsStartupOrMaintenance() const noexcept;

  /// should not allow DDL operations / transactions
  bool ReadOnly() const noexcept;

  /// sets server read-only, returns previously held value
  bool SetReadOnly(ReadOnlyMode mode) noexcept;

 private:
  std::atomic<Mode> _mode = Mode::Default;
  std::atomic_bool _read_only = false;
};

}  // namespace sdb
