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

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string_view>

namespace sdb::backup {

inline constexpr std::string_view kPgControlFile = "global/pg_control";
inline constexpr std::string_view kPgVersionFile = "PG_VERSION";

struct ControlState {
  uint64_t system_identifier = 0;
  uint64_t checkpoint = 0;
};

std::optional<ControlState> ReadPgControl(
  const std::filesystem::path& data_dir);
void WritePgControl(const std::filesystem::path& data_dir,
                    const ControlState& state);
void EnsurePgVersion(const std::filesystem::path& data_dir);

class PositionSource final {
 public:
  static PositionSource& Instance();

  void Open(const std::filesystem::path& data_dir);
  uint64_t SystemIdentifier();
  uint64_t Issue();

 private:
  absl::Mutex _mutex;
  std::filesystem::path _data_dir ABSL_GUARDED_BY(_mutex);
  std::optional<ControlState> _state ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::backup
