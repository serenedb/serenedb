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

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "server/utils/unique_id.h"

namespace sdb::backup {

inline constexpr std::string_view kPgControlFile = "global/pg_control";
inline constexpr std::string_view kPgVersionFile = "PG_VERSION";
inline constexpr uint32_t kTimeline = 1;
inline constexpr uint32_t kWalSegmentSize = uint32_t{16} << 20;
inline constexpr size_t kMockNonceLen = 32;

using MockNonce = std::array<uint8_t, kMockNonceLen>;

struct ControlState {
  uint64_t system_identifier = 0;
  uint64_t checkpoint = 0;
  MockNonce mock_nonce{};

  bool operator==(const ControlState&) const = default;
};

std::optional<ControlState> ReadPgControl(const std::string& data_dir);
void WritePgControl(const std::string& data_dir, const ControlState& state);
std::string OpenPgVersion(const std::string& data_dir);
MockNonce NewMockNonce();

class PositionSource final {
 public:
  static PositionSource& Instance();

  void Adopt(const std::string& data_dir, const UniqueId& server_id,
             const MockNonce& mock_nonce, bool fresh);
  ControlState State();
  UniqueId ServerId();
  uint64_t SystemIdentifier();
  uint64_t Issue();
  MockNonce GetMockNonce();

 private:
  absl::Mutex _mutex;
  std::string _data_dir ABSL_GUARDED_BY(_mutex);
  UniqueId _server_id ABSL_GUARDED_BY(_mutex);
  std::optional<ControlState> _state ABSL_GUARDED_BY(_mutex);
  std::optional<MockNonce> _volatile_nonce ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::backup
