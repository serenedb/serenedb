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

#include <cstdint>

namespace sdb::pg {

inline constexpr uint8_t kPgsqlAfInet = 2;
inline constexpr uint8_t kPgsqlAfInet6 = 3;
inline constexpr uint8_t kInetWireV4Size = sizeof(uint32_t);
inline constexpr uint8_t kInetWireV6Size = 2 * sizeof(uint64_t);

struct InetWireHeader {
  uint8_t family;
  uint8_t bits;
  uint8_t is_cidr;
  uint8_t size;
};

}  // namespace sdb::pg
