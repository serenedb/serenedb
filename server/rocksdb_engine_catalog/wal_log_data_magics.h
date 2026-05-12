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

namespace sdb::wal_log_data {

// Single-byte magics that appear as the first byte of a rocksdb PutLogData
// blob. New consumers MUST add their code here so the next reader can audit
// collisions at a glance.
//
// Value 0xFF is reserved as an "extended" marker: when we exhaust the
// single-byte space (255 codes), 0xFF will mean the actual code lives in the
// next byte, opening another 255 codes. Don't use it for a real magic.

inline constexpr uint8_t kIndexOnlyCp = 0x01;  // see indexonly_marker.h
inline constexpr uint8_t kIndexOnlyRd = 0x02;  // see indexonly_marker.h

inline constexpr uint8_t kExtended = 0xFF;

}  // namespace sdb::wal_log_data
