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
#include <string_view>

namespace sdb::pg {

inline constexpr std::string_view kPgCopySignature{"PGCOPY\n\xFF\r\n\0", 11};
inline constexpr std::string_view kPgCopyHeader{
  "PGCOPY\n\xFF\r\n\0"
  "\0\0\0\0"
  "\0\0\0\0",
  19};
inline constexpr int16_t kPgCopyTrailer = -1;
inline constexpr int32_t kPgCopyNull = -1;

}  // namespace sdb::pg
