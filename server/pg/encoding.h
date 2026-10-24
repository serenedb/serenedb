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
#include <string>
#include <string_view>

namespace sdb::pg {

inline constexpr int32_t kSqlAsciiEncoding = 0;
inline constexpr int32_t kUtf8Encoding = 6;
inline constexpr int32_t kLatin1Encoding = 8;
inline constexpr int32_t kLatin2Encoding = 9;
inline constexpr int32_t kLatin9Encoding = 16;
inline constexpr int32_t kWin1250Encoding = 29;

int32_t EncodingByName(std::string_view name);

std::string_view EncodingName(int32_t encoding);

std::string_view ConvertEncoding(std::string_view input, int32_t from,
                                 int32_t to, std::string& buffer);

}  // namespace sdb::pg
