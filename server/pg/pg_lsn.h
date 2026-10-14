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

#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

namespace sdb::pg {

inline constexpr size_t kLsnMaxTextLen = 17;

inline std::string FormatLsn(uint64_t lsn) {
  return absl::StrFormat("%X/%X", static_cast<uint32_t>(lsn >> 32),
                         static_cast<uint32_t>(lsn));
}

inline size_t RenderLsn(char* dst, uint64_t lsn) noexcept {
  const auto text = FormatLsn(lsn);
  std::memcpy(dst, text.data(), text.size());
  return text.size();
}

inline std::optional<uint64_t> ParseLsn(std::string_view text) noexcept {
  const auto slash = text.find('/');
  if (slash == std::string_view::npos || slash == 0 || slash > 8 ||
      text.size() - slash - 1 == 0 || text.size() - slash - 1 > 8) {
    return std::nullopt;
  }
  uint32_t high = 0;
  uint32_t low = 0;
  if (!absl::SimpleHexAtoi(text.substr(0, slash), &high) ||
      !absl::SimpleHexAtoi(text.substr(slash + 1), &low)) {
    return std::nullopt;
  }
  return (uint64_t{high} << 32) | low;
}

}  // namespace sdb::pg
