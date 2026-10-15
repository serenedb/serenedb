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

#include "pg/types/pg_lsn.h"

#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>

namespace sdb::pg {
namespace {

constexpr size_t kLsnPartDigits = 8;

std::optional<uint32_t> ParseLsnPart(std::string_view digits) {
  if (digits.empty() || digits.size() > kLsnPartDigits) {
    return std::nullopt;
  }
  uint32_t value = 0;
  const auto* end = digits.data() + digits.size();
  const auto [ptr, ec] = fast_float::from_chars(digits.data(), end, value, 16);
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return value;
}

}  // namespace

std::optional<uint64_t> ParseLsn(std::string_view text) {
  const auto slash = text.find('/');
  if (slash == std::string_view::npos) {
    return std::nullopt;
  }
  const auto high = ParseLsnPart(text.substr(0, slash));
  const auto low = ParseLsnPart(text.substr(slash + 1));
  if (!high || !low) {
    return std::nullopt;
  }
  return (static_cast<uint64_t>(*high) << 32) | *low;
}

size_t WriteLsn(uint64_t lsn, char* buffer) {
  return static_cast<size_t>(absl::SNPrintF(buffer, kLsnBufferSize, "%X/%X",
                                            static_cast<uint32_t>(lsn >> 32),
                                            static_cast<uint32_t>(lsn)));
}

std::string FormatLsn(uint64_t lsn) {
  char buffer[kLsnBufferSize];
  return {buffer, WriteLsn(lsn, buffer)};
}

}  // namespace sdb::pg
