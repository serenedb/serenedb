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

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace sdb {

class UniqueId final {
 public:
  static constexpr size_t kBytes = 16;

  static UniqueId New();
  static std::optional<UniqueId> FromBytes(std::string_view bytes);
  static std::optional<UniqueId> FromHex(std::string_view hex);

  explicit operator bool() const noexcept;
  uint64_t High() const noexcept;
  std::string_view Bytes() const noexcept {
    return {reinterpret_cast<const char*>(_bytes.data()), _bytes.size()};
  }
  std::string Hex() const;

  auto operator<=>(const UniqueId&) const = default;

 private:
  std::array<uint8_t, kBytes> _bytes{};
};

}  // namespace sdb
