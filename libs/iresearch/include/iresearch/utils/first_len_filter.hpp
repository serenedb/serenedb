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

#include <algorithm>
#include <array>
#include <cstdint>
#include <string_view>

namespace irs {

// Negative prefilter for small string sets probed by miss-dominated lookups:
// one bit per (first byte, min(length, 15)) pair. MayContain() == false
// guarantees the key is absent; true falls through to the exact probe.
// Permissive (passes everything) until the first Add(), so containers built
// without populating the filter stay correct and merely skip the shortcut.
class FirstLenFilter {
 public:
  void Add(std::string_view value) noexcept {
    if (!_armed) {
      _table.fill(0);
      _has_empty = false;
      _armed = true;
    }
    if (value.empty()) {
      _has_empty = true;
      return;
    }
    _table[static_cast<uint8_t>(value.front())] |= LenBit(value.size());
  }

  bool MayContain(std::string_view value) const noexcept {
    if (value.empty()) [[unlikely]] {
      return _has_empty;
    }
    return (_table[static_cast<uint8_t>(value.front())] &
            LenBit(value.size())) != 0;
  }

 private:
  static constexpr size_t kBuckets = 16;

  static uint16_t LenBit(size_t size) noexcept {
    return static_cast<uint16_t>(uint32_t{1}
                                 << std::min<size_t>(size, kBuckets - 1));
  }

  static constexpr std::array<uint16_t, 256> Permissive() noexcept {
    std::array<uint16_t, 256> table{};
    table.fill(0xFFFF);
    return table;
  }

  std::array<uint16_t, 256> _table = Permissive();
  bool _has_empty = true;
  bool _armed = false;
};

}  // namespace irs
