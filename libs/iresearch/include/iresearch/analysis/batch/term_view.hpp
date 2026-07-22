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

#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>

#include "basics/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

// Loads bytes [data, data+size) zero-extended into a word without branching
// on size: reads stay within data's 4KB page on either side of the boundary
// check, so the only data-dependent branch is on the address bit, which is
// stable while consecutive tokens walk one value.
IRS_FORCE_INLINE inline uint64_t LoadPadded8(const char* data,
                                             uint32_t size) noexcept {
  if (size == 0) [[unlikely]] {
    return 0;
  }
  if ((reinterpret_cast<uintptr_t>(data) & 2048) == 0) [[likely]] {
    uint64_t w;
    std::memcpy(&w, data, sizeof w);
    if (size == 8) {
      return w;
    }
    return w & ((uint64_t{1} << (8 * size)) - 1);
  }
  uint64_t w;
  std::memcpy(&w, data + size - 8, sizeof w);
  return w >> (8 * (8 - size));
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(const char* data,
                                                      uint32_t size) noexcept {
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  if (size <= duckdb::string_t::INLINE_LENGTH) {
    uint64_t lo;
    uint64_t hi;
    if (size <= 8) [[likely]] {
      lo = LoadPadded8(data, size);
      hi = 0;
    } else {
      std::memcpy(&lo, data, sizeof lo);
      uint32_t tail;
      std::memcpy(&tail, data + size - 4, sizeof tail);
      hi = uint64_t{tail} >> (8 * (12 - size));
    }
    const uint64_t w0 = size | (lo << 32);
    const uint64_t w1 = (lo >> 32) | (hi << 32);
    std::memcpy(slot, &w0, sizeof w0);
    std::memcpy(slot + 8, &w1, sizeof w1);
  } else {
    std::memcpy(slot, &size, sizeof size);
    std::memcpy(slot + sizeof size, data, 4);
    std::memcpy(slot + 8, &data, sizeof data);
  }
  return std::bit_cast<duckdb::string_t>(slot);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(const byte_type* data,
                                                      uint32_t size) noexcept {
  return MakeTermView(reinterpret_cast<const char*>(data), size);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  bytes_view term) noexcept {
  return MakeTermView(reinterpret_cast<const char*>(term.data()),
                      static_cast<uint32_t>(term.size()));
}

}  // namespace irs
