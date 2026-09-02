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

#include <emmintrin.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <string_view>

#include "basics/assert.h"
#include "basics/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

static_assert(std::endian::native == std::endian::little,
              "term-view word packing assumes little-endian");
static_assert(sizeof(duckdb::string_t) == 16);
static_assert(duckdb::string_t::INLINE_BYTES == 12);

inline constexpr size_t kTermViewSlack = 16;

IRS_FORCE_INLINE inline bytes_view AsBytesView(
  const duckdb::string_t& s) noexcept {
  return {reinterpret_cast<const byte_type*>(s.GetData()), s.GetSize()};
}

namespace detail {

inline constexpr uint8_t kTermViewSlide[2 * kTermViewSlack] = {
  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};

IRS_FORCE_INLINE inline uint32_t LoadU32(const char* p) noexcept {
  uint32_t w;
  std::memcpy(&w, p, sizeof w);
  return w;
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewMasked(
  const char* data, uint32_t size) noexcept {
  const __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
  const __m128i mask = _mm_loadu_si128(
    reinterpret_cast<const __m128i*>(kTermViewSlide + kTermViewSlack - size));
  return std::bit_cast<duckdb::string_t>(
    _mm_or_si128(_mm_slli_si128(_mm_and_si128(bytes, mask), 4),
                 _mm_cvtsi32_si128(static_cast<int>(size))));
}

}  // namespace detail

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  const char* data, uint32_t size, const char* end) noexcept {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
  if (end - data >= static_cast<ptrdiff_t>(kTermViewSlack)) [[likely]] {
    return detail::MakeTermViewMasked(data, size);
  }
  uint64_t lo = 0;
  uint64_t hi = 0;
  if (size >= 4) [[likely]] {
    if (size <= 8) [[likely]] {
      lo = detail::LoadU32(data) |
           (uint64_t{detail::LoadU32(data + size - 4)} << (8 * (size - 4)));
    } else {
      std::memcpy(&lo, data, sizeof lo);
      hi = uint64_t{detail::LoadU32(data + size - 4)} >>
           (8 * (duckdb::string_t::INLINE_BYTES - size));
    }
  } else if (size != 0) {
    lo =
      uint64_t{static_cast<uint8_t>(data[0])} |
      (uint64_t{static_cast<uint8_t>(data[size >> 1])} << (8 * (size >> 1))) |
      (uint64_t{static_cast<uint8_t>(data[size - 1])} << (8 * (size - 1)));
  }
  const uint64_t w0 = size | (lo << 32);
  const uint64_t w1 = (lo >> 32) | (hi << 32);
  return std::bit_cast<duckdb::string_t>((__uint128_t{w1} << 64) | w0);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(const char* data,
                                                      uint32_t size) noexcept {
  return MakeTermView(data, size, data + size);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  std::string_view term) noexcept {
  return MakeTermView(term.data(), static_cast<uint32_t>(term.size()));
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  bytes_view term) noexcept {
  return MakeTermView(reinterpret_cast<const char*>(term.data()),
                      static_cast<uint32_t>(term.size()));
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewPadded(
  const byte_type* data, uint32_t size) noexcept {
  const auto* chars = reinterpret_cast<const char*>(data);
  return MakeTermView(chars, size, chars + size + kTermViewSlack);
}

IRS_FORCE_INLINE inline __uint128_t InlineTermHandle(
  const duckdb::string_t& term) noexcept {
  SDB_ASSERT(term.GetSize() <= duckdb::string_t::INLINE_LENGTH);
  return std::bit_cast<__uint128_t>(term);
}

IRS_FORCE_INLINE inline __uint128_t InlineTermHandle(
  std::string_view term) noexcept {
  return InlineTermHandle(MakeTermView(term));
}

}  // namespace irs
