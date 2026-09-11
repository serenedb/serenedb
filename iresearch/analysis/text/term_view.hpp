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
#include <tmmintrin.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <string_view>

#include "iresearch/utils/assert.h"
#include "iresearch/utils/shared.hpp"
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

IRS_FORCE_INLINE inline uint32_t LoadU32(const char* p) noexcept {
  uint32_t w;
  std::memcpy(&w, p, sizeof w);
  return w;
}

IRS_FORCE_INLINE inline __m128i InlineTermRegister(const char* data,
                                                   uint32_t size) noexcept {
  const __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
  const __m128i lanes =
    _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  const __m128i keep =
    _mm_cmpgt_epi8(_mm_set1_epi8(static_cast<char>(size)), lanes);
  return _mm_or_si128(_mm_slli_si128(_mm_and_si128(bytes, keep), 4),
                      _mm_cvtsi32_si128(static_cast<int>(size)));
}

IRS_FORCE_INLINE inline void StoreHalves(duckdb::string_t* dst,
                                         __m128i term) noexcept {
  _mm_storel_epi64(reinterpret_cast<__m128i*>(dst), term);
  asm("" : "+x"(term));
  _mm_storeh_pd(reinterpret_cast<double*>(dst) + 1, _mm_castsi128_pd(term));
}

struct alignas(32) InlineTermPattern {
  std::array<uint8_t, 16> shuffle;
  std::array<uint32_t, 4> size_lane;
};

inline constexpr auto kInlineTermPatterns = [] {
  std::array<InlineTermPattern, duckdb::string_t::INLINE_LENGTH + 1> out{};
  for (uint32_t size = 0; size <= duckdb::string_t::INLINE_LENGTH; ++size) {
    auto& p = out[size];
    p.shuffle.fill(0x80);
    for (uint32_t i = 0; i < size; ++i) {
      p.shuffle[sizeof(uint32_t) + i] = static_cast<uint8_t>(i);
    }
    p.size_lane = {size, 0, 0, 0};
  }
  return out;
}();

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewScalar(
  const char* data, uint32_t size) noexcept {
  uint64_t lo = 0;
  uint64_t hi = 0;
  if (size >= 4) [[likely]] {
    if (size <= 8) [[likely]] {
      lo = LoadU32(data) |
           (uint64_t{LoadU32(data + size - 4)} << (8 * (size - 4)));
    } else {
      std::memcpy(&lo, data, sizeof lo);
      hi = uint64_t{LoadU32(data + size - 4)} >>
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

}  // namespace detail

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  const char* data, uint32_t size, const char* end) noexcept {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
  if (end - data >= static_cast<ptrdiff_t>(kTermViewSlack)) [[likely]] {
    return std::bit_cast<duckdb::string_t>(
      detail::InlineTermRegister(data, size));
  }
  return detail::MakeTermViewScalar(data, size);
}

IRS_FORCE_INLINE inline void StoreTermView(duckdb::string_t* dst,
                                           const char* data, uint32_t size,
                                           const char* end) noexcept {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    *dst = duckdb::string_t{data, size};
    return;
  }
  if (end - data >= static_cast<ptrdiff_t>(kTermViewSlack)) [[likely]] {
    detail::StoreHalves(dst, detail::InlineTermRegister(data, size));
    return;
  }
  *dst = detail::MakeTermViewScalar(data, size);
}

IRS_FORCE_INLINE inline __m128i LoadTermBytes(const char* data) noexcept {
  return _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
}

IRS_FORCE_INLINE inline void StoreInlineTerm(
  duckdb::string_t* dst, __m128i bytes,
  const detail::InlineTermPattern& p) noexcept {
  const auto ctl =
    _mm_load_si128(reinterpret_cast<const __m128i*>(p.shuffle.data()));
  const auto lane =
    _mm_load_si128(reinterpret_cast<const __m128i*>(p.size_lane.data()));
  detail::StoreHalves(dst, _mm_or_si128(_mm_shuffle_epi8(bytes, ctl), lane));
}

IRS_FORCE_INLINE inline void StoreInlineTerm(duckdb::string_t* dst,
                                             __m128i bytes,
                                             uint32_t size) noexcept {
  SDB_ASSERT(size <= duckdb::string_t::INLINE_LENGTH);
  StoreInlineTerm(dst, bytes, detail::kInlineTermPatterns[size]);
}

class PaddedTail {
 public:
  IRS_FORCE_INLINE PaddedTail(const char* base, const char* end) noexcept
    : _end{end}, _mirror{end - std::min<size_t>(end - base, kTermViewSlack)} {
    std::memcpy(_bytes, _mirror, end - _mirror);
  }

  IRS_FORCE_INLINE __m128i Load(const char* data) const noexcept {
    if (_end - data >= static_cast<ptrdiff_t>(kTermViewSlack)) [[likely]] {
      return LoadTermBytes(data);
    }
    return LoadTermBytes(_bytes + (data - _mirror));
  }

 private:
  alignas(16) char _bytes[2 * kTermViewSlack];
  const char* _end;
  const char* _mirror;
};

IRS_FORCE_INLINE inline void StoreTermViewPadded(duckdb::string_t* dst,
                                                 const byte_type* data,
                                                 uint32_t size) noexcept {
  const auto* chars = reinterpret_cast<const char*>(data);
  StoreTermView(dst, chars, size, chars + size + kTermViewSlack);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  std::string_view term) noexcept {
  const auto size = static_cast<uint32_t>(term.size());
  return MakeTermView(term.data(), size, term.data() + size);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewPadded(
  const byte_type* data, uint32_t size) noexcept {
  const auto* chars = reinterpret_cast<const char*>(data);
  return MakeTermView(chars, size, chars + size + kTermViewSlack);
}

}  // namespace irs
