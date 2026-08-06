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
#if defined(__SSE2__) || \
  (defined(_MSC_VER) && (defined(_M_AMD64) || defined(_M_X64)))
#define IRS_TERM_VIEW_SSE2 1
#include <emmintrin.h>
#endif
#include <bit>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <duckdb/common/types/string_type.hpp>

#include "basics/assert.h"
#include "basics/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

// The word packing below (masks, shifts, size|lo<<32) puts a string's first
// byte in a word's LOW byte: little-endian only, by construction. Fail the
// build on a big-endian port instead of silently building garbage views.
static_assert(std::endian::native == std::endian::little,
              "term-view word packing assumes little-endian");

IRS_FORCE_INLINE inline uint32_t LoadU32(const char* p) noexcept {
  uint32_t w;
  std::memcpy(&w, p, sizeof w);
  return w;
}

// MakeTermView hand-assembles string_t's in-memory layout via bit_cast, so a
// duckdb layout change would silently yield garbage views. Pin the dimensions
// it hard-codes (16-byte object, 12-byte inline capacity) at compile time;
// the field offsets it assumes (length at byte 0, inline data at byte 4) are
// pinned behaviorally by TermViewTest.MakeTermViewMatchesDuckDbCtor, which
// compares against string_t's own ctor.
static_assert(sizeof(duckdb::string_t) == 16);
static_assert(duckdb::string_t::INLINE_BYTES == 12);

// Readable slack a buffer must guarantee past a term's end for the
// branchless masked build (MakeTermViewPadded / the slack tier of the
// 3-arg MakeTermView). TokenSink::Reserve pads every reservation by
// this amount.
inline constexpr size_t kTermViewSlack = 16;

#ifdef IRS_TERM_VIEW_SSE2
namespace detail {
// Sliding byte mask for the slack fast path: an unaligned load at
// (16 - size) yields exactly the first `size` lanes set. One 32-byte
// constant, one L1 load, any size in [0, 16].
struct TermViewSlideMask {
  alignas(16) uint8_t m[32];
  constexpr TermViewSlideMask() : m{} {
    for (int j = 0; j < 16; ++j) {
      m[j] = 0xFF;
    }
  }
};
inline constexpr TermViewSlideMask kTermViewSlide{};

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewMasked(
  const char* data, uint32_t size) noexcept {
  const __m128i bytes =
    _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
  const __m128i mask = _mm_loadu_si128(
    reinterpret_cast<const __m128i*>(kTermViewSlide.m + 16 - size));
  const __m128i value =
    _mm_or_si128(_mm_slli_si128(_mm_and_si128(bytes, mask), 4),
                 _mm_cvtsi32_si128(static_cast<int>(size)));
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  _mm_store_si128(reinterpret_cast<__m128i*>(slot), value);
  return std::bit_cast<duckdb::string_t>(slot);
}
}  // namespace detail
#endif

// THE term-view builder. `end` is one past the readable buffer holding
// `data`: the value's end for tokens emitted as views into it, data + size
// when nothing beyond the token is owned (the 2-arg overload). Tiered
// internals, one entry:
//   size > 12          -> duckdb's ctor (non-inline: prefix + pointer; rare)
//   >= 16 bytes slack  -> branchless masked build: one 16-byte load, one
//                         mask-table load keyed by size, and + lane shift +
//                         or the length. In-bounds of the caller's buffer,
//                         no size branches -- size-class branches devastate
//                         mixed-length token streams (-40% cycles on random
//                         1-12B tokens vs the exact build below).
//   else               -> exact-bounds build from overlapping in-bounds
//                         loads (two dwords for size >= 4, three byte loads
//                         under that): no memset, no variable memcpy, every
//                         read within [data, data+size).
// All tiers produce the byte-identical string_t (test-pinned against
// duckdb's ctor across the size x slack matrix).
IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  const char* data, uint32_t size, const char* end) noexcept {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
#ifdef IRS_TERM_VIEW_SSE2
  if (end - data >= static_cast<ptrdiff_t>(kTermViewSlack)) [[likely]] {
    return detail::MakeTermViewMasked(data, size);
  }
#else
  (void)end;
#endif
  uint64_t lo;
  uint64_t hi;
  if (size >= 4) [[likely]] {
    if (size <= 8) [[likely]] {
      lo = LoadU32(data) |
           (uint64_t{LoadU32(data + size - 4)} << (8 * (size - 4)));
      hi = 0;
    } else {
      std::memcpy(&lo, data, sizeof lo);
      hi = uint64_t{LoadU32(data + size - 4)} >>
           (8 * (duckdb::string_t::INLINE_BYTES - size));
    }
  } else if (size != 0) {
    lo = uint64_t{static_cast<uint8_t>(data[0])} |
         (uint64_t{static_cast<uint8_t>(data[size >> 1])}
          << (8 * (size >> 1))) |
         (uint64_t{static_cast<uint8_t>(data[size - 1])}
          << (8 * (size - 1)));
    hi = 0;
  } else {
    lo = 0;
    hi = 0;
  }
  const uint64_t w0 = size | (lo << 32);
  const uint64_t w1 = (lo >> 32) | (hi << 32);
  alignas(duckdb::string_t) char slot[sizeof(duckdb::string_t)];
  std::memcpy(slot, &w0, sizeof w0);
  std::memcpy(slot + 8, &w1, sizeof w1);
  return std::bit_cast<duckdb::string_t>(slot);
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(
  const byte_type* data, uint32_t size, const byte_type* end) noexcept {
  return MakeTermView(reinterpret_cast<const char*>(data), size,
                      reinterpret_cast<const char*>(end));
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermView(const char* data,
                                                      uint32_t size) noexcept {
  return MakeTermView(data, size, data + size);
}

// For terms built inside a padded buffer: the caller guarantees at least
// kTermViewSlack readable bytes past data + size (TokenSink::Reserve
// pads every reservation by exactly that). The static guarantee makes the
// `end` bound of the 3-arg builder redundant AND drops its slack branch:
// the masked build applies unconditionally.
IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewPadded(
  const char* data, uint32_t size) noexcept {
  if (size > duckdb::string_t::INLINE_LENGTH) [[unlikely]] {
    return duckdb::string_t{data, size};
  }
#ifdef IRS_TERM_VIEW_SSE2
  return detail::MakeTermViewMasked(data, size);
#else
  return MakeTermView(data, size);
#endif
}

IRS_FORCE_INLINE inline duckdb::string_t MakeTermViewPadded(
  const byte_type* data, uint32_t size) noexcept {
  return MakeTermViewPadded(reinterpret_cast<const char*>(data), size);
}

// ASCII case fold fused onto a built INLINE view (size <= 12): flips bit 5
// of every slot byte inside the letter range, in one register op pair. The
// whole 16-byte slot is folded -- safe because the length word can't alias
// the range (its live byte is the size, <= 12; the rest is zero) and
// unfolded lanes are zero or non-letter. Unsigned compares leave bytes
// above 'z' alone, matching absl's table fold on any input byte. Never
// call on a non-inline view: it would corrupt the data pointer.
template<bool kToLower>
IRS_FORCE_INLINE inline duckdb::string_t FoldTermViewAscii(
  duckdb::string_t view) noexcept {
  SDB_ASSERT(view.GetSize() <= duckdb::string_t::INLINE_LENGTH);
  using Block = uint8_t __attribute__((vector_size(sizeof(duckdb::string_t))));
  constexpr uint8_t kLo = kToLower ? 'A' : 'a';
  constexpr uint8_t kHi = kToLower ? 'Z' : 'z';
  auto b = std::bit_cast<Block>(view);
  b ^= std::bit_cast<Block>((b >= kLo) & (b <= kHi)) & uint8_t{0x20};
  return std::bit_cast<duckdb::string_t>(b);
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
