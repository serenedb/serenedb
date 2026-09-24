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
#include <bit>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/words/masks.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::words {
namespace detail {

struct WordCodePoints {
  std::array<uint64_t, 0x10000 / 64> alnum{};
  std::array<uint64_t, 0x10000 / 64> letters{};
};

inline const WordCodePoints& BmpWordCodePoints() noexcept {
  static const WordCodePoints kTable = [] {
    WordCodePoints table;
    for (uint32_t c = 0x80; c < 0x10000; ++c) {
      const char category = utf8_utils::CharPrimaryCategory(c);
      const uint64_t bit = uint64_t{1} << (c & 63);
      if (category == 'L' || category == 'M') {
        table.alnum[c >> 6] |= bit;
        table.letters[c >> 6] |= bit;
      } else if (category == 'N') {
        table.alnum[c >> 6] |= bit;
      }
    }
    return table;
  }();
  return kTable;
}

template<bool Letters>
IRS_FORCE_INLINE bool IsWordCodePoint(const WordCodePoints& table,
                                      uint32_t c) noexcept {
  if (c < 0x10000) [[likely]] {
    const auto& bits = Letters ? table.letters : table.alnum;
    return ((bits[c >> 6] >> (c & 63)) & 1) != 0;
  }
  if (c > 0x10FFFF) {
    return false;
  }
  const char category = utf8_utils::CharPrimaryCategory(c);
  return category == 'L' || category == 'M' || (!Letters && category == 'N');
}

IRS_FORCE_INLINE inline uint32_t SpaceLength(const byte_type* p,
                                             const byte_type* end) noexcept {
  const auto left = end - p;
  if (left < 2) {
    return 0;
  }
  const byte_type b1 = p[1];
  if (p[0] == 0xC2) {
    return b1 == 0x85 || b1 == 0xA0 ? 2 : 0;
  }
  if (left < 3) {
    return 0;
  }
  const byte_type b2 = p[2];
  switch (p[0]) {
    case 0xE1:
      return b1 == 0x9A && b2 == 0x80 ? 3 : 0;
    case 0xE2:
      if (b1 == 0x80) {
        return static_cast<uint8_t>(b2 - 0x80) <= 0x0A || b2 == 0xA8 ||
                   b2 == 0xA9 || b2 == 0xAF
                 ? 3
                 : 0;
      }
      return b1 == 0x81 && b2 == 0x9F ? 3 : 0;
    case 0xE3:
      return b1 == 0x80 && b2 == 0x80 ? 3 : 0;
    default:
      return 0;
  }
}

IRS_FORCE_INLINE inline uint32_t SpaceSpill(const byte_type* block,
                                            const byte_type* begin,
                                            const byte_type* end) noexcept {
  uint32_t bits = 0;
  if (block - begin >= 2 && SpaceLength(block - 2, end) == 3) {
    bits |= 1;
  }
  if (const uint32_t len = SpaceLength(block - 1, end); len != 0) {
    bits |= (uint32_t{1} << (len - 1)) - 1;
  }
  return bits;
}

}  // namespace detail

template<bool KnownAscii>
IRS_FORCE_INLINE inline uint32_t ClassifyNonSpace(classify::Block b,
                                                  const byte_type* block,
                                                  const byte_type* data,
                                                  size_t size) noexcept {
  const bool padded = size < classify::kClassifyBlock;
  const byte_type* const begin = padded ? block : data;
  const byte_type* const end = begin + size;
  uint32_t space = classify::MoveMask((b == uint8_t{' '}) |
                                      (b - uint8_t{0x09} <= uint8_t{0x04}));
  if constexpr (!KnownAscii) {
    uint32_t leads = classify::MoveMask((b == uint8_t{0xC2}) |
                                        (b - uint8_t{0xE1} <= uint8_t{0x02}));
    while (leads != 0) {
      const auto at = static_cast<uint32_t>(std::countr_zero(leads));
      leads &= leads - 1;
      const uint32_t len = detail::SpaceLength(block + at, end);
      space |= static_cast<uint32_t>(((uint64_t{1} << len) - 1) << at);
    }
    if (block != begin && (block[0] & 0xC0) == 0x80) {
      space |= detail::SpaceSpill(block, begin, end);
    }
  }
  return padded ? ~space & classify::LowBits(size) : ~space;
}

template<bool KnownAscii = false, typename EmitFn>
IRS_FORCE_INLINE void SplitByNonSpace(duckdb::string_t data, EmitFn&& emit) {
  const auto* const bytes = reinterpret_cast<const byte_type*>(data.GetData());
  const size_t size = data.GetSize();
  classify::ForEachRun(
    bytes, size,
    [bytes, size](const byte_type* block) IRS_FORCE_INLINE {
      return ClassifyNonSpace<KnownAscii>(classify::Load(block), block, bytes,
                                          size);
    },
    emit);
}

template<bool KeepNonAscii = false, typename EmitFn>
IRS_FORCE_INLINE void SplitByNonAlpha(duckdb::string_t data, EmitFn&& emit) {
  classify::ForEachRun(
    reinterpret_cast<const byte_type*>(data.GetData()), data.GetSize(),
    [](const byte_type* block)
      IRS_FORCE_INLINE { return ClassifyAlnumBlock<KeepNonAscii>(block); },
    emit);
}

template<typename EmitFn>
IRS_FORCE_INLINE void SplitByNonLetter(duckdb::string_t data, EmitFn&& emit) {
  classify::ForEachRun(
    reinterpret_cast<const byte_type*>(data.GetData()), data.GetSize(),
    [](const byte_type* block)
      IRS_FORCE_INLINE { return ClassifyAlphaBlock(block); },
    emit);
}

template<bool Letters, typename EmitFn>
IRS_FORCE_INLINE void SplitByNonAlnum(duckdb::string_t data, EmitFn&& emit) {
  constexpr size_t kBlock = classify::kClassifyBlock;
  const auto* const bytes = reinterpret_cast<const byte_type*>(data.GetData());
  const size_t size = data.GetSize();
  const auto& table = detail::BmpWordCodePoints();
  uint32_t carry = 0;
  bool open = false;
  size_t begin = 0;
  for (size_t base = 0; base < size; base += kBlock) {
    const size_t n = std::min(kBlock, size - base);
    const auto block = n == kBlock ? classify::Load(bytes + base)
                                   : classify::LoadPadded(bytes + base, n);
    const auto cmps = detail::WordCmpsOf(block);
    uint32_t mask = Letters ? classify::MoveMask(cmps.alpha)
                            : classify::MoveMask(cmps.alpha | cmps.digit);
    const uint32_t high = classify::MoveMask(block >= uint8_t{0x80});
    if (high != 0) {
      mask |= carry;
      carry = 0;
      uint32_t leads =
        high & ~classify::MoveMask((block & uint8_t{0xC0}) == uint8_t{0x80});
      while (leads != 0) {
        const auto at = static_cast<uint32_t>(std::countr_zero(leads));
        leads &= leads - 1;
        const auto* it = bytes + base + at;
        const uint32_t cp = utf8_utils::ToChar32(it, bytes + size);
        if (detail::IsWordCodePoint<Letters>(table, cp)) {
          const auto len = static_cast<uint32_t>(it - (bytes + base + at));
          const uint64_t bits = ((uint64_t{1} << len) - 1) << at;
          mask |= static_cast<uint32_t>(bits);
          carry = static_cast<uint32_t>(bits >> kBlock);
        }
      }
    }
    const uint32_t before = (mask << 1) | uint32_t{open};
    uint32_t starts = mask & ~before;
    uint32_t ends = ~mask & before;
    if (open && ends != 0) {
      emit(begin, base + std::countr_zero(ends));
      ends &= ends - 1;
      open = false;
    }
    while (ends != 0) {
      emit(base + std::countr_zero(starts), base + std::countr_zero(ends));
      starts &= starts - 1;
      ends &= ends - 1;
    }
    if (starts != 0) {
      begin = base + std::countr_zero(starts);
      open = true;
    }
  }
  if (open) {
    emit(begin, size);
  }
}

}  // namespace irs::analysis::words
