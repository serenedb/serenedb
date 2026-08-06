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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/types.hpp"

namespace irs {
namespace classify {

inline constexpr size_t kClassifyBlock = 32;

using Block = uint8_t __attribute__((vector_size(kClassifyBlock)));
using Cmp = int8_t __attribute__((vector_size(kClassifyBlock)));
using Bits = bool __attribute__((ext_vector_type(kClassifyBlock)));

IRS_FORCE_INLINE inline Block Load(const byte_type* block) noexcept {
  Block b;
  std::memcpy(&b, block, sizeof b);
  return b;
}

IRS_FORCE_INLINE inline uint32_t MoveMask(Cmp cmp) noexcept {
  return std::bit_cast<uint32_t>(__builtin_convertvector(cmp, Bits));
}

}  // namespace classify

// Bit i set iff block[i] == target; block must have kClassifyBlock readable
// bytes.
IRS_FORCE_INLINE inline uint32_t ClassifyEqBlock(const byte_type* block,
                                                 byte_type target) noexcept {
  return classify::MoveMask(classify::Load(block) == target);
}

// OR of ClassifyEqBlock over every target byte.
IRS_FORCE_INLINE inline uint32_t ClassifyAnyEqBlock(
  const byte_type* block, std::span<const byte_type> targets) noexcept {
  const auto b = classify::Load(block);
  classify::Cmp acc{};
  for (const auto target : targets) {
    acc |= b == target;
  }
  return classify::MoveMask(acc);
}

// Per-byte membership masks for the ASCII word set: word = [A-Za-z0-9_],
// alpha = [A-Za-z], digit = [0-9]. `block` must have kClassifyBlock readable
// bytes.
struct WordMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
};

IRS_FORCE_INLINE inline WordMasks ClassifyWordBlock(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  const classify::Cmp digit = (b >= '0') & (b <= '9');
  const auto folded = b | uint8_t{0x20};
  const classify::Cmp alpha = (folded >= 'a') & (folded <= 'z');
  const classify::Cmp word = digit | alpha | (b == '_');
  return {classify::MoveMask(word), classify::MoveMask(alpha),
          classify::MoveMask(digit)};
}

// Bit i set iff block[i] starts a UTF-8 sequence (is not a continuation
// byte); block must have kClassifyBlock readable bytes.
IRS_FORCE_INLINE inline uint32_t ClassifyUtf8LeadBlock(
  const byte_type* block) noexcept {
  const auto b = classify::Load(block);
  return classify::MoveMask((b & uint8_t{0xC0}) != uint8_t{0x80});
}

// ASCII check for token-sized inputs: simdutf's runtime-dispatched call
// loses to a pair of overlapping in-bounds loads at these sizes; every read
// stays within [data, data + size). Callers tier on size <= 16.
IRS_FORCE_INLINE inline bool IsAsciiShort(const char* data,
                                          size_t size) noexcept {
  uint64_t acc;
  if (size >= 8) {
    uint64_t lo;
    uint64_t hi;
    std::memcpy(&lo, data, sizeof lo);
    std::memcpy(&hi, data + size - 8, sizeof hi);
    acc = lo | hi;
  } else if (size >= 4) {
    uint32_t lo;
    uint32_t hi;
    std::memcpy(&lo, data, sizeof lo);
    std::memcpy(&hi, data + size - 4, sizeof hi);
    acc = uint64_t{lo} | hi;
  } else if (size != 0) {
    acc = static_cast<uint8_t>(data[0]) |
          static_cast<uint8_t>(data[size >> 1]) |
          static_cast<uint8_t>(data[size - 1]);
  } else {
    return true;
  }
  return (acc & UINT64_C(0x8080808080808080)) == 0;
}

bool AsciiCaseSafe(const char* locale_name) noexcept;

// Drains a classify mask lowest-bit-first, invoking `visit` with each set
// bit's index within the block.
template<typename Visitor>
IRS_FORCE_INLINE void VisitSetBits(uint32_t mask, Visitor&& visit) {
  while (mask != 0) {
    visit(static_cast<uint32_t>(std::countr_zero(mask)));
    mask &= mask - 1;
  }
}

// Rebuilds `out` as the codepoint start offsets of [data, data + size) plus
// a final `size` sentinel: block-classified lead scan when the caller proved
// the bytes valid UTF-8, a defensive per-codepoint walk otherwise.
void BuildUtf8CpBounds(const byte_type* data, size_t size, bool valid_utf8,
                       std::vector<uint32_t>& out);

}  // namespace irs
