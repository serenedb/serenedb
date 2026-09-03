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

#include <simdutf.h>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/types.hpp"

namespace irs::analysis::classify {

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

IRS_FORCE_INLINE inline uint32_t ClassifyEqBlock(const byte_type* block,
                                                 byte_type target) noexcept {
  return MoveMask(Load(block) == target);
}

IRS_FORCE_INLINE inline uint32_t ClassifyAnyEqBlock(
  const byte_type* block, std::span<const byte_type> targets) noexcept {
  const auto b = Load(block);
  Cmp acc{};
  for (const auto target : targets) {
    acc |= b == target;
  }
  return MoveMask(acc);
}

struct ByteSet {
  IRS_FORCE_INLINE void Add(byte_type b) noexcept {
    words[b >> 6] |= uint64_t{1} << (b & 63);
  }
  IRS_FORCE_INLINE bool Contains(byte_type b) const noexcept {
    return ((words[b >> 6] >> (b & 63)) & 1) != 0;
  }

  std::array<uint64_t, 4> words{};
};

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

IRS_FORCE_INLINE inline bool IsAsciiValue(const char* data,
                                          size_t size) noexcept {
  if (size <= 16) {
    return IsAsciiShort(data, size);
  }
  return simdutf::validate_ascii(data, size);
}

bool AsciiCaseSafe(const char* locale_name) noexcept;

bool SimpleCaseSafe(const char* locale_name) noexcept;

template<typename Visitor>
IRS_FORCE_INLINE void VisitSetBits(uint32_t mask, Visitor&& visit) {
  while (mask != 0) {
    visit(static_cast<uint32_t>(std::countr_zero(mask)));
    mask &= mask - 1;
  }
}

template<typename ClassifyBlock, typename IsDelim, typename OnDelim>
IRS_FORCE_INLINE void DrainClassified(const byte_type* data, size_t size,
                                      bool use_blocks, ClassifyBlock classify,
                                      IsDelim is_delim, OnDelim on_delim) {
  if (!use_blocks || size < kClassifyBlock) {
    for (size_t offset = 0; offset < size; ++offset) {
      if (is_delim(data[offset])) {
        on_delim(offset);
      }
    }
    return;
  }
  size_t offset = 0;
  for (; size - offset >= kClassifyBlock; offset += kClassifyBlock) {
    VisitSetBits(classify(data + offset), [&](uint32_t bit) IRS_FORCE_INLINE {
      on_delim(offset + bit);
    });
  }
  if (offset == size) {
    return;
  }
  const size_t base = size - kClassifyBlock;
  const uint32_t seen = (uint32_t{1} << (offset - base)) - 1;
  VisitSetBits(classify(data + base) & ~seen,
               [&](uint32_t bit) IRS_FORCE_INLINE { on_delim(base + bit); });
}

size_t BuildUtf8CpBounds(const byte_type* data, size_t size, bool valid_utf8,
                         std::vector<uint32_t>& out);

}  // namespace irs::analysis::classify
