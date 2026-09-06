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

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "basics/assert.h"
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

struct ByteRange {
  byte_type lo;
  byte_type span;
};

IRS_FORCE_INLINE inline uint32_t ClassifyAnyInRangeBlock(
  const byte_type* block, std::span<const ByteRange> ranges) noexcept {
  const auto b = Load(block);
  Cmp acc{};
  for (const auto [lo, span] : ranges) {
    acc |= (b - lo) <= span;
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
  SDB_ASSERT(size <= 16);
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

template<typename Visitor>
IRS_FORCE_INLINE void VisitSetBits(uint32_t mask, Visitor&& visit) {
  while (mask != 0) {
    visit(static_cast<uint32_t>(std::countr_zero(mask)));
    mask &= mask - 1;
  }
}

IRS_FORCE_INLINE inline Block LoadPadded(const byte_type* data,
                                         size_t size) noexcept {
  SDB_ASSERT(size < kClassifyBlock);
  std::array<uint64_t, 4> words{};
  if (size >= 16) {
    std::memcpy(words.data(), data, 16);
    if (size > 16) {
      __uint128_t tail;
      std::memcpy(&tail, data + size - 16, sizeof tail);
      tail >>= 8 * (kClassifyBlock - size);
      std::memcpy(words.data() + 2, &tail, sizeof tail);
    }
  } else if (size >= 8) {
    std::memcpy(words.data(), data, 8);
    if (size > 8) {
      uint64_t tail;
      std::memcpy(&tail, data + size - 8, sizeof tail);
      words[1] = tail >> (8 * (16 - size));
    }
  } else if (size >= 4) {
    uint32_t head;
    uint32_t tail;
    std::memcpy(&head, data, sizeof head);
    std::memcpy(&tail, data + size - 4, sizeof tail);
    words[0] = head | (uint64_t{tail} << (8 * (size - 4)));
  } else if (size != 0) {
    words[0] = uint64_t{data[0]} |
               (uint64_t{data[size >> 1]} << (8 * (size >> 1))) |
               (uint64_t{data[size - 1]} << (8 * (size - 1)));
  }
  return std::bit_cast<Block>(words);
}

IRS_FORCE_INLINE inline uint32_t LowBits(size_t count) noexcept {
  SDB_ASSERT(count < kClassifyBlock);
  return ~(~uint32_t{0} << count);
}

template<typename ClassifyBlock>
IRS_FORCE_INLINE uint32_t ClassifyPadded(const byte_type* data, size_t size,
                                         ClassifyBlock& classify) {
  alignas(kClassifyBlock) byte_type block[kClassifyBlock];
  const Block padded = LoadPadded(data, size);
  std::memcpy(block, &padded, sizeof block);
  return classify(block) & LowBits(size);
}

template<typename ClassifyBlock, typename IsDelim, typename OnDelim>
IRS_FORCE_INLINE void DrainClassified(const byte_type* data, size_t size,
                                      bool use_blocks, ClassifyBlock classify,
                                      IsDelim is_delim, OnDelim on_delim) {
  if (!use_blocks) {
    for (size_t offset = 0; offset < size; ++offset) {
      if (is_delim(data[offset])) {
        on_delim(offset);
      }
    }
    return;
  }
  if (size < kClassifyBlock) {
    VisitSetBits(ClassifyPadded(data, size, classify),
                 [&](uint32_t bit) IRS_FORCE_INLINE { on_delim(bit); });
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

template<typename ClassifyBlock, typename OnRun>
IRS_FORCE_INLINE void ForEachRun(const byte_type* data, size_t size,
                                 ClassifyBlock classify, OnRun on_run) {
  constexpr size_t kBlock = kClassifyBlock;
  constexpr size_t kChunk = 2 * kBlock;
  constexpr uint64_t kAll = ~uint64_t{0};
  bool open = false;
  size_t begin = 0;
  const auto step = [&](uint64_t mask, size_t base, uint64_t carry,
                        uint64_t next_at, uint64_t unseen) IRS_FORCE_INLINE {
    uint64_t starts = mask & ~((mask << 1) | carry) & unseen;
    uint64_t ends = mask & ~((mask >> 1) | next_at) & unseen;
    if ((starts | ends) == 0) {
      return;
    }
    if (open) {
      on_run(begin, base + std::countr_zero(ends) + 1);
      ends &= ends - 1;
      open = false;
    }
    while (ends != 0) {
      on_run(base + std::countr_zero(starts),
             base + std::countr_zero(ends) + 1);
      starts &= starts - 1;
      ends &= ends - 1;
    }
    if (starts != 0) {
      begin = base + std::countr_zero(starts);
      open = true;
    }
  };
  if (size < kBlock) {
    step(ClassifyPadded(data, size, classify), 0, 0, 0, kAll);
    return;
  }
  size_t base = 0;
  uint64_t carry = 0;
  uint32_t lo = classify(data);
  while (base + kChunk + kBlock <= size) {
    const uint32_t hi = classify(data + base + kBlock);
    const uint32_t ahead = classify(data + base + kChunk);
    step(lo | (uint64_t{hi} << kBlock), base, carry,
         uint64_t{ahead & 1} << (kChunk - 1), kAll);
    carry = hi >> (kBlock - 1);
    lo = ahead;
    base += kChunk;
  }
  uint64_t mask = lo;
  size_t width = kBlock;
  if (size - base >= kChunk) {
    mask |= uint64_t{classify(data + base + kBlock)} << kBlock;
    width = kChunk;
  }
  const size_t end = base + width;
  if (end == size) {
    step(mask, base, carry, 0, kAll);
    SDB_ASSERT(!open);
    return;
  }
  const size_t tail_base = size - kBlock;
  const uint32_t tail = classify(data + tail_base);
  const size_t shift = end - tail_base;
  step(mask, base, carry, uint64_t{(tail >> shift) & 1} << (width - 1), kAll);
  step(tail, tail_base, mask >> (width - 1), 0, kAll << shift);
  SDB_ASSERT(!open);
}

size_t BuildUtf8CpBounds(const byte_type* data, size_t size, bool valid_utf8,
                         std::vector<uint32_t>& out);

}  // namespace irs::analysis::classify
