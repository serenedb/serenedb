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
#include <bit>
#include <cstdlib>
#include <memory>

#include "basics/bit_utils.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

class BitsetStorage {
 public:
  static constexpr auto kBits = BitsRequired<uint64_t>();
  static constexpr auto kWordShift = std::countr_zero(kBits);
  static constexpr doc_id_t kMin = doc_limits::min();

  static constexpr doc_id_t WindowMin(doc_id_t doc) noexcept {
    return doc - (doc - kMin) % kWindowDocs;
  }

  BitsetStorage() = default;

  explicit BitsetStorage(doc_id_t docs_count)
    : _end{kMin + docs_count},
      _words{static_cast<uint32_t>((docs_count + (kBits - 1)) / kBits)},
      _alloc{_words + kWindowDocs / kBits + 1},
      _bits{std::make_unique<uint64_t[]>(_alloc + 1)} {}

  uint64_t* Words() noexcept { return _bits.get() + 1; }
  const uint64_t* Words() const noexcept { return _bits.get() + 1; }

  uint32_t WordCount() const noexcept { return _words; }
  uint32_t Alloc() const noexcept { return _alloc; }

  doc_id_t End() const noexcept { return _end; }

  void Trim() noexcept {
    auto* const words = Words();
    const auto used = _end - kMin;
    if (const auto tail = used % kBits; tail != 0) {
      words[used / kBits] &= ~uint64_t{0} >> (kBits - tail);
    }
    std::fill(words + _words, words + _alloc, uint64_t{0});
  }

 private:
  doc_id_t _end = 0;
  uint32_t _words = 0;
  uint32_t _alloc = 0;
  std::unique_ptr<uint64_t[]> _bits;
};

inline uint64_t CountBits(const BitsetStorage& set) noexcept {
  const auto* const words = set.Words();
  uint64_t total = 0;
  for (uint32_t i = 0, n = set.WordCount(); i != n; ++i) {
    total += static_cast<uint64_t>(std::popcount(words[i]));
  }
  return total;
}

inline doc_id_t NextBit(const BitsetStorage& set, doc_id_t from) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  SDB_ASSERT(doc_limits::valid(from));
  if (from >= set.End()) {
    return doc_limits::eof();
  }
  const auto* const bits = set.Words();
  const auto count = set.WordCount();
  const auto offset = from - BitsetStorage::kMin;
  auto word = static_cast<uint32_t>(offset / kBits);
  auto rest = bits[word] & (~uint64_t{0} << (offset % kBits));
  while (rest == 0) {
    if (++word == count) {
      return doc_limits::eof();
    }
    rest = bits[word];
  }
  return BitsetStorage::kMin +
         static_cast<doc_id_t>(size_t{word} * kBits +
                               static_cast<size_t>(std::countr_zero(rest)));
}

inline void OrWindow(const BitsetStorage& set, doc_id_t min, doc_id_t max,
                     uint64_t* IRS_RESTRICT mask) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  const auto stop = std::min(max, set.End());
  if (min >= stop) {
    return;
  }
  const auto* const bits = set.Words();
  const auto base = min - BitsetStorage::kMin;
  const auto at = [&](size_t offset) IRS_FORCE_INLINE {
    const auto word = offset / kBits;
    const auto shift = offset % kBits;
    const auto value = bits[word] >> shift;
    if (shift == 0) {
      return value;
    }
    return value | (bits[word + 1] << (kBits - shift));
  };
  const uint32_t len = stop - min;
  const uint32_t full = len / kBits;
  for (uint32_t w = 0; w != full; ++w) {
    mask[w] |= at(base + size_t{w} * kBits);
  }
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] |=
      at(base + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest));
  }
}

inline void AndWindow(const BitsetStorage& set, doc_id_t min, doc_id_t max,
                      uint64_t* IRS_RESTRICT mask) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  const auto words = WindowWords(min, max);
  const auto stop = std::min(max, set.End());
  if (min >= stop) {
    Clear(mask, words);
    return;
  }
  const auto* const bits = set.Words();
  const auto base = min - BitsetStorage::kMin;
  const auto at = [&](size_t offset) IRS_FORCE_INLINE {
    const auto word = offset / kBits;
    const auto shift = offset % kBits;
    const auto value = bits[word] >> shift;
    if (shift == 0) {
      return value;
    }
    return value | (bits[word + 1] << (kBits - shift));
  };
  const uint32_t len = stop - min;
  const uint32_t full = len / kBits;
  for (uint32_t w = 0; w != full; ++w) {
    mask[w] &= at(base + size_t{w} * kBits);
  }
  auto tail = full;
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] &=
      at(base + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest));
    ++tail;
  }
  for (auto w = tail; w != words; ++w) {
    mask[w] = 0;
  }
}

inline void AndNotWindow(const BitsetStorage& set, doc_id_t min, doc_id_t max,
                         uint64_t* IRS_RESTRICT mask) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  const auto stop = std::min(max, set.End());
  if (min >= stop) {
    return;
  }
  const auto* const bits = set.Words();
  const auto base = min - BitsetStorage::kMin;
  const auto at = [&](size_t offset) IRS_FORCE_INLINE {
    const auto word = offset / kBits;
    const auto shift = offset % kBits;
    const auto value = bits[word] >> shift;
    if (shift == 0) {
      return value;
    }
    return value | (bits[word + 1] << (kBits - shift));
  };
  const uint32_t len = stop - min;
  const uint32_t full = len / kBits;
  for (uint32_t w = 0; w != full; ++w) {
    mask[w] &= ~at(base + size_t{w} * kBits);
  }
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] &=
      ~(at(base + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest)));
  }
}

inline IRS_FORCE_INLINE void OrBlock(uint64_t* IRS_RESTRICT dst, int64_t begin,
                                     const uint64_t* IRS_RESTRICT src,
                                     uint32_t words) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  SDB_ASSERT(words != 0);
  SDB_ASSERT(begin >= -1);
  dst += begin >> BitsetStorage::kWordShift;
  const auto shift = static_cast<uint32_t>(begin & (kBits - 1));
  if (shift == 0) {
    for (uint32_t i = 0; i != words; ++i) {
      dst[i] |= src[i];
    }
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != words; ++i) {
    const auto word = src[i];
    dst[i] |= (word << shift) | carry;
    carry = word >> (kBits - shift);
  }
  dst[words] |= carry;
}

inline IRS_FORCE_INLINE void ClearBlock(uint64_t* IRS_RESTRICT dst,
                                        int64_t begin,
                                        const uint64_t* IRS_RESTRICT src,
                                        uint32_t words) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  SDB_ASSERT(words != 0);
  SDB_ASSERT(begin >= -1);
  dst += begin >> BitsetStorage::kWordShift;
  const auto shift = static_cast<uint32_t>(begin & (kBits - 1));
  if (shift == 0) {
    for (uint32_t i = 0; i != words; ++i) {
      dst[i] &= ~src[i];
    }
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != words; ++i) {
    const auto word = src[i];
    dst[i] &= ~((word << shift) | carry);
    carry = word >> (kBits - shift);
  }
  dst[words] &= ~carry;
}

inline IRS_FORCE_INLINE uint64_t CountBlock(const uint64_t* IRS_RESTRICT dst,
                                            int64_t begin,
                                            const uint64_t* IRS_RESTRICT src,
                                            uint32_t words) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  SDB_ASSERT(words != 0);
  SDB_ASSERT(begin >= -1);
  dst += begin >> BitsetStorage::kWordShift;
  const auto shift = static_cast<uint32_t>(begin & (kBits - 1));
  uint64_t total = 0;
  if (shift == 0) {
    for (uint32_t i = 0; i != words; ++i) {
      total += static_cast<uint64_t>(std::popcount(dst[i] & src[i]));
    }
    return total;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != words; ++i) {
    const auto word = src[i];
    total +=
      static_cast<uint64_t>(std::popcount(dst[i] & ((word << shift) | carry)));
    carry = word >> (kBits - shift);
  }
  return total + static_cast<uint64_t>(std::popcount(dst[words] & carry));
}

inline IRS_FORCE_INLINE void RetainBlock(uint64_t* IRS_RESTRICT dst,
                                         int64_t begin,
                                         const uint64_t* IRS_RESTRICT src,
                                         uint32_t words,
                                         uint64_t last) noexcept {
  constexpr auto kBits = BitsetStorage::kBits;
  SDB_ASSERT(words != 0);
  SDB_ASSERT(begin >= -1);
  SDB_ASSERT(static_cast<int64_t>(last) > begin);
  const auto shift = static_cast<uint32_t>(begin & (kBits - 1));
  auto* const base = dst + (begin >> BitsetStorage::kWordShift);
  const auto stop = static_cast<uint32_t>(
    (static_cast<int64_t>(last) >> BitsetStorage::kWordShift) -
    (begin >> BitsetStorage::kWordShift));
  const auto top = last & (kBits - 1);
  const uint64_t above =
    top == kBits - 1 ? uint64_t{0} : (~uint64_t{0} << (top + 1));
  uint64_t keep = (uint64_t{2} << shift) - 1;
  uint64_t carry = 0;
  for (uint32_t i = 0; i <= stop; ++i) {
    const auto word = i < words ? src[i] : uint64_t{0};
    uint64_t mask;
    if (shift == 0) {
      mask = word;
    } else {
      mask = (word << shift) | carry;
      carry = word >> (kBits - shift);
    }
    mask |= keep;
    keep = 0;
    if (i == stop) {
      mask |= above;
    }
    base[i] &= mask;
  }
}

}  // namespace irs::search
