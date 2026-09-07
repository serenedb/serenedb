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

  BitsetStorage() = default;

  explicit BitsetStorage(doc_id_t docs_count)
    : _end{docs_count + doc_limits::min()},
      _words{static_cast<uint32_t>((_end + (kBits - 1)) / kBits)},
      _alloc{_words + kWindowDocs / kBits + 1},
      _bits{std::make_unique<uint64_t[]>(_alloc)} {}

  uint64_t* Words() noexcept { return _bits.get(); }
  const uint64_t* Words() const noexcept { return _bits.get(); }

  uint32_t WordCount() const noexcept { return _words; }
  uint32_t Alloc() const noexcept { return _alloc; }

  doc_id_t End() const noexcept { return _end; }

  void Trim() noexcept {
    if (const auto tail = _end % kBits; tail != 0) {
      _bits[_end / kBits] &= ~uint64_t{0} >> (kBits - tail);
    }
    std::fill(_bits.get() + _words, _bits.get() + _alloc, uint64_t{0});
    SDB_ASSERT(!CheckBit(_bits[0], 0));
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
  if (from >= set.End()) {
    return doc_limits::eof();
  }
  const auto* const bits = set.Words();
  const auto count = set.WordCount();
  auto word = static_cast<uint32_t>(from / kBits);
  auto rest = bits[word] & (~uint64_t{0} << (from % kBits));
  while (rest == 0) {
    if (++word == count) {
      return doc_limits::eof();
    }
    rest = bits[word];
  }
  return static_cast<doc_id_t>(size_t{word} * kBits +
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
    mask[w] |= at(min + size_t{w} * kBits);
  }
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] |=
      at(min + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest));
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
    mask[w] &= at(min + size_t{w} * kBits);
  }
  auto tail = full;
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] &=
      at(min + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest));
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
    mask[w] &= ~at(min + size_t{w} * kBits);
  }
  if (const auto rest = len % kBits; rest != 0) {
    mask[full] &=
      ~(at(min + size_t{full} * kBits) & (~uint64_t{0} >> (kBits - rest)));
  }
}

}  // namespace irs::search
