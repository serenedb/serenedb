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

#include <absl/algorithm/container.h>

#include <array>
#include <bit>
#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

std::vector<uint32_t> DecodePrefixes(bytes_view encoded, field_id id);

class PrefixIndex {
 public:
  explicit PrefixIndex(std::vector<uint32_t> prefixes);

  size_t MemoryBytes() const noexcept {
    return sizeof(*this) +
           (_prefixes.capacity() + _pair_starts.capacity()) * sizeof(uint32_t);
  }

  uint32_t NextLabel(bytes_view head, uint32_t from,
                     const uint32_t*& at) const noexcept {
    if (head.empty()) {
      from = NextBit(_firsts.data(), from);
      if (from != kNoLabel) {
        at = _prefixes.data() + _buckets[from];
      }
      return from;
    }
    const auto first = head[0];
    if (head.size() == 1) {
      from = NextBit(_pairs.data() + first * kRowWords, from);
      if (from != kNoLabel) {
        at = _prefixes.data() + _pair_starts[Rank(first, from)];
      }
      return from;
    }
    const auto second = head[1];
    if (!HasBit(_pairs.data() + first * kRowWords, second)) {
      return kNoLabel;
    }
    const auto pair = Rank(first, second);
    const auto* begin = _prefixes.data() + _pair_starts[pair];
    const auto* end = _prefixes.data() + _pair_starts[pair + 1];
    const uint32_t shift = 8 * (kTermPrefix - 1 - head.size());
    const uint32_t base = TermPrefix(head);
    const uint32_t key = base | from << shift;
    if (!at || at < begin || at > end || (at != begin && at[-1] >= key)) {
      at = begin;
    }
    at = Gallop(at, end, key);
    if (at == end || ((*at ^ base) >> shift >> 8) != 0) {
      return kNoLabel;
    }
    return *at >> shift & 0xFF;
  }

 private:
  static constexpr size_t kRowWords = kNoLabel / 64;

  static bool HasBit(const uint64_t* row, uint32_t bit) noexcept {
    return (row[bit / 64] >> (bit % 64) & 1) != 0;
  }

  static uint32_t NextBit(const uint64_t* row, uint32_t from) noexcept {
    for (uint32_t word = from / 64; word < kRowWords; ++word) {
      uint64_t bits = row[word];
      if (word == from / 64) {
        bits &= ~uint64_t{0} << (from % 64);
      }
      if (bits != 0) {
        return word * 64 + static_cast<uint32_t>(std::countr_zero(bits));
      }
    }
    return kNoLabel;
  }

  uint32_t Rank(uint32_t first, uint32_t second) const noexcept {
    const auto* row = _pairs.data() + first * kRowWords;
    uint32_t rank = _pair_rows[first];
    for (uint32_t word = 0; word != second / 64; ++word) {
      rank += static_cast<uint32_t>(std::popcount(row[word]));
    }
    const uint64_t below = (uint64_t{1} << (second % 64)) - 1;
    return rank +
           static_cast<uint32_t>(std::popcount(row[second / 64] & below));
  }

  static const uint32_t* Gallop(const uint32_t* at, const uint32_t* end,
                                uint32_t key) noexcept {
    if (at == end || *at >= key) {
      return at;
    }
    size_t step = 1;
    while (step < static_cast<size_t>(end - at) && at[step] < key) {
      at += step;
      step <<= 1;
    }
    const std::span rest{
      at + 1, step < static_cast<size_t>(end - at) ? at + step + 1 : end};
    return rest.data() + (absl::c_lower_bound(rest, key) - rest.begin());
  }

  std::vector<uint32_t> _prefixes;
  std::vector<uint32_t> _pair_starts;
  std::array<uint64_t, kRowWords * kNoLabel> _pairs{};
  std::array<uint64_t, kRowWords> _firsts{};
  std::array<uint32_t, kNoLabel + 1> _buckets{};
  std::array<uint32_t, kNoLabel + 1> _pair_rows{};
};

}  // namespace irs::term_dict
