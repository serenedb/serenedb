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
#include <cstdint>

#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class MaskedLeaf {
 public:
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;

  void Single() noexcept {
    _len = 1;
    _at = kBlock - 1;
    _index = kBlock - 1;
    _packed = true;
  }

  void Reset(const block_io::FillLeaf& leaf, const UnalignedWord* bitset,
             uint32_t len) noexcept {
    _bitset = bitset;
    _len = len;
    _at = kBlock - len;
    _words = leaf.words;
    _run = leaf.IsRun();
    _packed = !leaf.Maskable();
    auto rank = kBlock - len;
    for (uint32_t w = 0; w != _words; ++w) {
      _ranks[w] = static_cast<uint8_t>(rank);
      rank += static_cast<uint32_t>(std::popcount(bitset[w]));
    }
  }

  uint32_t Index() const noexcept { return _index; }

  uint32_t Len() const noexcept { return _len; }

  void Materialize(doc_id_t base, doc_id_t* out) const noexcept {
    if (_packed) {
      return;
    }
    if (_run) {
      block_codec::FillProgression(out, _len, base, 1);
      return;
    }
    uint64_t words[block_codec::kMaxBitsetWords];
    std::memcpy(words, _bitset, size_t{_words} * sizeof(uint64_t));
    block_io::MaterializeBitsetFrom(base, words, 0, words[0], _words, out);
  }

  IRS_FORCE_INLINE doc_id_t Find(const doc_id_t* docs, doc_id_t base,
                                 doc_id_t target) noexcept {
    if (_packed) [[likely]] {
      if (_len == kBlock) [[likely]] {
        const auto* const it = BranchlessLowerBound<kBlock>(docs, target);
        _index = static_cast<uint32_t>(it - docs);
        return *it;
      }
      for (auto i = _at;; ++i) {
        if (target <= docs[i]) {
          _at = i;
          _index = i;
          return docs[i];
        }
      }
    }
    return FindMasked(base, target);
  }

  template<uint32_t N, typename Mask>
  IRS_FORCE_INLINE doc_id_t Match(const doc_id_t* docs, doc_id_t base,
                                  const doc_id_t* targets, uint32_t first,
                                  uint32_t end, Mask& hits,
                                  uint32_t* index) noexcept {
    if (!_packed) [[unlikely]] {
      doc_id_t doc = 0;
      for (auto i = first; i != end; ++i) {
        doc = FindMasked(base, targets[i]);
        hits |= static_cast<Mask>(doc == targets[i]) << i;
        index[i] = _index;
      }
      return doc;
    }
    const auto from = LowerBound(docs, _at, targets[first]);
    const auto to = LowerBound(docs, from, targets[end - 1] + 1);
    if (to - from < end - first) {
      for (auto at = from; at != to; ++at) {
        const auto doc = docs[at];
        const auto i = CountLess<N>(targets, doc);
        hits |= static_cast<Mask>(targets[i] == doc) << i;
        index[i] = at;
      }
    } else {
      for (auto i = first, at = from; i != end; ++i) {
        at = LowerBound(docs, at, targets[i]);
        hits |= static_cast<Mask>(docs[at] == targets[i]) << i;
        index[i] = at;
      }
    }
    _at = _index = std::min(to, kBlock - 1);
    return docs[_at];
  }

 private:
  static constexpr auto kBits = BitsRequired<uint64_t>();

  IRS_FORCE_INLINE uint32_t LowerBound(const doc_id_t* docs, uint32_t from,
                                       doc_id_t target) const noexcept {
    if (_len == kBlock) [[likely]] {
      return static_cast<uint32_t>(BranchlessLowerBound<kBlock>(docs, target) -
                                   docs);
    }
    while (from != kBlock && docs[from] < target) {
      ++from;
    }
    return from;
  }

  IRS_NO_INLINE doc_id_t FindMasked(doc_id_t base, doc_id_t target) noexcept {
    const auto first = base + 1;
    const auto bit = std::max(target, first) - first;
    if (_run) {
      _index = kBlock - _len + bit;
      return first + bit;
    }
    auto w = bit / kBits;
    const uint64_t word = _bitset[w];
    const auto shift = bit % kBits;
    _index = _ranks[w] + static_cast<uint32_t>(
                           std::popcount(word & ((uint64_t{1} << shift) - 1)));
    if (const auto rest = word >> shift; rest != 0) [[likely]] {
      return first + bit + static_cast<uint32_t>(std::countr_zero(rest));
    }
    while (++w != _words) {
      if (const uint64_t next = _bitset[w]; next != 0) {
        return static_cast<doc_id_t>(first + w * kBits +
                                     std::countr_zero(next));
      }
    }
    return doc_limits::eof();
  }

  const UnalignedWord* _bitset = nullptr;
  uint32_t _words = 0;
  uint32_t _len = 0;
  uint32_t _at = kBlock;
  uint32_t _index = kBlock - 1;
  bool _run = false;
  bool _packed = true;
  uint8_t _ranks[block_codec::kMaxBitsetWords];
};

}  // namespace irs::detail
