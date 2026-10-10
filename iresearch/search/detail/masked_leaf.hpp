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

  void Reset(const block_io::FillLeaf& leaf, const uint64_t* bitset,
             uint32_t len) noexcept {
    _bitset = bitset;
    _len = len;
    _at = kBlock - len;
    _words = leaf.words;
    _prefix_word = 0;
    _prefix_bits = 0;
    _run = leaf.IsRun();
    _packed = !leaf.Maskable();
  }

  uint32_t Index() const noexcept { return _index; }

  uint32_t Len() const noexcept { return _len; }

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

 private:
  static constexpr auto kBits = BitsRequired<uint64_t>();

  IRS_NO_INLINE doc_id_t FindMasked(doc_id_t base, doc_id_t target) noexcept {
    const auto first = base + 1;
    if (target < first) {
      target = first;
    }
    if (_run) {
      _index = kBlock - _len + (target - first);
      return target;
    }
    auto bit = static_cast<uint64_t>(target) - first;
    for (auto w = bit / kBits; w != _words; ++w) {
      const auto word = _bitset[w] & (~uint64_t{0} << (bit % kBits));
      if (word != 0) {
        const auto tz = static_cast<uint32_t>(std::countr_zero(word));
        for (; _prefix_word != w; ++_prefix_word) {
          _prefix_bits +=
            static_cast<uint32_t>(std::popcount(_bitset[_prefix_word]));
        }
        _index = kBlock - _len + _prefix_bits +
                 static_cast<uint32_t>(
                   std::popcount(_bitset[w] & ((uint64_t{1} << tz) - 1)));
        return static_cast<doc_id_t>(first + w * kBits + tz);
      }
      bit = (w + 1) * kBits;
    }
    return doc_limits::eof();
  }

  const uint64_t* _bitset = nullptr;
  uint64_t _prefix_word = 0;
  uint32_t _words = 0;
  uint32_t _len = 0;
  uint32_t _at = kBlock;
  uint32_t _index = kBlock - 1;
  uint32_t _prefix_bits = 0;
  bool _run = false;
  bool _packed = true;
};

}  // namespace irs::detail
