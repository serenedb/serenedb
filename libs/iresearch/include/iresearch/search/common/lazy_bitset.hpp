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
#include <cstdint>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

inline IRS_FORCE_INLINE uint64_t CountBitRange(
  const uint64_t* IRS_RESTRICT words, uint64_t begin, uint64_t end) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(begin < end);
  const auto first = begin / kBits;
  const auto last = (end - 1) / kBits;
  const uint64_t head = ~uint64_t{0} << (begin % kBits);
  const uint64_t tail = ~uint64_t{0} >> (kBits - 1 - (end - 1) % kBits);
  if (first == last) {
    return static_cast<uint64_t>(std::popcount(words[first] & head & tail));
  }
  auto total = static_cast<uint64_t>(std::popcount(words[first] & head));
  for (auto i = first + 1; i != last; ++i) {
    total += static_cast<uint64_t>(std::popcount(words[i]));
  }
  return total + static_cast<uint64_t>(std::popcount(words[last] & tail));
}

inline IRS_FORCE_INLINE uint64_t CountBitsetAt(const uint64_t* IRS_RESTRICT dst,
                                               uint64_t prev,
                                               const uint64_t* IRS_RESTRICT src,
                                               uint32_t words) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(words != 0);
  dst += prev / kBits;
  const auto shift = prev % kBits;
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

class LazyBitset {
 public:
  static constexpr auto kBits = BitsetStorage::kBits;

  LazyBitset(BitsetStorage&& set, const DocumentMask* removals) noexcept
    : _set{std::move(set)}, _filled{_set.End()} {
    Drop(removals, 0, _set.WordCount());
  }

  LazyBitset(FillNode::ptr&& node, doc_id_t docs_count,
             const DocumentMask* removals)
    : _set{docs_count}, _node{std::move(node)}, _removals{removals} {
    SDB_ASSERT(_node);
  }

  const uint64_t* Words() const noexcept { return _set.Words(); }

  doc_id_t Filled() const noexcept { return _filled; }

  doc_id_t End() const noexcept { return _set.End(); }

  void Reach(doc_id_t upto) {
    if (upto <= _filled) {
      return;
    }
    const auto end = _set.End();
    if (upto > end) {
      upto = end;
    }
    auto* const words = _set.Words();
    do {
      const auto min = _filled;
      const auto next =
        _node->FillOr(min, min + kWindowDocs, words + min / kBits);
      Drop(_removals, min / kBits, min / kBits + kWindowWords);
      if (next >= end) {
        _filled = end;
        break;
      }
      _filled = std::max(min + kWindowDocs, next - next % kWindowDocs);
    } while (_filled < upto);
    if (_filled >= end) {
      Finish();
    }
  }

  bool Contains(doc_id_t doc) {
    SDB_ASSERT(doc_limits::valid(doc));
    Reach(doc + 1);
    return CheckBit(_set.Words()[doc / kBits], doc % kBits);
  }

  doc_id_t Probe(doc_id_t target) {
    const auto end = _set.End();
    for (;;) {
      if (target >= end) {
        return doc_limits::eof();
      }
      if (target < _filled) {
        const auto stop = std::min(_filled, end);
        if (const auto doc = NextIn(_set.Words(), target, stop);
            doc_limits::valid(doc)) {
          return doc;
        }
        target = stop;
        continue;
      }
      Reach(target + 1);
    }
  }

 private:
  static doc_id_t NextIn(const uint64_t* IRS_RESTRICT words, doc_id_t from,
                         doc_id_t stop) noexcept {
    if (from >= stop) {
      return doc_limits::invalid();
    }
    auto word = static_cast<uint32_t>(from / kBits);
    const auto last = static_cast<uint32_t>((stop - 1) / kBits);
    auto rest = words[word] & (~uint64_t{0} << (from % kBits));
    while (rest == 0) {
      if (word == last) {
        return doc_limits::invalid();
      }
      rest = words[++word];
    }
    const auto doc = static_cast<doc_id_t>(
      size_t{word} * kBits + static_cast<size_t>(std::countr_zero(rest)));
    return doc < stop ? doc : doc_limits::invalid();
  }

  void Drop(const DocumentMask* removals, size_t first, size_t last) noexcept {
    if (removals == nullptr) {
      return;
    }
    auto* const words = _set.Words();
    last = std::min(last, size_t{_set.WordCount()});
    for (auto w = first; w < last; ++w) {
      auto rest = words[w];
      while (rest != 0) {
        const auto bit = static_cast<size_t>(std::countr_zero(rest));
        rest &= rest - 1;
        const auto doc = static_cast<doc_id_t>(w * kBits + bit);
        if (removals->contains(doc)) {
          UnsetBit(words[w], bit);
        }
      }
    }
  }

  void Finish() noexcept {
    if (_node) {
      _set.Trim();
      _node.reset();
    }
  }

  BitsetStorage _set;
  FillNode::ptr _node;
  const DocumentMask* _removals = nullptr;
  doc_id_t _filled = 0;
};

class CountAgainst {
 public:
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr bool kOrdered = true;

  explicit CountAgainst(LazyBitset& set) noexcept : _set{&set} {}

  uint64_t Total() const noexcept { return _total; }

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) {
    const auto begin = prev + 1;
    const auto end = begin + len;
    _set->Reach(static_cast<doc_id_t>(end));
    _total += CountBitRange(_set->Words(), begin, end);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev, const uint64_t* IRS_RESTRICT src,
                               uint32_t n, uint64_t max) {
    _set->Reach(static_cast<doc_id_t>(max + 1));
    _total += CountBitsetAt(_set->Words(), prev, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) {
    _set->Reach(static_cast<doc_id_t>(doc + 1));
    _total +=
      static_cast<uint64_t>(CheckBit(_set->Words()[doc / kBits], doc % kBits));
  }

  IRS_FORCE_INLINE void Finish(uint32_t) noexcept {}

 private:
  LazyBitset* _set;
  uint64_t _total = 0;
};

}  // namespace irs::search
