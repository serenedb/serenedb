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
#include <cmath>
#include <cstdint>
#include <utility>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

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

// One set the segment's predicates fold into, filled window by window as it
// is asked about docs further on: the fill node's docs, less the removals,
// less what the table filter's column predicates reject. Without a fill node
// every doc is in until a predicate takes it out. The table filter's column
// scans only move forward, which the sequential fill honours; the set is what
// a walk that visits docs in any order asks, and what a count sums.
class LazyBitset {
 public:
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr auto kMin = BitsetStorage::kMin;

  LazyBitset(BitsetStorage&& set, const DocumentMask* removals,
             TableFilter* table = nullptr) noexcept
    : _set{std::move(set)}, _filled{_set.End()} {
    Drop(removals, 0, _set.WordCount());
    if (table != nullptr) {
      SDB_ASSERT(table->Foldable());
      _set.Trim();
      auto* const words = _set.Words();
      for (uint32_t first = 0, n = _set.WordCount(); first < n;
           first += kWindowWords) {
        table->Narrow(kMin + static_cast<doc_id_t>(first) * kBits,
                      words + first, nullptr,
                      std::min<uint32_t>(kWindowWords, n - first));
      }
    }
  }

  LazyBitset(FillNode::ptr&& node, doc_id_t docs_count,
             const DocumentMask* removals, TableFilter* table = nullptr)
    : _set{docs_count},
      _node{std::move(node)},
      _removals{removals},
      _table{table} {
    SDB_ASSERT(_node);
    SDB_ASSERT(_table == nullptr || _table->Foldable());
  }

  // Every doc of the segment, narrowed by the removals and the table filter.
  LazyBitset(doc_id_t docs_count, const DocumentMask* removals,
             TableFilter* table)
    : _set{docs_count}, _removals{removals}, _table{table} {
    SDB_ASSERT(_table == nullptr || _table->Foldable());
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
      const auto first = (min - kMin) / kBits;
      doc_id_t next;
      if (_node) {
        next = _node->FillOr(min, min + kWindowDocs, words + first);
      } else {
        std::fill_n(words + first, kWindowWords, ~uint64_t{0});
        next = min + kWindowDocs;
      }
      Drop(_removals, first, first + kWindowWords);
      if (_table != nullptr) {
        const auto last =
          std::min<size_t>(first + kWindowWords, _set.WordCount());
        if (last == _set.WordCount()) {
          // The table reads the docs the bits name, so none may lie past the
          // segment's end.
          _set.Trim();
        }
        _table->Narrow(min, words + first, nullptr,
                       static_cast<uint32_t>(last - first));
      }
      if (next >= end) {
        _filled = end;
        break;
      }
      // Windows the node has no doc in stay zero and are skipped; a table only
      // ever looks at set bits, and skipping forward is the one direction its
      // column scans move in.
      _filled = std::max(min + kWindowDocs, BitsetStorage::WindowMin(next));
    } while (_filled < upto);
    if (_filled >= end) {
      Finish();
    }
  }

  // The number of docs in the set; fills whatever is left first.
  uint64_t Count() {
    const auto end = _set.End();
    Reach(end);
    if (end <= kMin) {
      return 0;
    }
    return CountBitRange(_set.Words(), 0, end - kMin);
  }

  // A bounded sample of the set, for a caller that has to choose a plan before
  // it can afford the whole thing. Counting the set outright is what forces a
  // column predicate over the entire segment; on a million rows that is a flat
  // 1.6 ms, spent before anything is known about whether the answer even needs
  // it. This fills a prefix of `windows` windows, counts that, and scales to
  // the segment, rounding up because a caller weighing a scan against a walk
  // can recover from too large a count and not from too small a one.
  //
  // The prefix is filled the way `Reach` fills it, so a caller that goes on to
  // fold the whole set pays for the sample once, not twice.
  uint64_t EstimateCount(uint32_t windows) {
    const auto end = _set.End();
    if (end <= kMin) {
      return 0;
    }
    const uint64_t total = end - kMin;
    const uint64_t want =
      std::min<uint64_t>(total, uint64_t{windows} * kWindowDocs);
    Reach(kMin + static_cast<doc_id_t>(want));
    const uint64_t filled = _filled - kMin;
    if (filled == 0) {
      return 0;
    }
    const uint64_t seen = CountBitRange(_set.Words(), 0, filled);
    if (filled >= total || seen == 0) {
      return seen;
    }
    const auto share =
      static_cast<long double>(seen) / static_cast<long double>(filled);
    const auto est = static_cast<uint64_t>(
      std::ceil(share * static_cast<long double>(total) * 1.125L));
    return std::min<uint64_t>(est, total);
  }

  bool Contains(doc_id_t doc) {
    SDB_ASSERT(doc_limits::valid(doc));
    Reach(doc + 1);
    const auto offset = doc - kMin;
    return CheckBit(_set.Words()[offset / kBits], offset % kBits);
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
    const auto offset = from - kMin;
    auto word = static_cast<uint32_t>(offset / kBits);
    const auto last = static_cast<uint32_t>((stop - 1 - kMin) / kBits);
    auto rest = words[word] & (~uint64_t{0} << (offset % kBits));
    while (rest == 0) {
      if (word == last) {
        return doc_limits::invalid();
      }
      rest = words[++word];
    }
    const auto doc =
      static_cast<doc_id_t>(kMin + size_t{word} * kBits +
                            static_cast<size_t>(std::countr_zero(rest)));
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
        const auto doc = static_cast<doc_id_t>(kMin + w * kBits + bit);
        if (removals->contains(doc)) {
          UnsetBit(words[w], bit);
        }
      }
    }
  }

  void Finish() noexcept {
    _set.Trim();
    _node.reset();
    _table = nullptr;
  }

  BitsetStorage _set;
  FillNode::ptr _node;
  const DocumentMask* _removals = nullptr;
  TableFilter* _table = nullptr;
  doc_id_t _filled = kMin;
};

class CountAgainst {
 public:
  static constexpr auto kBits = BitsetStorage::kBits;
  static constexpr auto kMin = BitsetStorage::kMin;
  static constexpr bool kOrdered = true;

  explicit CountAgainst(LazyBitset& set) noexcept : _set{&set} {}

  uint64_t Total() const noexcept { return _total; }

  IRS_FORCE_INLINE void Run(uint64_t prev, uint32_t len) {
    const auto begin = prev + 1;
    const auto end = begin + len;
    _set->Reach(static_cast<doc_id_t>(end));
    _total += CountBitRange(_set->Words(), begin - kMin, end - kMin);
  }

  IRS_FORCE_INLINE void Bitset(uint64_t prev, const uint64_t* IRS_RESTRICT src,
                               uint32_t n, uint64_t max) {
    _set->Reach(static_cast<doc_id_t>(max + 1));
    _total +=
      CountBlock(_set->Words(), static_cast<int64_t>(prev) - kMin, src, n);
  }

  IRS_FORCE_INLINE void Doc(size_t doc) {
    _set->Reach(static_cast<doc_id_t>(doc + 1));
    const auto offset = doc - kMin;
    _total += static_cast<uint64_t>(
      CheckBit(_set->Words()[offset / kBits], offset % kBits));
  }

  IRS_FORCE_INLINE void Finish(uint32_t) noexcept {}

 private:
  LazyBitset* _set;
  uint64_t _total = 0;
};

}  // namespace irs::detail
