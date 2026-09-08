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
#include <array>
#include <numeric>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/conjunction_leaves.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/slop_phrase.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Matcher, typename Leaf, size_t N = 0>
class PhraseFixedSlots {
 public:
  template<typename... Args>
  PhraseFixedSlots(std::span<const PostingMeta* const> metas,
                   std::span<const TermInterval> intervals,
                   const IndexInput& doc_in, IndexFeatures layout,
                   const IndexInput& pos_in, const IndexInput* pay_in,
                   Args&&... args)
    : _leaves(metas.size()),
      _matcher{metas.size(), std::forward<Args>(args)...} {
    SDB_ASSERT(metas.size() == intervals.size());
    _leaves.Open(
      metas,
      [&](Leaf& leaf, const PostingMeta& meta) {
        leaf.Prepare(meta, doc_in, layout, pos_in, pay_in);
      },
      [&](uint32_t j, typename Leaves::Slot& slot) {
        _matcher.Position(j) = {&slot.leaf.Positions(), intervals[j]};
      });
    _matcher.Finish();
  }

  PhraseFixedSlots(PhraseFixedSlots&&) = delete;
  PhraseFixedSlots& operator=(PhraseFixedSlots&&) = delete;

  doc_id_t Seek(doc_id_t from) { return _leaves.Seek(from); }

  doc_id_t Next(doc_id_t) { return _leaves.Next(); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  bool Match(doc_id_t) {
    if constexpr (requires { _matcher.Match(); }) {
      return _matcher.Match();
    } else {
      return _matcher.template Match<false>();
    }
  }

  static constexpr bool kHasFreqBound =
    requires(Matcher& m) { m.DocFreqBound(); };

  uint32_t FreqBound()
    requires(kHasFreqBound)
  {
    return _matcher.DocFreqBound();
  }

  bool MatchOrdered(doc_id_t)
    requires(kHasFreqBound)
  {
    return _matcher.template Match<true>();
  }

  uint32_t Freq() const noexcept { return _matcher.GetFreq(); }

  static constexpr bool kOffsets = Leaf::kOffsets;
  static_assert(kOffsets == Matcher::kOffsets);

  std::pair<uint32_t, uint32_t> Offsets() const noexcept
    requires(kOffsets)
  {
    return _matcher.Offsets();
  }

  bool NextAlignment()
    requires(kOffsets)
  {
    return _matcher.NextAlignment();
  }

  static constexpr bool kHasBoost = Matcher::kHasBoost;

  score_t Boost() const noexcept
    requires(kHasBoost)
  {
    return _matcher.GetBoost();
  }

 private:
  using Leaves = ConjunctionLeaves<Leaf, N>;

  Leaves _leaves;
  Matcher _matcher;
};

}  // namespace irs::search
