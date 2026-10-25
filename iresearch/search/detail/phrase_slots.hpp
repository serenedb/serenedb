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

#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/conjunction_leaves.hpp"
#include "iresearch/search/detail/phrase_matcher.hpp"
#include "iresearch/search/detail/phrase_variadic_pos.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Matcher, typename Leaf, size_t N = 0, typename Slot = Leaf>
class PhraseSlots {
  static constexpr bool kVariadic = !std::is_same_v<Slot, Leaf>;

 public:
  template<typename... Args>
  PhraseSlots(std::span<const PostingMeta> metas,
              std::span<const TermInterval> intervals, const IndexInput& doc_in,
              IndexFeatures layout, const IndexInput& pos_in,
              const IndexInput* pay_in, Args&&... args)
    : _matcher{metas.size(), std::forward<Args>(args)...},
      _leaves{metas.size(), [&](uint32_t j) { return metas[j].docs_count; },
              [&](uint32_t j) {
                return std::forward_as_tuple(metas[j], doc_in, layout, pos_in,
                                             pay_in);
              },
              [&](uint32_t j, Leaf& leaf) {
                _matcher.Position(j) = {&leaf.Positions(), intervals[j]};
              }} {
    static_assert(!kVariadic);
    SDB_ASSERT(metas.size() == intervals.size());
    SDB_ASSERT(metas.size() > 1);
    _matcher.Finish();
  }

  template<typename... Args>
  PhraseSlots(std::span<const PostingMeta> metas,
              std::span<const uint32_t> offsets,
              std::span<const score_t> boosts,
              std::span<const TermInterval> intervals, const IndexInput& doc_in,
              IndexFeatures layout, const IndexInput& pos_in,
              const IndexInput* pay_in, Args&&... args)
    : _terms{metas.size(), std::piecewise_construct,
             [&](size_t i) {
               return std::forward_as_tuple(metas[i], doc_in, layout, pos_in,
                                            pay_in);
             }},
      _matcher{intervals.size(), std::forward<Args>(args)...},
      _leaves{intervals.size(),
              [&](uint32_t j) {
                uint64_t estimate = 0;
                for (auto i = offsets[j]; i != offsets[j + 1]; ++i) {
                  estimate += metas[i].docs_count;
                }
                return estimate;
              },
              [&](uint32_t j) {
                const auto offset = offsets[j];
                return std::tuple<Leaf*, uint32_t, const score_t*>{
                  _terms.data() + offset, offsets[j + 1] - offset,
                  boosts.empty() ? nullptr : boosts.data() + offset};
              },
              [&](uint32_t j, Slot& slot) {
                _matcher.Position(j) = {&slot.Positions(), intervals[j]};
              }} {
    static_assert(kVariadic);
    SDB_ASSERT(offsets.size() == intervals.size() + 1);
    SDB_ASSERT(offsets.back() == metas.size());
    SDB_ASSERT(boosts.empty() == !Slot::Position::kHasBoost);
    SDB_ASSERT(boosts.empty() || boosts.size() == metas.size());
    SDB_ASSERT(intervals.size() > 1 || metas.size() > 1);
    _matcher.Finish();
    for (auto& slot : _leaves) {
      if (auto* leaf = slot.Single()) {
        _bound = leaf;
        break;
      }
    }
  }

  PhraseSlots(PhraseSlots&&) = delete;
  PhraseSlots& operator=(PhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t from) { return _leaves.Seek(from); }

  doc_id_t Next(doc_id_t) { return _leaves.Next(); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  bool Match(doc_id_t) {
    Collect();
    return _matcher.Match();
  }

  static constexpr bool kHasFreqBound =
    requires(Matcher& m) { m.DocFreqBound(); };

  uint32_t FreqBound()
    requires(kHasFreqBound)
  {
    Collect();
    return _matcher.DocFreqBound();
  }

  bool MatchOrdered(doc_id_t)
    requires(kHasFreqBound)
  {
    return _matcher.template Match<true>();
  }

  uint32_t FreqBoundOf(uint32_t freq) const noexcept
    requires(kHasFreqBound)
  {
    return _matcher.FreqBoundOf(freq);
  }

  Leaf* BoundLeaf() noexcept {
    if constexpr (kVariadic) {
      return _bound;
    } else {
      return &_leaves.Lead();
    }
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

  score_t Scale() const noexcept
    requires(Matcher::kHasScale)
  {
    return _matcher.GetScale();
  }

 private:
  using Leaves = ConjunctionLeaves<Slot, N>;

  void Collect() {
    if constexpr (kVariadic) {
      for (auto& slot : _leaves) {
        slot.OnMatch();
      }
    }
  }

  [[no_unique_address]] utils::Need<kVariadic, containers::Fixed<Leaf>> _terms;
  Matcher _matcher;
  Leaves _leaves;
  [[no_unique_address]] utils::Need<kVariadic, Leaf*> _bound{};
};

}  // namespace irs::detail
