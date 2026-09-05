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
#include <cstdint>
#include <numeric>
#include <span>
#include <type_traits>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/node_of.hpp"
#include "iresearch/search/common/phrase_fixed_slots.hpp"
#include "iresearch/search/common/phrase_variadic_slots.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_pos.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"
#include "iresearch/search/slop_phrase.hpp"

namespace irs::search {

enum class PhraseMatch : uint8_t {
  Plain,
  Intervals,
  Slop,
};

template<typename Query>
PhraseMatch MatchOf(const Query& query) noexcept {
  if (query.slop != 0) {
    return PhraseMatch::Slop;
  }
  return query.has_intervals ? PhraseMatch::Intervals : PhraseMatch::Plain;
}

template<typename Query, typename Slop, typename Intervals, typename Plain>
auto ResolveMatch(const Query& query, Slop&& slop, Intervals&& intervals,
                  Plain&& plain) {
  switch (MatchOf(query)) {
    case PhraseMatch::Slop:
      return slop();
    case PhraseMatch::Intervals:
      return intervals();
    case PhraseMatch::Plain:
      break;
  }
  return plain();
}

template<PhraseMatch M, bool Bounds, typename Input, bool HasFreq = false,
         bool Offs = false, bool HasBoost = false, typename Query, typename F>
auto ResolveSlotMatcherOf(const Query& query, F&& f) {
  using Leaf = search::PostingPos<Input, Bounds, Offs>;
  if constexpr (M == PhraseMatch::Slop) {
    using Slot =
      std::pair<search::PhraseVariadicPositions<Leaf>*, TermInterval>;
    return f.template operator()<SlopPhrase<Slot, Offs, HasFreq>, Leaf>(
      query.slop, BuildExpectedSteps(query.positions));
  } else {
    static constexpr bool kIntervals = M == PhraseMatch::Intervals;
    using Slot =
      std::pair<search::PhraseVariadicPositions<Leaf, HasBoost>*, TermInterval>;
    return f.template operator()<
      PhraseFrequency<Slot, Offs, HasFreq, kIntervals, HasBoost>, Leaf>();
  }
}

template<PhraseMatch M, size_t N, bool HasFreq = false, bool Offs = false,
         typename Query, typename F>
auto ResolveMatcherOf(const Query& query, F&& f) {
  if constexpr (M == PhraseMatch::Slop) {
    return f.template operator()<SlopPhraseFrequency<Offs, HasFreq, N>>(
      query.slop, BuildExpectedSteps(query.positions));
  } else {
    static constexpr bool kIntervals = M == PhraseMatch::Intervals;
    return f.template
    operator()<FixedPhraseFrequency<Offs, HasFreq, kIntervals, N>>();
  }
}

template<PhraseMatch M, typename Leaf, typename Result,
         template<typename> class Impl, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename Query,
         typename... Prefix>
Result MakePhraseNodeOf(const Query& query,
                        std::span<const PostingMeta* const> metas,
                        std::span<const TermInterval> intervals,
                        const PhraseHandles& h, Prefix&&... prefix) {
  return ResolveArity<kSlotArity, kSlotFloor>(
    metas.size(), [&]<size_t N> -> Result {
      static constexpr size_t kSlots = N == 1 ? 0 : N;
      return ResolveMatcherOf<M, kSlots, Scored>(
        query, [&]<typename Matcher>(auto&&... args) -> Result {
          using Slots = search::PhraseFixedSlots<Matcher, Leaf, kSlots>;
          using Node = NodeOf<Wrap, Result, Slots>;
          return memory::make_managed<Impl<Node>>(
            std::forward<Prefix>(prefix)..., metas, intervals, *h.doc,
            h.Layout(), *h.pos, h.pay, std::forward<decltype(args)>(args)...);
        });
    });
}

template<PhraseMatch M, template<typename> class Impl, typename Result,
         bool Scored = false, template<typename> class Wrap = DeducedNode,
         typename... Prefix>
Result MakeFixedPhraseOf(const FixedPhraseQuery& query, Prefix&&... prefix) {
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span metas{state.metas.data(), state.metas.size()};
  return ResolveBounds(h.bounds, [&]<bool Bounds> -> Result {
    return ResolveInput(*h.doc, [&]<typename Input> -> Result {
      using Leaf = search::PostingPos<Input, Bounds>;
      return MakePhraseNodeOf<M, Leaf, Result, Impl, Scored, Wrap>(
        query, metas, query.positions, h, std::forward<Prefix>(prefix)...);
    });
  });
}

template<PhraseMatch M, template<typename> class Impl, typename Result,
         bool Scored = false, template<typename> class Wrap = DeducedNode,
         typename... Prefix>
Result MakeVariadicPhraseOf(const VariadicPhraseQuery& query,
                            Prefix&&... prefix) {
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span metas{state.metas.data(), state.metas.size()};
  const std::span widths{state.num_terms.data(), state.num_terms.size()};
  const std::span intervals{query.positions};
  SDB_ASSERT(metas.size() != widths.size());

  return ResolveBounds(h.bounds, [&]<bool Bounds> -> Result {
    return ResolveInput(*h.doc, [&]<typename Input> -> Result {
      return ResolveBool(
        Scored && state.volatile_boost, [&]<bool Weighed> -> Result {
          return ResolveSlotMatcherOf < M, Bounds, Input, Scored, false,
                 Scored &&
                   Weighed >
                     (query,
                      [&]<typename Matcher, typename Leaf>(
                        auto&&... args) -> Result {
                        using SlotPositions = std::remove_pointer_t<
                          typename Matcher::TermPosition::first_type>;
                        using Slots =
                          search::PhraseVariadicSlots<Matcher, Leaf,
                                                      SlotPositions::kHasBoost>;
                        using Node = NodeOf<Wrap, Result, Slots>;
                        const std::span boosts =
                          SlotPositions::kHasBoost
                            ? std::span{state.boosts.data(),
                                        state.boosts.size()}
                            : std::span<const score_t>{};
                        return memory::make_managed<Impl<Node>>(
                          std::forward<Prefix>(prefix)..., metas.size(),
                          [&](Leaf& leaf, size_t i) {
                            leaf.Prepare(*metas[i], *h.doc, h.Layout(), *h.pos,
                                         h.pay);
                          },
                          widths, boosts, intervals,
                          std::forward<decltype(args)>(args)...);
                      });
        });
    });
  });
}

template<template<typename> class Impl, typename Result, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename... Prefix>
Result MakeFixedPhrase(const FixedPhraseQuery& query, Prefix&&... prefix) {
  switch (MatchOf(query)) {
    case PhraseMatch::Slop:
      return MakeFixedPhraseOf<PhraseMatch::Slop, Impl, Result, Scored, Wrap>(
        query, std::forward<Prefix>(prefix)...);
    case PhraseMatch::Intervals:
      return MakeFixedPhraseOf<PhraseMatch::Intervals, Impl, Result, Scored,
                               Wrap>(query, std::forward<Prefix>(prefix)...);
    case PhraseMatch::Plain:
      break;
  }
  return MakeFixedPhraseOf<PhraseMatch::Plain, Impl, Result, Scored, Wrap>(
    query, std::forward<Prefix>(prefix)...);
}

template<template<typename> class Impl, typename Result, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename... Prefix>
Result MakeVariadicPhrase(const VariadicPhraseQuery& query,
                          Prefix&&... prefix) {
  switch (MatchOf(query)) {
    case PhraseMatch::Slop:
      return MakeVariadicPhraseOf<PhraseMatch::Slop, Impl, Result, Scored,
                                  Wrap>(query, std::forward<Prefix>(prefix)...);
    case PhraseMatch::Intervals:
      return MakeVariadicPhraseOf<PhraseMatch::Intervals, Impl, Result, Scored,
                                  Wrap>(query, std::forward<Prefix>(prefix)...);
    case PhraseMatch::Plain:
      break;
  }
  return MakeVariadicPhraseOf<PhraseMatch::Plain, Impl, Result, Scored, Wrap>(
    query, std::forward<Prefix>(prefix)...);
}

}  // namespace irs::search
