////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <limits>

#include "basics/misc.hpp"
#include "iresearch/search/estimate.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/phrase_state.hpp"

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;

template<typename StateType>
class PhraseQuery : public QueryBuilder {
  static_assert(std::is_same_v<StateType, FixedPhraseState> ||
                std::is_same_v<StateType, VariadicPhraseState>);

 public:
  using positions_t = std::vector<TermInterval>;

  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  PhraseQuery(const SubReader& segment, StateType&& state,
              positions_t&& positions, score_t boost,
              PosAttr::value_t slop = 0) noexcept
    : QueryBuilder{segment},
      state{std::move(state)},
      positions{std::move(positions)},
      boost{boost},
      slop{this->positions.size() > 1 ? slop : PosAttr::value_t{0}},
      has_intervals{this->positions.size() > 1 &&
                    absl::c_any_of(this->positions, [](const auto& pos) {
                      return pos.offs_max != pos.offs_min;
                    })} {
    uint32_t least = std::numeric_limits<uint32_t>::max();
    if constexpr (std::is_same_v<StateType, FixedPhraseState>) {
      for (const auto& term : this->state.terms) {
        least = std::min(least, term.first.docs_count);
      }
    } else {
      size_t begin = 0;
      for (const auto count : this->state.num_terms) {
        uint64_t slot = 0;
        for (size_t i = begin; i != begin + count; ++i) {
          slot += this->state.terms[i].first.docs_count;
        }
        begin += count;
        least = std::min(least, ClampEstimate(slot, segment));
      }
    }
    _estimate_max = least == std::numeric_limits<uint32_t>::max() ? 0 : least;
  }

  score_t Boost() const noexcept final { return boost; }

  void SetBoost(score_t value) noexcept final { boost = value; }

  StateType state;
  positions_t positions;
  score_t boost;
  PosAttr::value_t slop{0};
  bool has_intervals;
};

class FixedPhraseQuery
  : public QueryBuilderImpl<FixedPhraseQuery, PhraseQuery<FixedPhraseState>> {
 public:
  FixedPhraseQuery(const SubReader& segment, FixedPhraseState&& state,
                   positions_t&& positions, score_t boost,
                   PosAttr::value_t slop = 0) noexcept
    : QueryBuilderImpl{segment, std::move(state), std::move(positions), boost,
                       slop} {}

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, state, boost * this->boost);
  }
};

class VariadicPhraseQuery
  : public QueryBuilderImpl<VariadicPhraseQuery,
                            PhraseQuery<VariadicPhraseState>> {
 public:
  VariadicPhraseQuery(const SubReader& segment, VariadicPhraseState&& state,
                      positions_t&& positions, score_t boost,
                      PosAttr::value_t slop = 0) noexcept
    : QueryBuilderImpl{segment, std::move(state), std::move(positions), boost,
                       slop} {}

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, state, boost * this->boost);
  }
};

}  // namespace irs
