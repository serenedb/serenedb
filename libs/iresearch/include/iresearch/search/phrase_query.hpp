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

#include "basics/misc.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/states/phrase_state.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;

// Prepared phrase query implementation
template<typename State>
class PhraseQuery : public Filter::Query {
  static_assert(std::is_same_v<State, FixedPhraseState> ||
                std::is_same_v<State, VariadicPhraseState>);

 public:
  using states_t = StatesCache<State>;
  using positions_t = std::vector<TermInterval>;

  // Returns features required for phrase filter
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  PhraseQuery(states_t&& states, positions_t&& positions, bstring&& stats,
              score_t boost) noexcept
    : states{std::move(states)},
      positions{std::move(positions)},
      stats{std::move(stats)},
      boost{boost} {}

  void visit(const SubReader& segment, PreparedStateVisitor& visitor,
             score_t boost) const final {
    if (auto state = states.find(segment)) {
      boost *= boost;
      if constexpr (std::is_same_v<State, FixedPhraseState>) {
        visitor.Visit(sdb::basics::downCast<FixedPhraseQuery>(*this), *state,
                      boost);
      } else if constexpr (std::is_same_v<State, VariadicPhraseState>) {
        visitor.Visit(sdb::basics::downCast<VariadicPhraseQuery>(*this), *state,
                      boost);
      }
    }
  }

  score_t Boost() const noexcept final { return boost; }

  states_t states;
  positions_t positions;
  bstring stats;
  score_t boost;
};

class FixedPhraseQuery : public PhraseQuery<FixedPhraseState> {
 public:
  FixedPhraseQuery(states_t&& states, positions_t&& positions, bstring&& stats,
                   score_t boost) noexcept
    : PhraseQuery{std::move(states), std::move(positions), std::move(stats),
                  boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  DocIterator::ptr ExecuteWithOffsets(const SubReader& segment) const;
};

class VariadicPhraseQuery : public PhraseQuery<VariadicPhraseState> {
 public:
  VariadicPhraseQuery(states_t&& states, positions_t&& positions,
                      bstring&& stats, score_t boost) noexcept
    : PhraseQuery{std::move(states), std::move(positions), std::move(stats),
                  boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  DocIterator::ptr ExecuteWithOffsets(const SubReader& segment) const;
};

}  // namespace irs
