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

namespace irs {

class FixedPhraseQuery;
class VariadicPhraseQuery;

// Prepared phrase query implementation
template<typename StateType>
class PhraseQuery : public QueryBuilder {
  static_assert(std::is_same_v<StateType, FixedPhraseState> ||
                std::is_same_v<StateType, VariadicPhraseState>);

 public:
  using positions_t = std::vector<TermInterval>;

  // Returns features required for phrase filter
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  PhraseQuery(const SubReader& segment, StateType&& state,
              positions_t&& positions, score_t boost) noexcept
    : QueryBuilder{segment},
      state{std::move(state)},
      positions{std::move(positions)},
      boost{boost} {}

  score_t Boost() const noexcept final { return boost; }

  StateType state;
  positions_t positions;
  score_t boost;
};

class FixedPhraseQuery : public PhraseQuery<FixedPhraseState> {
 public:
  FixedPhraseQuery(const SubReader& segment, FixedPhraseState&& state,
                   positions_t&& positions, score_t boost) noexcept
    : PhraseQuery{segment, std::move(state), std::move(positions), boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx) const final;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, state, boost * this->boost);
  }

  DocIterator::ptr ExecuteWithOffsets(const SubReader& segment) const;
};

class VariadicPhraseQuery : public PhraseQuery<VariadicPhraseState> {
 public:
  VariadicPhraseQuery(const SubReader& segment, VariadicPhraseState&& state,
                      positions_t&& positions, score_t boost) noexcept
    : PhraseQuery{segment, std::move(state), std::move(positions), boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx) const final;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, state, boost * this->boost);
  }

  DocIterator::ptr ExecuteWithOffsets(const SubReader& segment) const;
};

}  // namespace irs
