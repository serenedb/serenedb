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
/// @author Andrei Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/states/ngram_state.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {

using NGramStates = StatesCache<NGramState>;

// Prepared ngram similarity query implementation
class NGramSimilarityQuery : public Filter::Query {
 public:
  // returns set of features required for filter
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  NGramSimilarityQuery(size_t min_match_count, NGramStates&& states,
                       bstring&& stats, score_t boost = kNoBoost)
    : _min_match_count{min_match_count},
      _states{std::move(states)},
      _stats{std::move(stats)},
      _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  void visit(const SubReader& segment, PreparedStateVisitor& visitor,
             score_t boost) const final {
    if (const auto* state = _states.find(segment); state) {
      visitor.Visit(*this, *state, boost * _boost);
    }
  }

  score_t Boost() const noexcept final { return _boost; }

  DocIterator::ptr ExecuteWithOffsets(const SubReader& rdr) const;

 private:
  size_t _min_match_count;
  NGramStates _states;
  bstring _stats;
  score_t _boost;
};

}  // namespace irs
