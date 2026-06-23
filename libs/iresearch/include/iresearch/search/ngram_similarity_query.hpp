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

namespace irs {

// Prepared ngram similarity query implementation
class NGramSimilarityQuery : public QueryBuilder {
 public:
  // returns set of features required for filter
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  NGramSimilarityQuery(const SubReader& segment, size_t min_match_count,
                       NGramState&& state, score_t boost = kNoBoost)
    : QueryBuilder{segment},
      _min_match_count{min_match_count},
      _state{std::move(state)},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final;

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, _state, boost * _boost);
  }

  score_t Boost() const noexcept final { return _boost; }

  DocIterator::ptr ExecuteWithOffsets() const;

 private:
  size_t _min_match_count;
  NGramState _state;
  score_t _boost;
};

}  // namespace irs
