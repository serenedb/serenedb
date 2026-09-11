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

#include <algorithm>

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/estimate.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/ngram_state.hpp"

namespace irs {

class NGramSimilarityQuery : public QueryBuilderImpl<NGramSimilarityQuery> {
 public:
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  NGramSimilarityQuery(const SubReader& segment, size_t min_match_count,
                       NGramState&& state, score_t boost = kNoBoost)
    : QueryBuilderImpl{segment},
      _min_match_count{min_match_count},
      _state{std::move(state)},
      _boost{boost} {
    SDB_ASSERT(_state.terms.size() >= _min_match_count);
    uint64_t sum = 0;
    for (const auto& meta : _state.terms) {
      sum += meta.docs_count;
    }
    _estimate_max =
      ClampEstimate(sum / std::max<size_t>(_min_match_count, 1), segment);
  }

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    visitor.Visit(*this, _state, boost * _boost);
  }

  size_t MinMatchCount() const noexcept { return _min_match_count; }

  size_t Present() const noexcept { return _state.terms.size(); }

  bool Every() const noexcept { return Present() == _min_match_count; }

  const NGramState& State() const noexcept { return _state; }

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  size_t _min_match_count;
  NGramState _state;
  score_t _boost;
};

}  // namespace irs
