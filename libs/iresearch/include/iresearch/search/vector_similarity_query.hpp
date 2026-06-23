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

#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/states/vector_state.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {

using VectorStates = StatesCache<VectorState>;

// Prepared vector query shared by the kNN (ByVectorSimilarity) and range
// (ByRadius) filters: per segment it holds the chosen cluster posting cookies
// and the rerank vector column. execute() unions the chosen clusters into one
// DocIterator and wraps it so each candidate is scored by exact distance to the
// query vector. A finite `radius` turns the wrapper into a membership gate
// (range search); a non-finite radius leaves every candidate to the outer
// top-K collector (kNN).
class VectorSimilarityQuery : public Filter::Query {
 public:
  VectorSimilarityQuery(VectorStates&& states, std::vector<float>&& query,
                        VectorMetric metric, float radius, bool inclusive,
                        score_t boost, Query::ptr&& inner = nullptr)
    : _states{std::move(states)},
      _query{std::move(query)},
      _inner{std::move(inner)},
      _metric{metric},
      _radius{radius},
      _inclusive{inclusive},
      _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  VectorStates _states;
  std::vector<float> _query;
  // Optional predicate intersected with the cluster-union candidates before
  // rerank (hybrid search, e.g. ByTerm + ByVectorSimilarity).
  Query::ptr _inner;
  VectorMetric _metric;
  float _radius;
  bool _inclusive;
  score_t _boost;
};

}  // namespace irs
