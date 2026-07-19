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
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/states/vector_state.hpp"

namespace irs {

class KnnVectorQuery : public QueryBuilder {
 public:
  KnnVectorQuery(const SubReader& segment, VectorState&& state,
                 std::span<const float> query, VectorMetric metric,
                 score_t boost, QueryBuilder::ptr&& inner = nullptr)
    : QueryBuilder{segment},
      _state{std::move(state)},
      _query{query},
      _inner{std::move(inner)},
      _metric{metric},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final;

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  VectorState _state;
  std::span<const float> _query;
  QueryBuilder::ptr _inner;
  VectorMetric _metric;
  score_t _boost;
};

// Prepared range query (ByRadius). Probes every cluster, reranks the union from
// the raw vectors (exact distances -- SQ8 would be approximate and unsound for
// a membership test), and gates each candidate by `radius`/`inclusive`,
// emitting the in-ball docs in ascending doc order. An optional `inner`
// predicate is intersected with the candidates.
class RangeVectorQuery : public QueryBuilder {
 public:
  RangeVectorQuery(const SubReader& segment, VectorState&& state,
                   std::span<const float> query, VectorMetric metric,
                   float radius, bool inclusive, score_t boost,
                   QueryBuilder::ptr&& inner = nullptr)
    : QueryBuilder{segment},
      _state{std::move(state)},
      _query{query},
      _inner{std::move(inner)},
      _metric{metric},
      _radius{radius},
      _inclusive{inclusive},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final;

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  VectorState _state;
  std::span<const float> _query;
  QueryBuilder::ptr _inner;
  VectorMetric _metric;
  float _radius;
  bool _inclusive;
  score_t _boost;
};

void RerankExactDistances(const SubReader& segment,
                          const ColumnReader& vector_column, uint32_t d,
                          std::span<const float> query, VectorMetric metric,
                          std::span<ScoreDoc> hits);

}  // namespace irs
