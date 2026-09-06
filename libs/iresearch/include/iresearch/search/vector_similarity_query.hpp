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

#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/estimate.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/vector_state.hpp"

namespace irs {

class VectorQueryBase {
 public:
  const VectorState& State() const noexcept { return _state; }

  std::span<const float> Query() const noexcept { return _query; }

  VectorMetric Metric() const noexcept { return _metric; }

  const QueryBuilder* Inner() const noexcept { return _inner.get(); }

  bool Rescored() const noexcept {
    return _state.quant != VectorQuantization::None &&
           _state.vector_column != nullptr && _state.col_reader != nullptr;
  }

 protected:
  VectorQueryBase(VectorState&& state, std::span<const float> query,
                  VectorMetric metric, QueryBuilder::ptr&& inner) noexcept
    : _state{std::move(state)},
      _query{query},
      _inner{std::move(inner)},
      _metric{metric} {}

  VectorState _state;
  std::span<const float> _query;
  QueryBuilder::ptr _inner;
  VectorMetric _metric;
};

class KnnVectorQuery : public QueryBuilderImpl<KnnVectorQuery>,
                       public VectorQueryBase {
 public:
  KnnVectorQuery(const SubReader& segment, VectorState&& state,
                 std::span<const float> query, VectorMetric metric,
                 score_t boost, QueryBuilder::ptr&& inner = nullptr)
    : QueryBuilderImpl{segment, ClampEstimate(state.estimation, segment),
                       QueryKind::Other},
      VectorQueryBase{std::move(state), query, metric, std::move(inner)},
      _boost{boost} {}

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  score_t _boost;
};

class RangeVectorQuery : public QueryBuilderImpl<RangeVectorQuery>,
                         public VectorQueryBase {
 public:
  RangeVectorQuery(const SubReader& segment, VectorState&& state,
                   std::span<const float> query, VectorMetric metric,
                   float radius, bool inclusive, score_t boost,
                   QueryBuilder::ptr&& inner = nullptr)
    : QueryBuilderImpl{segment, ClampEstimate(state.estimation, segment),
                       QueryKind::Other},
      VectorQueryBase{std::move(state), query, metric, std::move(inner)},
      _radius{radius},
      _boost{boost},
      _inclusive{inclusive} {}

  score_t Threshold() const noexcept {
    return VectorMetricIsAngular(_metric) ? _radius : -_radius;
  }

  bool Inclusive() const noexcept { return _inclusive; }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  float _radius;
  score_t _boost;
  bool _inclusive;
};

void RerankExactDistances(const SubReader& segment,
                          const ColumnReader& vector_column, uint32_t d,
                          std::span<const float> query, VectorMetric metric,
                          std::span<ScoreDoc> hits);

}  // namespace irs
