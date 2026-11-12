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

#include <optional>
#include <span>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs {

class ExactVectorQuery : public QueryBuilderImpl<ExactVectorQuery> {
 public:
  ExactVectorQuery(QueryMemory& memory, const SubReader& segment,
                   const ColumnReader& column, const ColReader& columns,
                   std::span<const float> query, VectorMetric metric,
                   score_t boost, QueryBuilder::ptr&& inner = nullptr,
                   AnnExactCodes codes = {},
                   std::optional<score_t> threshold = {},
                   bool inclusive = false)
    : QueryBuilderImpl{segment, static_cast<uint32_t>(segment.docs_count()),
                       QueryKind::Ann},
      _column{&column},
      _columns{&columns},
      _inner{std::move(inner)},
      _codes{std::move(codes)},
      _query{memory.Arena(), query.size(),
             [&](float& slot, size_t i) noexcept { slot = query[i]; }},
      _metric{metric},
      _boost{boost},
      _threshold{threshold},
      _inclusive{inclusive} {}

  const ColumnReader& Column() const noexcept { return *_column; }
  const ColReader& Columns() const noexcept { return *_columns; }
  std::span<const float> Query() const noexcept { return _query; }
  VectorMetric Metric() const noexcept { return _metric; }
  const QueryBuilder* Inner() const noexcept { return _inner.get(); }
  const AnnExactCodes& Codes() const noexcept { return _codes; }

  ann::Root::ptr PlanAnn(const ann::Context& ctx) const final;
  std::optional<score_t> Threshold() const noexcept { return _threshold; }
  bool Inclusive() const noexcept { return _inclusive; }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  const ColumnReader* _column;
  const ColReader* _columns;
  QueryBuilder::ptr _inner;
  AnnExactCodes _codes;
  containers::Fixed<float> _query;
  VectorMetric _metric;
  score_t _boost;
  std::optional<score_t> _threshold;
  bool _inclusive;
};

extern template class QueryBuilderImpl<ExactVectorQuery>;

}  // namespace irs
