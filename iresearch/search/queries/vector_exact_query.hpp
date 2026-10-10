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

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs {

// The exact answer to a vector query: every row of the segment (or every row
// the inner predicate admits) scored against the query from its stored
// vector, no index. What an approximate index is measured against, and the
// right plan for a predicate that admits few rows.
class ExactVectorQuery : public QueryBuilderImpl<ExactVectorQuery> {
 public:
  ExactVectorQuery(const SubReader& segment, const ColumnReader& column,
                   const ColReader& columns, std::vector<float> query,
                   VectorMetric metric, score_t boost,
                   QueryBuilder::ptr&& inner = nullptr)
    : QueryBuilderImpl{segment},
      _column{&column},
      _columns{&columns},
      _inner{std::move(inner)},
      _query{std::move(query)},
      _metric{metric},
      _boost{boost} {}

  const ColumnReader& Column() const noexcept { return *_column; }
  const ColReader& Columns() const noexcept { return *_columns; }
  std::span<const float> Query() const noexcept { return _query; }
  VectorMetric Metric() const noexcept { return _metric; }
  const QueryBuilder* Inner() const noexcept { return _inner.get(); }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  const ColumnReader* _column;
  const ColReader* _columns;
  QueryBuilder::ptr _inner;
  std::vector<float> _query;
  VectorMetric _metric;
  score_t _boost;
};

}  // namespace irs
