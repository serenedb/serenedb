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

#include "iresearch/search/filters/vector_exact_filter.hpp"

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/queries/vector_exact_query.hpp"

namespace irs {

QueryBuilder::ptr ByVectorExact::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  const auto* column = segment.Column(field_id());
  const auto* columns = segment.GetColReader();
  if (column == nullptr || columns == nullptr || column->Child() == nullptr ||
      column->ArraySize() != opts.query.size() || column->RowCount() == 0) {
    return QueryBuilder::Empty();
  }
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(opts.inner, segment, ctx, inner)) {
    return QueryBuilder::Empty();
  }
  auto built = memory::make_tracked<ExactVectorQuery>(
    ctx.memory, segment, *column, *columns, opts.query, opts.metric,
    ctx.boost * GetBoost(), std::move(inner));
  built->SetStats(ctx.Record());
  return built;
}

PrepareCollector::ptr ByVectorExact::MakeCollectorImpl(const Scorer* scorer,
                                                       StatsArena& stats,
                                                       uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

}  // namespace irs
