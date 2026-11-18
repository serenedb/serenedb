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
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/queries/vector_exact_query.hpp"

namespace irs {

QueryBuilder::ptr PrepareExactVectors(
  const SubReader& segment, const PrepareContext& ctx, field_id column_id,
  std::span<const float> query, field_id ann_id, VectorMetric metric,
  const std::shared_ptr<const Filter>& inner_filter, float radius,
  bool inclusive, score_t boost) {
  const auto* column = segment.Column(column_id);
  const auto* columns = segment.GetColReader();
  if (!column || !columns || !column->Child() ||
      column->ArraySize() != query.size() || column->RowCount() == 0) {
    return QueryBuilder::Empty();
  }
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(inner_filter, segment, ctx, inner)) {
    return QueryBuilder::Empty();
  }
  AnnExactCodes codes;
  if (const auto* ann = segment.Ann(ann_id)) {
    codes = ann->PrepareExact(segment, query, metric);
  }
  std::optional<score_t> threshold;
  if (radius != std::numeric_limits<float>::max()) {
    threshold = RadiusThreshold(metric, radius);
  }
  auto built = memory::make_tracked<ExactVectorQuery>(
    ctx.memory, segment, *column, *columns,
    std::vector<float>{query.begin(), query.end()}, metric, ctx.boost * boost,
    std::move(inner), std::move(codes), threshold, inclusive);
  built->SetStats(ctx.Record());
  return built;
}

QueryBuilder::ptr ByVectorExact::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  return PrepareExactVectors(segment, ctx, field_id(), opts.query, opts.ann_id,
                             opts.metric, opts.inner, opts.radius,
                             opts.inclusive, GetBoost());
}

PrepareCollector::ptr ByVectorExact::MakeCollectorImpl(const Scorer* scorer,
                                                       StatsArena& stats,
                                                       uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

}  // namespace irs
