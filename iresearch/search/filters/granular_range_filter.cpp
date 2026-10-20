////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "granular_range_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs {

QueryBuilder::ptr ByGranularRange::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  return PrepareSegment(segment, sub_ctx, field_id(), options());
}

QueryBuilder::ptr ByGranularRange::PrepareSegment(const SubReader& segment,
                                                  const PrepareContext& ctx,
                                                  const irs::field_id field,
                                                  const options_type& options) {
  const auto& rng = options.range;
  if (rng.max_type == BoundType::Exclusive && rng.max.empty()) {
    return QueryBuilder::Empty();
  }
  const auto* reader = segment.field(field);
  if (!reader) {
    return QueryBuilder::Empty();
  }
  const bool has_min = rng.min_type != BoundType::Unbounded && !rng.min.empty();
  const bool has_max = rng.max_type != BoundType::Unbounded && !rng.max.empty();
  bstring from;
  if (has_min) {
    from = rng.min;
    if (rng.min_type == BoundType::Exclusive) {
      from.push_back(0);
    }
  } else if (has_max) {
    from.assign(1, rng.max.front());
  }
  bstring to;
  if (has_max) {
    to = rng.max;
    if (rng.max_type == BoundType::Inclusive) {
      to.push_back(0);
    }
  } else if (has_min && rng.min.front() != 0xFF) {
    to.assign(1, rng.min.front() + 1);
  }
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum);
  MultiTermVisitor mtv{ctx, query->State(), *reader};
  reader->VisitRange(from, to, [&](bytes_view term, const PostingMeta& meta) {
    mtv.Visit(term, meta, kNoBoost);
  });
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr ByGranularRange::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

}  // namespace irs
