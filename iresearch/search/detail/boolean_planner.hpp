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

#include <cstdint>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/conjunction.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail::builder {

template<typename Family, Slice kSlice, typename Ctx>
Result<Family> MakeRequired(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            Terms uniformity, uint32_t min_match,
                            const SubReader& segment, const Ctx& ctx,
                            ScoreMergeType merge, score_t absorbed);

template<typename Family, typename Ctx, Slice kSlice>
Result<Family> MakeExclusionIn(const BooleanQuery& query, const Ctx& ctx,
                               ScoreMergeType merge, score_t absorbed);

template<typename Family, typename Ctx>
Result<Family> MakeExclusion(const BooleanQuery& query, const Ctx& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  return ResolveSlice(query.Queries(Occur::MustNot), query.Segment(),
                      [&]<Slice kSlice> -> Result<Family> {
                        return MakeExclusionIn<Family, Ctx, kSlice>(
                          query, ctx, merge, absorbed);
                      });
}

template<typename Family, typename Ctx>
Result<Family> Make(const BooleanQuery& query, const Ctx& ctx,
                    ScoreMergeType merge) {
  score_t absorbed = 0;
  if constexpr (kScoredPlan<Family, Ctx>) {
    const auto& scored = Family::Scoring(ctx);
    SDB_ASSERT(scored.fetcher);
    absorbed = query.Absorbed(*scored.fetcher);
  }
  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
    return MakeExclusion<Family>(query, ctx, merge, absorbed);
  }
  if constexpr (kScoredPlan<Family, Ctx>) {
    if constexpr (Family::kWrapsMerge) {
      if (const auto own = query.MergeType(); own != merge) {
        auto child = Make<Family>(query, ctx, own);
        if (!child) {
          return {};
        }
        return Family::WrapMerge(merge, std::move(child));
      }
    }
  }
  return ResolveInputSlice<MaskSlice::Unmasked>(
    query.Segment(), [&]<Slice kSlice> -> Result<Family> {
      return MakeRequired<Family, kSlice>(
        query.Terms(Occur::Must), query.Queries(Occur::Must),
        query.Terms(Occur::Should), query.Queries(Occur::Should),
        query.Uniformity(Occur::Should), query.MinShouldMatch(),
        query.Segment(), ctx, merge, absorbed);
    });
}

}  // namespace irs::detail::builder
namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const BooleanQuery& query, const Ctx& ctx) {
  if constexpr (kProbePlan<Family>) {
    if constexpr (kScoredPlan<Family, Ctx>) {
      const auto& segment = query.Segment();
      return probe::MakeBoolean(
        query, Family::Interrogations(ctx),
        TermScoring{
          .recipe = {.segment = &segment, .fetcher = ctx.scored.fetcher},
          .merge = query.MergeType(),
        },
        ctx.scored);
    } else {
      return probe::MakeBoolean(query, Family::Interrogations(ctx), NoScoring{},
                                ScoredCtx{});
    }
  } else {
    return builder::Make<Family>(query, ctx, MergeOf(query, ctx));
  }
}

}  // namespace irs::detail
