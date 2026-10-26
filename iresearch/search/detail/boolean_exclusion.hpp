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

#include <algorithm>
#include <cstdint>
#include <span>
#include <tuple>
#include <utility>

#include "iresearch/search/detail/boolean_planner.hpp"
#include "iresearch/search/lead/family.hpp"
#include "iresearch/search/lead/posting_scored.hpp"

namespace irs::detail::builder {

extern template lead::Node::ptr MakeRequired<lead::Family, utils::Empty>(
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>,
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>, Terms,
  uint32_t, const SubReader&, const utils::Empty&, ScoreMergeType, score_t);

extern template lead::Node::ptr MakeRequired<lead::Family, ScoredCtx>(
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>,
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>, Terms,
  uint32_t, const SubReader&, const ScoredCtx&, ScoreMergeType, score_t);

template<typename Family, typename Ctx>
Result<Family> MakeExclusionWindow(const BooleanQuery& query, const Ctx& ctx,
                                   ScoreMergeType merge, score_t absorbed,
                                   uint64_t candidates);

template<typename Family, typename Ctx>
Result<Family> MakeNegation(std::span<const PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment, uint64_t candidates,
                            const Ctx& ctx);

template<typename Family, typename Ctx>
Result<Family> MakeExclusion(const BooleanQuery& query, const Ctx& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  auto candidates = IncludeCandidates(must, must_filters, segment);
  if (no_must && min_match != 0) {
    candidates = std::min(
      candidates, LeadCandidates(should, should_filters, segment.docs_count()));
  }
  if constexpr (kScoredPlan<Family, Ctx>) {
    if constexpr (Family::kRoot) {
      if (auto windowed = MakeExclusionWindow<Family>(query, ctx, merge,
                                                      absorbed, candidates)) {
        return windowed;
      }
    }
    if (absorbed == 0 && should.empty() && should_filters.empty() &&
        must.size() == 1 && must_filters.empty() &&
        ScoresPerDocTerm(must.front())) {
      const auto& posting = must.front();
      const auto& own = *posting.state.reader;
      const auto& doc = *DocOf(own);
      const auto recipe = RecipeOf<Family>(segment, ctx);
      return ResolveInput(doc, [&]<typename Input> -> Result<Family> {
        return BuildExcludesOf<Result<Family>, Input>(
          Family::kRoot ? ExcludeUse::PerBlock : ExcludeUse::PerDoc, excludes,
          exclude_filters, nullptr, segment, candidates, candidates,
          [&]<typename Exclude>(auto&& negated) -> Result<Family> {
            if constexpr (Family::kRoot) {
              return Family::template MakeExcludedPosting<Input, Exclude>(
                ctx, std::forward<decltype(negated)>(negated), posting, doc,
                segment, own, recipe);
            } else {
              return Family::template MakeSparse<
                PostingLeadScored<Input>, utils::Empty, utils::Empty, Exclude>(
                ctx, merge, Inherited{},
                std::forward_as_tuple(
                  posting.state.cookie, doc, segment, own,
                  recipe.Args(posting.stats, posting.boost)),
                std::forward_as_tuple(), std::forward_as_tuple(),
                std::forward<decltype(negated)>(negated));
            }
          });
      });
    }
    auto include = MakeRequired<lead::Family>(
      must, must_filters, should, should_filters,
      query.Uniformity(Occur::Should), min_match, segment,
      ScoredCtx{Family::Scoring(ctx)}, query.MergeType(), absorbed);
    if (!include) {
      return {};
    }
    if constexpr (Family::kRoot) {
      return MakeSparseExclusionOf<Family>(std::move(include), excludes,
                                           exclude_filters, segment, candidates,
                                           ctx, merge, Scored{merge, 0});
    } else {
      return MakeSparseExclusionOf<Family>(std::move(include), excludes,
                                           exclude_filters, segment, candidates,
                                           ctx, merge, Inherited{});
    }
  } else {
    if (min_match == 0) {
      if (no_must) {
        if constexpr (requires {
                        Family::MakeNegation(excludes, exclude_filters, segment,
                                             candidates, ctx);
                      }) {
          return Family::MakeNegation(excludes, exclude_filters, segment,
                                      candidates, ctx);
        } else {
          return MakeNegation<Family>(excludes, exclude_filters, segment,
                                      candidates, ctx);
        }
      }
      if (auto folded =
            MakeBitset<Family>({.must = must,
                                .must_filters = must_filters,
                                .must_not = excludes,
                                .must_not_filters = exclude_filters},
                               segment, ctx)) {
        return folded;
      }
      if (auto windowed = MakeExclusionWindow<Family>(query, ctx, merge,
                                                      absorbed, candidates)) {
        return windowed;
      }
      return MakeSparseExclusion<Family>(must, must_filters, excludes,
                                         exclude_filters, segment, candidates,
                                         ctx);
    }
    auto include = MakeRequired<lead::Family>(
      must, must_filters, should, should_filters, Terms::Mixed, min_match,
      segment, utils::Empty{}, ScoreMergeType::Noop, score_t{0});
    if (!include) {
      return {};
    }
    return MakeSparseExclusionOf<Family>(std::move(include), excludes,
                                         exclude_filters, segment, candidates,
                                         ctx, merge, utils::Empty{});
  }
}

}  // namespace irs::detail::builder
