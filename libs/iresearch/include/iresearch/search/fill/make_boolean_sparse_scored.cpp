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

#include <span>
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/make_boolean.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::fill {
namespace {

template<typename Lead, typename Probes, typename Optional, typename Excludes,
         typename Score, typename LeadArgs, typename ProbesArgs,
         typename OptionalArgs, typename ExcludesArgs>
Node::ptr MakeSparseScored(ScoreMergeType merge, ColumnArgsFetcher& fetcher,
                           LeadArgs&& lead, ProbesArgs&& probes,
                           OptionalArgs&& optional, ExcludesArgs&& excludes,
                           Score score) {
  using Node = lead::BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
  return memory::make_managed<ByWalkScored<Node>>(
    merge, fetcher, std::piecewise_construct, std::forward<LeadArgs>(lead),
    std::forward<ProbesArgs>(probes), std::forward<OptionalArgs>(optional),
    std::forward<ExcludesArgs>(excludes), score);
}

}  // namespace

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  return search::BuildScoredConjunction<Node::ptr>(
    terms, filters, nullptr, nullptr, kNoBoost, segment, recipe,
    [&](const search::PostingClause& posting, const QueryBuilder* child,
        uint64_t interrogations) -> probe::Node::ptr {
      if (child == nullptr) {
        return probe::MakePostingScored(posting, segment, recipe);
      }
      return child->PlanProbe(ctx, interrogations);
    },
    [&](const QueryBuilder& child) -> lead::Node::ptr {
      return child.PlanLead(ctx);
    },
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      return MakeSparseScored<Head, Tail, utils::Empty, utils::Empty>(
        merge, *ctx.fetcher, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
        std::forward_as_tuple(), search::Scored{ScoreMergeType::Sum, absorbed});
    });
}

Node::ptr MakeSparseExclusionScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, ScoreMergeType own,
  score_t absorbed) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  if (absorbed == 0 && should_terms.empty() && should_filters.empty() &&
      must_filters.empty() && must_terms.size() == 1 &&
      search::ScoresPerDocTerm(must_terms.front())) {
    const auto& posting = must_terms.front();
    const auto& reader = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto& input = *search::DocOf(reader);
    const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
    return search::ResolveInput(input, [&]<typename Input> -> Node::ptr {
      using Include = search::PostingLeadScored<Input>;
      return search::BuildExcludeSideOf<Node::ptr, Input>(
        exclude_terms, exclude_filters, nullptr, segment, candidates,
        [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
          return MakeSparseScored<Include, utils::Empty, utils::Empty, Exclude>(
            merge, *ctx.fetcher,
            std::forward_as_tuple(meta, input, segment, reader,
                                  recipe.Args(posting.stats, posting.boost)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            std::forward<decltype(exclude)>(exclude), search::Inherited{});
        });
    });
  }
  auto include = lead::MakeRequiredScored(
    must_terms, must_filters, should_terms, should_filters, should_uniformity,
    min_should_match, segment, ctx, own, absorbed);
  if (!include) {
    return {};
  }
  return search::BuildExcludeSide<Node::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeSparseScored<lead::Erased, utils::Empty, utils::Empty,
                              Exclude>(
        merge, *ctx.fetcher, std::forward_as_tuple(std::move(include)),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude), search::Inherited{});
    });
}

Node::ptr MakeSparseBoostScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  SDB_ASSERT(!should_terms.empty() || !should_filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto clause = probe::ScoredClauseOf(segment, ctx, recipe);
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return search::BuildOptionalLeaves<Node::ptr>(
      should_terms, should_filters, uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Optional>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            return MakeSparseScored<Head, utils::Empty,
                                    probe::BoostLeaves<Optional, N>,
                                    utils::Empty>(
              merge, *ctx.fetcher, std::forward<decltype(head)>(head),
              std::forward_as_tuple(),
              std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
              std::forward_as_tuple(), search::Scored{ScoreMergeType::Sum, 0});
          });
      });
  };
  if (absorbed == 0 && must_filters.empty() && must_terms.size() == 1 &&
      search::ScoresPerDocTerm(must_terms.front())) {
    const auto& posting = must_terms.front();
    const auto& reader = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto& input = *search::DocOf(reader);
    return search::ResolveInput(input, [&]<typename Input> -> Node::ptr {
      using Head = search::PostingLeadScored<Input>;
      return build.template operator()<Head>(
        std::forward_as_tuple(meta, input, segment, reader,
                              recipe.Args(posting.stats, posting.boost)));
    });
  }
  lead::Node::ptr head =
    must_terms.empty() && must_filters.empty()
      ? lead::MakeAllScored(segment, absorbed)
      : lead::MakeSparseConjunctionScored(must_terms, must_filters, segment,
                                          ctx, ScoreMergeType::Sum, absorbed);
  if (!head) {
    return {};
  }
  return build.template operator()<lead::Erased>(
    std::forward_as_tuple(std::move(head)));
}

}  // namespace irs::fill
