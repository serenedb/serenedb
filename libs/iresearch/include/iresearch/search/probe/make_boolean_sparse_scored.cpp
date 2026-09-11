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

#include <algorithm>
#include <span>
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/score_policy.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/probe/posting_scored.hpp"

namespace irs::probe {
namespace {

template<typename Musts, typename Optional, typename Excludes, typename Score,
         typename MustsArgs, typename OptionalArgs, typename ExcludesArgs>
Node::ptr MakeSparseScored(MustsArgs&& musts, OptionalArgs&& optional,
                           ExcludesArgs&& excludes, Score score) {
  using Node = BooleanSparse<Musts, Optional, Excludes, Score>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward<MustsArgs>(musts),
    std::forward<OptionalArgs>(optional), std::forward<ExcludesArgs>(excludes),
    score);
}

}  // namespace

Node::ptr MakeSparseConjunctionScored(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, detail::Terms uniformity,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const detail::ScoredCtx& ctx, score_t absorbed) {
  const auto size = terms.size() + filters.size();
  if (size == 0) {
    return absorbed != 0 ? MakeAllScored(segment, absorbed) : Node::ptr{};
  }
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  if (size == 1) {
    auto only =
      filters.empty()
        ? clause(terms.front(), nullptr, interrogations)
        : clause(detail::PostingClause{TermState{nullptr, PostingMeta{}}},
                 filters.front().get(), interrogations);
    if (absorbed == 0 || !only) {
      return only;
    }
    return MakeSparseScored<Erased, utils::Empty, utils::Empty>(
      std::forward_as_tuple(std::move(only)), std::forward_as_tuple(),
      std::forward_as_tuple(), detail::Scored{merge, absorbed});
  }
  return detail::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, nullptr, nullptr, kNoBoost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return MakeSparseScored<AndLeaves<Leaf, N>, utils::Empty,
                                  utils::Empty>(
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            detail::Scored{merge, absorbed});
        });
    },
    detail::ProbeOrder::Narrowest);
}

Node::ptr MakeSparseThresholdScored(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, detail::Terms uniformity,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint32_t min_match, uint64_t interrogations, const detail::ScoredCtx& ctx,
  score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  if (terms.size() + filters.size() < min_match) {
    return {};
  }
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  return detail::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, nullptr, nullptr, kNoBoost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return MakeSparseScored<utils::Empty, ThresholdLeaves<Leaf, N, true>,
                                  utils::Empty>(
            std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init),
                                  min_match),
            std::forward_as_tuple(), detail::Scored{merge, absorbed});
        });
    },
    detail::ProbeOrder::Densest);
}

Node::ptr MakeSparseExclusionScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, uint32_t min_should_match,
  std::span<const detail::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const detail::ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const detail::ScoredCtx& ctx, score_t absorbed) {
  const bool no_must = must.empty() && must_filters.empty();
  const uint64_t docs_count = segment.docs_count();
  uint64_t lead = detail::IncludeCandidates(must, must_filters, segment);
  if (no_must && min_should_match != 0) {
    lead = std::min(docs_count,
                    detail::LeadCandidates(should, should_filters,
                                           static_cast<doc_id_t>(docs_count)));
  }
  const auto candidates = std::max<uint64_t>(
    1, docs_count == 0 ? 0 : interrogations * lead / docs_count);
  if (absorbed == 0 && min_should_match == 0 && must.size() == 1 &&
      must_filters.empty() && ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& own = *posting.state.reader;
    const auto* const doc = detail::DocOf(own);
    return detail::ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
      using Include = detail::PostingProbeScored<Input>;
      return detail::BuildExcludeSideOf<Node::ptr, Input>(
        exclude, exclude_filters, nullptr, segment, candidates, lead,
        [&]<typename Exclude>(auto&& excluded) -> Node::ptr {
          return MakeSparseScored<Include, utils::Empty, Exclude>(
            std::forward_as_tuple(posting.state.cookie, *doc, segment, own,
                                  recipe.Args(posting.stats, posting.boost)),
            std::forward_as_tuple(), std::forward<decltype(excluded)>(excluded),
            detail::Inherited{});
        });
    });
  }
  auto include =
    MakeRequiredScored(must, must_filters, must_uniformity, should,
                       should_filters, should_uniformity, min_should_match,
                       segment, recipe, merge, interrogations, ctx, absorbed);
  if (!include) {
    return {};
  }
  return detail::BuildExcludeSide<Node::ptr>(
    exclude, exclude_filters, nullptr, segment, candidates, lead,
    [&]<typename Exclude>(auto&& excluded) -> Node::ptr {
      return MakeSparseScored<Erased, utils::Empty, Exclude>(
        std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward<decltype(excluded)>(excluded), detail::Inherited{});
    });
}

Node::ptr MakeSparseBoostScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, const SubReader& segment,
  const detail::ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const detail::ScoredCtx& ctx, score_t absorbed) {
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const auto no_must = must.empty() && must_filters.empty();
  const auto reach =
    no_must ? interrogations
            : std::min(interrogations,
                       detail::IncludeCandidates(must, must_filters, segment));
  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return detail::BuildOptionalLeaves<Node::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, reach, clause,
      [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
        return detail::ResolveArity<detail::kTailArity, detail::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            return MakeSparseScored<Head, BoostLeaves<Leaf, N>, utils::Empty>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
              std::forward_as_tuple(), detail::Scored{merge, absorbed});
          });
      });
  };
  if (no_must) {
    return build.template operator()<utils::Empty>(std::forward_as_tuple());
  }
  if (must.size() == 1 && must_filters.empty() &&
      detail::ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& own = *posting.state.reader;
    const auto* const doc = detail::DocOf(own);
    return detail::ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
      using Head = detail::PostingProbeScored<Input>;
      return build.template operator()<Head>(
        std::forward_as_tuple(posting.state.cookie, *doc, segment, own,
                              recipe.Args(posting.stats, posting.boost)));
    });
  }
  auto head =
    MakeSparseConjunctionScored(must, must_filters, must_uniformity, segment,
                                recipe, merge, interrogations, ctx);
  if (!head) {
    return {};
  }
  return build.template operator()<Erased>(
    std::forward_as_tuple(std::move(head)));
}

}  // namespace irs::probe
