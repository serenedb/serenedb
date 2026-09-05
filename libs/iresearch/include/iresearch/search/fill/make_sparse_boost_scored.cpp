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
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/lead/sparse_boost_scored.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::fill {

Node::ptr MakeSparseBoostScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  SDB_ASSERT(!should_terms.empty() || !should_filters.empty());
  const search::ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto clause = [&](const search::PostingClause& posting,
                          const QueryBuilder* child,
                          uint64_t interrogations) -> probe::Node::ptr {
    if (child == nullptr) {
      return probe::MakePostingScored(posting, segment, recipe);
    }
    return child->PlanProbe(ctx, interrogations);
  };
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);

  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return search::BuildOptionalLeaves<Node::ptr>(
      should_terms, should_filters, uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Optional>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            using Node = lead::SparseBoostScored<Head, Optional, N>;
            return memory::make_managed<ByWalkScored<Node>>(
              merge, std::piecewise_construct, ScoreMergeType::Sum,
              std::forward<decltype(head)>(head), size,
              std::forward<decltype(init)>(init));
          });
      });
  };

  if (absorbed == 0 && must_filters.empty() && must_terms.size() == 1) {
    const auto& posting = must_terms.front();
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& reader = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto* const input = search::DocOf(reader);
    if (meta.docs_count != 1 && posting.stats.stats != nullptr &&
        input != nullptr && search::FreqOf(reader) &&
        ScoresPerDoc(posting.stats.scorer)) {
      return search::ResolveInput(*input, [&]<typename Input> -> Node::ptr {
        using Head = search::PostingLeadScored<Input>;
        return build.template operator()<Head>(
          std::forward_as_tuple(meta, *input, segment, reader,
                                recipe.Args(posting.stats, posting.boost)));
      });
    }
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
