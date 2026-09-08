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
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/lead/sparse_boost_scored.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::lead {

Node::ptr MakeSparseBoostScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto clause = probe::ScoredClauseOf(segment, ctx, recipe);
  const auto candidates = IncludeCandidates(must, must_filters, segment);

  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return search::BuildOptionalLeaves<Node::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Optional>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            using Node = SparseBoostScored<Head, Optional, N>;
            return memory::make_managed<Impl<Node>>(
              std::piecewise_construct, merge,
              std::forward<decltype(head)>(head), size,
              std::forward<decltype(init)>(init));
          });
      });
  };

  if (absorbed == 0 && must.size() == 1 && must_filters.empty()) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    if (meta.docs_count != 1 && posting.stats.stats != nullptr &&
        search::DocOf(own) != nullptr && search::FreqOf(own) &&
        ScoresPerDoc(posting.stats.scorer)) {
      const auto& doc = *search::DocOf(own);
      return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
        using Head = PostingLeadScored<Input>;
        return build.template operator()<Head>(std::forward_as_tuple(
          meta, doc, segment, own, recipe.Args(posting.stats, posting.boost)));
      });
    }
  }

  Node::ptr head = (must.empty() && must_filters.empty())
                     ? MakeAllScored(segment, absorbed)
                     : MakeSparseConjunctionScored(must, must_filters, segment,
                                                   ctx, merge, absorbed);
  if (!head) {
    return {};
  }
  return build.template operator()<Erased>(
    std::forward_as_tuple(std::move(head)));
}

}  // namespace irs::lead
