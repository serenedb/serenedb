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
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/lead/sparse_exclusion_scored.hpp"

namespace irs::fill {
namespace {

template<typename Input, typename Include, typename IncludeArgs>
Node::ptr MakeNode(IncludeArgs&& include,
                   std::span<const search::PostingClause> exclude_terms,
                   std::span<const QueryBuilder::ptr> exclude_filters,
                   const SubReader& segment, uint64_t candidates,
                   ColumnArgsFetcher& fetcher, ScoreMergeType merge) {
  return search::BuildExcludeSideOf<Node::ptr, Input>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      using Node = lead::SparseExclusionScored<Include, Exclude>;
      return memory::make_managed<ByWalkScored<Node>>(
        merge, fetcher, std::piecewise_construct,
        std::forward<IncludeArgs>(include),
        std::forward<decltype(exclude)>(exclude));
    });
}

}  // namespace

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
      must_filters.empty() && must_terms.size() == 1) {
    const auto& posting = must_terms.front();
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& reader = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto* const input = search::DocOf(reader);
    if (meta.docs_count != 1 && posting.stats.stats != nullptr &&
        input != nullptr && search::FreqOf(reader) &&
        ScoresPerDoc(posting.stats.scorer)) {
      const search::ScoreRecipe recipe{.segment = &segment,
                                       .fetcher = ctx.fetcher};
      return search::ResolveInput(*input, [&]<typename Input> -> Node::ptr {
        using Include = search::PostingLeadScored<Input>;
        return MakeNode<Input, Include>(
          std::forward_as_tuple(meta, *input, segment, reader,
                                recipe.Args(posting.stats, posting.boost)),
          exclude_terms, exclude_filters, segment, candidates, *ctx.fetcher,
          merge);
      });
    }
  }

  auto include = lead::MakeRequiredScored(
    must_terms, must_filters, should_terms, should_filters, should_uniformity,
    min_should_match, segment, ctx, own, absorbed);
  if (!include) {
    return {};
  }
  return MakeNode<void, lead::Erased>(std::forward_as_tuple(std::move(include)),
                                      exclude_terms, exclude_filters, segment,
                                      candidates, *ctx.fetcher, merge);
}

}  // namespace irs::fill
