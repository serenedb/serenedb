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

#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/lead/sparse_exclusion_scored.hpp"

namespace irs::lead {
namespace {

template<typename Input, typename Include, typename IncludeArgs>
Node::ptr MakeNode(IncludeArgs&& include,
                   std::span<const PostingClause> excludes,
                   std::span<const QueryBuilder::ptr> exclude_filters,
                   const SubReader& segment, uint64_t candidates) {
  return search::BuildExcludeSideOf<Node::ptr, Input>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      using Node = SparseExclusionScored<Include, Exclude>;
      return memory::make_managed<Impl<Node>>(
        std::piecewise_construct, std::forward<IncludeArgs>(include),
        std::forward<decltype(exclude)>(exclude));
    });
}

}  // namespace

Node::ptr MakeSparseExclusionScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates = IncludeCandidates(must, must_filters, segment);

  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty()) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    if (meta.docs_count != 1 && posting.stats.stats != nullptr &&
        search::DocOf(own) != nullptr && search::FreqOf(own) &&
        ScoresPerDoc(posting.stats.scorer)) {
      const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
      const auto& doc = *search::DocOf(own);
      return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
        using Include = PostingLeadScored<Input>;
        return MakeNode<Input, Include>(
          std::forward_as_tuple(meta, doc, segment, own,
                                recipe.Args(posting.stats, posting.boost)),
          excludes, exclude_filters, segment, candidates);
      });
    }
  }

  auto include = MakeRequiredScored(must, must_filters, should, should_filters,
                                    should_uniformity, min_should_match,
                                    segment, ctx, merge, absorbed);
  if (!include) {
    return {};
  }
  return MakeNode<void, Erased>(std::forward_as_tuple(std::move(include)),
                                excludes, exclude_filters, segment, candidates);
}

}  // namespace irs::lead
