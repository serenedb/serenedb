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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/scored/sparse_exclusion.hpp"

namespace irs::scored {
namespace {

template<typename Input, typename Include, typename IncludeArgs>
Root::ptr MakeNode(IncludeArgs&& include,
                   std::span<const PostingClause> excludes,
                   std::span<const QueryBuilder::ptr> exclude_filters,
                   const SubReader& segment, ColumnArgsFetcher& fetcher,
                   uint64_t candidates, const Context& ctx) {
  return search::BuildExcludeSideOf<Root::ptr, Input>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Excludes>(auto&& negated) -> Root::ptr {
      return MakeShape<SparseExclusion, Include, Excludes>(
        ctx, std::piecewise_construct, fetcher,
        std::forward<IncludeArgs>(include),
        std::forward<decltype(negated)>(negated));
    });
}

}  // namespace

Root::ptr MakeSparseExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must, must_filters, segment);

  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty()) {
    const auto& posting = must.front();
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto* const doc = search::DocOf(own);
    if (meta.docs_count != 1 && posting.stats.stats != nullptr &&
        doc != nullptr && search::FreqOf(own) &&
        ScoresPerDoc(posting.stats.scorer)) {
      const search::ScoreRecipe recipe{.segment = &segment,
                                       .fetcher = &ctx.fetcher};
      return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
        using Include = search::PostingLeadScored<Input>;
        return MakeNode<Input, Include>(
          std::forward_as_tuple(meta, *doc, segment, own,
                                recipe.Args(posting.stats, posting.boost)),
          excludes, exclude_filters, segment, ctx.fetcher, candidates, ctx);
      });
    }
  }

  auto include = lead::MakeRequiredScored(
    must, must_filters, should, should_filters, query.Uniformity(Occur::Should),
    query.MinShouldMatch(), segment, ScoredOf(ctx), merge, absorbed);
  if (!include) {
    return {};
  }
  return MakeNode<void, lead::Erased>(std::forward_as_tuple(std::move(include)),
                                      excludes, exclude_filters, segment,
                                      ctx.fetcher, candidates, ctx);
}

}  // namespace irs::scored
