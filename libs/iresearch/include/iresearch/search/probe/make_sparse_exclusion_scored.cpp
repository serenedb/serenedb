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
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/probe/sparse_exclusion_scored.hpp"

namespace irs::probe {

Node::ptr MakeSparseExclusionScored(
  std::span<const search::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  search::Terms must_uniformity, std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const ScoredCtx& ctx, score_t absorbed) {
  const auto candidates = std::min(
    search::IncludeCandidates(must, must_filters, segment), interrogations);

  const auto build =
    [&]<typename Input, typename Include>(auto&& include) -> Node::ptr {
    return search::BuildExcludeSideOf<Node::ptr, Input>(
      exclude, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& excluded) -> Node::ptr {
        using Node = SparseExclusionScored<Include, Exclude>;
        return memory::make_managed<Impl<Node>>(
          std::piecewise_construct, std::forward<decltype(include)>(include),
          std::forward<decltype(excluded)>(excluded));
      });
  };

  if (absorbed == 0 && min_should_match == 0 && must.size() == 1 &&
      must_filters.empty()) {
    const auto& posting = must.front();
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    const auto* const doc = search::DocOf(own);
    if (posting.state.cookie.docs_count != 1 &&
        posting.stats.stats != nullptr && doc != nullptr &&
        search::FreqOf(own) && ScoresPerDoc(posting.stats.scorer)) {
      return search::ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
        using Include = search::PostingProbeScored<Input>;
        return build.template operator()<Input, Include>(
          std::forward_as_tuple(posting.state.cookie, *doc, segment, own,
                                recipe.Args(posting.stats, posting.boost)));
      });
    }
  }

  auto include =
    MakeRequiredScored(must, must_filters, must_uniformity, should,
                       should_filters, should_uniformity, min_should_match,
                       segment, recipe, merge, interrogations, ctx, absorbed);
  if (!include) {
    return {};
  }
  return build.template operator()<void, Erased>(
    std::forward_as_tuple(std::move(include)));
}

}  // namespace irs::probe
