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
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/probe/sparse_boost_scored.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"
#include "iresearch/search/probe/sparse_conjunction_scored.hpp"

namespace irs::probe {

Node::ptr MakeSparseBoostScored(
  std::span<const search::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  search::Terms must_uniformity, std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, const SubReader& segment,
  const ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const ScoredCtx& ctx, score_t absorbed) {
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const auto no_must = must.empty() && must_filters.empty();
  const auto reach =
    no_must ? interrogations
            : std::min(interrogations,
                       search::IncludeCandidates(must, must_filters, segment));

  if (no_must) {
    return search::BuildOptionalLeaves<Node::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, reach, clause,
      [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            using Node = SparseConjunctionScored<SparseBoostScored<Leaf, N>>;
            return memory::make_managed<Impl<Node>>(
              std::piecewise_construct,
              std::forward_as_tuple(size, std::forward<decltype(init)>(init),
                                    merge),
              merge, absorbed);
          });
      });
  }

  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return search::BuildOptionalLeaves<Node::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, reach, clause,
      [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            using Both = BothLeaves<Head, SparseBoostScored<Leaf, N>>;
            using Node = SparseConjunctionScored<Both>;
            return memory::make_managed<Impl<Node>>(
              std::piecewise_construct,
              std::forward_as_tuple(
                std::piecewise_construct, std::forward<decltype(head)>(head),
                std::forward_as_tuple(size, std::forward<decltype(init)>(init),
                                      merge)),
              merge, absorbed);
          });
      });
  };

  if (must.size() == 1 && must_filters.empty()) {
    const auto& posting = must.front();
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    const auto* const doc = search::DocOf(own);
    if (posting.state.cookie.docs_count != 1 &&
        posting.stats.stats != nullptr && doc != nullptr &&
        search::FreqOf(own) && ScoresPerDoc(posting.stats.scorer)) {
      return search::ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
        using Head = search::PostingProbeScored<Input>;
        return build.template operator()<Head>(
          std::forward_as_tuple(posting.state.cookie, *doc, segment, own,
                                recipe.Args(posting.stats, posting.boost)));
      });
    }
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
