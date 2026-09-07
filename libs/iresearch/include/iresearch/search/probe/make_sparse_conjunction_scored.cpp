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
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"
#include "iresearch/search/probe/sparse_conjunction_scored.hpp"

namespace irs::probe {

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, search::Terms uniformity,
  const SubReader& segment, const ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const ScoredCtx& ctx, score_t absorbed) {
  const auto size = terms.size() + filters.size();
  if (size == 0) {
    return absorbed != 0 ? MakeAllScored(segment, absorbed) : Node::ptr{};
  }
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  if (size == 1) {
    auto only =
      filters.empty()
        ? clause(terms.front(), nullptr, interrogations)
        : clause(search::PostingClause{TermState{nullptr, PostingMeta{}}},
                 filters.front().get(), interrogations);
    if (absorbed == 0 || !only) {
      return only;
    }
    using Node = SparseConjunctionScored<Erased>;
    return memory::make_managed<Impl<Node>>(Erased{std::move(only)}, merge,
                                            absorbed);
  }
  return search::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, nullptr, nullptr, kNoBoost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = SparseConjunctionScored<SparseConjunctionDocs<Leaf, N>>;
          return memory::make_managed<Impl<Node>>(
            std::piecewise_construct,
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
            merge, absorbed);
        });
    },
    search::ProbeOrder::Narrowest);
}

Node::ptr MakeRequiredScored(
  std::span<const search::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  search::Terms must_uniformity, std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  const SubReader& segment, const ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const ScoredCtx& ctx, score_t absorbed) {
  if (min_should_match == 0) {
    return MakeSparseConjunctionScored(must, must_filters, must_uniformity,
                                       segment, recipe, merge, interrogations,
                                       ctx, absorbed);
  }
  const auto no_must = must.empty() && must_filters.empty();
  const auto reach =
    no_must ? interrogations
            : std::min(interrogations,
                       search::IncludeCandidates(must, must_filters, segment));
  const auto optional_absorbed = no_must ? absorbed : score_t{0};
  auto optional =
    min_should_match == 1
      ? MakeSparseDisjunctionScored(
          should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
          segment, recipe, merge, reach, ScoredClauseOf(segment, ctx, recipe),
          optional_absorbed)
      : MakeSparseThresholdScored(should, should_filters, should_uniformity,
                                  segment, recipe, merge, min_should_match,
                                  reach, ctx, optional_absorbed);
  if (!optional) {
    return {};
  }
  if (no_must) {
    return optional;
  }
  auto required =
    MakeSparseConjunctionScored(must, must_filters, must_uniformity, segment,
                                recipe, merge, interrogations, ctx, absorbed);
  if (!required) {
    return {};
  }
  using Leaves = BothLeaves<Erased, Erased>;
  using Node = SparseConjunctionScored<Leaves>;
  return memory::make_managed<Impl<Node>>(
    Leaves{Erased{std::move(required)}, Erased{std::move(optional)}}, merge);
}

}  // namespace irs::probe
