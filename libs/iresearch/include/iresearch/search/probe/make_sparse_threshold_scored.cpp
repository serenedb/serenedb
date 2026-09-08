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
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/sparse_threshold_scored.hpp"

namespace irs::probe {

Node::ptr MakeSparseThresholdScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, search::Terms uniformity,
  const SubReader& segment, const ScoreRecipe& recipe, ScoreMergeType merge,
  uint32_t min_match, uint64_t interrogations, const ScoredCtx& ctx,
  score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  if (terms.size() + filters.size() < min_match) {
    return {};
  }
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  return search::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, nullptr, nullptr, kNoBoost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = SparseThresholdScored<Leaf, N>;
          return memory::make_managed<Impl<Node>>(
            size, std::forward<decltype(init)>(init), min_match, merge,
            absorbed);
        });
    },
    search::ProbeOrder::Densest);
}

}  // namespace irs::probe
