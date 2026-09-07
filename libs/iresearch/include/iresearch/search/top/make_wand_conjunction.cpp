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

#include <cmath>
#include <cstddef>
#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/top/detail/prune_leaves.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/wand_conjunction.hpp"

namespace irs::top {
namespace {

inline constexpr double kPruneMatchesPerHit = 30.0;
inline constexpr double kPruneMatchesPerHitPair = 75.0;

}  // namespace

Root::ptr MakeWandConjunction(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              search::Terms uniformity,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge) {
  if (merge != ScoreMergeType::Sum || !filters.empty() || terms.size() < 2 ||
      uniformity != search::Terms::Bounded) {
    return {};
  }
  const auto docs = static_cast<double>(segment.docs_count());
  const auto lead = static_cast<double>(terms.front().state.cookie.docs_count);
  double share = 1.0;
  for (size_t i = 1; i != terms.size(); ++i) {
    share *= static_cast<double>(terms[i].state.cookie.docs_count) / docs;
  }
  const double matches = lead * std::sqrt(share);
  const auto per_hit =
    terms.size() == 2 ? kPruneMatchesPerHitPair : kPruneMatchesPerHit;
  if (matches < static_cast<double>(ctx.k) * per_hit) {
    return {};
  }

  const auto* const doc =
    search::DocOf(search::FieldOf(terms.front(), nullptr));
  SDB_ASSERT(doc != nullptr);

  const auto size = terms.size();

  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Lead = search::PostingPrunedLead<Input>;
    using Clause = search::PostingPrunedClause<Input>;

    const auto init = [&](auto& leaf, size_t i) {
      const auto& posting = terms[i];
      const auto& own = *posting.state.reader;
      SDB_ASSERT(search::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, search::LayoutOf(own), segment,
                   own,
                   ScoreArgs{.scorer = posting.stats.scorer,
                             .stats = posting.stats.stats,
                             .fetcher = &ctx.fetcher,
                             .boost = posting.boost});
    };

    using Others = detail::PruneLeaves<Clause>;
    return MakeShape<WandConjunction, Lead, Others>(ctx, ctx.fetcher, size,
                                                    init);
  });
}

}  // namespace irs::top
