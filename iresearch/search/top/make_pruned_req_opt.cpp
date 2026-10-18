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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/prune_leaves.hpp"
#include "iresearch/search/top/pruned_req_opt.hpp"

namespace irs::top {

Root::ptr MakePrunedReqOpt(const irs::detail::PostingClause& required,
                           std::span<const irs::detail::PostingClause> optional,
                           const SubReader& segment, const Context& ctx,
                           ScoreMergeType merge) {
  if (merge != ScoreMergeType::Sum || optional.empty()) {
    return {};
  }
  const auto* const doc =
    irs::detail::DocOf(irs::detail::FieldOf(required, nullptr));
  SDB_ASSERT(doc != nullptr);
  const auto size = optional.size() + 1;
  return irs::detail::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Lead = PostingPrunedLead<Input>;
    using Clause = PostingPrunedClause<Input>;
    const auto init = [&](auto& leaf, size_t i) {
      const auto& posting = i == 0 ? required : optional[i - 1];
      const auto& own = *posting.state.reader;
      SDB_ASSERT(irs::detail::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, irs::detail::LayoutOf(own),
                   segment, own,
                   irs::detail::ScoreArgs{.scorer = posting.stats.scorer,
                                          .stats = posting.stats.stats,
                                          .fetcher = &ctx.fetcher,
                                          .boost = posting.boost});
    };
    if (size == 2) {
      return MakeShape<PrunedReqOpt, Lead, PruneLeaves<Clause, 1>>(
        ctx, ctx.fetcher, size, init);
    }
    return MakeShape<PrunedReqOpt, Lead, PruneLeaves<Clause>>(ctx, ctx.fetcher,
                                                              size, init);
  });
}

}  // namespace irs::top
