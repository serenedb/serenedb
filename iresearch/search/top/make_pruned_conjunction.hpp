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

#pragma once

#include <absl/algorithm/container.h>

#include <span>
#include <tuple>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/pruned_conjunction.hpp"
#include "iresearch/search/top/pruned_leaves.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::top {

template<irs::detail::MaskSlice kSlice>
Root::ptr MakePrunedConjunctionIn(
  std::span<const irs::detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, irs::detail::Terms uniformity,
  std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge) {
  if (merge != ScoreMergeType::Sum) {
    return {};
  }
  if (!filters.empty()) {
    return MakeNestedPrunedConjunctionIn<kSlice>(
      terms, filters, excludes, exclude_filters, segment, ctx, merge);
  }
  if (terms.size() < 2 || uniformity != irs::detail::Terms::Bounded) {
    return {};
  }
  const auto* const doc =
    irs::detail::DocOf(irs::detail::FieldOf(terms.front(), nullptr));
  SDB_ASSERT(doc != nullptr);
  const auto size = terms.size();
  const auto docs = static_cast<doc_id_t>(segment.docs_count());
  const auto joins = [&](const irs::detail::PostingClause& term) {
    return !irs::detail::DensePosting(term.state.cookie.docs_count, docs);
  };
  const auto joined =
    static_cast<size_t>(absl::c_count_if(terms.subspan(1), joins));
  const bool windowed =
    joined != 0 && irs::detail::DenseConjunction(terms, docs);
  const auto walks = PruneByBounds(ctx) ? Walks::Learned : Walks::Always;
  return irs::detail::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Lead = PostingPrunedLead<Input>;
    using Fill = irs::detail::PostingFill<Input>;
    const auto init = [&](auto& leaf, size_t i) {
      const auto& posting = terms[i];
      const auto& own = *posting.state.reader;
      SDB_ASSERT(irs::detail::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, irs::detail::LayoutOf(own),
                   segment, own, posting.Args(&ctx.fetcher));
    };
    return irs::detail::ResolveArity<1, 1>(
      size - 1, [&]<size_t N> -> Root::ptr {
        using Others = PrunedLeaves<PostingPrunedClause<Input>, N>;
        const auto build = [&]<typename Exclude>(auto&& negated) -> Root::ptr {
          if (!windowed) {
            return MakeShape<PrunedConjunction, Lead, Others, utils::Empty,
                             Exclude>(ctx, walks, ctx.fetcher, size, init,
                                      std::tuple<>{},
                                      std::forward<decltype(negated)>(negated));
          }
          return MakeShape<PrunedConjunction, Lead, Others,
                           fill::AndLeaves<Fill, N>, Exclude>(
            ctx, walks, ctx.fetcher, size, init,
            std::forward_as_tuple(
              joined,
              [&, next = size_t{1}](Fill& leaf, size_t) mutable {
                while (!joins(terms[next])) {
                  ++next;
                }
                irs::detail::PreparePlain(leaf, terms[next++], nullptr, *doc);
              }),
            std::forward<decltype(negated)>(negated));
        };
        if (excludes.empty() && exclude_filters.empty()) {
          return build.template operator()<utils::Empty>(
            std::forward_as_tuple());
        }
        const uint64_t lead = terms.front().state.cookie.docs_count;
        return irs::detail::BuildBlockExcludes<Root::ptr, kSlice>(
          excludes, exclude_filters, nullptr, segment, lead, lead, build);
      });
  });
}

}  // namespace irs::top

#define IRS_MAKE_PRUNED_CONJUNCTION(Slice)                                \
  template Root::ptr MakePrunedConjunctionIn<Slice>(                      \
    std::span<const irs::detail::PostingClause>,                          \
    std::span<const QueryBuilder::ptr>, irs::detail::Terms,               \
    std::span<const irs::detail::PostingClause>,                          \
    std::span<const QueryBuilder::ptr>, const SubReader&, const Context&, \
    ScoreMergeType)
