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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/fault_points.hpp>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/search/top/pruned_disjunction.hpp"
#include "iresearch/search/top/union_member.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::top {

template<bool kExcludes, typename Leaf, typename P = NoPins, typename Init>
Root::ptr MakeDisjunctionOf(
  Init& init, size_t size, uint64_t postings,
  std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if constexpr (kExcludes) {
    if (!excludes.empty() || !exclude_filters.empty()) {
      const auto candidates =
        std::min<uint64_t>(postings, segment.docs_count());
      return irs::detail::BuildBlockExcludes<Root::ptr, P>(
        ctx.memory, excludes, exclude_filters, nullptr, segment, candidates,
        candidates, [&]<typename Exclude>(auto&& negated) -> Root::ptr {
          return irs::detail::MakeRemovable(
            std::type_identity<Exclude>{},
            std::forward<decltype(negated)>(negated),
            [&]<typename Removable>(auto&& args) -> Root::ptr {
              return MakeShape<PrunedDisjunction, Leaf, Removable>(
                ctx, ctx.memory, size, docs_count, init,
                std::forward<decltype(args)>(args));
            });
        });
    }
  }
  return MakeShape<PrunedDisjunction, Leaf, utils::Empty>(
    ctx, ctx.memory, size, docs_count, init, std::forward_as_tuple());
}

template<bool kExcludes, typename Term, typename P>
Root::ptr PrunedDisjunctionMaker<kExcludes, Term, P>::Make(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  irs::detail::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge, uint32_t min_match) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  SDB_ASSERT(min_match != 0);
  SDB_ASSERT(kExcludes || (excludes.empty() && exclude_filters.empty()));
  if (merge != ScoreMergeType::Sum ||
      uniformity != irs::detail::Terms::Bounded || min_match != 1) {
    return {};
  }
  for (size_t i = 0; i != terms.size(); ++i) {
    if (!irs::detail::ScoresOf(terms[i], scorer)) {
      return {};
    }
  }
  SDB_IF_FAILURE("irs::PruningIterator") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  if (!filters.empty()) {
    memory::ArenaVector<UnionMember::ptr> members{ctx.memory.Scratch()};
    memory::ArenaVector<uint32_t> costs{ctx.memory.Scratch()};
    members.reserve(terms.size() + filters.size());
    costs.reserve(terms.size() + filters.size());
    uint64_t postings = 0;
    for (size_t i = 0; i != terms.size(); ++i) {
      const auto posting =
        irs::detail::ClauseOf(terms[i], field, scorer, boost);
      const auto& own = *posting.state.reader;
      const auto* const doc = irs::detail::DocOf(own);
      members.push_back(
        Resolve<P>(*doc, [&]<typename Input> -> UnionMember::ptr {
          return ctx.memory.Make<TermUnionMember<Input>>(
            posting.state.cookie, *doc, irs::detail::LayoutOf(own), segment,
            own, posting.Args(ctx.memory, &ctx.fetcher));
        }));
      costs.push_back(posting.state.cookie.docs_count);
      postings += costs.back();
    }
    for (const auto& filter : filters) {
      auto member = MakeUnionMember(*filter, ctx);
      if (!member) {
        return {};
      }
      members.push_back(std::move(member));
      costs.push_back(filter->EstimateMax());
      postings += costs.back();
    }
    const auto init = [&](ErasedUnionLeaf& leaf, size_t i) {
      leaf.Reset(std::move(members[i]));
      return costs[i];
    };
    return MakeDisjunctionOf<kExcludes, ErasedUnionLeaf, P>(
      init, members.size(), postings, excludes, exclude_filters, segment, ctx);
  }
  const auto* const doc =
    irs::detail::DocOf(irs::detail::FieldOf(terms.front(), field));
  return Resolve<P>(*doc, [&]<typename Input> -> Root::ptr {
    using Leaf = PostingPrunedDisj<Input>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto posting =
        irs::detail::ClauseOf(terms[i], field, scorer, boost);
      const auto& own = *posting.state.reader;
      SDB_ASSERT(irs::detail::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, irs::detail::LayoutOf(own),
                   segment, own, posting.Args(ctx.memory, &ctx.fetcher));
      return posting.state.cookie.docs_count;
    };
    return MakeDisjunctionOf<kExcludes, Leaf, P>(
      init, terms.size(), irs::detail::SumDocs(terms), excludes,
      exclude_filters, segment, ctx);
  });
}

}  // namespace irs::top
