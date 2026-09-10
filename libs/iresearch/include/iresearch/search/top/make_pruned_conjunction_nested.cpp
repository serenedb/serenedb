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
#include <limits>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/top/detail/disjunction_leaves.hpp"
#include "iresearch/search/top/detail/prune_leaves.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/pruned_conjunction.hpp"

namespace irs::top {
namespace {

inline constexpr double kNestedMatchesPerHit = 30.0;
inline constexpr double kNestedMatchesPerHitPair = 75.0;
inline constexpr uint64_t kNestedOthersOverLead = 8;

struct NestedClause {
  std::span<const PostingClause> terms;
  uint64_t docs = 0;
};

bool BoundedPosting(const PostingClause& posting) noexcept {
  return search::ScoresOf(posting, nullptr) &&
         search::BoundsOf(*posting.state.reader);
}

bool BoundedDisjunction(const QueryBuilder& child, NestedClause& out) noexcept {
  if (child.Kind() != QueryKind::Boolean) {
    return false;
  }
  const auto& nested = sdb::basics::downCast<BooleanQuery>(child);
  const auto& should = nested.Bucket(Occur::Should);
  if (nested.MergeType() != ScoreMergeType::Sum || nested.Absorbed() != 0 ||
      nested.MinShouldMatch() != 1 || !nested.Bucket(Occur::Must).empty() ||
      !nested.Bucket(Occur::MustNot).empty() || !should.filters.empty() ||
      !should.all_docs.empty() ||
      nested.Uniformity(Occur::Should) != search::Terms::Bounded) {
    return false;
  }
  const std::span<const PostingClause> terms{should.postings};
  if (terms.size() < 2) {
    return false;
  }
  out = {.terms = terms, .docs = nested.EstimateMax()};
  return true;
}

}  // namespace

Root::ptr MakeNestedPrunedConjunction(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge) {
  SDB_ASSERT(!filters.empty());
  if (merge != ScoreMergeType::Sum || terms.size() + filters.size() < 2) {
    return {};
  }
  std::vector<NestedClause> clauses;
  clauses.reserve(terms.size() + filters.size());
  if (!search::VisitOrderedOf(
        terms, filters, true, 0, std::numeric_limits<size_t>::max(),
        [&](const PostingClause& term) {
          if (!BoundedPosting(term)) {
            return false;
          }
          clauses.push_back({.terms = std::span{&term, 1},
                             .docs = term.state.cookie.docs_count});
          return true;
        },
        [&](const QueryBuilder& child) {
          return BoundedDisjunction(child, clauses.emplace_back());
        })) {
    return {};
  }
  const auto docs = static_cast<double>(segment.docs_count());
  double share = 1.0;
  uint64_t others = 0;
  for (size_t i = 1; i != clauses.size(); ++i) {
    share *= static_cast<double>(clauses[i].docs) / docs;
    others += clauses[i].docs;
  }
  if (clauses.size() != 2 &&
      others < kNestedOthersOverLead * clauses.front().docs) {
    return {};
  }
  const double matches =
    static_cast<double>(clauses.front().docs) * std::sqrt(share);
  const auto per_hit =
    clauses.size() == 2 ? kNestedMatchesPerHitPair : kNestedMatchesPerHit;
  if (matches < static_cast<double>(ctx.k) * per_hit) {
    return {};
  }
  const auto* const doc =
    search::DocOf(search::FieldOf(clauses.front().terms.front(), nullptr));
  SDB_ASSERT(doc != nullptr);
  const bool posting_lead = clauses.front().terms.size() == 1;
  const auto size = clauses.size();
  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Clause = probe::OrLeaves<search::PostingPrunedClause<Input>, 0, true>;
    using Others = detail::PruneLeaves<Clause>;
    const auto args = [&](const PostingClause& posting) {
      return ScoreArgs{.scorer = posting.stats.scorer,
                       .stats = posting.stats.stats,
                       .fetcher = &ctx.fetcher,
                       .boost = posting.boost};
    };
    const auto prepare = [&](auto& leaf, const PostingClause& posting) {
      const auto& own = *posting.state.reader;
      SDB_ASSERT(search::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, search::LayoutOf(own), segment,
                   own, args(posting));
    };
    const auto each = [&](const NestedClause& clause) {
      return [&clause, &prepare](auto& one, size_t j) {
        prepare(one, clause.terms[j]);
      };
    };
    const auto others_args = [&](size_t i) {
      const auto& clause = clauses[i + 1];
      return std::make_tuple(clause.terms.size(), each(clause));
    };
    const auto make = [&]<typename Lead>(auto&& lead) -> Root::ptr {
      if (excludes.empty() && exclude_filters.empty()) {
        return MakeShape<PrunedConjunction, Lead, Others, utils::Empty>(
          ctx, ctx.fetcher, size, std::piecewise_construct,
          std::forward<decltype(lead)>(lead), others_args,
          std::forward_as_tuple());
      }
      const uint64_t lead_docs = clauses.front().docs;
      return search::BuildBlockExcludes<Root::ptr>(
        excludes, exclude_filters, nullptr, segment, lead_docs, lead_docs,
        [&]<typename Exclude>(auto&& negated) -> Root::ptr {
          return MakeShape<PrunedConjunction, Lead, Others, Exclude>(
            ctx, ctx.fetcher, size, std::piecewise_construct,
            std::forward<decltype(lead)>(lead), others_args,
            std::forward<decltype(negated)>(negated));
        });
    };
    const auto& first = clauses.front();
    if (posting_lead) {
      const auto& posting = first.terms.front();
      const auto& own = *posting.state.reader;
      SDB_ASSERT(search::DocOf(own) == doc);
      return make.template operator()<search::PostingPrunedLead<Input>>(
        std::forward_as_tuple(posting.state.cookie, *doc, search::LayoutOf(own),
                              segment, own, args(posting)));
    }
    return make.template operator()<detail::DisjunctionLead<Input>>(
      std::forward_as_tuple(first.terms.size(), each(first)));
  });
}

}  // namespace irs::top
