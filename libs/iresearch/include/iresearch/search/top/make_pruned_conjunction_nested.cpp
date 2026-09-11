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
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/probe/boolean_window.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/top/detail/disjunction_leaves.hpp"
#include "iresearch/search/top/detail/prune_leaves.hpp"
#include "iresearch/search/top/detail/pruned_clause.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/pruned_conjunction.hpp"

namespace irs::top {
namespace {

inline constexpr double kNestedMatchesPerHit = 30.0;
inline constexpr double kNestedMatchesPerHitPair = 75.0;
inline constexpr uint64_t kNestedOthersOverLead = 8;
inline constexpr uint64_t kConstantLeadDensity = 16;

struct NestedClause {
  std::span<const search::PostingClause> terms;
  uint64_t docs = 0;
  const QueryBuilder* constant = nullptr;
  score_t bound = 0;
};

bool BoundedPosting(const search::PostingClause& posting) noexcept {
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
  const std::span<const search::PostingClause> terms{should.postings};
  if (terms.size() < 2) {
    return false;
  }
  out = {.terms = terms, .docs = nested.EstimateMax()};
  return true;
}

bool ConstantTerms(const QueryBuilder& child, const SubReader& segment,
                   const Context& ctx, NestedClause& out) {
  if (child.Kind() != QueryKind::Terms) {
    return false;
  }
  const auto& query = sdb::basics::downCast<MultiTermQuery>(child);
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ScoredOf(ctx)).scorer;
  if (query.MergeType() != ScoreMergeType::Sum || field == nullptr ||
      scorer == nullptr || search::DocOf(*field) == nullptr ||
      search::UniformityOf(*field, scorer) != search::Terms::Constant) {
    return false;
  }
  const auto boost = query.Boost();
  score_t bound = 0;
  for (const auto& entry : state.Terms()) {
    if (entry.stats == nullptr) {
      return false;
    }
    bound +=
      search::AllDocsScore(segment, search::ScoreArgs{.scorer = scorer,
                                              .stats = entry.stats,
                                              .fetcher = &ctx.fetcher,
                                              .boost = entry.boost * boost});
  }
  out = {.docs = query.EstimateMax(), .constant = &child, .bound = bound};
  return true;
}

}  // namespace

Root::ptr MakeNestedPrunedConjunction(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> excludes,
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
        [&](const search::PostingClause& term) {
          if (!BoundedPosting(term)) {
            return false;
          }
          clauses.push_back({.terms = std::span{&term, 1},
                             .docs = term.state.cookie.docs_count});
          return true;
        },
        [&](const QueryBuilder& child) {
          auto& clause = clauses.emplace_back();
          return BoundedDisjunction(child, clause) ||
                 ConstantTerms(child, segment, ctx, clause);
        })) {
    return {};
  }
  const auto lead = std::find_if(
    clauses.begin(), clauses.end(),
    [](const NestedClause& clause) { return clause.constant == nullptr; });
  if (lead == clauses.end()) {
    return {};
  }
  std::rotate(clauses.begin(), lead, lead + 1);
  const auto docs = static_cast<double>(segment.docs_count());
  double share = 1.0;
  uint64_t others = 0;
  bool constants = false;
  for (size_t i = 1; i != clauses.size(); ++i) {
    share *= static_cast<double>(clauses[i].docs) / docs;
    others += clauses[i].docs;
    constants = constants || clauses[i].constant != nullptr;
  }
  if (clauses.size() != 2 &&
      others < kNestedOthersOverLead * clauses.front().docs) {
    return {};
  }
  if (constants &&
      clauses.front().docs * kConstantLeadDensity < segment.docs_count()) {
    return {};
  }
  const double matches =
    static_cast<double>(clauses.front().docs) * std::sqrt(share);
  const auto per_hit =
    clauses.size() == 2 ? kNestedMatchesPerHitPair : kNestedMatchesPerHit;
  if (matches < static_cast<double>(ctx.k) * per_hit) {
    return {};
  }
  std::vector<fill::Node::ptr> fills(clauses.size());
  for (size_t i = 1; i != clauses.size(); ++i) {
    if (clauses[i].constant == nullptr) {
      continue;
    }
    fills[i] = clauses[i].constant->PlanFill(ScoredOf(ctx), merge);
    if (!fills[i]) {
      return {};
    }
  }
  const auto* const doc =
    search::DocOf(search::FieldOf(clauses.front().terms.front(), nullptr));
  SDB_ASSERT(doc != nullptr);
  const bool posting_lead = clauses.front().terms.size() == 1;
  const auto size = clauses.size();
  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Leaf = search::PostingPrunedClause<Input>;
    using Group = probe::OrLeaves<Leaf, 0, true>;
    using Window =
      probe::BooleanWindow<search::OrGroup<fill::SetLeaves<fill::Erased>>,
                           search::Scored, true>;
    const auto args = [&](const search::PostingClause& posting) {
      return search::ScoreArgs{.scorer = posting.stats.scorer,
                       .stats = posting.stats.stats,
                       .fetcher = &ctx.fetcher,
                       .boost = posting.boost};
    };
    const auto prepare = [&](auto& leaf, const search::PostingClause& posting) {
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
    const auto erased = [&](size_t i) -> detail::PrunedClause::ptr {
      const auto& clause = clauses[i];
      if (clause.constant != nullptr) {
        return memory::make_managed<detail::PrunedClauseImpl<Window>>(
          std::piecewise_construct,
          std::forward_as_tuple(size_t{1},
                                [&](fill::Erased& leaf, size_t) {
                                  leaf = fill::Erased{std::move(fills[i])};
                                }),
          search::Scored{merge, 0}, clause.bound);
      }
      if (clause.terms.size() == 1) {
        const auto& posting = clause.terms.front();
        const auto& own = *posting.state.reader;
        return memory::make_managed<detail::PrunedClauseImpl<Leaf>>(
          posting.state.cookie, *doc, search::LayoutOf(own), segment, own,
          args(posting));
      }
      return memory::make_managed<detail::PrunedClauseImpl<Group>>(
        clause.terms.size(), each(clause));
    };
    const auto build = [&]<typename Lead, typename Others>(
                         auto&& lead, auto&& others_args) -> Root::ptr {
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
    const auto make = [&]<typename Lead>(auto&& lead) -> Root::ptr {
      if (constants) {
        using Others = detail::PruneLeaves<detail::ErasedClause>;
        return build.template operator()<Lead, Others>(
          std::forward<decltype(lead)>(lead),
          [&](size_t i) { return std::make_tuple(erased(i + 1)); });
      }
      using Others = detail::PruneLeaves<Group>;
      return build.template operator()<Lead, Others>(
        std::forward<decltype(lead)>(lead), [&](size_t i) {
          const auto& clause = clauses[i + 1];
          return std::make_tuple(clause.terms.size(), each(clause));
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
