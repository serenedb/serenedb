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

#include <cstdint>
#include <limits>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_bitset.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/conjunction.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_count.hpp"
#include "iresearch/search/detail/posting_fill.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail::builder {

template<typename Family>
using Result = typename Family::Result;

template<typename Family, typename Ctx>
ScoreRecipe RecipeOf(const SubReader& segment, const Ctx& ctx) noexcept {
  return {.segment = &segment, .fetcher = Family::Scoring(ctx).fetcher};
}

template<typename Family, typename Scoring>
consteval bool LazyGroups() {
  if constexpr (kScores<Scoring>) {
    return Family::kLazyGroups;
  } else {
    return false;
  }
}

template<typename Family, typename Ctx>
Result<Family> MakeBitset(const BooleanGroups& groups, const SubReader& segment,
                          const Ctx& ctx) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  return MakeBooleanBitset<Result<Family>>(groups, segment,
                                           Family::BitsetTable(ctx),
                                           Family::BitsetSpan(ctx, docs_count));
}

template<typename Family, typename Excludes, typename ExcludesArgs,
         typename Ctx>
Result<Family> MakeWindowOfTerms(std::span<const PostingClause> terms,
                                 const IndexInput& doc, ExcludesArgs&& excludes,
                                 const Ctx& ctx) {
  SDB_ASSERT(terms.size() >= 2);
  return ResolveInput(doc, [&]<typename Input> -> Result<Family> {
    using Leaf = PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& own = FieldOf(terms.front(), nullptr);
    const auto& front = CookieOf(terms.front());
    return Family::template MakeWindow<Leaf, Others, utils::Empty, Excludes>(
      ctx, ScoreMergeType::Noop, utils::Empty{},
      std::forward_as_tuple(front, doc, LayoutOf(own),
                            front.docs_count != 1 && BoundsOf(own),
                            front.docs_count != 1 && FreqOf(own)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = FieldOf(terms[i + 1], nullptr);
          const auto& meta = CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc, LayoutOf(other),
                       meta.docs_count != 1 && BoundsOf(other),
                       meta.docs_count != 1 && FreqOf(other));
        }),
      std::forward_as_tuple(), std::forward<ExcludesArgs>(excludes));
  });
}

template<typename Family, typename Term, typename Ctx>
Result<Family> MakeWindowDisjunctionOfTerms(std::span<const Term> terms,
                                            const TermReader* field,
                                            const IndexInput& doc,
                                            const Ctx& ctx) {
  SDB_ASSERT(terms.size() > 1);
  return ResolveInput(doc, [&]<typename Input> -> Result<Family> {
    using Leaf = PostingFill<Input>;
    using Optional = OrGroup<fill::SetLeaves<Leaf>>;
    return Family::template MakeWindow<utils::Empty, utils::Empty, Optional,
                                       utils::Empty>(
      ctx, ScoreMergeType::Noop, utils::Empty{}, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(terms.size(),
                            [&](Leaf& leaf, size_t i) {
                              const auto& own = FieldOf(terms[i], field);
                              const auto& meta = CookieOf(terms[i]);
                              SDB_ASSERT(meta.docs_count != 0);
                              leaf.Prepare(
                                meta, doc, LayoutOf(own),
                                meta.docs_count != 1 && BoundsOf(own),
                                meta.docs_count != 1 && FreqOf(own));
                            }),
      std::forward_as_tuple());
  });
}

template<typename Family, typename Term, typename Ctx>
Result<Family> MakeDisjunctionOfTerms(std::span<const Term> terms,
                                      const TermReader* field,
                                      const IndexInput& doc,
                                      doc_id_t docs_count, const Ctx& ctx) {
  SDB_ASSERT(terms.size() > 1);
  if (Family::BitsetSpan(ctx, docs_count) >= docs_count) {
    if (auto folded = MakeBitsetOf<Result<Family>>(
          terms, field, doc, docs_count, Family::BitsetTable(ctx))) {
      return folded;
    }
  }
  return MakeWindowDisjunctionOfTerms<Family>(terms, field, doc, ctx);
}

template<typename Family, typename Ctx>
Result<Family> MakeWindowOfNodes(std::span<const PostingClause> terms,
                                 std::span<const QueryBuilder::ptr> filters,
                                 const SubReader& segment, const Ctx& ctx) {
  std::vector<FillNode::ptr> nodes;
  nodes.reserve(terms.size() + filters.size());
  const auto take = [&](FillNode::ptr node) {
    if (!node) {
      return false;
    }
    nodes.emplace_back(std::move(node));
    return true;
  };
  if (!VisitOrderedOf(
        terms, filters, true, 0, std::numeric_limits<size_t>::max(),
        [&](const PostingClause& term) {
          return take(FillOf(term, nullptr, segment));
        },
        [&](const QueryBuilder& child) {
          return take(child.PlanFill({}, ScoreMergeType::Noop));
        })) {
    return {};
  }
  using Others = fill::AndLeaves<fill::Erased>;
  return Family::template MakeWindow<fill::Erased, Others, utils::Empty,
                                     utils::Empty>(
    ctx, ScoreMergeType::Noop, utils::Empty{},
    std::forward_as_tuple(std::move(nodes.front())),
    std::forward_as_tuple(nodes.size() - 1,
                          [&](fill::Erased& leaf, size_t i) {
                            leaf = fill::Erased{std::move(nodes[i + 1])};
                          }),
    std::forward_as_tuple(), std::forward_as_tuple());
}

template<typename Family, typename Scoring, typename Ctx>
FillNode::ptr PlanFillOf(const QueryBuilder& child, const Ctx& ctx,
                         const Scoring& scoring) {
  if constexpr (kScores<Scoring>) {
    return child.PlanFill(Family::Scoring(ctx), scoring.merge);
  } else {
    return child.PlanFill({}, ScoreMergeType::Noop);
  }
}

template<typename Family, typename Ctx>
TermScoring ScoringOf(const SubReader& segment, const Ctx& ctx,
                      ScoreMergeType merge, Terms uniformity = Terms::Mixed) {
  return {.scorer = nullptr,
          .boost = kNoBoost,
          .uniformity = uniformity,
          .recipe = RecipeOf<Family>(segment, ctx),
          .merge = merge};
}

template<typename Family, typename Term, typename Scoring, typename Ctx>
Result<Family> MakeWindowDisjunction(std::span<const Term> terms,
                                     const TermReader* field,
                                     const IndexInput* doc,
                                     std::vector<FillNode::ptr>& rest,
                                     const Scoring& scoring, const Ctx& ctx,
                                     score_t absorbed) {
  return BuildWindowSet<Result<Family>>(
    terms, field, doc, rest, scoring,
    [&]<typename Set>(auto&&... args) -> Result<Family> {
      return Family::template MakeWindow<utils::Empty, utils::Empty,
                                         OrGroup<Set>, utils::Empty>(
        ctx, scoring.merge, ScoreOf(scoring, absorbed), std::forward_as_tuple(),
        std::forward_as_tuple(),
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        std::forward_as_tuple());
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeWindowConjunction(std::span<const PostingClause> terms,
                                     std::span<const QueryBuilder::ptr> filters,
                                     const SubReader& segment, const Ctx& ctx) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if constexpr (Family::kWindowNodes) {
    if (!filters.empty()) {
      if (HeadEstimate(terms, filters) <
          docs_count / kDensityThresholdInverse) {
        return {};
      }
      return MakeWindowOfNodes<Family>(terms, filters, segment, ctx);
    }
  }
  const IndexInput* doc = nullptr;
  if (!WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!DenseConjunction(terms, docs_count)) {
    return {};
  }
  return MakeWindowOfTerms<Family, utils::Empty>(terms, *doc,
                                                 std::forward_as_tuple(), ctx);
}

template<typename Result, typename Term, typename Make>
Result BuildNegationWindow(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const SubReader& segment, Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  using Excludes = fill::FilledAndNot<fill::SetLeaves<fill::Erased>>;
  const auto split = SplitMask(filters);
  const auto with_lead = [&]<typename Lead>(auto&& lead) -> Result {
    if (terms.empty() && split.rest.empty()) {
      return make.template operator()<Lead, utils::Empty>(
        std::forward<decltype(lead)>(lead), std::forward_as_tuple());
    }
    std::vector<FillNode::ptr> nodes;
    if (!CollectFills(terms, split.rest, nullptr, segment, nodes)) {
      return {};
    }
    return make.template operator()<Lead, Excludes>(
      std::forward<decltype(lead)>(lead),
      std::forward_as_tuple(
        std::piecewise_construct,
        std::forward_as_tuple(nodes.size(), [&](fill::Erased& leaf, size_t i) {
          leaf = fill::Erased{std::move(nodes[i])};
        })));
  };
  if (!split.masked) {
    return with_lead.template operator()<fill::AllDocs>(
      std::forward_as_tuple(segment));
  }
  return ResolveDocsMask(
    segment, [&]<DocsMaskType Mask>(Mask docs_mask) -> Result {
      return with_lead.template operator()<fill::LiveDocs<Mask>>(
        std::forward_as_tuple(std::move(docs_mask), LiveEnd(segment)));
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeWindowNegation(
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Ctx& ctx) {
  return BuildNegationWindow<Result<Family>>(
    exclude_terms, exclude_filters, segment,
    [&]<typename Lead, typename Excludes>(auto&& lead,
                                          auto&& excludes) -> Result<Family> {
      return Family::template MakeWindow<Lead, utils::Empty, utils::Empty,
                                         Excludes>(
        ctx, ScoreMergeType::Noop, utils::Empty{},
        std::forward<decltype(lead)>(lead), std::forward_as_tuple(),
        std::forward_as_tuple(), std::forward<decltype(excludes)>(excludes));
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeWindowExclusion(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Ctx& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (terms.size() + filters.size() == 1) {
    if (HeadIsTerm(terms, filters)) {
      const ExcludeCosts<PostingClause> costs{
        exclude_terms, exclude_filters, candidates,
        candidates,    docs_count,      ExcludeUse::PerDoc};
      if (!costs.TakesWindowLead(
            SegmentDoc(segment) != nullptr, Family::kWindowLeadDrains,
            Family::kSparseLeadCost, Family::kWindowLeadRefills, candidates)) {
        return {};
      }
      const auto& own = FieldOf(terms.front(), nullptr);
      const auto* const doc = DocOf(own);
      if (doc == nullptr) {
        return {};
      }
      const auto& front = CookieOf(terms.front());
      return ResolveInput(*doc, [&]<typename Input> -> Result<Family> {
        return BuildWindowExcludes<Result<Family>>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Excludes>(auto&& excludes) -> Result<Family> {
            return Family::template MakeWindow<PostingFill<Input>, utils::Empty,
                                               utils::Empty, Excludes>(
              ctx, ScoreMergeType::Noop, utils::Empty{},
              std::forward_as_tuple(front, *doc, LayoutOf(own),
                                    front.docs_count != 1 && BoundsOf(own),
                                    front.docs_count != 1 && FreqOf(own)),
              std::forward_as_tuple(), std::forward_as_tuple(),
              std::forward<decltype(excludes)>(excludes));
          });
      });
    }
    auto node = filters.front()->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return {};
    }
    return BuildWindowExcludes<Result<Family>>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Excludes>(auto&& excludes) -> Result<Family> {
        return Family::template MakeWindow<fill::Erased, utils::Empty,
                                           utils::Empty, Excludes>(
          ctx, ScoreMergeType::Noop, utils::Empty{},
          std::forward_as_tuple(std::move(node)), std::forward_as_tuple(),
          std::forward_as_tuple(), std::forward<decltype(excludes)>(excludes));
      });
  }
  const IndexInput* doc = nullptr;
  if (!WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!DenseConjunction(terms, docs_count)) {
    return {};
  }
  return BuildWindowExcludes<Result<Family>>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Excludes>(auto&& excludes) -> Result<Family> {
      return MakeWindowOfTerms<Family, Excludes>(
        terms, *doc, std::forward<decltype(excludes)>(excludes), ctx);
    });
}

template<typename Family, typename Scoring, typename Ctx>
Result<Family> MakeWindowThreshold(std::span<const PostingClause> terms,
                                   const IndexInput* doc,
                                   std::vector<FillNode::ptr>& rest,
                                   uint32_t min_match, const Scoring& scoring,
                                   const Ctx& ctx, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + rest.size() >= min_match);
  const score_t base = [&] {
    if constexpr (kScores<Scoring>) {
      return Family::Base(absorbed);
    } else {
      return score_t{0};
    }
  }();
  const auto window =
    [&]<template<typename, bool> typename Group, typename Set>(
      auto&&... args) -> Result<Family> {
    return Family::template MakeWindow<
      utils::Empty, utils::Empty, Group<Set, LazyGroups<Family, Scoring>()>,
      utils::Empty>(
      ctx, scoring.merge, ScoreOf(scoring, absorbed), std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(
        std::piecewise_construct,
        std::forward_as_tuple(std::forward<decltype(args)>(args)...), min_match,
        base),
      std::forward_as_tuple());
  };
  if (min_match > kBitplaneMaxMatch && rest.empty() &&
      scoring.uniformity != Terms::Mixed) {
    auto counted = ResolveCountLeaves<Result<Family>>(
      *doc, scoring, [&]<typename Leaf, typename Plain> -> Result<Family> {
        return BuildWindowTerms<Result<Family>, Leaf, Plain>(
          terms, nullptr, doc, scoring,
          [&]<typename Set>(auto&&... args) -> Result<Family> {
            return window.template operator()<TallyGroup, Set>(
              std::forward<decltype(args)>(args)...);
          });
      });
    if (counted) {
      return counted;
    }
  }
  return BuildWindowSet<Result<Family>>(
    terms, nullptr, doc, rest, scoring,
    [&]<typename Set>(auto&&... args) -> Result<Family> {
      return window.template operator()<ThresholdGroup, Set>(
        std::forward<decltype(args)>(args)...);
    });
}

template<typename Family, typename Scoring, typename Make, typename Ctx>
Result<Family> BuildSparseConjunction(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const Ctx& ctx, const Scoring& scoring, Make&& make) {
  if constexpr (kScores<Scoring>) {
    const auto& child = Family::Scoring(ctx);
    return BuildConjunction<Result<Family>>(
      terms, filters, nullptr, segment, scoring,
      probe::ScoredClauseOf(segment, child, scoring.recipe),
      [&](const QueryBuilder& filter) -> LeadNode::ptr {
        return filter.PlanLead(child);
      },
      std::forward<Make>(make));
  } else {
    return BuildConjunction<Result<Family>>(
      terms, filters, nullptr, segment, scoring, ProbeClauseOf(segment),
      kPlanLeadDocs, std::forward<Make>(make));
  }
}

template<typename Family, typename Scoring, typename Score, typename Ctx>
Result<Family> MakeSparseConjunction(std::span<const PostingClause> terms,
                                     std::span<const QueryBuilder::ptr> filters,
                                     const SubReader& segment, const Ctx& ctx,
                                     const Scoring& scoring, Score score) {
  return BuildSparseConjunction<Family>(
    terms, filters, segment, ctx, scoring,
    [&]<typename Head, typename Tail>(auto&& head,
                                      auto&& tail) -> Result<Family> {
      return Family::template MakeSparse<Head, Tail, utils::Empty,
                                         utils::Empty>(
        ctx, scoring.merge, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
        std::forward_as_tuple());
    });
}

template<typename Family, typename Score, typename Ctx>
Result<Family> MakeSparseExclusionOf(
  LeadNode::ptr include, std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Ctx& ctx, ScoreMergeType merge, Score score) {
  SDB_ASSERT(include);
  return BuildExcludeSide<Result<Family>>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Result<Family> {
      return Family::template MakeSparse<lead::Erased, utils::Empty,
                                         utils::Empty, Exclude>(
        ctx, merge, score, std::forward_as_tuple(std::move(include)),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude));
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeSparseNegation(
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Ctx& ctx) {
  if (OnlyMask(exclude_terms, exclude_filters)) {
    return MakeWindowNegation<Family>(exclude_terms, exclude_filters, segment,
                                      ctx);
  }
  const auto split = SplitMask(exclude_filters);
  if (!split.masked) {
    auto driven = lead::MakeAllDocs(segment);
    if (!driven) {
      return {};
    }
    return MakeSparseExclusionOf<Family>(
      std::move(driven), exclude_terms, exclude_filters, segment, candidates,
      ctx, ScoreMergeType::Noop, utils::Empty{});
  }
  return ResolveDocsMask(
    segment, [&]<DocsMaskType Mask>(Mask docs_mask) -> Result<Family> {
      using Lead = LiveDocs<Mask>;
      return BuildRestExcludes<Result<Family>, void, false>(
        ExcludeUse::PerDoc, exclude_terms, split.rest, exclude_filters, nullptr,
        segment, candidates, candidates, kNoMaskFold,
        [&]<typename Exclude>(auto&& exclude) -> Result<Family> {
          return Family::template MakeSparse<Lead, utils::Empty, utils::Empty,
                                             Exclude>(
            ctx, ScoreMergeType::Noop, utils::Empty{},
            std::forward_as_tuple(std::move(docs_mask), LiveEnd(segment)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            std::forward<decltype(exclude)>(exclude));
        });
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeSparseExclusion(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Ctx& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    if (auto folded = MakeConjunctionBitset<LeadNode::ptr>(
          terms, filters, nullptr, segment, nullptr)) {
      return MakeSparseExclusionOf<Family>(
        std::move(folded), exclude_terms, exclude_filters, segment, candidates,
        ctx, ScoreMergeType::Noop, utils::Empty{});
    }
    return BuildSparseConjunction<Family>(
      terms, filters, segment, ctx, NoScoring{},
      [&]<typename Head, typename Tail>(auto&& head,
                                        auto&& tail) -> Result<Family> {
        return BuildExcludeSide<Result<Family>>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Exclude>(auto&& exclude) -> Result<Family> {
            return Family::template MakeSparse<Head, Tail, utils::Empty,
                                               Exclude>(
              ctx, ScoreMergeType::Noop, utils::Empty{},
              std::forward<decltype(head)>(head),
              std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
              std::forward<decltype(exclude)>(exclude));
          });
      });
  }
  if (!HeadIsTerm(terms, filters)) {
    auto lead = filters.front()->PlanLead({});
    if (!lead) {
      return {};
    }
    return MakeSparseExclusionOf<Family>(
      std::move(lead), exclude_terms, exclude_filters, segment, candidates, ctx,
      ScoreMergeType::Noop, utils::Empty{});
  }
  const auto& own = *terms.front().state.reader;
  const auto& meta = terms.front().state.cookie;
  return ResolveInput(*DocOf(own), [&]<typename Input> -> Result<Family> {
    using Include = PostingLead<Input>;
    return BuildExcludeSideOf<Result<Family>, Input>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Result<Family> {
        return Family::template MakeSparse<Include, utils::Empty, utils::Empty,
                                           Exclude>(
          ctx, ScoreMergeType::Noop, utils::Empty{},
          std::forward_as_tuple(meta, *DocOf(own), LayoutOf(own),
                                BoundsOf(own)),
          std::forward_as_tuple(), std::forward_as_tuple(),
          std::forward<decltype(exclude)>(exclude));
      });
  });
}

template<typename Family, typename Term, typename Scoring, typename Ctx>
Result<Family> MakeDisjunction(std::span<const Term> terms,
                               std::span<const QueryBuilder::ptr> filters,
                               const TermReader* field,
                               const SubReader& segment, const Scoring& scoring,
                               const Ctx& ctx, score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, field, doc, rest,
                    [&](const QueryBuilder& child) {
                      return PlanFillOf<Family>(child, ctx, scoring);
                    })) {
    return {};
  }
  if constexpr (!kScores<Scoring>) {
    if (auto folded = MakeBitset<Family>(
          {.should = terms, .should_filters = filters, .should_fills = &rest},
          segment, ctx)) {
      return folded;
    }
  }
  return MakeWindowDisjunction<Family>(terms, field, doc, rest, scoring, ctx,
                                       absorbed);
}

template<typename Family, typename Scoring, typename Ctx>
Result<Family> MakeThreshold(std::span<const PostingClause> terms,
                             std::span<const QueryBuilder::ptr> filters,
                             const SubReader& segment, uint32_t min_match,
                             const Scoring& scoring, const Ctx& ctx,
                             score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest,
                    [&](const QueryBuilder& child) {
                      return PlanFillOf<Family>(child, ctx, scoring);
                    })) {
    return {};
  }
  return MakeWindowThreshold<Family>(terms, doc, rest, min_match, scoring, ctx,
                                     absorbed);
}

}  // namespace irs::detail::builder
