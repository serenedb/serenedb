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

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_bitset.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/boolean_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_count.hpp"
#include "iresearch/search/detail/posting_fill.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::detail::builder {

template<typename Api>
using Result = typename Api::Result;

template<typename Api>
using Context = typename Api::Context;

template<typename Api>
Result<Api> MakeBitset(const BooleanGroups& groups, const SubReader& segment,
                       const Context<Api>& ctx) {
  return MakeBooleanBitset<Result<Api>>(groups, segment, Api::BitsetTable(ctx));
}

template<typename Api, typename Excludes, typename ExcludesArgs>
Result<Api> MakeWindowOfTerms(std::span<const PostingClause> terms,
                              const IndexInput& doc, ExcludesArgs&& excludes,
                              const Context<Api>& ctx) {
  SDB_ASSERT(terms.size() >= 2);
  return ResolveInput(doc, [&]<typename Input> -> Result<Api> {
    using Leaf = PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& own = FieldOf(terms.front(), nullptr);
    const auto& front = CookieOf(terms.front());
    return Api::template MakeWindow<Leaf, Others, utils::Empty, Excludes>(
      ctx,
      std::forward_as_tuple(front, doc, front.docs_count != 1 && BoundsOf(own),
                            front.docs_count != 1 && FreqOf(own)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = FieldOf(terms[i + 1], nullptr);
          const auto& meta = CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && BoundsOf(other),
                       meta.docs_count != 1 && FreqOf(other));
        }),
      std::forward_as_tuple(), std::forward<ExcludesArgs>(excludes));
  });
}

template<typename Api>
Result<Api> MakeWindowOfNodes(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment,
                              const Context<Api>& ctx) {
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
  return Api::template MakeWindow<fill::Erased, Others, utils::Empty,
                                  utils::Empty>(
    ctx, std::forward_as_tuple(std::move(nodes.front())),
    std::forward_as_tuple(nodes.size() - 1,
                          [&](fill::Erased& leaf, size_t i) {
                            leaf = fill::Erased{std::move(nodes[i + 1])};
                          }),
    std::forward_as_tuple(), std::forward_as_tuple());
}

template<typename Api>
Result<Api> MakeWindowDisjunction(std::span<const PostingClause> terms,
                                  const IndexInput* doc,
                                  std::vector<FillNode::ptr>& rest,
                                  const Context<Api>& ctx) {
  return BuildDense<Result<Api>>(
    terms, nullptr, doc, rest,
    [&]<typename Set>(auto&&... args) -> Result<Api> {
      return Api::template MakeWindow<utils::Empty, utils::Empty, OrGroup<Set>,
                                      utils::Empty>(
        ctx, std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeWindowConjunction(std::span<const PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment,
                                  const Context<Api>& ctx) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if constexpr (Api::kWindowNodes) {
    if (!filters.empty()) {
      if (HeadEstimate(terms, filters) <
          docs_count / kDensityThresholdInverse) {
        return {};
      }
      return MakeWindowOfNodes<Api>(terms, filters, segment, ctx);
    }
  }
  const IndexInput* doc = nullptr;
  if (!WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!DenseConjunction(terms, docs_count)) {
    return {};
  }
  return MakeWindowOfTerms<Api, utils::Empty>(terms, *doc,
                                              std::forward_as_tuple(), ctx);
}

template<typename Api>
Result<Api> MakeWindowNegation(
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context<Api>& ctx) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  std::vector<FillNode::ptr> nodes;
  if (!CollectFills(exclude_terms, exclude_filters, nullptr, segment, nodes)) {
    return {};
  }
  using Excludes = fill::FilledAndNot<fill::SetLeaves<fill::Erased>>;
  return Api::template MakeWindow<fill::AllDocs, utils::Empty, utils::Empty,
                                  Excludes>(
    ctx, std::forward_as_tuple(segment), std::forward_as_tuple(),
    std::forward_as_tuple(),
    std::forward_as_tuple(
      std::piecewise_construct,
      std::forward_as_tuple(nodes.size(), [&](fill::Erased& leaf, size_t i) {
        leaf = fill::Erased{std::move(nodes[i])};
      })));
}

template<typename Api>
Result<Api> MakeWindowExclusion(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context<Api>& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (terms.size() + filters.size() == 1) {
    if (HeadIsTerm(terms, filters)) {
      const ExcludeCosts<PostingClause> costs{
        exclude_terms, exclude_filters, candidates,
        candidates,    docs_count,      ExcludeUse::PerDoc};
      if (!costs.TakesWindowLead(SegmentDoc(segment) != nullptr,
                                 Api::kWindowLeadDrains, Api::kSparseLeadCost,
                                 Api::kWindowLeadRefills, candidates)) {
        return {};
      }
      const auto& own = FieldOf(terms.front(), nullptr);
      const auto* const doc = DocOf(own);
      if (doc == nullptr) {
        return {};
      }
      const auto& front = CookieOf(terms.front());
      return ResolveInput(*doc, [&]<typename Input> -> Result<Api> {
        return BuildWindowExcludes<Result<Api>>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Excludes>(auto&& excludes) -> Result<Api> {
            return Api::template MakeWindow<PostingFill<Input>, utils::Empty,
                                            utils::Empty, Excludes>(
              ctx,
              std::forward_as_tuple(front, *doc,
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
    return BuildWindowExcludes<Result<Api>>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Excludes>(auto&& excludes) -> Result<Api> {
        return Api::template MakeWindow<fill::Erased, utils::Empty,
                                        utils::Empty, Excludes>(
          ctx, std::forward_as_tuple(std::move(node)), std::forward_as_tuple(),
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
  return BuildWindowExcludes<Result<Api>>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Excludes>(auto&& excludes) -> Result<Api> {
      return MakeWindowOfTerms<Api, Excludes>(
        terms, *doc, std::forward<decltype(excludes)>(excludes), ctx);
    });
}

template<typename Api>
Result<Api> MakeWindowThreshold(std::span<const PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                uint32_t min_match, const Context<Api>& ctx) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + rest.size() >= min_match);
  if (min_match > kBitplaneMaxMatch && rest.empty()) {
    const auto& in = *doc;
    return ResolveInput(in, [&]<typename Input> -> Result<Api> {
      using Leaf = PostingCount<Input>;
      const auto init = [&](Leaf& leaf, size_t i) {
        const auto& own = FieldOf(terms[i], nullptr);
        const auto& meta = CookieOf(terms[i]);
        leaf.Prepare(meta, in, meta.docs_count != 1 && BoundsOf(own),
                     meta.docs_count != 1 && FreqOf(own));
      };
      return Api::template MakeWindow<utils::Empty, utils::Empty,
                                      TallyGroup<fill::SetLeaves<Leaf>>,
                                      utils::Empty>(
        ctx, std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward_as_tuple(terms.size(), init),
                              min_match, score_t{0}),
        std::forward_as_tuple());
    });
  }
  return BuildDense<Result<Api>>(
    terms, nullptr, doc, rest,
    [&]<typename Set>(auto&&... args) -> Result<Api> {
      return Api::template MakeWindow<utils::Empty, utils::Empty,
                                      ThresholdGroup<Set>, utils::Empty>(
        ctx, std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward_as_tuple(
          std::piecewise_construct,
          std::forward_as_tuple(std::forward<decltype(args)>(args)...),
          min_match, score_t{0}),
        std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeSparseConjunction(std::span<const PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment,
                                  const Context<Api>& ctx) {
  return BuildConjunction<Result<Api>>(
    terms, filters, nullptr, segment, 0,
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Result<Api> {
      return Api::template MakeSparse<Head, Tail, utils::Empty>(
        ctx, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeSparseConjunctionWith(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other, const Context<Api>& ctx) {
  SDB_ASSERT(other);
  return BuildRequiredLeadOf<Result<Api>>(
    terms, filters, nullptr, segment,
    [&]<typename Head>(auto&& head) -> Result<Api> {
      return Api::template MakeSparse<Head, probe::Erased, utils::Empty>(
        ctx, std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)), std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeSparseExclusionOf(
  LeadNode::ptr include, std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context<Api>& ctx) {
  SDB_ASSERT(include);
  return BuildExcludeSide<Result<Api>>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Result<Api> {
      return Api::template MakeSparse<lead::Erased, utils::Empty, Exclude>(
        ctx, std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude));
    });
}

template<typename Api>
Result<Api> MakeSparseNegation(
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context<Api>& ctx) {
  auto driven = lead::MakeAllDocs(segment);
  if (!driven) {
    return {};
  }
  return MakeSparseExclusionOf<Api>(std::move(driven), exclude_terms,
                                    exclude_filters, segment, candidates, ctx);
}

template<typename Api>
Result<Api> MakeSparseExclusion(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context<Api>& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    if (auto folded = MakeConjunctionBitset<LeadNode::ptr>(
          terms, filters, nullptr, segment, nullptr)) {
      return MakeSparseExclusionOf<Api>(std::move(folded), exclude_terms,
                                        exclude_filters, segment, candidates,
                                        ctx);
    }
    return BuildConjunction<Result<Api>>(
      terms, filters, nullptr, segment, 0,
      [&]<typename Head, typename Tail>(auto&& head,
                                        auto&& tail) -> Result<Api> {
        return BuildExcludeSide<Result<Api>>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Exclude>(auto&& exclude) -> Result<Api> {
            return Api::template MakeSparse<Head, Tail, Exclude>(
              ctx, std::forward<decltype(head)>(head),
              std::forward<decltype(tail)>(tail),
              std::forward<decltype(exclude)>(exclude));
          });
      });
  }
  if (!HeadIsTerm(terms, filters)) {
    auto lead = filters.front()->PlanLead({});
    if (!lead) {
      return {};
    }
    return MakeSparseExclusionOf<Api>(std::move(lead), exclude_terms,
                                      exclude_filters, segment, candidates,
                                      ctx);
  }
  const auto& own = *terms.front().state.reader;
  const auto& meta = terms.front().state.cookie;
  return ResolveInput(*DocOf(own), [&]<typename Input> -> Result<Api> {
    using Include = PostingLead<Input>;
    return BuildExcludeSideOf<Result<Api>, Input>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Result<Api> {
        return Api::template MakeSparse<Include, utils::Empty, Exclude>(
          ctx,
          std::forward_as_tuple(meta, *DocOf(own), LayoutOf(own),
                                BoundsOf(own)),
          std::forward_as_tuple(), std::forward<decltype(exclude)>(exclude));
      });
  });
}

template<typename Api>
Result<Api> MakeDisjunction(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, const Context<Api>& ctx) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (auto folded = MakeBitset<Api>(
        {.should = terms, .should_filters = filters, .should_fills = &rest},
        segment, ctx)) {
    return folded;
  }
  return MakeWindowDisjunction<Api>(terms, doc, rest, ctx);
}

template<typename Api>
Result<Api> MakeConjunction(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, const Context<Api>& ctx) {
  SDB_ASSERT(terms.size() + filters.size() != 0);
  if (terms.size() + filters.size() == 1) {
    return HeadIsTerm(terms, filters)
             ? Api::MakeTerm(terms.front(), segment, ctx)
             : Api::PlanChild(*filters.front(), ctx);
  }
  if (auto windowed =
        MakeWindowConjunction<Api>(terms, filters, segment, ctx)) {
    return windowed;
  }
  if (!filters.empty()) {
    if (auto folded = MakeBitset<Api>({.must = terms, .must_filters = filters},
                                      segment, ctx)) {
      return folded;
    }
  }
  return MakeSparseConjunction<Api>(terms, filters, segment, ctx);
}

template<typename Api>
Result<Api> MakeThreshold(std::span<const PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, uint32_t min_match,
                          const Context<Api>& ctx) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  SDB_ASSERT(min_match != terms.size() + filters.size());
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  return MakeWindowThreshold<Api>(terms, doc, rest, min_match, ctx);
}

template<typename Api>
Result<Api> MakeRequired(std::span<const PostingClause> must_terms,
                         std::span<const QueryBuilder::ptr> must_filters,
                         std::span<const PostingClause> should_terms,
                         std::span<const QueryBuilder::ptr> should_filters,
                         uint32_t min_match, const SubReader& segment,
                         const Context<Api>& ctx) {
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (min_match == 0) {
    if (no_must) {
      return Api::MakeAll(segment, ctx);
    }
    return MakeConjunction<Api>(must_terms, must_filters, segment, ctx);
  }
  if (no_must) {
    return min_match == 1
             ? MakeDisjunction<Api>(should_terms, should_filters, segment, ctx)
             : MakeThreshold<Api>(should_terms, should_filters, segment,
                                  min_match, ctx);
  }
  auto probe = probe::BuildOptionalProbe(
    should_terms, should_filters, min_match, segment,
    IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWith<Api>(must_terms, must_filters, segment,
                                        std::move(probe), ctx);
}

template<typename Api>
Result<Api> MakeRequired(const BooleanQuery& query, const Context<Api>& ctx) {
  return MakeRequired<Api>(query.Terms(Occur::Must), query.Queries(Occur::Must),
                           query.Terms(Occur::Should),
                           query.Queries(Occur::Should), query.MinShouldMatch(),
                           query.Segment(), ctx);
}

template<typename Api>
Result<Api> MakeExclusion(const BooleanQuery& query, const Context<Api>& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  auto candidates = IncludeCandidates(must_terms, must_filters, segment);
  const auto min_match = query.MinShouldMatch();
  if (min_match != 0) {
    const std::span should_terms = query.Terms(Occur::Should);
    const std::span should_filters = query.Queries(Occur::Should);
    if (must_terms.empty() && must_filters.empty()) {
      candidates = std::min(
        candidates,
        LeadCandidates(should_terms, should_filters, segment.docs_count()));
    }
    auto driven = lead::MakeRequiredDocs(must_terms, must_filters, should_terms,
                                         should_filters, min_match, segment);
    if (!driven) {
      return {};
    }
    return MakeSparseExclusionOf<Api>(std::move(driven), exclude_terms,
                                      exclude_filters, segment, candidates,
                                      ctx);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return Api::MakeNegation(exclude_terms, exclude_filters, segment,
                             candidates, ctx);
  }
  if (auto folded = MakeBitset<Api>({.must = must_terms,
                                     .must_filters = must_filters,
                                     .must_not = exclude_terms,
                                     .must_not_filters = exclude_filters},
                                    segment, ctx)) {
    return folded;
  }
  if (auto windowed =
        MakeWindowExclusion<Api>(must_terms, must_filters, exclude_terms,
                                 exclude_filters, segment, candidates, ctx)) {
    return windowed;
  }
  return MakeSparseExclusion<Api>(must_terms, must_filters, exclude_terms,
                                  exclude_filters, segment, candidates, ctx);
}

template<typename Api>
Result<Api> Make(const BooleanQuery& query, const Context<Api>& ctx) {
  if (query.Terms(Occur::MustNot).empty() &&
      query.Queries(Occur::MustNot).empty()) {
    return MakeRequired<Api>(query, ctx);
  }
  return MakeExclusion<Api>(query, ctx);
}

}  // namespace irs::detail::builder
