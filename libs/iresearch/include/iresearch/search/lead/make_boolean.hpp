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
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/scored_node_builder.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/boolean_window.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/scorers/score_policy.hpp"

namespace irs::lead {

struct ScoredApi {
  using Result = Node::ptr;
  using Context = detail::ScoredCtx;
  using SparseApi = ScoredApi;

  static constexpr bool kLazyGroups = true;
  static constexpr bool kSingleClause = true;
  static constexpr bool kWrapsMerge = false;

  static score_t Base(score_t) noexcept { return 0; }

  static ScoreMergeType Inner(ScoreMergeType merge) noexcept { return merge; }

  template<typename Optional, typename OptionalArgs>
  static Result MakeWindow(detail::Scored score, OptionalArgs&& optional) {
    using Window = BooleanWindow<utils::Empty, utils::Empty, Optional,
                                 utils::Empty, detail::Scored>;
    return memory::make_managed<Impl<Window>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(), std::forward<OptionalArgs>(optional),
      std::forward_as_tuple(), score);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs, typename Score>
  static Result MakeSparse(const Context&, ScoreMergeType, LeadArgs&& lead,
                           ProbesArgs&& probes, OptionalArgs&& optional,
                           ExcludesArgs&& excludes, Score score) {
    using Sparse = BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
    return memory::make_managed<Impl<Sparse>>(
      std::piecewise_construct, std::forward<LeadArgs>(lead),
      std::forward<ProbesArgs>(probes), std::forward<OptionalArgs>(optional),
      std::forward<ExcludesArgs>(excludes), score);
  }

  static Result MakeAll(const SubReader& segment, const Context&,
                        ScoreMergeType, score_t absorbed) {
    return MakeAllScored(segment, absorbed);
  }

  static Result MakeRequiredWith(
    std::span<const detail::PostingClause> must,
    std::span<const QueryBuilder::ptr> must_filters,
    std::span<const detail::PostingClause> should,
    std::span<const QueryBuilder::ptr> should_filters, detail::Terms uniformity,
    uint32_t min_match, const SubReader& segment, const Context& ctx,
    ScoreMergeType merge, score_t absorbed) {
    return detail::builder::MakeNodeConjunctionWith<ScoredApi>(
      must, must_filters, should, should_filters, uniformity, min_match,
      segment, ctx, merge, absorbed);
  }
};

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return detail::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = detail::PostingFill<Input>;
    using Optional = detail::OrGroup<fill::SetLeaves<Leaf>>;
    using Node =
      BooleanWindow<utils::Empty, utils::Empty, Optional, utils::Empty>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(
        terms.size(),
        [&](Leaf& leaf, size_t i) {
          const auto& own = detail::FieldOf(terms[i], field);
          const auto& meta = detail::CookieOf(terms[i]);
          SDB_ASSERT(meta.docs_count != 0);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && detail::BoundsOf(own),
                       meta.docs_count != 1 && detail::FreqOf(own));
        }),
      std::forward_as_tuple());
  });
}

template<typename Term>
Node::ptr MakeDisjunctionOfTermsDocs(std::span<const Term> terms,
                                     const TermReader* field,
                                     const IndexInput& doc,
                                     doc_id_t docs_count) {
  SDB_ASSERT(terms.size() > 1);
  if (detail::TakeBitset<Node::ptr>(terms, doc, docs_count)) {
    return detail::MakeBitsetOf<Node::ptr>(terms, field, doc, docs_count,
                                           nullptr);
  }
  return MakeWindowDisjunctionOfTermsDocs<Term>(terms, field, doc);
}

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsScored(
  std::span<const Term> terms, const TermReader* field, const Scorer* scorer,
  score_t boost, const IndexInput& doc, detail::Terms uniformity,
  const SubReader& segment, const detail::ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  const detail::ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  std::vector<fill::Node::ptr> rest;
  return detail::builder::MakeNodeDisjunctionWindow<ScoredApi, Term>(
    terms, field, scorer, boost, &doc, rest, uniformity, recipe, merge,
    absorbed);
}

}  // namespace irs::lead
