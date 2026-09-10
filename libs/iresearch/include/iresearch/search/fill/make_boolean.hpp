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
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/common/scored_node_builder.hpp"
#include "iresearch/search/fill/boolean_window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/fill/window_scored.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"

namespace irs::fill {

struct ScoredApi {
  using Result = Node::ptr;
  using Context = ScoredCtx;

  static constexpr bool kLazyGroups = false;
  static constexpr bool kSingleClause = false;
  static constexpr bool kWrapsMerge = true;

  static score_t Base(score_t absorbed) noexcept { return absorbed; }

  static ScoreMergeType Inner(ScoreMergeType) noexcept {
    return ScoreMergeType::Sum;
  }

  template<typename Optional, typename OptionalArgs>
  static Result MakeWindow(search::Scored score, OptionalArgs&& optional) {
    using Window = BooleanWindow<utils::Empty, utils::Empty, Optional,
                                 utils::Empty, search::Scored>;
    return memory::make_managed<Impl<Window>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(), std::forward<OptionalArgs>(optional),
      std::forward_as_tuple(), score);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs, typename Score>
  static Result MakeSparse(const Context& ctx, ScoreMergeType merge,
                           LeadArgs&& lead, ProbesArgs&& probes,
                           OptionalArgs&& optional, ExcludesArgs&& excludes,
                           Score score) {
    using Sparse = lead::BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
    return memory::make_managed<ByWalkScored<Sparse>>(
      merge, *ctx.fetcher, std::piecewise_construct,
      std::forward<LeadArgs>(lead), std::forward<ProbesArgs>(probes),
      std::forward<OptionalArgs>(optional),
      std::forward<ExcludesArgs>(excludes), score);
  }

  static Result MakeAll(const SubReader& segment, const Context&,
                        ScoreMergeType merge, score_t absorbed) {
    return MakeAllScored(segment, merge, absorbed);
  }

  static Result MakeRequiredWith(
    std::span<const search::PostingClause> must,
    std::span<const QueryBuilder::ptr> must_filters,
    std::span<const search::PostingClause> should,
    std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
    uint32_t min_match, const SubReader& segment, const Context& ctx,
    ScoreMergeType merge, score_t absorbed) {
    auto node = lead::MakeRequiredScored(must, must_filters, should,
                                         should_filters, uniformity, min_match,
                                         segment, ctx, merge, absorbed);
    if (!node) {
      return {};
    }
    return memory::make_managed<ByWalkScored<lead::Erased>>(
      merge, *ctx.fetcher, lead::Erased{std::move(node)});
  }

  static Result WrapMerge(ScoreMergeType merge, Result child) {
    return memory::make_managed<ByWindowScored<Erased>>(
      merge, Erased{std::move(child)});
  }
};

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return search::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingFill<Input>;
    using Optional = search::OrGroup<SetLeaves<Leaf>>;
    using Node =
      BooleanWindow<utils::Empty, utils::Empty, Optional, utils::Empty>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(
        terms.size(),
        [&](Leaf& leaf, size_t i) {
          const auto& own = search::FieldOf(terms[i], field);
          const auto& meta = search::CookieOf(terms[i]);
          SDB_ASSERT(meta.docs_count != 0);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                       meta.docs_count != 1 && search::FreqOf(own));
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
  if (search::TakeBitset<Node::ptr>(terms, doc, docs_count)) {
    return search::MakeBitsetOf<Node::ptr>(terms, field, doc, docs_count,
                                           nullptr);
  }
  return MakeWindowDisjunctionOfTermsDocs(terms, field, doc);
}

template<typename Term>
Node::ptr MakeWindowDisjunctionScored(
  std::span<const Term> terms, const TermReader* field, const Scorer* scorer,
  score_t boost, const IndexInput* doc, std::vector<Node::ptr>& rest,
  search::Terms uniformity, const ScoreRecipe& recipe, ScoreMergeType merge,
  score_t absorbed = 0) {
  return search::builder::MakeNodeDisjunctionWindow<ScoredApi, Term>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge,
    absorbed);
}

}  // namespace irs::fill
