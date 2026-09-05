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
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/sparse_boost_scored.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/scored/sparse_conjunction.hpp"

namespace irs::scored {

Root::ptr MakeSparseConjunction(const BooleanQuery& query,
                                const SubReader& segment, const Context& ctx,
                                ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = !should.empty() || !should_filters.empty();
  SDB_ASSERT(!no_must || optional);
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};
  const auto child_ctx = ScoredOf(ctx);
  const auto clause = probe::ScoredClauseOf(segment, child_ctx, recipe);
  const auto candidates =
    search::IncludeCandidates(must, must_filters, segment);

  const auto conjunction = [&]<typename Make>(Make&& make) -> Root::ptr {
    return search::BuildScoredConjunction<Root::ptr>(
      must, must_filters, nullptr, nullptr, kNoBoost, segment, recipe, clause,
      [&](const QueryBuilder& child) -> lead::Node::ptr {
        return child.PlanLead(ScoredOf(ctx));
      },
      std::forward<Make>(make));
  };

  if (optional && min_should_match == 0) {
    return search::BuildOptionalLeaves<Root::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Leaf>(auto&&... args) -> Root::ptr {
        const auto build = [&]<typename Held>(auto&& held) -> Root::ptr {
          if (no_must) {
            auto all = lead::MakeAllScored(
              segment,
              search::AllDocsScore(segment, ScoreArgs{.scorer = &ctx.scorer,
                                                      .fetcher = &ctx.fetcher,
                                                      .boost = kNoBoost}));
            if (!all) {
              return {};
            }
            return MakeShape<SparseConjunction, lead::Erased, Held>(
              ctx, std::piecewise_construct, ctx.fetcher, absorbed, merge,
              std::forward_as_tuple(lead::Erased{std::move(all)}),
              std::forward<decltype(held)>(held));
          }
          SDB_ASSERT(!no_must);
          return conjunction([&]<typename Head, typename Tail>(
                               auto&& head, auto&& tail) -> Root::ptr {
            using Both = probe::BothLeaves<Tail, Held>;
            return MakeShape<SparseConjunction, Head, Both>(
              ctx, std::piecewise_construct, ctx.fetcher, absorbed, merge,
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(std::piecewise_construct,
                                    std::forward<decltype(tail)>(tail),
                                    std::forward<decltype(held)>(held)));
          });
        };

        return build.template operator()<probe::SparseBoostScored<Leaf>>(
          std::forward_as_tuple(std::forward<decltype(args)>(args)..., merge));
      });
  }

  SDB_ASSERT(!no_must);

  probe::Node::ptr held;
  if (optional) {
    held = probe::MakeRequiredScored(
      {}, {}, search::Terms::Mixed, should, should_filters, should_uniformity,
      min_should_match, segment, recipe, merge, candidates, child_ctx);
    if (!held) {
      return {};
    }
  }

  if (must.size() + must_filters.size() == 1 && !held && absorbed == 0) {
    Root::ptr only;
    query.VisitHead(
      Occur::Must,
      [&](const PostingClause& posting) {
        only = posting.state.cookie.docs_count == 1
                 ? MakeSinglePosting(posting, segment, ctx)
                 : MakePosting(posting, segment, ctx);
        return true;
      },
      [&](const QueryBuilder& child) {
        only = child.PlanScored(ctx);
        return true;
      });
    return only;
  }

  return conjunction([&]<typename Head, typename Tail>(
                       auto&& head, auto&& tail) -> Root::ptr {
    if (!held) {
      return MakeShape<SparseConjunction, Head, Tail>(
        ctx, std::piecewise_construct, ctx.fetcher, absorbed, merge,
        std::forward<decltype(head)>(head), std::forward<decltype(tail)>(tail));
    }
    using Both = probe::BothLeaves<Tail, probe::Erased>;
    return MakeShape<SparseConjunction, Head, Both>(
      ctx, std::piecewise_construct, ctx.fetcher, absorbed, merge,
      std::forward<decltype(head)>(head),
      std::forward_as_tuple(std::piecewise_construct,
                            std::forward<decltype(tail)>(tail),
                            std::forward_as_tuple(std::move(held))));
  });
}

}  // namespace irs::scored
