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
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/make_boolean.hpp"

namespace irs::lead {
namespace {

template<typename Set>
using LazyThreshold = search::ThresholdGroup<Set, true>;
template<typename Set>
using LazyTally = search::TallyGroup<Set, true>;

template<template<typename> class Group, typename Set, typename... Args>
Node::ptr MakeThresholdWindow(uint32_t min_match, search::Scored score,
                              Args&&... args) {
  using Node = BooleanWindow<utils::Empty, utils::Empty, Group<Set>,
                             utils::Empty, search::Scored>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward_as_tuple(), std::forward_as_tuple(),
    std::forward_as_tuple(std::piecewise_construct,
                          std::forward_as_tuple(std::forward<Args>(args)...),
                          min_match, score_t{0}),
    std::forward_as_tuple(), score);
}

}  // namespace

Node::ptr MakeWindowThresholdScored(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    search::Terms uniformity,
                                    const SubReader& segment,
                                    const ScoredCtx& ctx, ScoreMergeType merge,
                                    uint32_t min_match, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  const auto plan = [&](const QueryBuilder& child) {
    return child.PlanFill(ctx, merge);
  };
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest, plan) ||
      terms.size() + rest.size() < min_match) {
    return {};
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const search::Scored score{merge, absorbed};
  if (min_match > search::kBitplaneMaxMatch && rest.empty() &&
      uniformity != search::Terms::Mixed) {
    auto counted = search::ResolveCountScored<Node::ptr>(
      *doc, uniformity >= search::Terms::Scored, merge,
      [&]<typename Leaf, typename Plain> -> Node::ptr {
        return search::BuildScoredTerms<Node::ptr, Leaf, Plain>(
          terms, nullptr, nullptr, kNoBoost, doc, recipe,
          [&]<typename Set>(auto&&... args) -> Node::ptr {
            return MakeThresholdWindow<LazyTally, Set>(
              min_match, score, std::forward<decltype(args)>(args)...);
          });
      });
    if (counted) {
      return counted;
    }
  }
  return search::BuildScoredWindow<Node::ptr>(
    terms, nullptr, nullptr, kNoBoost, doc, rest, uniformity, recipe, merge,
    [&]<typename Set>(auto&&... args) -> Node::ptr {
      return MakeThresholdWindow<LazyThreshold, Set>(
        min_match, score, std::forward<decltype(args)>(args)...);
    });
}

Node::ptr MakeWindowDisjunctionScored(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  const auto plan = [&](const QueryBuilder& child) {
    return child.PlanFill(ctx, merge);
  };
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest, plan)) {
    return {};
  }
  const auto make = [&]<typename Set>(auto&&... args) -> Node::ptr {
    using Node = BooleanWindow<utils::Empty, utils::Empty, search::OrGroup<Set>,
                               utils::Empty, search::Scored>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(std::forward<decltype(args)>(args)...),
      std::forward_as_tuple(), search::Scored{merge, absorbed});
  };
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  return search::BuildScoredWindow<Node::ptr>(terms, nullptr, nullptr, kNoBoost,
                                              doc, rest, uniformity, recipe,
                                              merge, make);
}

}  // namespace irs::lead
