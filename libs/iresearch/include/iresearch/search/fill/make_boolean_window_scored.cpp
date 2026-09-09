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

#include <tuple>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/fill/make_boolean.hpp"

namespace irs::fill {
namespace {

template<template<typename> class Group, typename Set, typename... Args>
Node::ptr MakeThresholdWindowScored(uint32_t min_match, search::Scored score,
                                    Args&&... args) {
  using Node = BooleanWindow<utils::Empty, utils::Empty, Group<Set>,
                             utils::Empty, search::Scored>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward_as_tuple(), std::forward_as_tuple(),
    std::forward_as_tuple(std::piecewise_construct,
                          std::forward_as_tuple(std::forward<Args>(args)...),
                          min_match, score.absorbed),
    std::forward_as_tuple(), score);
}

}  // namespace

Node::ptr MakeWindowThresholdScored(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  std::vector<Node::ptr>& rest, search::Terms uniformity,
  const ScoreRecipe& recipe, ScoreMergeType merge, uint32_t min_match,
  score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + rest.size() >= min_match);
  const search::Scored score{merge, absorbed};
  if (min_match > search::kBitplaneMaxMatch && rest.empty() &&
      uniformity != search::Terms::Mixed) {
    auto counted = search::ResolveCountScored<Node::ptr>(
      *doc, uniformity >= search::Terms::Scored, merge,
      [&]<typename Leaf, typename Plain> -> Node::ptr {
        return search::BuildScoredTerms<Node::ptr, Leaf, Plain>(
          terms, nullptr, nullptr, kNoBoost, doc, recipe,
          [&]<typename Set>(auto&&... args) -> Node::ptr {
            return MakeThresholdWindowScored<search::TallyGroup, Set>(
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
      return MakeThresholdWindowScored<search::ThresholdGroup, Set>(
        min_match, score, std::forward<decltype(args)>(args)...);
    });
}

}  // namespace irs::fill
