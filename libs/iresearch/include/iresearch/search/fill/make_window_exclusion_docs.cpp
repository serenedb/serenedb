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
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/window_disjunction.hpp"
#include "iresearch/search/fill/window_docs.hpp"

namespace irs::fill {
namespace {

template<typename Init>
Node::ptr MakeDriven(size_t size, Init&& init,
                     std::span<const search::PostingClause> exclude_terms,
                     std::span<const QueryBuilder::ptr> exclude_filters,
                     const SubReader& segment, uint64_t candidates) {
  return search::BuildExcludeSide<Node::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      using Lead = WindowDisjunctionDocs<SetLeaves<Erased>>;
      using Excludes = ProbedAndNot<Exclude>;
      using Node = WindowDocs<Lead, utils::Empty, Excludes>;
      return memory::make_managed<Impl<Node>>(
        std::piecewise_construct,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward_as_tuple(size, init)),
        std::forward_as_tuple(),
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)));
    });
}

}  // namespace

Node::ptr MakeWindowExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    if (search::HeadIsTerm(terms, filters)) {
      return {};
    }
    auto node = filters.front()->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return {};
    }
    return MakeDriven(
      1, [&](Erased& leaf, size_t) { leaf = Erased{std::move(node)}; },
      exclude_terms, exclude_filters, segment, candidates);
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms,
                                static_cast<doc_id_t>(segment.docs_count()))) {
    return {};
  }
  return search::BuildExcludeSide<Node::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeWindowOfTerms<Node::ptr, ProbedAndNot<Exclude>>(
        terms, nullptr, *doc,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)));
    });
}

}  // namespace irs::fill
