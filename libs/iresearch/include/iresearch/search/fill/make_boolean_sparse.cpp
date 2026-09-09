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

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/fill/make_boolean.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/probe/impl.hpp"

namespace irs::fill {
namespace {

template<typename Lead, typename Probes, typename Excludes, typename LeadArgs,
         typename ProbesArgs, typename ExcludesArgs>
Node::ptr MakeSparse(LeadArgs&& lead, ProbesArgs&& probes,
                     ExcludesArgs&& excludes) {
  using Node = lead::BooleanSparse<Lead, Probes, utils::Empty, Excludes>;
  return memory::make_managed<ByWalkDocs<Node>>(
    std::piecewise_construct, std::forward<LeadArgs>(lead),
    std::forward<ProbesArgs>(probes), std::forward_as_tuple(),
    std::forward<ExcludesArgs>(excludes));
}

}  // namespace

Node::ptr MakeSparseConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment) {
  return BuildConjunction<Node::ptr>(
    terms, filters, nullptr, segment, 0,
    []<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      return MakeSparse<Head, Tail, utils::Empty>(
        std::forward<decltype(head)>(head), std::forward<decltype(tail)>(tail),
        std::forward_as_tuple());
    });
}

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other) {
  SDB_ASSERT(other);
  return search::BuildRequiredLeadOf<Node::ptr>(
    terms, filters, nullptr, segment,
    [&]<typename Head>(auto&& head) -> Node::ptr {
      return MakeSparse<Head, probe::Erased, utils::Empty>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)), std::forward_as_tuple());
    });
}

Node::ptr MakeSparseExclusionOfDocs(
  LeadNode::ptr include, std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(include);
  return search::BuildExcludeSide<Node::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeSparse<lead::Erased, utils::Empty, Exclude>(
        std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude));
    });
}

Node::ptr MakeSparseExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    if (auto folded = search::MakeConjunctionBitset<LeadNode::ptr>(
          terms, filters, nullptr, segment, nullptr)) {
      return MakeSparseExclusionOfDocs(std::move(folded), exclude_terms,
                                       exclude_filters, segment, candidates);
    }
    return BuildConjunction<Node::ptr>(
      terms, filters, nullptr, segment, 0,
      [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
        return search::BuildExcludeSide<Node::ptr>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
            return MakeSparse<Head, Tail, Exclude>(
              std::forward<decltype(head)>(head),
              std::forward<decltype(tail)>(tail),
              std::forward<decltype(exclude)>(exclude));
          });
      });
  }
  if (!search::HeadIsTerm(terms, filters)) {
    auto lead = filters.front()->PlanLead({});
    if (!lead) {
      return {};
    }
    return MakeSparseExclusionOfDocs(std::move(lead), exclude_terms,
                                     exclude_filters, segment, candidates);
  }
  const auto& own = *terms.front().state.reader;
  const auto& meta = terms.front().state.cookie;
  return ResolveInput(*search::DocOf(own), [&]<typename Input> -> Node::ptr {
    using Include = PostingLead<Input>;
    return search::BuildExcludeSideOf<Node::ptr, Input>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
        return MakeSparse<Include, utils::Empty, Exclude>(
          std::forward_as_tuple(meta, *search::DocOf(own),
                                search::LayoutOf(own), search::BoundsOf(own)),
          std::forward_as_tuple(), std::forward<decltype(exclude)>(exclude));
      });
  });
}

}  // namespace irs::fill
