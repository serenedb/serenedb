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
#include "iresearch/search/common/boolean_bitset.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/make_boolean.hpp"
#include "iresearch/search/probe/impl.hpp"

namespace irs::lead {
namespace {

template<typename Lead, typename Probes, typename Excludes, typename LeadArgs,
         typename ProbesArgs, typename ExcludesArgs>
Node::ptr MakeSparse(LeadArgs&& lead, ProbesArgs&& probes,
                     ExcludesArgs&& excludes) {
  using Node = BooleanSparse<Lead, Probes, utils::Empty, Excludes>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward<LeadArgs>(lead),
    std::forward<ProbesArgs>(probes), std::forward_as_tuple(),
    std::forward<ExcludesArgs>(excludes));
}

}  // namespace

Node::ptr MakeSparseConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment) {
  return BuildConjunction<Node::ptr>(
    terms, filters, nullptr, segment, 0,
    []<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      return MakeSparse<Head, Tail, utils::Empty>(
        std::forward<decltype(head)>(head), std::forward<decltype(tail)>(tail),
        std::forward_as_tuple());
    });
}

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters, const SubReader& segment,
  ProbeNode::ptr other) {
  SDB_ASSERT(other);
  return search::BuildRequiredLeadOf<Node::ptr>(
    must, must_filters, nullptr, segment,
    [&]<typename Head>(auto&& head) -> Node::ptr {
      return MakeSparse<Head, probe::Erased, utils::Empty>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)), std::forward_as_tuple());
    });
}

Node::ptr MakeSparseExclusionOfDocs(
  Node::ptr include, std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(include);
  return search::BuildExcludeSide<Node::ptr>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeSparse<Erased, utils::Empty, Exclude>(
        std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude));
    });
}

Node::ptr MakeSparseExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  SDB_ASSERT(!must.empty() || !must_filters.empty());
  if (must.size() + must_filters.size() > 1) {
    if (auto folded = search::MakeConjunctionBitset<Node::ptr>(
          must, must_filters, nullptr, segment, nullptr)) {
      return MakeSparseExclusionOfDocs(std::move(folded), excludes,
                                       exclude_filters, segment, candidates);
    }
    return BuildConjunction<Node::ptr>(
      must, must_filters, nullptr, segment, 0,
      [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
        return search::BuildExcludeSide<Node::ptr>(
          excludes, exclude_filters, nullptr, segment, candidates,
          [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
            return MakeSparse<Head, Tail, Exclude>(
              std::forward<decltype(head)>(head),
              std::forward<decltype(tail)>(tail),
              std::forward<decltype(exclude)>(exclude));
          });
      });
  }
  if (search::HeadIsTerm(must, must_filters) &&
      must.front().state.cookie.docs_count != 1) {
    const auto& own = *must.front().state.reader;
    const auto& meta = must.front().state.cookie;
    return ResolveInput(*search::DocOf(own), [&]<typename Input> -> Node::ptr {
      using Include = PostingLead<Input>;
      return search::BuildExcludeSideOf<Node::ptr, Input>(
        excludes, exclude_filters, nullptr, segment, candidates,
        [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
          return MakeSparse<Include, utils::Empty, Exclude>(
            std::forward_as_tuple(meta, *search::DocOf(own),
                                  search::LayoutOf(own), search::BoundsOf(own)),
            std::forward_as_tuple(), std::forward<decltype(exclude)>(exclude));
        });
    });
  }
  auto include = search::HeadIsTerm(must, must_filters)
                   ? LeadOf(must.front(), nullptr, segment)
                   : must_filters.front()->PlanLead({});
  if (!include) {
    return {};
  }
  return MakeSparseExclusionOfDocs(std::move(include), excludes,
                                   exclude_filters, segment, candidates);
}

}  // namespace irs::lead
