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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/sparse_conjunction_docs.hpp"

namespace irs::fill {
namespace {

template<typename Head, typename Tail>
Node::ptr MakeNode(auto&& head, auto&& tail) {
  using Node = lead::SparseConjunctionDocs<Head, Tail>;
  return memory::make_managed<ByWalkDocs<Node>>(
    std::piecewise_construct, std::forward<decltype(head)>(head),
    std::forward<decltype(tail)>(tail));
}

}  // namespace

Node::ptr MakeSparseConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment) {
  return BuildConjunction<Node::ptr>(
    terms, filters, nullptr, segment, 0,
    []<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      return MakeNode<Head, Tail>(std::forward<decltype(head)>(head),
                                  std::forward<decltype(tail)>(tail));
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
      return MakeNode<Head, probe::Erased>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)));
    });
}

}  // namespace irs::fill
