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
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/sparse_conjunction.hpp"

namespace irs::count {
namespace {

template<typename Head, typename Tail>
Root::ptr MakeNode(auto&& head, auto&& tail, const Context& ctx) {
  return MakeShape<SparseConjunction, Head, Tail>(
    ctx, std::piecewise_construct, std::forward<decltype(head)>(head),
    std::forward<decltype(tail)>(tail));
}

}  // namespace

Root::ptr MakeSparseConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx) {
  return BuildConjunction<Root::ptr>(
    terms, filters, nullptr, segment, 0,
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Root::ptr {
      return MakeNode<Head, Tail>(std::forward<decltype(head)>(head),
                                  std::forward<decltype(tail)>(tail), ctx);
    });
}

Root::ptr MakeSparseConjunctionWith(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other, const Context& ctx) {
  SDB_ASSERT(other);
  return search::BuildRequiredLeadOf<Root::ptr>(
    terms, filters, nullptr, segment,
    [&]<typename Head>(auto&& head) -> Root::ptr {
      return MakeNode<Head, probe::Erased>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)), ctx);
    });
}

}  // namespace irs::count
