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
#include "iresearch/search/count/boolean_sparse.hpp"
#include "iresearch/search/count/make_boolean.hpp"

namespace irs::count {
namespace {

template<typename Lead, typename Probes, typename Excludes, typename... Args>
Root::ptr MakeSparse(const Context& ctx, Args&&... args) {
  return MakeShape<BooleanSparse, Lead, Probes, Excludes>(
    ctx, std::piecewise_construct, std::forward<Args>(args)...);
}

}  // namespace

Root::ptr MakeSparseConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx) {
  return BuildConjunction<Root::ptr>(
    terms, filters, nullptr, segment, 0,
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Root::ptr {
      return MakeSparse<Head, Tail, utils::Empty>(
        ctx, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple());
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
      return MakeSparse<Head, probe::Erased, utils::Empty>(
        ctx, std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(other)), std::forward_as_tuple());
    });
}

Root::ptr MakeSparseExclusionOf(
  LeadNode::ptr include, std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  SDB_ASSERT(include);
  return search::BuildExcludeSide<Root::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
      return MakeSparse<lead::Erased, utils::Empty, Exclude>(
        ctx, std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward<decltype(exclude)>(exclude));
    });
}

Root::ptr MakeSparseExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    if (auto folded = search::MakeConjunctionBitset<LeadNode::ptr>(
          terms, filters, nullptr, segment, nullptr)) {
      return MakeSparseExclusionOf(std::move(folded), exclude_terms,
                                   exclude_filters, segment, candidates, ctx);
    }
    return BuildConjunction<Root::ptr>(
      terms, filters, nullptr, segment, 0,
      [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Root::ptr {
        return search::BuildExcludeSide<Root::ptr>(
          exclude_terms, exclude_filters, nullptr, segment, candidates,
          [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
            return MakeSparse<Head, Tail, Exclude>(
              ctx, std::forward<decltype(head)>(head),
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
    return MakeSparseExclusionOf(std::move(lead), exclude_terms,
                                 exclude_filters, segment, candidates, ctx);
  }
  const auto& own = *terms.front().state.reader;
  const auto& meta = terms.front().state.cookie;
  return ResolveInput(*search::DocOf(own), [&]<typename Input> -> Root::ptr {
    using Include = PostingLead<Input>;
    return search::BuildExcludeSideOf<Root::ptr, Input>(
      exclude_terms, exclude_filters, nullptr, segment, candidates,
      [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
        return MakeSparse<Include, utils::Empty, Exclude>(
          ctx,
          std::forward_as_tuple(meta, *search::DocOf(own),
                                search::LayoutOf(own), search::BoundsOf(own)),
          std::forward_as_tuple(), std::forward<decltype(exclude)>(exclude));
      });
  });
}

}  // namespace irs::count
