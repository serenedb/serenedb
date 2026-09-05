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
#include "iresearch/search/common/conjunction_of.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/sparse_exclusion.hpp"

namespace irs::count {
namespace {

template<typename Input, typename Include, typename IncludeArgs>
Root::ptr MakeWalked(IncludeArgs&& include,
                     std::span<const search::PostingClause> exclude_terms,
                     std::span<const QueryBuilder::ptr> exclude_filters,
                     const SubReader& segment, uint64_t candidates,
                     const Context& ctx) {
  return search::BuildExcludeSideOf<Root::ptr, Input>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
      return MakeShape<SparseExclusion, Include, Exclude>(
        ctx, std::piecewise_construct, std::forward<IncludeArgs>(include),
        std::forward<decltype(exclude)>(exclude));
    });
}

}  // namespace

Root::ptr MakeSparseExclusionOf(
  LeadNode::ptr include, std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  SDB_ASSERT(include);
  return MakeWalked<void, lead::Erased>(
    std::forward_as_tuple(std::move(include)), exclude_terms, exclude_filters,
    segment, candidates, ctx);
}

Root::ptr MakeSparseExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    auto lead = search::BuildConjunctionOf<LeadNode::ptr>(terms, filters,
                                                          nullptr, segment, 0);
    if (!lead) {
      return {};
    }
    return MakeSparseExclusionOf(std::move(lead), exclude_terms,
                                 exclude_filters, segment, candidates, ctx);
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
    return MakeWalked<Input, Include>(
      std::forward_as_tuple(meta, *search::DocOf(own), search::LayoutOf(own),
                            search::BoundsOf(own)),
      exclude_terms, exclude_filters, segment, candidates, ctx);
  });
}

}  // namespace irs::count
