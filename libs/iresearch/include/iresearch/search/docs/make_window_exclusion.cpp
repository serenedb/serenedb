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

#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/docs/complement.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/window.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/window_disjunction.hpp"

namespace irs::docs {
namespace {

template<typename Init>
Root::ptr MakeDriven(size_t size, Init&& init,
                     std::span<const search::PostingClause> exclude_terms,
                     std::span<const QueryBuilder::ptr> exclude_filters,
                     const SubReader& segment, uint64_t candidates,
                     const Context& ctx) {
  return search::BuildExcludeSide<Root::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
      using Lead = fill::WindowDisjunctionDocs<fill::SetLeaves<fill::Erased>>;
      using Excludes = fill::ProbedAndNot<Exclude>;
      return MakeShape<Window, Lead, utils::Empty, Excludes>(
        ctx, std::piecewise_construct,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward_as_tuple(size, init)),
        std::forward_as_tuple(),
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)));
    });
}

}  // namespace

Root::ptr MakeComplement(std::span<const search::PostingClause> exclude_terms,
                         std::span<const QueryBuilder::ptr> exclude_filters,
                         const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  std::vector<fill::Erased> leaves;
  leaves.reserve(exclude_terms.size() + exclude_filters.size());
  const auto take = [&](const search::PostingClause& posting,
                        const QueryBuilder* child) noexcept {
    auto node = FillOf(posting, child, segment);
    if (!node) {
      return false;
    }
    leaves.emplace_back(std::move(node));
    return true;
  };
  if (!search::VisitOrderedOf(
        exclude_terms, exclude_filters, false, 0,
        std::numeric_limits<size_t>::max(),
        [&](const search::PostingClause& posting) {
          return take(posting, nullptr);
        },
        [&](const QueryBuilder& child) {
          return take(search::PostingClause{TermState{nullptr, PostingMeta{}}},
                      &child);
        })) {
    return {};
  }
  using Leaves = fill::SetLeaves<fill::Erased>;
  return MakeShape<Complement, Leaves>(
    ctx, std::piecewise_construct,
    std::forward_as_tuple(
      leaves.size(),
      [&](fill::Erased& leaf, size_t i) { leaf = std::move(leaves[i]); }),
    static_cast<doc_id_t>(segment.docs_count()));
}

Root::ptr MakeWindowExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
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
      1,
      [&](fill::Erased& leaf, size_t) { leaf = fill::Erased{std::move(node)}; },
      exclude_terms, exclude_filters, segment, candidates, ctx);
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms,
                                static_cast<doc_id_t>(segment.docs_count()))) {
    return {};
  }
  return search::BuildExcludeSide<Root::ptr>(
    exclude_terms, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Root::ptr {
      return MakeWindowOfTerms<fill::ProbedAndNot<Exclude>>(
        terms, nullptr, *doc,
        std::forward_as_tuple(std::piecewise_construct,
                              std::forward<decltype(exclude)>(exclude)),
        ctx);
    });
}

}  // namespace irs::docs
