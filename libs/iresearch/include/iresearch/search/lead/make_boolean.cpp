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

#include "iresearch/search/lead/make_boolean.hpp"

#include <span>
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/queries/boolean_query.hpp"

namespace irs::lead {
namespace {

struct Api {
  using Result = Node::ptr;
  using Context = utils::Empty;

  static constexpr bool kWindowNodes = false;
  static constexpr bool kWindowLeadDrains = true;
  static constexpr double kSparseLeadCost = 1.0;
  static constexpr bool kWindowLeadRefills = true;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeWindow(const Context&, Args&&... args) {
    using Window = BooleanWindow<Lead, Others, Optional, Excludes>;
    return memory::make_managed<Impl<Window>>(std::piecewise_construct,
                                              std::forward<Args>(args)...);
  }

  template<typename Lead, typename Probes, typename Excludes, typename LeadArgs,
           typename ProbesArgs, typename ExcludesArgs>
  static Result MakeSparse(const Context&, LeadArgs&& lead, ProbesArgs&& probes,
                           ExcludesArgs&& excludes) {
    using Sparse = BooleanSparse<Lead, Probes, utils::Empty, Excludes>;
    return memory::make_managed<Impl<Sparse>>(
      std::piecewise_construct, std::forward<LeadArgs>(lead),
      std::forward<ProbesArgs>(probes), std::forward_as_tuple(),
      std::forward<ExcludesArgs>(excludes));
  }

  static Result PlanChild(const QueryBuilder& child, const Context&) {
    return child.PlanLead({});
  }

  static Result MakeTerm(const detail::PostingClause& term,
                         const SubReader& segment, const Context&) {
    return LeadOf(term, nullptr, segment);
  }

  static Result MakeAll(const SubReader& segment, const Context&) {
    return MakeAllDocs(segment);
  }

  static detail::TableFilter* BitsetTable(const Context&) noexcept {
    return nullptr;
  }

  static Result MakeNegation(
    std::span<const detail::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t candidates, const Context& ctx) {
    return detail::builder::MakeSparseNegation<Api>(
      exclude_terms, exclude_filters, segment, candidates, ctx);
  }
};

}  // namespace

Node::ptr MakeRequiredDocs(std::span<const detail::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const detail::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  return detail::builder::MakeRequired<Api>(
    must, must_filters, should, should_filters, min_should_match, segment, {});
}

Node::ptr Make(const BooleanQuery& query) {
  return detail::builder::Make<Api>(query, {});
}

}  // namespace irs::lead
