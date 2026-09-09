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

#include "iresearch/search/docs/make_boolean.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/boolean_builder.hpp"
#include "iresearch/search/docs/boolean_sparse.hpp"

namespace irs::docs {
namespace {

struct Api {
  using Result = Root::ptr;
  using Context = docs::Context;

  static constexpr bool kWindowNodes = true;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeWindow(const Context& ctx, Args&&... args) {
    return MakeShape<BooleanWindow, Lead, Others, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  template<typename Lead, typename Probes, typename Excludes, typename... Args>
  static Result MakeSparse(const Context& ctx, Args&&... args) {
    return MakeShape<BooleanSparse, Lead, Probes, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx) {
    return child.PlanDocs(ctx);
  }

  static Result MakeTerm(const search::PostingClause& term,
                         const SubReader& segment, const Context& ctx) {
    return MakePosting(term, segment, ctx);
  }

  static Result MakeAll(const SubReader& segment, const Context& ctx) {
    return docs::MakeAll(static_cast<doc_id_t>(segment.docs_count()), ctx);
  }

  static search::TableFilter* BitsetTable(const Context&) noexcept {
    return nullptr;
  }

  static Result MakeNegation(
    std::span<const search::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t, const Context& ctx) {
    return search::builder::MakeWindowNegation<Api>(
      exclude_terms, exclude_filters, segment, ctx);
  }
};

}  // namespace

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  return search::builder::Make<Api>(query, ctx);
}

}  // namespace irs::docs
