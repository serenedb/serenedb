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

#include "connector/scan/deferred_verify.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

using irs::utils::downCast;

struct VerifyBindData final : duckdb::FunctionData {
  explicit VerifyBindData(std::shared_ptr<const re2::RE2> matcher)
    : matcher{std::move(matcher)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<VerifyBindData>(matcher);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return matcher == other.Cast<VerifyBindData>().matcher;
  }

  std::shared_ptr<const re2::RE2> matcher;
};

void VerifyStoredTerms(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  const auto& func = state.expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& matcher = *func.BindInfo()->Cast<VerifyBindData>().matcher;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t terms) {
      return irs::MatchStoredTerms(
        matcher, {reinterpret_cast<const irs::byte_type*>(terms.GetData()),
                  terms.GetSize()});
    });
}

duckdb::unique_ptr<duckdb::TableFilter> MakeVerifyFilter(
  std::shared_ptr<const re2::RE2> matcher) {
  duckdb::ScalarFunction fn(duckdb::Identifier{"sdb_wildcard_ngram_verify"},
                            {duckdb::LogicalType::BLOB},
                            duckdb::LogicalType::BOOLEAN, VerifyStoredTerms);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
    duckdb::LogicalType::BLOB, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children),
    duckdb::make_uniq<VerifyBindData>(std::move(matcher)));
  return duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(expr));
}

template<typename F, typename Visitor>
void VisitConjuncts(F& root, Visitor&& visit) {
  if (root.type() != irs::Type<irs::BooleanFilter>::id()) {
    visit(root);
    return;
  }
  for (const auto& child :
       downCast<const irs::BooleanFilter>(root).Filters(irs::Occur::Must)) {
    visit(static_cast<F&>(*child));
  }
}

void Defer(irs::Filter& filter) {
  if (filter.type() == irs::Type<irs::ByWildcardNGram>::id()) {
    auto& options = *downCast<irs::ByWildcardNGram>(filter).mutable_options();
    options.deferred_verify = options.matcher != nullptr;
  }
}

void AddDeferred(ScanGlobalState& state, const irs::Filter& filter) {
  if (filter.type() != irs::Type<irs::ByWildcardNGram>::id()) {
    return;
  }
  const auto& options = downCast<const irs::ByWildcardNGram>(filter).options();
  if (!options.deferred_verify) {
    return;
  }
  const auto& owned =
    state.verify_filters.emplace_back(MakeVerifyFilter(options.matcher));
  auto& cf = state.col_filters.emplace_back();
  cf.field = options.store_field_id;
  cf.filter = owned.get();
  cf.row_gather = true;
  cf.type = duckdb::LogicalType::BLOB;
}

}  // namespace

void DeferWildcardVerify(irs::Filter& root) {
  VisitConjuncts(root, [](irs::Filter& filter) { Defer(filter); });
}

void AddDeferredVerifyFilters(ScanGlobalState& state, const irs::Filter& root) {
  const auto pushed = state.col_filters.size();
  VisitConjuncts(
    root, [&](const irs::Filter& filter) { AddDeferred(state, filter); });
  if (state.col_filters.size() == pushed) {
    return;
  }
  using ColFilter = ScanGlobalState::ColFilter;
  const bool can_throw =
    absl::c_any_of(state.col_filters, [](const ColFilter& cf) {
      return !cf.is_score &&
             cf.filter->Cast<duckdb::ExpressionFilter>().expr->CanThrow();
    });
  if (can_throw) {
    absl::c_stable_partition(state.col_filters,
                             [](const ColFilter& cf) { return cf.row_gather; });
  }
}

}  // namespace sdb::connector
