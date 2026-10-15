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

#include "connector/scan/deferred_check.h"

#include <algorithm>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

DeferredCheck Split(const irs::Filter& source, irs::field_id column,
                    const duckdb::LogicalType& type, std::string_view name,
                    duckdb::scalar_function_t function,
                    duckdb::unique_ptr<duckdb::FunctionData> bind,
                    duckdb::init_local_state_t init) {
  duckdb::ScalarFunction fn(duckdb::Identifier{name}, {type},
                            duckdb::LogicalType::BOOLEAN, std::move(function));
  fn.SetInitStateCallback(init);
  fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.emplace_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children), std::move(bind));
  return {
    .source = &source,
    .column = column,
    .check = std::make_shared<const duckdb::ExpressionFilter>(std::move(expr))};
}

std::vector<DeferredCheck> MakeChecks(irs::Deferral deferral) {
  std::vector<DeferredCheck> checks;
  checks.reserve(deferral.filters.size());
  for (auto& deferred : deferral.filters) {
    const auto& source = *deferred.filter;
    switch (deferred.kind) {
      case irs::DeferredKind::Phrase:
        checks.emplace_back(MakePhraseCheck(std::move(deferred.owned),
                                            deferred.column, deferred.type,
                                            deferral.reader));
        break;
      case irs::DeferredKind::Wildcard:
        checks.emplace_back(
          MakeWildcardCheck(source, deferred.column, deferred.type));
        break;
      case irs::DeferredKind::Geo:
        checks.emplace_back(MakeGeoCheck(
          source, irs::utils::downCast<irs::GeoFilter>(source).options(),
          deferred.column, deferred.type));
        break;
      case irs::DeferredKind::GeoDistance:
        checks.emplace_back(MakeGeoCheck(
          source,
          irs::utils::downCast<irs::GeoDistanceFilter>(source).options(),
          deferred.column, deferred.type));
        break;
    }
  }
  return checks;
}

void AddDeferredChecks(ScanGlobalState& state,
                       std::span<const DeferredCheck> checks) {
  if (checks.empty()) {
    return;
  }
  for (const auto& check : checks) {
    auto& cf = state.col_filters.emplace_back();
    cf.field = check.column;
    cf.filter = check.check.get();
    cf.row_gather = true;
  }
  auto& filters = state.col_filters;
  const auto added = filters.end() - checks.size();
  const auto first_throwing = std::find_if(
    filters.begin(), added, [](const ScanGlobalState::ColFilter& cf) {
      return !cf.is_score &&
             cf.filter->Cast<duckdb::ExpressionFilter>().expr->CanThrow();
    });
  std::rotate(first_throwing, added, filters.end());
}

}  // namespace sdb::connector
