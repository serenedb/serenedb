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
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

DeferredCheck Split(std::shared_ptr<const irs::Filter> source,
                    irs::field_id column, const duckdb::LogicalType& type,
                    std::string_view name, duckdb::scalar_function_t function,
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
    .source = std::move(source),
    .column = column,
    .check = std::make_shared<const duckdb::ExpressionFilter>(std::move(expr))};
}

namespace {

struct Sources {
  std::shared_ptr<const irs::Filter> tree;
  std::vector<irs::Filter::ptr> owned;
};

}  // namespace

std::vector<DeferredCheck> MakeChecks(irs::Deferral deferral,
                                      std::shared_ptr<const irs::Filter> tree) {
  if (deferral.filters.empty()) {
    return {};
  }
  const auto sources =
    std::make_shared<Sources>(std::move(tree), std::move(deferral.owned));
  std::vector<DeferredCheck> checks;
  checks.reserve(deferral.filters.size());
  for (const auto& deferred : deferral.filters) {
    std::shared_ptr<const irs::Filter> source{sources, deferred.filter};
    const auto type = deferred.filter->type();
    if (type == irs::Type<irs::ByWildcardNGram>::id()) {
      checks.emplace_back(
        MakeWildcardCheck(std::move(source), deferred.column, deferred.type));
    } else if (type == irs::Type<irs::ByPhrase>::id() ||
               type == irs::Type<irs::ByShinglePhrase>::id()) {
      checks.emplace_back(MakePhraseCheck(std::move(source), deferred.column,
                                          deferred.type, deferral.reader));
    } else {
      checks.emplace_back(
        MakeGeoCheck(std::move(source), deferred.column, deferred.type));
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
