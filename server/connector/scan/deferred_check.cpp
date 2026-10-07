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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

using irs::utils::downCast;

std::optional<DeferredCheck> Defer(irs::Filter::ptr& filter,
                                   const DeferContext& ctx) {
  const auto type = filter->type();
  if (type == irs::Type<irs::ByWildcardNGram>::id()) {
    return DeferWildcard(filter, ctx);
  }
  if (type == irs::Type<irs::ByPhrase>::id()) {
    return DeferPhrase(filter, ctx);
  }
  if (type == irs::Type<irs::GeoFilter>::id()) {
    return DeferGeo(filter, ctx);
  }
  if (type == irs::Type<irs::GeoDistanceFilter>::id()) {
    return DeferGeoDistance(filter, ctx);
  }
  return std::nullopt;
}

bool Spliceable(const irs::Filter& filter, const irs::BooleanFilter& parent) {
  if (filter.type() != irs::Type<irs::BooleanFilter>::id()) {
    return false;
  }
  const auto& boolean = downCast<irs::BooleanFilter>(filter);
  return boolean.Transparent() && boolean.MergeType() == parent.MergeType();
}

}  // namespace

DeferredCheck Split(irs::Filter::ptr& filter, irs::Filter::ptr index,
                    irs::field_id column, const duckdb::LogicalType& type,
                    const char* name, duckdb::scalar_function_t function,
                    duckdb::unique_ptr<duckdb::FunctionData> bind,
                    duckdb::init_local_state_t init) {
  duckdb::ScalarFunction fn(duckdb::Identifier{name}, {type},
                            duckdb::LogicalType::BOOLEAN, std::move(function));
  fn.SetInitStateCallback(init);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children), std::move(bind));
  DeferredCheck check{
    .source = std::move(filter),
    .column = column,
    .type = type,
    .check = std::make_shared<const duckdb::ExpressionFilter>(std::move(expr))};
  filter = std::move(index);
  return check;
}

std::vector<DeferredCheck> DeferChecks(irs::Filter::ptr& root,
                                       const DeferContext& ctx) {
  std::vector<DeferredCheck> checks;
  const auto defer = [&](irs::Filter::ptr& filter) {
    auto check = Defer(filter, ctx);
    if (check) {
      checks.push_back(std::move(*check));
    }
    return check.has_value();
  };
  if (root->type() != irs::Type<irs::BooleanFilter>::id()) {
    defer(root);
    return checks;
  }
  auto& boolean = downCast<irs::BooleanFilter>(*root);
  auto& must = boolean.Bucket(irs::Occur::Must).filters;
  std::vector<irs::Filter::ptr> spliced;
  for (auto& child : must) {
    if (defer(child) && Spliceable(*child, boolean)) {
      spliced.push_back(std::move(child));
    }
  }
  if (spliced.empty()) {
    return checks;
  }
  std::erase(must, nullptr);
  for (auto& conjunction : spliced) {
    downCast<irs::BooleanFilter>(*conjunction).SpliceInto(boolean);
  }
  auto& terms = boolean.Bucket(irs::Occur::Must).terms;
  terms.erase(std::ranges::unique(terms).begin(), terms.end());
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
    cf.type = check.type;
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
