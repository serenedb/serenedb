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

#pragma once

#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>

#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <optional>
#include <utility>

#include "catalog/entry/inverted_index.h"
#include "connector/optimizer/plan_index.h"
#include "connector/scan/scan_bind.h"
#include "connector/search_filter_builder.hpp"

namespace sdb::optimizer {

std::optional<duckdb::TableIndex> SingleReferencedTableIndex(
  const duckdb::Expression& expr);

bool TryClaimIResearchConjunct(
  irs::BooleanFilter& root,
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context, connector::FilterScorers* scorers = nullptr);

inline connector::SearchColumnInfo MakeSearchColumnInfo(
  irs::field_id field, const catalog::InvertedIndexField* info,
  duckdb::LogicalType type, catalog::ColumnTokenizer tokenizer) {
  const bool json_leaves = info && info->HasJsonLeafFields();
  return {
    .field_id = field,
    .nullable = info != nullptr,
    .layout = connector::term_dict::LayoutOf(type, json_leaves),
    .logical_type = std::move(type),
    .tokenizer = std::move(tokenizer),
  };
}

using BindingColumnId = absl::AnyInvocable<connector::ColumnId(
  const duckdb::BoundColumnRefExpression&) const>;

struct SearchGetters {
  const connector::ColumnGetter& getter;
  const connector::ExpressionGetter& expr_getter;
  irs::containers::FlatHashSet<irs::field_id>& analyzed_fields;
  const BindingColumnId& column_id;
};

bool WithSearchGetters(PlannedScan& scan, duckdb::ClientContext& context,
                       absl::FunctionRef<bool(const SearchGetters&)> fn);

}  // namespace sdb::optimizer
