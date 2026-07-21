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

#include <absl/functional/function_ref.h>

#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/fwd.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_table_function.h"
#include "connector/search_filter_builder.hpp"

namespace sdb::optimizer {

std::optional<duckdb::TableIndex> SingleReferencedTableIndex(
  const duckdb::Expression& expr);

catalog::Column::Id ResolveColumnId(
  duckdb::ColumnBinding binding,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get);

std::vector<catalog::Column::Id> BuildProjectedColumnIds(
  const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data);

struct FoundScan {
  duckdb::LogicalGet* get;
  connector::SereneDBScanBindData* bind_data;
};

std::optional<FoundScan> AsSearchScan(duckdb::LogicalOperator& op);
std::optional<FoundScan> FindIResearchScan(duckdb::LogicalOperator& op,
                                           duckdb::TableIndex table_index);

struct FoundScanColumn {
  FoundScan found;
  duckdb::ColumnBinding binding;
};

struct ResolvedProjection {
  duckdb::ColumnBinding binding;
  const duckdb::Expression* stop_expr;
};

ResolvedProjection WalkProjections(duckdb::LogicalOperator& root,
                                   duckdb::ColumnBinding binding);
duckdb::ColumnBinding ResolveBindingThroughProjections(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding);
std::optional<FoundScanColumn> ResolveIResearchScanColumn(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding);

duckdb::ColumnBinding ExposeGetColumnAt(duckdb::LogicalOperator& root,
                                        duckdb::TableIndex anchor_ti,
                                        const duckdb::LogicalGet& target_get,
                                        duckdb::idx_t get_col_idx,
                                        std::string_view col_name,
                                        const duckdb::LogicalType& col_type);

duckdb::idx_t AppendVirtualGetColumn(connector::SereneDBScanBindData& bind_data,
                                     duckdb::LogicalGet& get,
                                     catalog::Column::Id virtual_id,
                                     const duckdb::LogicalType& col_type,
                                     std::string_view col_name);

bool TryClaimIResearchConjunct(
  irs::And& and_root, const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context);

inline connector::SearchColumnInfo MakeSearchColumnInfo(
  irs::field_id field, const catalog::InvertedIndexEntryInfo* info,
  duckdb::LogicalType type, catalog::ColumnTokenizer tokenizer) {
  return {
    .field_id = field,
    .null_field_id = info ? info->null_field_id : irs::field_limits::invalid(),
    .bool_field_id = info ? info->bool_field_id : irs::field_limits::invalid(),
    .numeric_field_id =
      info ? info->numeric_field_id : irs::field_limits::invalid(),
    .logical_type = std::move(type),
    .tokenizer = std::move(tokenizer),
  };
}

struct SearchGetters {
  const connector::ColumnGetter& getter;
  const connector::ExpressionGetter& expr_getter;
  containers::FlatHashSet<irs::field_id>& analyzed_fields;
  containers::FlatHashMap<irs::field_id, irs::field_id>& null_markers;
};

bool WithSearchGetters(duckdb::LogicalGet& get,
                       connector::SereneDBScanBindData& bind_data,
                       const catalog::InvertedIndex& index,
                       const std::shared_ptr<const catalog::Snapshot>& snapshot,
                       duckdb::ClientContext& context,
                       absl::FunctionRef<bool(const SearchGetters&)> fn);

}  // namespace sdb::optimizer
