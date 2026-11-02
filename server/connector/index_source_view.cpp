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

#include "connector/index_source_view.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/column_id.h"

namespace sdb::connector {

duckdb::idx_t SourceColumns::operator()(std::string_view name) const {
  if (const auto it = _by_name.find(name); it != _by_name.end()) {
    return it->second;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                  ERR_MSG("column \"", name,
                          "\" of the indexed view is not a column of its "
                          "source"));
}

void ViewIndexSourceBase::InitProjection(
  duckdb::ClientContext& context,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  absl::FunctionRef<duckdb::idx_t(std::string_view)> col_by_name,
  absl::FunctionRef<duckdb::LogicalType(duckdb::idx_t)> add_source_column) {
  _real_proj_slots.reserve(projected_columns.size());
  _scratch_types.reserve(projected_columns.size());
  _projected_types.reserve(projected_columns.size());
  for (duckdb::idx_t proj = 0; proj < projected_columns.size(); ++proj) {
    const auto bind_col = projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(bind_col < bind_column_ids.size());
    duckdb::idx_t source_col;
    if (_fast_path.projection_columns.empty()) {
      source_col = bind_column_ids[bind_col];
    } else {
      const auto view_col_idx = bind_column_ids[bind_col];
      SDB_ASSERT(view_col_idx < _fast_path.projection_columns.size());
      source_col = col_by_name(_fast_path.projection_columns[view_col_idx]);
    }
    _real_proj_slots.push_back(proj);
    _scratch_types.push_back(add_source_column(source_col));
    _projected_types.push_back(projected_types[proj]);
  }
  _cast_executors.resize(_scratch_types.size());
  for (size_t c = 0; c < _scratch_types.size(); ++c) {
    if (_scratch_types[c] == _projected_types[c]) {
      continue;
    }
    auto ref =
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(_scratch_types[c], c);
    auto cast_expr = duckdb::BoundCastExpression::AddCastToType(
      context, std::move(ref), _projected_types[c]);
    auto exec = duckdb::make_uniq<duckdb::ExpressionExecutor>(context);
    exec->AddExpression(*cast_expr);
    _cast_expressions.push_back(std::move(cast_expr));
    _cast_executors[c] = std::move(exec);
  }
  _tf_target.Initialize(context, _scratch_types);

  // The doc-id-keyed columns GatherNonLookupColumns reorders are every output
  // slot the lookup does not write; this set is fixed for the query, so compute
  // it once here (plus a reusable selection) instead of per batch.
  std::vector<bool> is_lookup(projected_columns.size(), false);
  for (const auto slot : _real_proj_slots) {
    is_lookup[slot] = true;
  }
  for (duckdb::idx_t c = 0; c < projected_columns.size(); ++c) {
    if (!is_lookup[c]) {
      _non_lookup_slots.push_back(c);
    }
  }
  _gather_sel.Initialize(STANDARD_VECTOR_SIZE);
}

void ViewIndexSourceBase::AliasOutput(duckdb::DataChunk& output) {
  _tf_target.Reset();
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (_cast_executors[c]) {
      continue;
    }
    _tf_target.data[c].Reference(output.data[_real_proj_slots[c]]);
  }
  _tf_target.SetCardinality(0);
}

void ViewIndexSourceBase::RunCastPass(duckdb::DataChunk& output,
                                      duckdb::idx_t row_count) {
  const bool has_cast = absl::c_any_of(
    _cast_executors, [](const auto& e) { return static_cast<bool>(e); });
  if (!has_cast || row_count == 0) {
    return;
  }
  duckdb::DataChunk cast_input;
  cast_input.InitializeEmpty(_scratch_types);
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (_cast_executors[c]) {
      cast_input.data[c].Reference(_tf_target.data[c]);
    }
  }
  cast_input.SetCardinality(row_count);
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (_cast_executors[c]) {
      _cast_executors[c]->ExecuteExpression(cast_input,
                                            output.data[_real_proj_slots[c]]);
    }
  }
}

void ViewIndexSourceBase::GatherNonLookupColumns(duckdb::DataChunk& output,
                                                 duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  SDB_ASSERT(count <= _survivor_idx.size());
  for (duckdb::idx_t k = 0; k < count; ++k) {
    _gather_sel.set_index(k, _survivor_idx[k]);
  }
  for (const auto c : _non_lookup_slots) {
    output.data[c].Slice(_gather_sel, count);
  }
}

}  // namespace sdb::connector
