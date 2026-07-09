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

#include <algorithm>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <numeric>

#include "basics/assert.h"

namespace sdb::connector {

void ViewIndexSourceBase::InitProjection(
  duckdb::ClientContext& context,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
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
      source_col = static_cast<duckdb::idx_t>(bind_column_ids[bind_col]);
    } else {
      const auto view_col_idx =
        static_cast<duckdb::idx_t>(bind_column_ids[bind_col]);
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
    auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      _scratch_types[c], static_cast<duckdb::idx_t>(c));
    auto cast_expr = duckdb::BoundCastExpression::AddCastToType(
      context, std::move(ref), _projected_types[c]);
    auto exec = duckdb::make_uniq<duckdb::ExpressionExecutor>(context);
    exec->AddExpression(*cast_expr);
    _cast_expressions.push_back(std::move(cast_expr));
    _cast_executors[c] = std::move(exec);
  }
  _tf_target.Initialize(context, _scratch_types);
}

void ViewIndexSourceBase::SortRows(const PrimaryKeyBatch& pk,
                                   duckdb::idx_t start, duckdb::idx_t count) {
  _sort_perm.resize(count);
  std::iota(_sort_perm.begin(), _sort_perm.end(), duckdb::idx_t{0});
  std::sort(_sort_perm.begin(), _sort_perm.end(),
            [&](duckdb::idx_t a, duckdb::idx_t b) {
              return pk.rows[start + a] < pk.rows[start + b];
            });
  _sorted_rows.resize(count);
  _output_positions.resize(count);
  for (duckdb::idx_t k = 0; k < count; ++k) {
    _sorted_rows[k] = pk.rows[start + _sort_perm[k]];
    _output_positions[k] = _sort_perm[k];
  }
}

void ViewIndexSourceBase::SortFilesRows(const PrimaryKeyBatch& pk,
                                        duckdb::idx_t start,
                                        duckdb::idx_t count) {
  _sort_perm.resize(count);
  std::iota(_sort_perm.begin(), _sort_perm.end(), duckdb::idx_t{0});
  std::sort(_sort_perm.begin(), _sort_perm.end(),
            [&](duckdb::idx_t a, duckdb::idx_t b) {
              if (pk.files[start + a] != pk.files[start + b]) {
                return pk.files[start + a] < pk.files[start + b];
              }
              return pk.rows[start + a] < pk.rows[start + b];
            });
  _sorted_files.resize(count);
  _sorted_rows.resize(count);
  _output_positions.resize(count);
  for (duckdb::idx_t k = 0; k < count; ++k) {
    _sorted_files[k] = pk.files[start + _sort_perm[k]];
    _sorted_rows[k] = pk.rows[start + _sort_perm[k]];
    _output_positions[k] = _sort_perm[k];
  }
}

void ViewIndexSourceBase::AliasOutput(duckdb::DataChunk& output) {
  _tf_target.Reset();
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (_cast_executors[c]) {
      continue;
    }
    _tf_target.data[c].Reference(output.data[_real_proj_slots[c]]);
  }
}

void ViewIndexSourceBase::PreNullOutput(duckdb::idx_t count) {
  for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
    duckdb::FlatVector::ValidityMutable(_tf_target.data[c])
      .SetAllInvalid(count);
  }
}

void ViewIndexSourceBase::RunCastPass(duckdb::DataChunk& output,
                                      duckdb::idx_t row_count) {
  const bool has_cast =
    std::any_of(_cast_executors.begin(), _cast_executors.end(),
                [](const auto& e) { return e != nullptr; });
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

}  // namespace sdb::connector
