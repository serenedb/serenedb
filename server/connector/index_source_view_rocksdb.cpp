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

#include "connector/index_source_view_rocksdb.h"

#include <algorithm>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/table.h"

namespace sdb::connector {

ViewRocksDBIndexSource::ViewRocksDBIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  const rocksdb::Snapshot* snapshot, rocksdb::Transaction* txn)
  : _fast_path{std::move(fast_path)} {
  SDB_ASSERT(catalog::IsRocksPK(_fast_path.pk_spec));
  SDB_ASSERT(_fast_path.base_table);
  const auto& base_cols = _fast_path.base_table->Columns();

  // Skip the hidden generated-PK column.
  std::vector<size_t> real_col_positions;
  real_col_positions.reserve(base_cols.size());
  for (size_t i = 0; i < base_cols.size(); ++i) {
    if (base_cols[i].GetId() != catalog::Column::kGeneratedPKId) {
      real_col_positions.push_back(i);
    }
  }
  containers::FlatHashMap<std::string_view, size_t> name_to_base_col;
  if (!_fast_path.projection_columns.empty()) {
    name_to_base_col.reserve(real_col_positions.size());
    for (auto i : real_col_positions) {
      name_to_base_col.emplace(base_cols[i].GetName(), i);
    }
  }
  _real_proj_slots.reserve(projected_columns.size());
  _scratch_types.reserve(projected_columns.size());
  _projected_types.reserve(projected_columns.size());

  // RocksDBIndexSource copies these on construction; discard afterwards.
  std::vector<duckdb::idx_t> inner_projected_columns;
  std::vector<duckdb::LogicalType> inner_projected_types;
  std::vector<catalog::Column::Id> inner_bind_column_ids;
  inner_projected_columns.reserve(projected_columns.size());
  inner_projected_types.reserve(projected_columns.size());
  inner_bind_column_ids.reserve(projected_columns.size());

  for (duckdb::idx_t proj = 0; proj < projected_columns.size(); ++proj) {
    const auto bind_col = projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(bind_col < bind_column_ids.size());
    size_t base_col_idx;
    if (_fast_path.projection_columns.empty()) {
      const auto view_col_idx = static_cast<size_t>(bind_column_ids[bind_col]);
      SDB_ASSERT(view_col_idx < real_col_positions.size());
      base_col_idx = real_col_positions[view_col_idx];
    } else {
      const auto view_col_idx = static_cast<size_t>(bind_column_ids[bind_col]);
      SDB_ASSERT(view_col_idx < _fast_path.projection_columns.size());
      const auto& reader_name = _fast_path.projection_columns[view_col_idx];
      auto it = name_to_base_col.find(reader_name);
      SDB_ASSERT(it != name_to_base_col.end());
      base_col_idx = it->second;
    }
    SDB_ASSERT(base_col_idx < base_cols.size());
    const auto& base_col = base_cols[base_col_idx];
    _real_proj_slots.push_back(proj);
    _scratch_types.push_back(base_col.type);
    _projected_types.push_back(projected_types[proj]);
    inner_projected_columns.push_back(
      static_cast<duckdb::idx_t>(inner_projected_columns.size()));
    inner_projected_types.push_back(base_col.type);
    inner_bind_column_ids.push_back(base_col.GetId());
  }

  _cast_executors.resize(_scratch_types.size());
  _per_col_scratch_cache.resize(_scratch_types.size());
  _per_col_scratch.resize(_scratch_types.size());
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  for (size_t c = 0; c < _scratch_types.size(); ++c) {
    if (_scratch_types[c] == _projected_types[c]) {
      // Matching col -- decode straight into output, no scratch needed.
      continue;
    }
    auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      _scratch_types[c], duckdb::idx_t{0});
    auto cast_expr = duckdb::BoundCastExpression::AddCastToType(
      context, std::move(ref), _projected_types[c]);
    auto exec = duckdb::make_uniq<duckdb::ExpressionExecutor>(context);
    exec->AddExpression(*cast_expr);
    _cast_expressions.push_back(std::move(cast_expr));
    _cast_executors[c] = std::move(exec);

    _per_col_scratch_cache[c] =
      duckdb::make_uniq<duckdb::VectorCache>(allocator, _scratch_types[c]);
    _per_col_scratch[c] =
      duckdb::make_uniq<duckdb::Vector>(*_per_col_scratch_cache[c]);
  }
  _decode_targets.assign(_scratch_types.size(), nullptr);

  _inner_lookup.emplace(
    _fast_path.base_table->GetId(), snapshot,
    std::span<const duckdb::idx_t>{inner_projected_columns},
    std::span<const duckdb::LogicalType>{inner_projected_types},
    std::span<const catalog::Column::Id>{inner_bind_column_ids}, txn);
}

duckdb::idx_t ViewRocksDBIndexSource::Materialize(duckdb::ClientContext&,
                                         PrimaryKeyBatch& batch,
                                         duckdb::idx_t start,
                                         duckdb::idx_t count,
                                         duckdb::DataChunk& output) {
  auto& arena = std::get<PrimaryKeysBytes>(batch);
  SDB_ASSERT(start + count <= arena.views.size());
  std::span<const std::string_view> pk_bytes{arena.views.data() + start, count};
  if (pk_bytes.empty()) {
    return count;
  }

  for (size_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (_cast_executors[c]) {
      _per_col_scratch[c]->ResetFromCache(*_per_col_scratch_cache[c]);
      _decode_targets[c] = _per_col_scratch[c].get();
    } else {
      _decode_targets[c] = &output.data[_real_proj_slots[c]];
    }
  }

  _inner_lookup->LookupInto(
    pk_bytes, std::span<duckdb::Vector* const>{_decode_targets.data(),
                                               _decode_targets.size()});

  bool needs_cast = std::any_of(_cast_executors.begin(), _cast_executors.end(),
                                [](const auto& e) { return e != nullptr; });
  if (!needs_cast) {
    return count;
  }
  duckdb::DataChunk single_col_input;
  duckdb::vector<duckdb::LogicalType> single_type{duckdb::LogicalType::SQLNULL};
  for (size_t c = 0; c < _real_proj_slots.size(); ++c) {
    if (!_cast_executors[c]) {
      continue;
    }
    single_type[0] = _scratch_types[c];
    single_col_input.InitializeEmpty(single_type);
    single_col_input.data[0].Reference(*_per_col_scratch[c]);
    single_col_input.SetCardinality(pk_bytes.size());
    _cast_executors[c]->ExecuteExpression(single_col_input,
                                          output.data[_real_proj_slots[c]]);
  }
  return count;
}

}  // namespace sdb::connector
