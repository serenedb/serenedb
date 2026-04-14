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

#include "connector/duckdb_constraint_verify.h"

#include <duckdb/common/index_map.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/constraints/bound_not_null_constraint.hpp>

#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {
namespace {

// "Failing row contains (v1, v2, ...)." formatter for PG DETAIL.
// positions: (chunk_pos, phys_idx) pairs; output is sorted by phys_idx so
// values appear in table-declaration order.
std::string BuildFailingRowDetail(
  duckdb::DataChunk& chunk, duckdb::idx_t row,
  std::vector<std::pair<duckdb::idx_t, duckdb::idx_t>> positions) {
  std::sort(positions.begin(), positions.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });
  std::string vals = "(";
  for (size_t i = 0; i < positions.size(); i++) {
    if (i > 0) {
      vals += ", ";
    }
    auto val = chunk.GetValue(positions[i].first, row);
    vals += val.IsNull() ? "null" : val.ToString();
  }
  vals += ")";
  return absl::StrCat("Failing row contains ", vals, ".");
}

// Collects (chunk_pos, phys_idx) for every column whose value is available
// in the chunk -- the updated columns plus the PK passthroughs for UPDATE,
// or all physical columns for INSERT.
using DetailPositions = std::vector<std::pair<duckdb::idx_t, duckdb::idx_t>>;

// Same as DuckDB's static VerifyNotNullConstraint in data_table.cpp,
// but throws our PG-compatible error.
void VerifyNotNullConstraint(const catalog::Table& table,
                             duckdb::Vector& vector, duckdb::idx_t count,
                             const std::string& col_name,
                             duckdb::DataChunk& chunk_for_detail,
                             const DetailPositions& detail_positions) {
  if (!duckdb::VectorOperations::HasNull(vector, count)) {
    return;
  }
  duckdb::UnifiedVectorFormat vdata;
  vector.ToUnifiedFormat(count, vdata);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = vdata.sel->get_index(i);
    if (!vdata.validity.RowIsValid(idx)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_NOT_NULL_VIOLATION),
        ERR_MSG("null value in column \"", col_name, "\" of relation \"",
                table.GetName(), "\" violates not-null constraint"),
        ERR_DETAIL(
          BuildFailingRowDetail(chunk_for_detail, i, detail_positions)));
    }
  }
}

// Same as DuckDB's static VerifyCheckConstraint in data_table.cpp,
// but throws our PG-compatible error with constraint name and DETAIL.
void VerifyCheckConstraint(duckdb::ClientContext& context,
                           const catalog::Table& table,
                           duckdb::Expression& expr, duckdb::DataChunk& chunk,
                           const std::string& constraint_name,
                           duckdb::DataChunk& chunk_for_detail,
                           const DetailPositions& detail_positions) {
  duckdb::ExpressionExecutor executor(context, expr);
  duckdb::Vector result(duckdb::LogicalType::INTEGER);
  executor.ExecuteExpression(chunk, result);

  for (duckdb::idx_t i = 0; i < chunk.size(); i++) {
    auto entry = result.GetValue(i);
    if (entry.IsNull()) {
      continue;
    }
    if (duckdb::BooleanValue::Get(
          entry.DefaultCastAs(duckdb::LogicalType::BOOLEAN))) {
      continue;
    }

    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CHECK_VIOLATION),
      ERR_MSG("new row for relation \"", table.GetName(),
              "\" violates check constraint \"", constraint_name, "\""),
      ERR_DETAIL(BuildFailingRowDetail(chunk_for_detail, i, detail_positions)));
  }
}

// Same as DuckDB's static CreateMockChunk in data_table.cpp.
// Builds a full-table-width DataChunk where referenced columns point at
// the real chunk's data via Reference (no data allocation).
void CreateMockChunk(duckdb::vector<duckdb::LogicalType>& types,
                     const std::vector<duckdb::PhysicalIndex>& column_ids,
                     duckdb::DataChunk& chunk, duckdb::DataChunk& mock_chunk) {
  mock_chunk.InitializeEmpty(types);
  for (duckdb::column_t i = 0; i < column_ids.size(); i++) {
    mock_chunk.data[column_ids[i].index].Reference(chunk.data[i]);
  }
  mock_chunk.SetCardinality(chunk.size());
}

// Same as DuckDB's overload of CreateMockChunk in data_table.cpp.
// Returns false if not all `desired_column_ids` are present in `column_ids`
// (CHECK doesn't reference any updated column -> old row already satisfied it).
bool CreateMockChunk(const catalog::Table& table,
                     const std::vector<duckdb::PhysicalIndex>& column_ids,
                     const duckdb::physical_index_set_t& desired_column_ids,
                     duckdb::DataChunk& chunk, duckdb::DataChunk& mock_chunk) {
  duckdb::idx_t found_columns = 0;
  for (duckdb::column_t i = 0; i < column_ids.size(); i++) {
    if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
      found_columns++;
    }
  }
  if (found_columns == 0) {
    return false;
  }
  if (found_columns != desired_column_ids.size()) {
    // Same internal error DuckDB throws -- binder should have added missing
    // cols
    THROW_SQL_ERROR(
      ERR_MSG("Not all columns required for the CHECK constraint are present "
              "in the UPDATED chunk!"));
  }
  duckdb::vector<duckdb::LogicalType> types;
  for (auto& c : table.Columns()) {
    if (c.id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    types.push_back(c.type);
  }
  CreateMockChunk(types, column_ids, chunk, mock_chunk);
  return true;
}

}  // namespace

// VerifyAppendConstraints (INSERT path) -- same shape as
// DataTable::VerifyAppendConstraints in data_table.cpp.
// The chunk has all physical columns at their PhysicalIndex positions.
void VerifyAppendConstraints(
  duckdb::ClientContext& context, const catalog::Table& table,
  const duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints,
  duckdb::DataChunk& chunk) {
  if (chunk.size() == 0 || bound_constraints.empty()) {
    return;
  }
  auto& columns = table.Columns();
  auto& check_constraints = table.CheckConstraints();
  size_t catalog_check_idx = 0;

  // INSERT: every physical column of the chunk sits at its PhysicalIndex.
  DetailPositions detail;
  detail.reserve(chunk.ColumnCount());
  for (duckdb::idx_t c = 0; c < chunk.ColumnCount(); ++c) {
    detail.emplace_back(c, c);
  }

  for (auto& constraint : bound_constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        auto& bound_nn = constraint->Cast<duckdb::BoundNotNullConstraint>();
        auto phys_idx = bound_nn.index.index;
        std::string col_name =
          phys_idx < columns.size() ? columns[phys_idx].name : "unknown";
        VerifyNotNullConstraint(table, chunk.data[phys_idx], chunk.size(),
                                col_name, chunk, detail);
        catalog_check_idx++;
        break;
      }
      case duckdb::ConstraintType::CHECK: {
        auto& bound_check = constraint->Cast<duckdb::BoundCheckConstraint>();
        std::string constraint_name;
        if (catalog_check_idx < check_constraints.size()) {
          constraint_name = check_constraints[catalog_check_idx].name;
        }
        catalog_check_idx++;
        VerifyCheckConstraint(context, table, *bound_check.expression, chunk,
                              constraint_name, chunk, detail);
        break;
      }
      default:
        break;
    }
  }
}

// VerifyUpdateConstraints (UPDATE path) -- same shape as
// DataTable::VerifyUpdateConstraints in data_table.cpp.
// `chunk` has updated column values at the leading column_ids.size() slots;
// pk_chunk_positions gives the slot of each PK in table.PKColumns() order.
void VerifyUpdateConstraints(
  duckdb::ClientContext& context, const catalog::Table& table,
  const duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints,
  duckdb::DataChunk& chunk,
  const std::vector<duckdb::PhysicalIndex>& column_ids,
  const std::vector<duckdb::idx_t>& pk_chunk_positions) {
  if (chunk.size() == 0 || bound_constraints.empty()) {
    return;
  }
  auto& columns = table.Columns();
  auto& check_constraints = table.CheckConstraints();
  size_t catalog_check_idx = 0;

  // Detail positions for PG "Failing row contains (...)". The updated cols
  // come from column_ids; the PK cols come from pk_chunk_positions, with
  // their PhysicalIndex resolved against the table.
  DetailPositions detail;
  detail.reserve(column_ids.size() + pk_chunk_positions.size());
  for (duckdb::idx_t i = 0; i < column_ids.size(); ++i) {
    detail.emplace_back(i, column_ids[i].index);
  }
  const auto& pk_ids = table.PKColumns();
  for (size_t i = 0; i < pk_chunk_positions.size() && i < pk_ids.size(); ++i) {
    duckdb::idx_t phys = 0;
    duckdb::idx_t counter = 0;
    for (const auto& col : columns) {
      if (col.id == catalog::Column::kGeneratedPKId) {
        continue;
      }
      if (col.id == pk_ids[i]) {
        phys = counter;
        break;
      }
      ++counter;
    }
    detail.emplace_back(pk_chunk_positions[i], phys);
  }

  for (auto& constraint : bound_constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        auto& bound_nn = constraint->Cast<duckdb::BoundNotNullConstraint>();
        // Find this column in the update set. If not updated, skip.
        for (duckdb::idx_t col_idx = 0; col_idx < column_ids.size();
             col_idx++) {
          if (column_ids[col_idx] == bound_nn.index) {
            std::string col_name = bound_nn.index.index < columns.size()
                                     ? columns[bound_nn.index.index].name
                                     : "unknown";
            VerifyNotNullConstraint(table, chunk.data[col_idx], chunk.size(),
                                    col_name, chunk, detail);
            break;
          }
        }
        catalog_check_idx++;
        break;
      }
      case duckdb::ConstraintType::CHECK: {
        auto& bound_check = constraint->Cast<duckdb::BoundCheckConstraint>();
        std::string constraint_name;
        if (catalog_check_idx < check_constraints.size()) {
          constraint_name = check_constraints[catalog_check_idx].name;
        }
        catalog_check_idx++;
        duckdb::DataChunk mock_chunk;
        if (CreateMockChunk(table, column_ids, bound_check.bound_columns, chunk,
                            mock_chunk)) {
          VerifyCheckConstraint(context, table, *bound_check.expression,
                                mock_chunk, constraint_name, chunk, detail);
        }
        break;
      }
      default:
        break;
    }
  }
}

}  // namespace sdb::connector
