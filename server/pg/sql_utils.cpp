////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "sql_utils.h"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>

#include "pg/sql_exception_macro.h"

namespace sdb::pg {

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         duckdb::idx_t column_id) {
  for (const auto& column : table.GetColumns().Logical()) {
    if (static_cast<duckdb::idx_t>(column.Oid()) == column_id) {
      return static_cast<int16_t>(column.Logical().index + 1);
    }
  }
  return 0;
}

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint) {
  if (constraint.HasIndex()) {
    return {static_cast<int16_t>(constraint.GetIndex().index + 1)};
  }
  const auto& columns = table.GetColumns();
  std::vector<int16_t> out;
  out.reserve(constraint.GetColumnNames().size());
  for (const auto& name : constraint.GetColumnNames()) {
    // Zero is what postgres writes for a key part this relation does not list.
    out.push_back(
      columns.ColumnExists(name)
        ? static_cast<int16_t>(columns.GetColumn(name).Logical().index + 1)
        : 0);
  }
  return out;
}

std::string ConstraintName(const duckdb::Identifier& table,
                           const duckdb::Constraint& constraint) {
  if (!constraint.constraint_name.empty() ||
      constraint.type != duckdb::ConstraintType::UNIQUE) {
    return constraint.constraint_name;
  }
  return constraint.Cast<duckdb::UniqueConstraint>()
    .GetName(table)
    .GetIdentifierName();
}

}  // namespace sdb::pg
