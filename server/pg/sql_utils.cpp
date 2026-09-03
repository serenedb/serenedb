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
#include <duckdb/parser/constraints/unique_constraint.hpp>

#include "pg/sql_exception_macro.h"

namespace sdb::pg {

std::string_view ToPgObjectTypeName(duckdb::CatalogType t) noexcept {
  switch (t) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      return "table";
    case SCHEMA_ENTRY:
      return "schema";
    case VIEW_ENTRY:
      return "view";
    case INDEX_ENTRY:
      return "index";
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return "function";
    case TYPE_ENTRY:
      return "type";
    case SEQUENCE_ENTRY:
      return "sequence";
    case DATABASE_ENTRY:
      return "database";
    case TOKENIZER_ENTRY:
      return "text search dictionary";
    case FOREIGN_SERVER_ENTRY:
      return "foreign server";
    case ROLE_ENTRY:
      return "role";
    default:
      return "object";
  }
}

ObjectName ParseObjectName(std::string_view name,
                           std::string_view default_schema) {
  const auto pos = name.find('.');
  auto schema_name =
    pos == std::string_view::npos ? default_schema : name.substr(0, pos);
  auto object_name =
    pos == std::string_view::npos ? name : name.substr(pos + 1);
  return {.schema = schema_name, .relation = object_name};
}

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

}  // namespace sdb::pg
