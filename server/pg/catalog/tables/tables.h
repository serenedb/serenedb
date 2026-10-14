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

#pragma once

#include <optional>
#include <ranges>
#include <span>

#include "catalog/entry/system_table.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/utils/oids.h"
#include "pg/types.h"

namespace sdb::pg {

#include "pg/catalog/generated/tables.gen.inc"

template<typename T>
std::optional<T> NonEmpty(T value) {
  return std::ranges::empty(value) ? std::nullopt
                                   : std::optional<T>{std::move(value)};
}

template<const auto& Rows>
SystemRows<std::ranges::range_value_t<decltype(Rows)>> LoadStatic(SystemScan&) {
  return {Rows};
}

template<const auto& RowShape, const auto& Rows>
class StaticTable final
  : public SystemTableScan<SystemShapeOf<RowShape>::kSql> {
  using Item = std::ranges::range_value_t<decltype(Rows)>;

 public:
  using SystemTableScan<SystemShapeOf<RowShape>::kSql>::SystemTableScan;

  static constexpr duckdb::idx_t kRows = std::size(Rows);

  static constexpr std::tuple kSources{
    ArraySource<Item>{&LoadStatic<Rows>, {}, {}}};

  void Row(const Item& row) { this->template Emit<RowShape>(row); }
};

inline std::span<const SystemSqlColumn> SystemColumns(
  const duckdb::TableCatalogEntry& table) {
  const auto* system =
    table.internal ? dynamic_cast<const catalog::SystemTableEntry*>(&table)
                   : nullptr;
  return system ? system->Table().Sql().columns
                : std::span<const SystemSqlColumn>{};
}

inline ColumnType DescribeColumn(const duckdb::ColumnDefinition& column,
                                 std::span<const SystemSqlColumn> system) {
  auto described = DescribeColumnType(column.Type());
  if (!system.empty()) {
    const auto& sql = system[column.Logical().index];
    described.collation = sql.collation;
    described.ndims = sql.ndims;
  }
  return described;
}

inline constexpr auto kOwner = [](const auto& entry) {
  return entry.permissions.owner;
};

inline constexpr auto kAcl = [](const auto& entry) -> const auto& {
  return entry.permissions.acl;
};

using Builtins = SystemRows<BuiltinFunction, BuiltinFunctions>;

inline Builtins LoadBuiltins(SystemScan& scan) {
  auto functions = GetBuiltinFunctions(scan.Context());
  return {functions->All(), std::move(functions)};
}

inline constexpr auto kBuiltinsByOid =
  SortedBy<BuiltinFunction, &BuiltinFunction::oid, BuiltinFunctions>;

}  // namespace sdb::pg
