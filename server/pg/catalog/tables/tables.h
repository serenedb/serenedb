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
#include "pg/catalog/oids.h"
#include "pg/types/mapping.h"

namespace sdb::pg {

#include "pg/catalog/generated/table_declarations.gen.inc"
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

template<const auto& RowShape, auto Load,
         duckdb::idx_t Rows = SystemScan::kRows>
class ArrayTable final : public SystemTableScan<SystemShapeOf<RowShape>::kSql> {
  using Item = std::remove_const_t<typename SystemShapeOf<RowShape>::Context>;

 public:
  using SystemTableScan<SystemShapeOf<RowShape>::kSql>::SystemTableScan;

  static constexpr duckdb::idx_t kRows = Rows;

  static constexpr std::tuple kSources{ArraySource<Item>{Load, {}, {}}};

  void Row(const Item& row) { this->template Emit<RowShape>(row); }
};

template<const auto& RowShape, const auto& Rows>
using StaticTable = ArrayTable<RowShape, &LoadStatic<Rows>, std::size(Rows)>;

inline std::span<const SystemSqlColumn> SystemColumns(
  const duckdb::TableCatalogEntry& table) {
  const auto* system = table.internal ? FindSystemTable(table.oid) : nullptr;
  return system ? system->Sql().columns : std::span<const SystemSqlColumn>{};
}

inline ColumnType DescribeColumn(const duckdb::LogicalType& type,
                                 const SystemSqlColumn* system) {
  auto described = DescribeColumnType(type);
  if (system) {
    described.collation = system->collation;
    described.ndims = system->ndims;
  }
  return described;
}

using Builtins = SystemRows<BuiltinFunction, BuiltinFunctions>;

inline Builtins LoadBuiltins(SystemScan& scan) {
  auto functions = GetBuiltinFunctions(scan.Context());
  return {functions->All(), std::move(functions)};
}

inline constexpr auto kBuiltinsByOid =
  SortedBy<BuiltinFunction, &BuiltinFunction::oid, BuiltinFunctions>;

}  // namespace sdb::pg
