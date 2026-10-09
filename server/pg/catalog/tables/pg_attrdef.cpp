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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <ranges>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {duckdb::CatalogType::TABLE_ENTRY};

constexpr SystemIndex kIndexes[] = {
  {kPgAttrdefSql["oid"], SystemLookup::Object},
  {kPgAttrdefSql["adrelid"], SystemLookup::Object},
};

struct AttrdefRow {
  const duckdb::TableCatalogEntry& table;
  const duckdb::ColumnDefinition& column;
};

class PgAttrdef final : public SystemTableScan<kPgAttrdefSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kAttrdef = Shape<kSql, const AttrdefRow>(
    Col<"oid">([](const auto& row) { return row.column.CatalogOid(); }),
    Col<"adrelid">([](const auto& row) { return row.table.oid; }),
    Col<"adnum">([](const auto& row) { return Attnum(row.column); }),
    Col<"adbin">([](const auto& row) {
      return DefaultExpression(row.column).ToString();
    }));

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"adrelid">(table.oid)) {
      return;
    }
    for (const auto& column :
         table.GetColumns().Logical() | std::views::filter(HasAttrdef)) {
      Emit<kAttrdef>({table, column});
    }
  }
};

}  // namespace

SystemTable gPgAttrdef = SystemTableOf<PgAttrdef>();

}  // namespace sdb::pg
