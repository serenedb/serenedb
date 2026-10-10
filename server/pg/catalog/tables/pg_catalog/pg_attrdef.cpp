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

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <memory>
#include <mutex>
#include <ranges>
#include <string>

#include "catalog/catalog.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kPgAttrdefSql["oid"], SystemLookup::Object},
  {kPgAttrdefSql["adrelid"], SystemLookup::Object},
};

struct AttrdefRow {
  const duckdb::TableCatalogEntry& table;
  const duckdb::ColumnDefinition& column;
  const std::string* text;
};

class PgAttrdef final : public SystemTableScan<kPgAttrdefSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTableTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kAttrdef = Shape<kSql, const AttrdefRow>(
    Col<"oid">([](const auto& row) { return row.column.CatalogOid(); }),
    Col<"adrelid">([](const auto& row) { return row.table.oid; }),
    Col<"adnum">([](const auto& row) { return Attnum(row.column); }),
    Col<"adbin">([](const auto& row) {
      return row.text ? *row.text : DefaultExpression(row.column).ToString();
    }));

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"adrelid">(table.oid)) {
      return;
    }
    const auto* defaults = Defaults(table);
    for (const auto& column :
         table.GetColumns().Logical() | std::views::filter(HasAttrdef)) {
      const std::string* text = nullptr;
      if (defaults) {
        if (const auto it = defaults->find(&column); it != defaults->end()) {
          text = &it->second;
        }
      } else {
        ++_rendered;
      }
      Emit<kAttrdef>({table, column, text});
    }
  }

 private:
  static constexpr uint32_t kRenderedBeforeCache = 64;

  const decltype(catalog::CatalogSnapshot::defaults)* Defaults(
    const duckdb::TableCatalogEntry& table) {
    if (!Reads<"adbin">() || _rendered < kRenderedBeforeCache) {
      return nullptr;
    }
    if (table.ParentSchemaOid() != _schema) {
      _schema = table.ParentSchemaOid();
      auto& schema = table.ParentSchema(Transaction());
      _snapshot =
        schema.ParentCatalog().Cast<catalog::SereneDBCatalog>().Snapshot(
          Context(),
          schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(table.type));
      if (_snapshot) {
        std::call_once(_snapshot->defaults_once, [&] {
          for (auto* entry : _snapshot->entries) {
            if (entry->type != duckdb::CatalogType::TABLE_ENTRY) {
              continue;
            }
            for (const auto& column : entry->Cast<duckdb::TableCatalogEntry>()
                                          .GetColumns()
                                          .Logical() |
                                        std::views::filter(HasAttrdef)) {
              _snapshot->defaults.emplace(&column,
                                          DefaultExpression(column).ToString());
            }
          }
        });
      }
    }
    return _snapshot ? &_snapshot->defaults : nullptr;
  }

  uint32_t _rendered = 0;
  duckdb::idx_t _schema = kInvalidOid;
  std::shared_ptr<const catalog::CatalogSnapshot> _snapshot;
};

}  // namespace

SystemTable gPgAttrdef = SystemTableOf<PgAttrdef>();

}  // namespace sdb::pg
