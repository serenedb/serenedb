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
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <ranges>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kTriggerIndexes[] = {
  {kPgTriggerSql["oid"], SystemLookup::Object},
  {kPgTriggerSql["tgrelid"], SystemLookup::Object},
};

int16_t TriggerType(const duckdb::TriggerCatalogEntry& trigger) {
  constexpr int16_t kRow = 1 << 0;
  constexpr int16_t kBefore = 1 << 1;
  constexpr int16_t kInsert = 1 << 2;
  constexpr int16_t kDelete = 1 << 3;
  constexpr int16_t kUpdate = 1 << 4;
  constexpr int16_t kInstead = 1 << 6;
  int16_t type = 0;
  switch (trigger.for_each) {
    case duckdb::TriggerForEach::STATEMENT:
      break;
    case duckdb::TriggerForEach::ROW:
      type |= kRow;
      break;
  }
  switch (trigger.timing) {
    case duckdb::TriggerTiming::BEFORE:
      type |= kBefore;
      break;
    case duckdb::TriggerTiming::AFTER:
      break;
    case duckdb::TriggerTiming::INSTEAD_OF:
      type |= kInstead;
      break;
  }
  switch (trigger.event_type) {
    case duckdb::TriggerEventType::INSERT_EVENT:
      type |= kInsert;
      break;
    case duckdb::TriggerEventType::DELETE_EVENT:
      type |= kDelete;
      break;
    case duckdb::TriggerEventType::UPDATE_EVENT:
      type |= kUpdate;
      break;
  }
  return type;
}

struct Trigger {
  const duckdb::TableCatalogEntry& table;
  const duckdb::TriggerCatalogEntry& trigger;
};

class PgTrigger final : public SystemTableScan<kPgTriggerSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTableTypes, SystemSchemas::Skip, kTriggerIndexes}};

  static constexpr auto kTrigger = Shape<kSql, const Trigger>(
    Col<"oid">([](const auto& row) { return row.trigger.oid; }),
    Col<"tgrelid">([](const auto& row) { return row.table.oid; }),
    Col<"tgname">(
      [](const auto& row) -> const auto& { return row.trigger.name; }),
    Col<"tgtype">([](const auto& row) { return TriggerType(row.trigger); }),
    Col<"tgattr">([](const auto& row) {
      return row.trigger.columns |
             std::views::transform(
               [&table = row.table](const duckdb::Identifier& column) {
                 return table.GetColumns().ColumnExists(column)
                          ? Attnum(table.GetColumns().GetColumn(column))
                          : int16_t{0};
               });
    }),
    Col<"tgoldtable">([](const auto& row) {
      return NonEmpty<std::string_view>(
        row.trigger.referencing_old_table.GetIdentifierName());
    }),
    Col<"tgnewtable">([](const auto& row) {
      return NonEmpty<std::string_view>(
        row.trigger.referencing_new_table.GetIdentifierName());
    }));

  void Row(const duckdb::TriggerCatalogEntry& trigger) {
    if (auto table =
          SiblingTable(Transaction(), trigger, trigger.base_table->Table())) {
      Emit<kTrigger>({*table, trigger});
    }
  }

  void Row(const duckdb::TableCatalogEntry& table) {
    table.ScanTriggers(Transaction(), [&](duckdb::CatalogEntry& trigger) {
      Emit<kTrigger>({table, trigger.Cast<duckdb::TriggerCatalogEntry>()});
    });
  }
};

}  // namespace

SystemTable gPgTrigger = SystemTableOf<PgTrigger>();

}  // namespace sdb::pg
