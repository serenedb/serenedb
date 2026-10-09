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

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <ranges>

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypeTypes[] = {duckdb::CatalogType::TYPE_ENTRY};
constexpr duckdb::CatalogType kSequenceTypes[] = {
  duckdb::CatalogType::SEQUENCE_ENTRY};
constexpr duckdb::CatalogType kViewTypes[] = {duckdb::CatalogType::VIEW_ENTRY};

constexpr SystemIndex kEnumIndexes[] = {
  {kPgEnumSql["enumtypid"], SystemLookup::Object},
};

constexpr SystemIndex kSequenceIndexes[] = {
  {kPgSequenceSql["seqrelid"], SystemLookup::Object},
};

constexpr SystemIndex kRewriteIndexes[] = {
  {kPgRewriteSql["oid"], SystemLookup::Object},
  {kPgRewriteSql["ev_class"], SystemLookup::Object},
};

constexpr SystemIndex kTriggerIndexes[] = {
  {kPgTriggerSql["oid"], SystemLookup::Object},
  {kPgTriggerSql["tgrelid"], SystemLookup::Object},
};

struct EnumLabel {
  const duckdb::TypeCatalogEntry& entry;
  duckdb::idx_t index;
  duckdb::string_t label;
};

class PgEnum final : public SystemTableScan<kPgEnumSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypeTypes, SystemSchemas::Skip, kEnumIndexes}};

  static constexpr auto kLabel = Shape<kSql, const EnumLabel>(
    Col<"oid">(
      [](const auto& row) { return row.entry.oid * 10000 + row.index + 1; }),
    Col<"enumtypid">([](const auto& row) { return row.entry.oid; }),
    Col<"enumsortorder">([](const auto& row) { return row.index + 1; }),
    Col<"enumlabel">([](const auto& row) {
      return std::string_view{row.label.GetData(), row.label.GetSize()};
    }));

  void Row(const duckdb::TypeCatalogEntry& entry) {
    const auto& type = entry.user_type;
    if (type.id() != duckdb::LogicalTypeId::ENUM) {
      return;
    }
    const auto size = duckdb::EnumType::GetSize(type);
    for (duckdb::idx_t i = 0; i < size; ++i) {
      Emit<kLabel>({entry, i, duckdb::EnumType::GetString(type, i)});
    }
  }
};

struct Sequence {
  const duckdb::SequenceCatalogEntry& entry;
  mutable std::optional<duckdb::SequenceData> data;

  const duckdb::SequenceData& Data() const {
    if (!data) {
      data.emplace(entry.GetData());
    }
    return *data;
  }
};

class PgSequence final : public SystemTableScan<kPgSequenceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kSequenceTypes, SystemSchemas::Skip, kSequenceIndexes}};

  static constexpr auto kSequence = Shape<kSql, const Sequence>(
    Col<"seqrelid">([](const auto& row) { return row.entry.oid; }),
    Col<"seqtypid">([](const auto&) { return kInt8; }),
    Col<"seqstart">([](const auto& row) { return row.Data().start_value; }),
    Col<"seqincrement">([](const auto& row) { return row.Data().increment; }),
    Col<"seqmax">([](const auto& row) { return row.Data().max_value; }),
    Col<"seqmin">([](const auto& row) { return row.Data().min_value; }),
    Col<"seqcache">([](const auto& row) { return row.Data().cache; }),
    Col<"seqcycle">([](const auto& row) { return row.Data().cycle; }));

  void Row(duckdb::SequenceCatalogEntry& sequence) {
    if (!NumbersRows(sequence)) {
      Emit<kSequence>({sequence, {}});
    }
  }
};

class PgRewrite final : public SystemTableScan<kPgRewriteSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kViewTypes, SystemSchemas::Skip, kRewriteIndexes}};

  static constexpr auto kRule = Shape<kSql, const duckdb::ViewCatalogEntry>(
    Col<"oid">(&duckdb::CatalogEntry::oid),
    Col<"rulename">([](const auto&) { return std::string_view{"_RETURN"}; }),
    Col<"ev_class">(&duckdb::CatalogEntry::oid),
    Col<"ev_action">([](const auto& view) { return view.query->ToString(); }));

  void Row(const duckdb::ViewCatalogEntry& view) { Emit<kRule>(view); }
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

std::optional<std::string_view> TransitionTable(
  const duckdb::Identifier& name) {
  if (name.empty()) {
    return std::nullopt;
  }
  return name.GetIdentifierName();
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
      return TransitionTable(row.trigger.referencing_old_table);
    }),
    Col<"tgnewtable">([](const auto& row) {
      return TransitionTable(row.trigger.referencing_new_table);
    }));

  void Row(const duckdb::TriggerCatalogEntry& trigger) {
    if (auto table = TriggerTable(*this, trigger)) {
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

SystemTable gPgEnum = SystemTableOf<PgEnum>();

SystemTable gPgSequence = SystemTableOf<PgSequence>();

SystemTable gPgRewrite = SystemTableOf<PgRewrite>();

SystemTable gPgTrigger = SystemTableOf<PgTrigger>();

}  // namespace sdb::pg
