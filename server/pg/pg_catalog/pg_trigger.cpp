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

#include "pg/pg_catalog/pg_trigger.h"

#include <deque>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <string>
#include <vector>

#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/schema.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

// tgqual is the WHEN clause and tgoldtable/tgnewtable the REFERENCING aliases;
// a trigger that has none reports NULL, as postgres does.
constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgTrigger::tgqual),
  GetIndex(&PgTrigger::tgoldtable),
  GetIndex(&PgTrigger::tgnewtable),
});

constexpr uint64_t Bit(size_t index) { return uint64_t{1} << index; }

// postgres packs when and on what a trigger fires into one int2; the bit
// positions are its own (pg_trigger.h TRIGGER_TYPE_*), and every reader --
// information_schema.triggers included -- decodes them by that layout.
constexpr int16_t kTypeRow = 1 << 0;
constexpr int16_t kTypeBefore = 1 << 1;
constexpr int16_t kTypeInsert = 1 << 2;
constexpr int16_t kTypeDelete = 1 << 3;
constexpr int16_t kTypeUpdate = 1 << 4;
constexpr int16_t kTypeInstead = 1 << 6;

int16_t TriggerType(const duckdb::TriggerCatalogEntry& trigger) {
  int16_t type = 0;
  if (trigger.for_each == duckdb::TriggerForEach::ROW) {
    type |= kTypeRow;
  }
  switch (trigger.timing) {
    case duckdb::TriggerTiming::BEFORE:
      type |= kTypeBefore;
      break;
    case duckdb::TriggerTiming::INSTEAD_OF:
      type |= kTypeInstead;
      break;
    case duckdb::TriggerTiming::AFTER:
      // AFTER and FOR EACH STATEMENT are both the zero bit.
      break;
  }
  switch (trigger.event_type) {
    case duckdb::TriggerEventType::INSERT_EVENT:
      type |= kTypeInsert;
      break;
    case duckdb::TriggerEventType::DELETE_EVENT:
      type |= kTypeDelete;
      break;
    case duckdb::TriggerEventType::UPDATE_EVENT:
      type |= kTypeUpdate;
      break;
  }
  return type;
}

// The attnums UPDATE OF names, in the order the trigger declared them. Zero for
// a name the relation does not list, which is what postgres writes for a key
// that is not a plain column.
std::vector<int16_t> UpdateOfAttnums(const duckdb::TriggerCatalogEntry& trigger,
                                     const catalog::SereneDBTableEntry& table) {
  std::vector<int16_t> attrs;
  attrs.reserve(trigger.columns.size());
  const auto& columns = table.GetColumns();
  for (const auto& column : trigger.columns) {
    attrs.push_back(
      columns.ColumnExists(column)
        ? static_cast<int16_t>(columns.GetColumn(column).Logical().index + 1)
        : 0);
  }
  return attrs;
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgTrigger>::GetTableData() {
  std::vector<PgTrigger> values;
  std::deque<std::string> name_storage;
  std::deque<std::vector<int16_t>> tgattr_storage;

  auto& context = _config.GetClientContext();

  // The entry itself, not the info: a trigger set hangs off the entry, and
  // reading it needs a transaction against the catalog holding it.
  catalog::VisitCatalogSetEntries(
    context, GetDatabaseId(), duckdb::CatalogType::TABLE_ENTRY,
    [&](const catalog::SereneDBSchemaEntry&,
        duckdb::CatalogEntry& object_entry) {
      // Views and the index-name-as-table wrappers share this set; neither is a
      // SereneDBTableEntry, so the cast is the filter.
      auto* table_ptr =
        dynamic_cast<catalog::SereneDBTableEntry*>(&object_entry);
      if (table_ptr == nullptr) {
        return;
      }
      auto& table = *table_ptr;
      const auto relid = catalog::IdOf(table).id();
      table.ScanTriggers(
        duckdb::CatalogTransaction(table.ParentCatalog(), context),
        [&](duckdb::CatalogEntry& entry) {
          const auto& trigger = entry.Cast<duckdb::TriggerCatalogEntry>();
          name_storage.emplace_back(trigger.name.GetIdentifierName());
          tgattr_storage.push_back(UpdateOfAttnums(trigger, table));
          auto row = PgTrigger{
            // A trigger is not a serenedb catalog object, so its identity is
            // the duckdb entry's own oid.
            .oid = trigger.oid,
            .tgrelid = relid,
            .tgparentid = 0,
            .tgname = name_storage.back(),
            // The action is an inline statement rather than a call, so there is
            // no function to name.
            .tgfoid = 0,
            .tgtype = TriggerType(trigger),
            .tgenabled = PgTrigger::Tgenabled::Origin,
            .tgisinternal = false,
            .tgconstrrelid = 0,
            .tgconstrindid = 0,
            .tgconstraint = 0,
            .tgdeferrable = false,
            .tginitdeferred = false,
            .tgnargs = 0,
            .tgattr = tgattr_storage.back(),
            .tgargs = {},
          };
          if (!trigger.referencing_old_table.empty()) {
            name_storage.emplace_back(
              trigger.referencing_old_table.GetIdentifierName());
            row.tgoldtable = name_storage.back();
          }
          if (!trigger.referencing_new_table.empty()) {
            name_storage.emplace_back(
              trigger.referencing_new_table.GetIdentifierName());
            row.tgnewtable = name_storage.back();
          }
          values.push_back(row);
        });
    });

  auto result = CreateColumns<PgTrigger>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    auto mask = kNullMask;
    if (!values[row].tgoldtable.v.empty()) {
      mask &= ~Bit(GetIndex(&PgTrigger::tgoldtable));
    }
    if (!values[row].tgnewtable.v.empty()) {
      mask &= ~Bit(GetIndex(&PgTrigger::tgnewtable));
    }
    WriteData(result, values[row], mask, row, Roles());
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
