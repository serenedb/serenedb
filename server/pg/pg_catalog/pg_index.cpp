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

#include "pg/pg_catalog/pg_index.h"

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/schema.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgIndex::indexrelid),
  GetIndex(&PgIndex::indrelid),
  GetIndex(&PgIndex::indnatts),
  GetIndex(&PgIndex::indnkeyatts),
  GetIndex(&PgIndex::indisunique),
  GetIndex(&PgIndex::indnullsnotdistinct),
  GetIndex(&PgIndex::indisprimary),
  GetIndex(&PgIndex::indisexclusion),
  GetIndex(&PgIndex::indimmediate),
  GetIndex(&PgIndex::indisclustered),
  GetIndex(&PgIndex::indisvalid),
  GetIndex(&PgIndex::indcheckxmin),
  GetIndex(&PgIndex::indisready),
  GetIndex(&PgIndex::indislive),
  GetIndex(&PgIndex::indisreplident),
  GetIndex(&PgIndex::indkey),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgIndex>::GetTableData() {
  std::vector<PgIndex> values;
  std::vector<std::vector<int16_t>> indkey_storage;

  auto& context = _config.GetClientContext();

  // Every base table of the database, by id: an index row needs the attnums of
  // the relation it hangs off, and the synthetic rows below are that relation's
  // own key constraints.
  containers::FlatHashMap<ObjectId, const catalog::SereneDBTableEntry*> tables;
  catalog::VisitTableEntries(context, GetDatabaseId(),
                             [&](const catalog::SereneDBSchemaEntry&,
                                 const catalog::SereneDBTableEntry& table) {
                               tables.emplace(catalog::IdOf(table), &table);
                             });

  // Explicit user-created indexes
  catalog::Visit<catalog::SereneDBIndexEntry>(
    &context, GetDatabaseId(), [&](const catalog::SereneDBIndexEntry& entry) {
      const auto record = entry.GetInfo();
      const auto& index = record->Cast<catalog::CreateIndexInfo>();
      const auto& column_ids = index.GetColumns();
      auto natts = static_cast<int16_t>(column_ids.size());

      // Build indkey: map column IDs to 1-based attnum in the parent table
      std::vector<int16_t> indkey;
      indkey.reserve(column_ids.size());

      // An index over a view has no attnums of its own to report.
      const auto table = tables.find(index.GetRelationId());
      if (table != tables.end()) {
        for (auto col_id : column_ids) {
          indkey.push_back(catalog::TableEntryAttnum(*table->second, col_id));
        }
      }
      const bool is_unique_index = index.IsUnique();
      indkey_storage.push_back(std::move(indkey));
      values.push_back({
        .indexrelid = index.GetId().id(),
        .indrelid = index.GetRelationId().id(),
        .indnatts = natts,
        .indnkeyatts = natts,
        .indisunique = is_unique_index,
        .indnullsnotdistinct = false,
        .indisprimary = false,
        .indisexclusion = false,
        .indimmediate = true,
        .indisclustered = false,
        // indisvalid = usable for QUERIES; indisready = maintained by WRITES
        // (pg_index semantics). An index only becomes visible when the
        // transaction that built it commits, and by then the build is done, so
        // every row this projection can produce is both valid and ready.
        .indisvalid = true,
        .indcheckxmin = false,
        .indisready = true,
        .indislive = true,
        .indisreplident = false,
        .indkey = indkey_storage.back(),
      });
    });

  // Synthetic indexes for the key constraints: postgres backs every PRIMARY
  // KEY and UNIQUE with an index of its own, whose oid is the one the
  // constraint carries and which pg_constraint.conindid and pg_class.oid both
  // name. Primary keys first, then the uniques, so the rows stay grouped the
  // way the tables that read them expect.
  const auto emit_keys = [&](bool primary) {
    catalog::VisitTableEntries(
      context, GetDatabaseId(),
      [&](const catalog::SereneDBSchemaEntry&,
          const catalog::SereneDBTableEntry& table) {
        for (const auto& constraint : table.GetConstraints()) {
          if (constraint->type != duckdb::ConstraintType::UNIQUE) {
            continue;
          }
          const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
          if (unique.IsPrimaryKey() != primary) {
            continue;
          }
          auto indkey = catalog::KeyConstraintAttnums(table, unique);
          auto natts = static_cast<int16_t>(indkey.size());
          indkey_storage.push_back(std::move(indkey));
          values.push_back({
            .indexrelid = unique.host_index_id,
            .indrelid = catalog::IdOf(table).id(),
            .indnatts = natts,
            .indnkeyatts = natts,
            .indisunique = true,
            .indnullsnotdistinct = false,
            .indisprimary = primary,
            .indisexclusion = false,
            .indimmediate = true,
            .indisclustered = false,
            .indisvalid = true,
            .indcheckxmin = false,
            .indisready = true,
            .indislive = true,
            .indisreplident = false,
            .indkey = indkey_storage.back(),
          });
        }
      });
  };
  emit_keys(true);
  emit_keys(false);

  auto result = CreateColumns<PgIndex>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
