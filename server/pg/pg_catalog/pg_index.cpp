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
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
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
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgIndex> values;
  std::vector<std::vector<int16_t>> indkey_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    SDB_ASSERT(schema);

    // Explicit user-created indexes
    for (const auto& index_ptr :
         catalog->GetIndexes(GetDatabaseId(), schema->GetName())) {
      SDB_ASSERT(index_ptr);
      auto& index = *index_ptr;
      const auto& column_ids = index.GetColumns();
      auto natts = static_cast<int16_t>(column_ids.size());

      // Build indkey: map column IDs to 1-based attnum in the parent table
      std::vector<int16_t> indkey;
      indkey.reserve(column_ids.size());

      auto table_obj = catalog->GetObject(index.GetRelationId());
      if (table_obj && table_obj->GetType() == catalog::ObjectType::Table) {
        auto& table = basics::downCast<catalog::Table>(*table_obj);
        for (auto col_id : column_ids) {
          const auto pos = table.ColumnPosById(col_id);
          indkey.push_back(
            pos < table.Columns().size() ? static_cast<int16_t>(pos + 1) : 0);
        }
      }
      bool is_unique_index =
        index.GetType() == catalog::ObjectType::SecondaryIndex &&
        basics::downCast<catalog::SecondaryIndex>(index).IsUnique();
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
        .indisvalid = true,
        .indcheckxmin = false,
        .indisready = true,
        .indislive = true,
        .indisreplident = false,
        .indkey = indkey_storage.back(),
      });
    }

    // Synthetic indexes for primary keys (PG semantics: each PK has a backing
    // index). Use the table's OID as both indexrelid and indrelid so it lines
    // up with pg_constraint.conindid and pg_class lookups.
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      auto& pk_columns = table->PKColumns();
      if (pk_columns.empty()) {
        continue;
      }
      std::vector<int16_t> indkey;
      indkey.reserve(pk_columns.size());
      for (auto pk_id : pk_columns) {
        const auto pos = table->ColumnPosById(pk_id);
        indkey.push_back(
          pos < table->Columns().size() ? static_cast<int16_t>(pos + 1) : 0);
      }
      auto natts = static_cast<int16_t>(indkey.size());
      indkey_storage.push_back(std::move(indkey));
      values.push_back({
        .indexrelid = PkIndexOid(table->GetId().id()),
        .indrelid = table->GetId().id(),
        .indnatts = natts,
        .indnkeyatts = natts,
        .indisunique = true,
        .indnullsnotdistinct = false,
        .indisprimary = true,
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

    // Synthetic indexes for UNIQUE constraints (PG: each UNIQUE has a backing
    // index). indexrelid matches pg_constraint.conindid and pg_class.oid.
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      const auto& uniques = table->UniqueConstraints();
      for (size_t uq_idx = 0; uq_idx < uniques.size(); ++uq_idx) {
        std::vector<int16_t> indkey;
        indkey.reserve(uniques[uq_idx].columns.size());
        for (auto col_id : uniques[uq_idx].columns) {
          const auto pos = table->ColumnPosById(col_id);
          indkey.push_back(
            pos < table->Columns().size() ? static_cast<int16_t>(pos + 1) : 0);
        }
        auto natts = static_cast<int16_t>(indkey.size());
        indkey_storage.push_back(std::move(indkey));
        values.push_back({
          .indexrelid = UniqueIndexOid(table->GetId().id(), uq_idx),
          .indrelid = table->GetId().id(),
          .indnatts = natts,
          .indnkeyatts = natts,
          .indisunique = true,
          .indnullsnotdistinct = false,
          .indisprimary = false,
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
    }
  }

  auto result = CreateColumns<PgIndex>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
