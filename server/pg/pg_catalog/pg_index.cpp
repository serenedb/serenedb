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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog1/entry/inverted_index.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_utils.h"
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
MaterializedData SystemTableSnapshot<PgIndex>::GetTableData() {
  std::vector<PgIndex> values;
  std::vector<std::vector<int16_t>> indkey_storage;

  auto& context = _context;

  // Explicit user-created indexes
  VisitEntries<duckdb::DuckIndexEntry>(
    context, GetDatabase(), [&](const duckdb::DuckIndexEntry& entry) {
      // The relation the index hangs off, by the only handle the entry
      // carries: its schema and table name.
      auto host = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
        context,
        duckdb::QualifiedName{GetDatabase().GetName(), entry.GetSchemaName(),
                              entry.GetTableName()},
        duckdb::OnEntryNotFound::RETURN_NULL);
      const auto& column_ids = entry.column_ids;
      auto natts = static_cast<int16_t>(column_ids.size());

      // Build indkey: map column IDs to 1-based attnum in the parent table
      std::vector<int16_t> indkey;
      indkey.reserve(column_ids.size());

      // An index over a view has no attnums of its own to report.
      if (host) {
        for (auto col_id : column_ids) {
          indkey.push_back(TableEntryAttnum(*host, col_id));
        }
      }
      const bool is_unique_index = entry.IsUnique();
      indkey_storage.push_back(std::move(indkey));
      values.push_back({
        .indexrelid = entry.oid,
        .indrelid = host ? host->oid : 0,
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
    VisitEntries<duckdb::TableCatalogEntry>(
      context, GetDatabase(), [&](const duckdb::TableCatalogEntry& table) {
        const auto& constraints = table.GetConstraints();
        for (size_t position = 0; position != constraints.size(); ++position) {
          if (constraints[position]->type != duckdb::ConstraintType::UNIQUE) {
            continue;
          }
          const auto& unique =
            constraints[position]->Cast<duckdb::UniqueConstraint>();
          if (unique.IsPrimaryKey() != primary) {
            continue;
          }
          auto indkey = KeyConstraintAttnums(table, unique);
          auto natts = static_cast<int16_t>(indkey.size());
          indkey_storage.push_back(std::move(indkey));
          values.push_back({
            .indexrelid = KeyIndexOid(table.oid, position),
            .indrelid = table.oid,
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
