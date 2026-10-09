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

#include "catalog/entry/database.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kDatabaseIndexes[] = {
  {kPgDatabaseSql["oid"], SystemLookup::Object},
  {kPgDatabaseSql["datname"], SystemLookup::Object},
};

class PgDatabase final : public SystemTableScan<kPgDatabaseSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSetSource{SystemCatalog::Cluster,
                     duckdb::CatalogType::DATABASE_ENTRY, kDatabaseIndexes}};

  static constexpr auto kDatabase =
    Shape<kSql, const catalog::DatabaseCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"datname">(&duckdb::CatalogEntry::name), Col<"datdba">(kOwner),
      Col<"datacl">(kAcl));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    Emit<kDatabase>(database);
  }
};

}  // namespace

SystemTable gPgDatabase = SystemTableOf<PgDatabase>();

}  // namespace sdb::pg
