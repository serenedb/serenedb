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

#include <string_view>

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct SharedDescription {
  duckdb::idx_t objoid;
  duckdb::idx_t classoid;
  std::string_view text;
};

class PgShdescription final : public SystemTableScan<kPgShdescriptionSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSetSource{
      SystemCatalog::Cluster, duckdb::CatalogType::DATABASE_ENTRY, {}},
    CatalogSetSource{
      SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, {}}};

  static constexpr auto kDescription = Shape<kSql, const SharedDescription>(
    Col<"objoid">(&SharedDescription::objoid),
    Col<"classoid">(&SharedDescription::classoid),
    Col<"description">(&SharedDescription::text));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    Comment(kPgDatabaseTable, database.oid, database.comment);
  }

  void Row(const catalog::RoleCatalogEntry& role) {
    Comment(kPgAuthidTable, role.oid, role.comment);
  }

 private:
  void Comment(duckdb::idx_t classoid, duckdb::idx_t objoid,
               const duckdb::Value& comment) {
    if (const auto* text = CommentText(comment)) {
      Emit<kDescription>({objoid, classoid, *text});
    }
  }
};

}  // namespace

SystemTable gPgShdescription = SystemTableOf<PgShdescription>();

}  // namespace sdb::pg
