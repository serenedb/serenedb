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

#include "catalog/entry/tablespace.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kTablespaceIndexes[] = {
  {kPgTablespaceSql["spcname"], SystemLookup::Object},
};

class PgTablespace final : public SystemTableScan<kPgTablespaceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = 10;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::TABLESPACE_ENTRY,
    kTablespaceIndexes}};

  static constexpr auto kTablespace =
    Shape<kSql, const catalog::TablespaceCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"spcname">(&duckdb::CatalogEntry::name),
      Col<"spcowner">(
        [](const auto& tablespace) { return tablespace.permissions.owner; }),
      Col<"spcacl">([](const auto& tablespace) -> const auto& {
        return tablespace.permissions.acl;
      }));

  void Row(const catalog::TablespaceCatalogEntry& tablespace) {
    Emit<kTablespace>(tablespace);
  }
};

}  // namespace

SystemTable gPgTablespace = SystemTableOf<PgTablespace>();

}  // namespace sdb::pg
