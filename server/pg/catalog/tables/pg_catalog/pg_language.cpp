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

#include <span>

#include "catalog/entry/database.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct LanguageRow {
  const SystemLanguage& language;
  std::span<const duckdb::AclItem> acl;
};

class PgLanguage final : public SystemTableScan<kPgLanguageSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = std::size(kSystemLanguages);

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::DATABASE_ENTRY, {}}};

  static constexpr auto kLanguage = Shape<kSql, const LanguageRow>(
    Col<"oid">([](const auto& row) { return row.language.oid; }),
    Col<"lanname">([](const auto& row) { return row.language.name; }),
    Col<"lanowner">([](const auto&) { return kRootUser; }),
    Col<"lanpltrusted">([](const auto& row) { return row.language.trusted; }),
    Col<"lanvalidator">([](const auto& row) { return row.language.validator; }),
    Col<"lanacl">([](const auto& row) { return row.acl; }));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    if (database.oid != Database().GetAttached().oid) {
      return;
    }
    for (const auto& language : kSystemLanguages) {
      Emit<kLanguage>(
        {language, database.permissions.LanguageAcl(language.oid)});
    }
  }
};

}  // namespace

SystemTable gPgLanguage = SystemTableOf<PgLanguage>();

}  // namespace sdb::pg
