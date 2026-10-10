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

#include <absl/strings/str_cat.h>

#include <ranges>

#include "catalog/entry/foreign_server.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kForeignServerIndexes[] = {
  {kPgForeignServerSql["oid"], SystemLookup::Object},
  {kPgForeignServerSql["srvname"], SystemLookup::Object},
};

class PgForeignServer final : public SystemTableScan<kPgForeignServerSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Database, duckdb::CatalogType::FOREIGN_SERVER_ENTRY,
    kForeignServerIndexes}};

  static constexpr auto kServer =
    Shape<kSql, const catalog::ForeignServerCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"srvname">(&duckdb::CatalogEntry::name), Col<"srvowner">(kOwner),
      Col<"srvfdw">([](const auto& server) {
        const auto* wrapper = FindForeignDataWrapper(server.FdwName());
        return wrapper ? wrapper->oid : kInvalidOid;
      }),
      Col<"srvacl">(kAcl), Col<"srvoptions">([](const auto& server) {
        return NonEmpty(server.Options() |
                        std::views::transform([](const auto& option) {
                          return absl::StrCat(option.first, "=", option.second);
                        }));
      }));

  void Row(const catalog::ForeignServerCatalogEntry& server) {
    Emit<kServer>(server);
  }
};

}  // namespace

SystemTable gPgForeignServer = SystemTableOf<PgForeignServer>();

}  // namespace sdb::pg
