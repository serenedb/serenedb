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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "server/utils/credits.h"

namespace sdb::pg {
namespace {

constexpr SystemSqlColumn kSerenedbContributorsColumns[] = {
  {"name", kText, 0, false, false, kCCollation, 0, SystemKey::None},
};

constexpr SystemSql kSerenedbContributorsSql{
  .oid = kSerenedbContributorsTable,
  .schema = "pg_catalog",
  .name = "serenedb_contributors",
  .relkind = 'r',
  .replident = 'n',
  .columns = kSerenedbContributorsColumns};

SystemRows<std::string_view> LoadContributors(SystemScan&) {
  return credits::Contributors();
}

constexpr auto kContributor =
  Shape<kSerenedbContributorsSql, const std::string_view>(
    Col<"name">([](std::string_view name) { return name; }));

}  // namespace

SystemTable gSerenedbContributors =
  SystemTableOf<ArrayTable<kContributor, &LoadContributors>>();

}  // namespace sdb::pg
