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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "server/utils/credits.h"

namespace sdb::pg {
namespace {

constexpr SystemSqlColumn kSerenedbLicensesColumns[] = {
  {"name", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"license", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"url", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"license_file", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"license_text", kText, 0, false, false, kCCollation, 0, SystemKey::None},
};

constexpr SystemSql kSerenedbLicensesSql{.oid = kSerenedbLicensesTable,
                                         .schema = "pg_catalog",
                                         .name = "serenedb_licenses",
                                         .relkind = 'r',
                                         .replident = 'n',
                                         .columns = kSerenedbLicensesColumns};

using credits::License;

SystemRows<License> LoadLicenses(SystemScan&) { return credits::Licenses(); }

constexpr auto kLicense = Shape<kSerenedbLicensesSql, const License>(
  Col<"name">(&License::name), Col<"license">(&License::license),
  Col<"url">(&License::url), Col<"license_file">([](const auto& row) {
    return NonEmpty<std::string_view>(row.file);
  }),
  Col<"license_text">(
    [](const auto& row) { return NonEmpty<std::string_view>(row.text); }));

}  // namespace

SystemTable gSerenedbLicenses =
  SystemTableOf<ArrayTable<kLicense, &LoadLicenses>>();

}  // namespace sdb::pg
