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

namespace sdb::pg {
namespace {

struct Language {
  duckdb::idx_t oid;
  std::string_view name;
  bool trusted;
  duckdb::idx_t validator;
};

constexpr std::array kLanguages{
  Language{kPgInternalLanguage, "internal", false,
           kPgInternalLanguageValidator},
  Language{kPgSqlLanguage, "sql", true, kPgSqlLanguageValidator},
};

SystemRows<Language> LoadLanguages(SystemScan&) { return {kLanguages}; }

class PgLanguage final : public SystemTableScan<kPgLanguageSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Language>{&LoadLanguages, {}}};

  static constexpr auto kLanguage = Shape<kSql, const Language>(
    Col<"oid">(&Language::oid), Col<"lanname">(&Language::name),
    Col<"lanowner">([](const auto&) { return kRootUser; }),
    Col<"lanpltrusted">(&Language::trusted),
    Col<"lanvalidator">(&Language::validator));

  void Row(const Language& language) { Emit<kLanguage>(language); }
};

}  // namespace

SystemTable gPgLanguage = SystemTableOf<PgLanguage>();

}  // namespace sdb::pg
