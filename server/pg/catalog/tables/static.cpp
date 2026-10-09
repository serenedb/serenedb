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

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

struct Am {
  duckdb::idx_t oid;
  std::string_view name;
  char type;
  duckdb::idx_t handler;
};

constexpr std::array kAms{
  Am{kPgAmHeap, "heap", 't', kPgAmHeapHandler},
  Am{kPgAmInverted, "inverted", 'i', kInvalidOid},
  Am{kPgAmIResearch, "iresearch", 't', kInvalidOid},
  Am{kPgAmSecondary, "secondary", 'i', kInvalidOid},
};

SystemRows<Am> LoadAms(SystemScan&) { return {kAms}; }

class PgAm final : public SystemTableScan<kPgAmSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{ArraySource<Am>{&LoadAms, {}}};

  static constexpr auto kAm = Shape<kSql, const Am>(
    Col<"oid">(&Am::oid), Col<"amname">(&Am::name),
    Col<"amhandler">(&Am::handler), Col<"amtype">(&Am::type));

  void Row(const Am& am) { Emit<kAm>(am); }
};

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

struct Tablespace {
  duckdb::idx_t oid;
  std::string_view name;
};

constexpr std::array kTablespaces{
  Tablespace{kPgDefaultTablespace, "pg_default"},
  Tablespace{kPgGlobalTablespace, "pg_global"},
};

SystemRows<Tablespace> LoadTablespaces(SystemScan&) { return {kTablespaces}; }

class PgTablespace final : public SystemTableScan<kPgTablespaceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Tablespace>{&LoadTablespaces, {}}};

  static constexpr auto kTablespace = Shape<kSql, const Tablespace>(
    Col<"oid">(&Tablespace::oid), Col<"spcname">(&Tablespace::name),
    Col<"spcowner">([](const auto&) { return kRootUser; }));

  void Row(const Tablespace& tablespace) { Emit<kTablespace>(tablespace); }
};

SystemRows<BuiltinCollation> LoadCollations(SystemScan&) {
  return BuiltinCollations();
}

std::optional<std::string_view> CollationLocale(
  const BuiltinCollation& collation) {
  if (collation.locale.empty()) {
    return std::nullopt;
  }
  return collation.locale;
}

class PgCollation final : public SystemTableScan<kPgCollationSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinCollation>{&LoadCollations, {}}};

  static constexpr auto kCollation = Shape<kSql, const BuiltinCollation>(
    Col<"oid">(&BuiltinCollation::oid),
    Col<"collname">(&BuiltinCollation::name),
    Col<"collnamespace">([](const auto&) { return kPgCatalogSchema; }),
    Col<"collowner">([](const auto&) { return kRootUser; }),
    Col<"collprovider">(&BuiltinCollation::provider),
    Col<"collcollate">(&CollationLocale), Col<"collctype">(&CollationLocale));

  void Row(const BuiltinCollation& collation) { Emit<kCollation>(collation); }
};

}  // namespace

SystemTable gPgAm = SystemTableOf<PgAm>();

SystemTable gPgLanguage = SystemTableOf<PgLanguage>();

SystemTable gPgTablespace = SystemTableOf<PgTablespace>();

SystemTable gPgCollation = SystemTableOf<PgCollation>();

}  // namespace sdb::pg
