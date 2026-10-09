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

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>

#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/tokenizer.h"
#include "network/pg/hba.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {duckdb::CatalogType::TOKENIZER_ENTRY};

constexpr SystemIndex kTsDictIndexes[] = {
  {kPgTsDictSql["oid"], SystemLookup::Object},
  {kPgTsDictSql["dictname"], SystemLookup::Object},
  {kPgTsDictSql["dictnamespace"], SystemLookup::Namespace},
};

constexpr SystemIndex kForeignServerIndexes[] = {
  {kPgForeignServerSql["oid"], SystemLookup::Object},
  {kPgForeignServerSql["srvname"], SystemLookup::Object},
};

constexpr SystemIndex kOpclassIndexes[] = {
  {kPgOpclassSql["oid"], SystemLookup::Object},
  {kPgOpclassSql["opcname"], SystemLookup::Object},
  {kPgOpclassSql["opcnamespace"], SystemLookup::Namespace},
};

template<typename T>
std::optional<T> NonEmpty(T value) {
  if (std::ranges::empty(value)) {
    return std::nullopt;
  }
  return value;
}

class PgTsDict final : public SystemTableScan<kPgTsDictSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kTypes, SystemSchemas::Skip, kTsDictIndexes}};

  static constexpr auto kDictionary =
    Shape<kSql, const catalog::TokenizerCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"dictname">(&duckdb::CatalogEntry::name),
      Col<"dictnamespace">(&duckdb::CatalogEntry::ParentSchemaOid),
      Col<"dictowner">(
        [](const auto& tokenizer) { return tokenizer.permissions.owner; }));

  void Row(const catalog::TokenizerCatalogEntry& tokenizer) {
    Emit<kDictionary>(tokenizer);
  }
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
      Col<"srvname">(&duckdb::CatalogEntry::name),
      Col<"srvowner">(
        [](const auto& server) { return server.permissions.owner; }),
      Col<"srvacl">([](const auto& server) -> const auto& {
        return server.permissions.acl;
      }),
      Col<"srvoptions">([](const auto& server) {
        return NonEmpty(server.Options() |
                        std::views::transform([](const auto& option) {
                          return absl::StrCat(option.first, "=", option.second);
                        }));
      }));

  void Row(const catalog::ForeignServerCatalogEntry& server) {
    Emit<kServer>(server);
  }
};

using HbaRule = network::pg::hba::RenderedRule;

SystemRows<HbaRule> LoadHbaRules(SystemScan&) {
  return network::pg::hba::RenderHbaRules();
}

class PgHbaFileRules final : public SystemTableScan<kPgHbaFileRulesSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{ArraySource<HbaRule>{&LoadHbaRules, {}}};

  static constexpr auto kRule = Shape<kSql, const HbaRule>(
    Col<"rule_number">(&HbaRule::rule_number),
    Col<"file_name">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.file_name);
    }),
    Col<"line_number">([](const auto& rule) -> std::optional<uint32_t> {
      if (rule.line_number == 0) {
        return std::nullopt;
      }
      return rule.line_number;
    }),
    Col<"type">(&HbaRule::type), Col<"database">(&HbaRule::databases),
    Col<"user_name">(&HbaRule::roles), Col<"address">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.address);
    }),
    Col<"netmask">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.netmask);
    }),
    Col<"auth_method">(&HbaRule::auth_method),
    Col<"options">(
      [](const auto& rule) { return NonEmpty(std::span{rule.options}); }));

  void Row(const HbaRule& rule) { Emit<kRule>(rule); }
};

struct Opclass {
  duckdb::idx_t oid;
  std::string_view name;
  int64_t type;
};

constexpr std::array kOpclasses{
  Opclass{kPgOpclassIvf, catalog::kIVFKind, kFloat4Array},
  Opclass{kPgOpclassHnsw, catalog::kHNSWKind, kFloat4Array},
  Opclass{kPgOpclassIncluded, catalog::kIncludedKind, kAny},
};

SystemRows<Opclass> LoadOpclasses(SystemScan&) { return {kOpclasses}; }

class PgOpclass final : public SystemTableScan<kPgOpclassSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Opclass>{&LoadOpclasses, {}},
    CatalogSource{kTypes, SystemSchemas::Skip, kOpclassIndexes}};

  static constexpr auto kOpclassMethod =
    Col<"opcmethod">([](const auto&) { return kPgAmInverted; });

  static constexpr auto kBuiltin = Shape<kSql, const Opclass>(
    Col<"oid">(&Opclass::oid), kOpclassMethod, Col<"opcname">(&Opclass::name),
    Col<"opcnamespace">([](const auto&) { return kPgCatalogSchema; }),
    Col<"opcowner">([](const auto&) { return kRootUser; }),
    Col<"opcintype">(&Opclass::type));

  static constexpr auto kTokenizer =
    Shape<kSql, const catalog::TokenizerCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid), kOpclassMethod,
      Col<"opcname">(&duckdb::CatalogEntry::name),
      Col<"opcnamespace">(&duckdb::CatalogEntry::ParentSchemaOid),
      Col<"opcowner">(
        [](const auto& tokenizer) { return tokenizer.permissions.owner; }),
      Col<"opcintype">([](const auto&) { return int64_t{kText}; }));

  void Row(const Opclass& opclass) { Emit<kBuiltin>(opclass); }

  void Row(const catalog::TokenizerCatalogEntry& tokenizer) {
    Emit<kTokenizer>(tokenizer);
  }
};

}  // namespace

SystemTable gPgTsDict = SystemTableOf<PgTsDict>();

SystemTable gPgForeignServer = SystemTableOf<PgForeignServer>();

SystemTable gPgHbaFileRules = SystemTableOf<PgHbaFileRules>();

SystemTable gPgOpclass = SystemTableOf<PgOpclass>();

}  // namespace sdb::pg
