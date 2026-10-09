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

#include "catalog/entry/tokenizer.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {duckdb::CatalogType::TOKENIZER_ENTRY};

constexpr SystemIndex kTsDictIndexes[] = {
  {kPgTsDictSql["oid"], SystemLookup::Object},
  {kPgTsDictSql["dictname"], SystemLookup::Object},
  {kPgTsDictSql["dictnamespace"], SystemLookup::Namespace},
};

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

}  // namespace

SystemTable gPgTsDict = SystemTableOf<PgTsDict>();

}  // namespace sdb::pg
