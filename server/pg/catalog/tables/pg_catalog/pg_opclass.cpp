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

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/tokenizer.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {duckdb::CatalogType::TOKENIZER_ENTRY};

constexpr SystemIndex kOpclassIndexes[] = {
  {kPgOpclassSql["oid"], SystemLookup::Object},
  {kPgOpclassSql["opcname"], SystemLookup::Object},
  {kPgOpclassSql["opcnamespace"], SystemLookup::Namespace},
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

class PgOpclass final : public SystemTableScan<kPgOpclassSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Opclass>{&LoadStatic<kOpclasses>, {}},
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
      Col<"opcowner">(kOwner),
      Col<"opcintype">([](const auto&) { return int64_t{kText}; }));

  void Row(const Opclass& opclass) { Emit<kBuiltin>(opclass); }

  void Row(const catalog::TokenizerCatalogEntry& tokenizer) {
    Emit<kTokenizer>(tokenizer);
  }
};

}  // namespace

SystemTable gPgOpclass = SystemTableOf<PgOpclass>();

}  // namespace sdb::pg
