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

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>

#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kViewTypes[] = {duckdb::CatalogType::VIEW_ENTRY};

constexpr SystemIndex kRewriteIndexes[] = {
  {kPgRewriteSql["oid"], SystemLookup::Object},
  {kPgRewriteSql["ev_class"], SystemLookup::Object},
};

class PgRewrite final : public SystemTableScan<kPgRewriteSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kViewTypes, SystemSchemas::Skip, kRewriteIndexes}};

  static constexpr auto kRule = Shape<kSql, const duckdb::ViewCatalogEntry>(
    Col<"oid">(&duckdb::CatalogEntry::oid),
    Col<"rulename">([](const auto&) { return std::string_view{"_RETURN"}; }),
    Col<"ev_class">(&duckdb::CatalogEntry::oid),
    Col<"ev_action">([](const auto& view) { return view.query->ToString(); }));

  void Row(const duckdb::ViewCatalogEntry& view) { Emit<kRule>(view); }
};

}  // namespace

SystemTable gPgRewrite = SystemTableOf<PgRewrite>();

}  // namespace sdb::pg
