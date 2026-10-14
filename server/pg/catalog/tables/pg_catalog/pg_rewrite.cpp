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
#include <iresearch/utils/static_strings.hpp>

#include "pg/catalog/engine/registry.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kRewriteIndexes[] = {
  {kPgRewriteSql["oid"], SystemLookup::Object},
  {kPgRewriteSql["ev_class"], SystemLookup::Object},
};

SystemRows<SystemObject> LoadNativeViews(SystemScan&) {
  static const auto kViews = [] {
    std::vector<SystemObject> views;
    for (const auto schema : {irs::StaticStrings::kPgCatalogSchema,
                              irs::StaticStrings::kInformationSchema}) {
      VisitSystemTables(schema, [&](const SystemTable& table) {
        if (table.Sql().relkind == 'v') {
          views.emplace_back(table.Sql().oid, table.Sql().name);
        }
      });
    }
    return views;
  }();
  return std::span{kViews};
}

constexpr std::tuple kReturnRule{
  Col<"rulename">([](const auto&) { return std::string_view{"_RETURN"}; }),
  Col<"ev_type">([](const auto&) { return '1'; }),
  Col<"ev_enabled">([](const auto&) { return 'O'; }),
  Col<"is_instead">([](const auto&) { return true; }),
  Col<"ev_qual">([](const auto&) { return std::string_view{"<>"}; })};

class PgRewrite final : public SystemTableScan<kPgRewriteSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kViewTypes, SystemSchemas::Visit, kRewriteIndexes},
    ArraySource<SystemObject>{&LoadNativeViews, {}, {}}};

  static constexpr auto kRule = Shape<kSql, const duckdb::ViewCatalogEntry>(
    kReturnRule, Col<"oid">(&duckdb::CatalogEntry::oid),
    Col<"ev_class">(&duckdb::CatalogEntry::oid),
    Col<"ev_action">([](const auto& view) { return view.query->ToString(); }));

  static constexpr auto kNativeRule =
    Shape<kSql, const SystemObject>(kReturnRule, Col<"oid">(&SystemObject::oid),
                                    Col<"ev_class">(&SystemObject::oid));

  void Row(const duckdb::ViewCatalogEntry& view) { Emit<kRule>(view); }
  void Row(const SystemObject& view) { Emit<kNativeRule>(view); }
};

}  // namespace

SystemTable gPgRewrite = SystemTableOf<PgRewrite>();

}  // namespace sdb::pg
