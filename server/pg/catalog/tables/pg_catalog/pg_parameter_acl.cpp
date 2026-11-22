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

#include "catalog/entry/parameter.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

class PgParameterAcl final : public SystemTableScan<kPgParameterAclSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::PARAMETER_ENTRY, {}}};

  static constexpr auto kParameter =
    Shape<kSql, const catalog::ParameterCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"parname">(&duckdb::CatalogEntry::name),
      Col<"paracl">(
        [](const auto& parameter) { return AclOf(parameter.permissions); }));

  void Row(const catalog::ParameterCatalogEntry& parameter) {
    Emit<kParameter>(parameter);
  }
};

}  // namespace

SystemTable gPgParameterAcl = SystemTableOf<PgParameterAcl>();

}  // namespace sdb::pg
