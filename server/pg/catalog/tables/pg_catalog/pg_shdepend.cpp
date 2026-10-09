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

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/main/attached_database.hpp>
#include <ranges>

#include "catalog/cluster.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kPgShdependSql["refobjid"], SystemLookup::Object},
};

bool Names(std::span<const duckdb::AclItem> acl, duckdb::idx_t role) {
  return absl::c_any_of(acl, [&](const duckdb::AclItem& item) {
    return item.grantee == role || item.grantor == role;
  });
}

struct SharedDependency {
  duckdb::idx_t dbid;
  duckdb::idx_t classid;
  duckdb::idx_t objid;
  int32_t objsubid;
  duckdb::idx_t refobjid;
  char deptype;
};

class PgShdepend final : public SystemTableScan<kPgShdependSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, kIndexes}};

  static constexpr auto kDependency = Shape<kSql, const SharedDependency>(
    Col<"dbid">(&SharedDependency::dbid),
    Col<"classid">(&SharedDependency::classid),
    Col<"objid">(&SharedDependency::objid),
    Col<"objsubid">(&SharedDependency::objsubid),
    Col<"refclassid">([](const auto&) { return kPgAuthidTable; }),
    Col<"refobjid">(&SharedDependency::refobjid),
    Col<"deptype">(&SharedDependency::deptype));

  void Row(catalog::RoleCatalogEntry& role) {
    if (role.oid == kRootUser) {
      return;
    }
    auto& cluster = catalog::ClusterOf(Context());
    cluster.GetDependencyManager()->ScanDependentEntries(
      cluster.GetCatalogTransaction(Context()), role,
      [&](duckdb::CatalogEntry& entry) {
        if (entry.type != duckdb::CatalogType::DATABASE_ENTRY) {
          Entry(entry.ParentCatalog().GetAttached().oid, role.oid, entry);
          return;
        }
        Entry(kInvalidOid, role.oid, entry);
        VisitDefaultAcls(
          Context(), entry,
          [&](duckdb::idx_t oid, const duckdb::DefaultAcl& row) {
            if (row.role == role.oid) {
              Depend(entry.oid, kPgDefaultAclTable, oid, 0, role.oid, 'o');
            } else if (Names(row.acl, role.oid)) {
              Depend(entry.oid, kPgDefaultAclTable, oid, 0, role.oid, 'a');
            }
          });
      });
  }

 private:
  void Entry(duckdb::idx_t database, duckdb::idx_t role,
             const duckdb::CatalogEntry& entry) {
    const auto classid = CatalogClassOid(entry.type);
    if (classid == kInvalidOid ||
        entry.type == duckdb::CatalogType::INDEX_ENTRY ||
        entry.type == duckdb::CatalogType::ROLE_ENTRY) {
      return;
    }
    if (entry.permissions.owner == role) {
      Depend(database, classid, entry.oid, 0, role, 'o');
      return;
    }
    if (Names(entry.permissions.acl, role)) {
      Depend(database, classid, entry.oid, 0, role, 'a');
    }
    if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    for (const auto& column :
         entry.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical() |
           std::views::filter([&](const duckdb::ColumnDefinition& column) {
             return Names(column.Acl(), role);
           })) {
      Depend(database, classid, entry.oid, Attnum(column), role, 'a');
    }
  }

  void Depend(duckdb::idx_t database, duckdb::idx_t classid,
              duckdb::idx_t objid, int32_t objsubid, duckdb::idx_t role,
              char deptype) {
    Emit<kDependency>({database, classid, objid, objsubid, role, deptype});
  }
};

}  // namespace

SystemTable gPgShdepend = SystemTableOf<PgShdepend>();

}  // namespace sdb::pg
