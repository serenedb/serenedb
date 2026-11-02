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
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/attached_database.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <ranges>
#include <span>
#include <vector>

#include "catalog/cluster.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/acl.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"

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
    for (auto* entry : Referencing(role.oid)) {
      if (entry->type != duckdb::CatalogType::DATABASE_ENTRY) {
        Entry(entry->ParentCatalog().GetAttached().oid, role.oid, *entry);
        continue;
      }
      Entry(kInvalidOid, role.oid, *entry);
      VisitDefaultAcls(
        Context(), *entry,
        [&](duckdb::idx_t oid, const duckdb::DefaultAcl& row) {
          if (row.role == role.oid) {
            Depend(entry->oid, kPgDefaultAclTable, oid, 0, role.oid, 'o');
          } else if (Names(row.acl, role.oid)) {
            Depend(entry->oid, kPgDefaultAclTable, oid, 0, role.oid, 'a');
          }
        });
      for (const auto& wrapper : entry->permissions.foreign_data_wrappers) {
        if (Names(wrapper.acl, role.oid)) {
          Depend(entry->oid, kPgForeignDataWrapperTable, wrapper.oid, 0,
                 role.oid, 'a');
        }
      }
    }
  }

 private:
  std::span<duckdb::CatalogEntry* const> Referencing(duckdb::idx_t role) {
    if (!_scanned) {
      _scanned = true;
      auto& cluster = catalog::ClusterOf(Context());
      cluster.GetDependencyManager()->ScanAllEntries(
        cluster.GetCatalogTransaction(Context()),
        [&](duckdb::CatalogEntry& entry) {
          duckdb::ForEachRoleReference(entry, [&](duckdb::idx_t role) {
            auto& entries = _referencing[role];
            if (entries.empty() || entries.back() != &entry) {
              entries.push_back(&entry);
            }
          });
        });
      for (auto& [_, entries] : _referencing) {
        absl::c_sort(entries, [](const auto* lhs, const auto* rhs) {
          return lhs->oid < rhs->oid;
        });
      }
    }
    const auto it = _referencing.find(role);
    if (it == _referencing.end()) {
      return {};
    }
    return it->second;
  }

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
    if (entry.type == duckdb::CatalogType::VIEW_ENTRY) {
      int32_t attnum = 0;
      for (const auto& acl :
           entry.Cast<duckdb::ViewCatalogEntry>().column_acls) {
        ++attnum;
        if (Names(acl, role)) {
          Depend(database, classid, entry.oid, attnum, role, 'a');
        }
      }
      return;
    }
    if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    for (const auto& column :
         entry.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
      if (Names(column.Acl(), role)) {
        Depend(database, classid, entry.oid, Attnum(column), role, 'a');
      }
    }
  }

  void Depend(duckdb::idx_t database, duckdb::idx_t classid,
              duckdb::idx_t objid, int32_t objsubid, duckdb::idx_t role,
              char deptype) {
    Emit<kDependency>({database, classid, objid, objsubid, role, deptype});
  }

  bool _scanned = false;
  irs::containers::FlatHashMap<duckdb::idx_t,
                               std::vector<duckdb::CatalogEntry*>>
    _referencing;
};

}  // namespace

SystemTable gPgShdepend = SystemTableOf<PgShdepend>();

}  // namespace sdb::pg
