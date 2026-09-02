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

#include "pg/pg_catalog/pg_shdepend.h"

#include <algorithm>
#include <duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "catalog/role.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/pg_catalog/pg_depend.h"

namespace sdb::pg {
namespace {

bool AclNames(catalog::AclView acl, ObjectId role) {
  return std::ranges::any_of(acl, [&](const catalog::AclItem& item) {
    return item.grantee == role || item.grantor == role;
  });
}

// The per-column grants of every table in the database, keyed by the table.
// A column grant makes the table name its grantee, and only the table's own
// definition knows about it.
using ColumnAclsByTable =
  containers::FlatHashMap<ObjectId, const catalog::ColumnAcls*>;

ColumnAclsByTable CollectColumnAcls(duckdb::ClientContext& context,
                                    ObjectId database) {
  ColumnAclsByTable out;
  catalog::VisitTableEntries(context, database,
                             [&](const catalog::SereneDBSchemaEntry&,
                                 const catalog::SereneDBTableEntry& table) {
                               if (!table.GetColumnAcls().empty()) {
                                 out.emplace(catalog::IdOf(table),
                                             &table.GetColumnAcls());
                               }
                             });
  return out;
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgShdepend>::GetTableData() {
  auto& context = _config.GetClientContext();
  const auto database_id = GetDatabaseId();
  // The same reverse index DROP ROLE consults: an object that names a role is
  // exactly an edge with the Block verb, which is the verb pg_depend leaves to
  // this table. Resolved once, not per role.
  auto dependents = catalog::EdgeAttachments(context);
  const auto column_acls = CollectColumnAcls(context, database_id);

  // Whether the dependent's grants name the role anywhere. Postgres records
  // one pg_shdepend row per (object, role, deptype), not one per privilege, so
  // a column grant on a table counts as the table naming the role.
  const auto grants_name = [&](ObjectId id, catalog::AclView acl,
                               ObjectId role) {
    if (AclNames(acl, role)) {
      return true;
    }
    const auto it = column_acls.find(id);
    return it != column_acls.end() &&
           std::ranges::any_of(*it->second, [&](const auto& entry) {
             return AclNames(entry.acl, role);
           });
  };

  // Collected before anything is resolved: reading an edge's dependent opens
  // the role set this walk is holding, and the lock behind it is not recursive.
  std::vector<ObjectId> roles;
  catalog::VisitRoles(&context, [&](const catalog::SereneDBRoleEntry& info) {
    roles.push_back(info.GetId());
  });

  std::vector<PgShdepend> values;
  for (const auto role : roles) {
    // Everything that names a role names it as owner, grantee or grantor:
    // nothing else points at one, which is why every dependent of a role is a
    // row here rather than a cascade anywhere.
    dependents.ScanDependents(
      catalog::DependencyInfo(role),
      [&](duckdb::optional_ptr<duckdb::CatalogEntry> dependent,
          duckdb::DependencyEntry& edge) {
        const auto id = catalog::DependencyInfoId(edge.EntryInfo());
        if (!dependent || !id.isSet()) {
          return;
        }
        // A database belongs to no database, which postgres writes as dbid 0.
        // Everything else is an entry of this database's own catalog, so the
        // lookup that finds it is also the database check.
        const catalog::Permissions* perm = nullptr;
        bool shared = false;
        auto database = catalog::FindDatabase(&context, id);
        auto held = catalog::LookupEntryById(context, id);
        if (database) {
          shared = true;
          perm = &database->permissions;
        } else if (held) {
          shared = catalog::IsRoot(held->type);
          perm = &held->permissions;
        } else {
          return;
        }
        const auto row = [&](PgShdepend::Deptype deptype) {
          values.push_back(PgShdepend{
            .dbid = shared ? Oid{0} : Oid{database_id.id()},
            .classid = CatalogClassOid(dependent->type),
            .objid = Oid{id.id()},
            .objsubid = 0,
            .refclassid = Oid{PgAuthid::kId},
            .refobjid = Oid{role.id()},
            .deptype = deptype,
          });
        };
        // Owner and grantee are separate rows in postgres, and an object can
        // be both.
        if (perm->owner == role) {
          row(PgShdepend::Deptype::Owner);
        }
        if (grants_name(id, perm->acl, role)) {
          row(PgShdepend::Deptype::Acl);
        }
      });
  }

  auto result = CreateColumns<PgShdepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
