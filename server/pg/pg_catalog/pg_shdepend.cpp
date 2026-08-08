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

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_dependency.h"
#include "catalog/duckdb_object_index.h"
#include "catalog/duckdb_table_entry.h"
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
                             [&](const duckdb::CreateSchemaInfo&,
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
  // this table.
  const catalog::DependencyView dependents{&context};
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
  catalog::VisitRoles(&context, [&](const catalog::CreateRoleInfo& info) {
    roles.push_back(info.GetId());
  });

  std::vector<PgShdepend> values;
  for (const auto role : roles) {
    // Everything that names a role names it as owner, grantee or grantor:
    // nothing else points at one, which is why every dependent of a role is a
    // row here rather than a cascade anywhere.
    for (const auto& dependent : dependents.Dependents(role)) {
      // A database belongs to no database, which postgres writes as dbid 0.
      // Everything else is an entry of this database's own catalog, so the
      // lookup that finds it is also the database check.
      const catalog::Permissions* perm = nullptr;
      bool shared = false;
      auto database = catalog::FindDatabase(&context, dependent.id);
      auto entry = catalog::LookupEntryById(context, dependent.id);
      if (database) {
        shared = true;
        perm = &database.perm;
      } else if (entry) {
        shared = catalog::IsRoot(entry->type);
        perm = &entry->permissions;
      } else {
        continue;
      }
      const auto row = [&](PgShdepend::Deptype deptype) {
        values.push_back(PgShdepend{
          .dbid = shared ? Oid{0} : Oid{database_id.id()},
          .classid = CatalogClassOid(dependent.type),
          .objid = Oid{dependent.id.id()},
          .objsubid = 0,
          .refclassid = Oid{PgAuthid::kId},
          .refobjid = Oid{role.id()},
          .deptype = deptype,
        });
      };
      // Owner and grantee are separate rows in postgres, and an object can be
      // both.
      if (perm->owner == role) {
        row(PgShdepend::Deptype::Owner);
      }
      if (grants_name(dependent.id, perm->acl, role)) {
        row(PgShdepend::Deptype::Acl);
      }
    }
  }

  auto result = CreateColumns<PgShdepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
