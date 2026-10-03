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

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <span>
#include <vector>

#include "catalog/cluster.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_database.h"
#include "pg/pg_catalog/pg_default_acl.h"
#include "pg/pg_catalog/pg_foreign_server.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_ts_dict.h"
#include "pg/pg_catalog/pg_type.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

uint64_t ClassOf(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  if (type == SCHEMA_ENTRY) {
    return PgNamespace::kId;
  }
  if (type == TABLE_ENTRY || type == VIEW_ENTRY || type == SEQUENCE_ENTRY) {
    return PgClass::kId;
  }
  if (type == TYPE_ENTRY) {
    return PgType::kId;
  }
  if (type == MACRO_ENTRY || type == TABLE_MACRO_ENTRY) {
    return PgProc::kId;
  }
  if (type == TOKENIZER_ENTRY) {
    return PgTsDict::kId;
  }
  if (type == FOREIGN_SERVER_ENTRY) {
    return PgForeignServer::kId;
  }
  if (type == DATABASE_ENTRY) {
    return PgDatabase::kId;
  }
  return kInvalidOid;
}

bool Names(std::span<const duckdb::AclItem> acl, duckdb::idx_t role) {
  return absl::c_any_of(acl, [&](const duckdb::AclItem& item) {
    return item.grantee == role || item.grantor == role;
  });
}

class Rows {
 public:
  Rows(std::vector<PgShdepend>& values, duckdb::idx_t role)
    : _values{values}, _role{role} {}

  void Entry(duckdb::idx_t database, duckdb::CatalogEntry& entry) {
    const auto classid = ClassOf(entry.type);
    if (classid == kInvalidOid) {
      return;
    }
    if (entry.permissions.owner == _role) {
      Emit(database, classid, entry.oid, 0, PgShdepend::Deptype::Owner);
    }
    if (Names(entry.permissions.acl, _role)) {
      Emit(database, classid, entry.oid, 0, PgShdepend::Deptype::Acl);
    }
    if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    const auto& table = entry.Cast<duckdb::TableCatalogEntry>();
    for (const auto& column : table.GetColumns().Logical()) {
      if (Names(column.Acl(), _role)) {
        Emit(database, classid, entry.oid,
             static_cast<int32_t>(column.Logical().index + 1),
             PgShdepend::Deptype::Acl);
      }
    }
  }

  void Defaults(duckdb::idx_t database, const duckdb::DefaultAcl& row) {
    if (row.role == _role) {
      Emit(database, PgDefaultAcl::kId, row.scope, 0,
           PgShdepend::Deptype::Owner);
    }
    if (Names(row.acl, _role)) {
      Emit(database, PgDefaultAcl::kId, row.scope, 0, PgShdepend::Deptype::Acl);
    }
  }

 private:
  void Emit(duckdb::idx_t database, uint64_t classid, duckdb::idx_t objid,
            int32_t objsubid, PgShdepend::Deptype deptype) {
    _values.emplace_back(PgShdepend{
      .dbid = database,
      .classid = classid,
      .objid = objid,
      .objsubid = objsubid,
      .refclassid = PgAuthid::kId,
      .refobjid = _role,
      .deptype = deptype,
    });
  }

  std::vector<PgShdepend>& _values;
  duckdb::idx_t _role;
};

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgShdepend>::GetTableData() {
  auto& cluster = catalog::ClusterOf(_context);
  auto transaction = cluster.GetCatalogTransaction(_context);
  std::vector<duckdb::reference<duckdb::CatalogEntry>> roles;
  cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& role) {
      if (role.oid != kRootUser) {
        roles.emplace_back(role);
      }
    });

  std::vector<PgShdepend> values;
  for (auto& role : roles) {
    Rows rows{values, role.get().oid};
    cluster.GetDependencyManager()->ScanDependentEntries(
      transaction, role.get(), [&](duckdb::CatalogEntry& entry) {
        if (entry.type != duckdb::CatalogType::DATABASE_ENTRY) {
          rows.Entry(entry.ParentCatalog().GetAttached().oid, entry);
          return;
        }
        rows.Entry(kInvalidOid, entry);
        auto database = duckdb::DatabaseManager::Get(_context).GetDatabase(
          _context, entry.name);
        if (!database) {
          return;
        }
        std::vector<duckdb::idx_t> schemas;
        VisitSchemas(_context, database->GetCatalog(),
                     [&](duckdb::SchemaCatalogEntry& schema) {
                       schemas.emplace_back(schema.oid);
                     });
        for (const auto& row : entry.permissions.defaults) {
          if (row.scope == kInvalidOid ||
              absl::c_contains(schemas, row.scope)) {
            rows.Defaults(entry.oid, row);
          }
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
