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

#include "pg/role_dependencies.h"

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_database.h"
#include "pg/pg_catalog/pg_default_acl.h"
#include "pg/pg_catalog/pg_foreign_server.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_ts_dict.h"
#include "pg/pg_catalog/pg_type.h"
#include "pg/pg_types.h"
#include "pg/system_table.h"

namespace sdb::pg {
namespace {

constexpr char kOwner = 'o';
constexpr char kAcl = 'a';

class Emitter {
 public:
  Emitter(absl::FunctionRef<void(const RoleDependency&)> visitor,
          duckdb::idx_t database)
    : _visitor{visitor}, _database{database} {}

  void Entry(uint64_t classid, const duckdb::CatalogEntry& entry) {
    const auto& perm = entry.permissions;
    Emit(classid, entry.oid, 0, perm.owner, kOwner);
    Acl(classid, entry.oid, 0, perm.acl);
  }

  void Defaults(const duckdb::DefaultAcl& entry) {
    Emit(PgDefaultAcl::kId, entry.scope, 0, entry.role, kOwner);
    Acl(PgDefaultAcl::kId, entry.scope, 0, entry.acl);
  }

  void Acl(uint64_t classid, duckdb::idx_t objid, int32_t objsubid,
           std::span<const duckdb::AclItem> acl) {
    std::vector<duckdb::idx_t> roles;
    for (const auto& item : acl) {
      for (const auto role : {item.grantee, item.grantor}) {
        if (!std::ranges::contains(roles, role)) {
          roles.push_back(role);
        }
      }
    }
    for (const auto role : roles) {
      Emit(classid, objid, objsubid, role, kAcl);
    }
  }

 private:
  void Emit(uint64_t classid, duckdb::idx_t objid, int32_t objsubid,
            duckdb::idx_t role, char deptype) {
    if (role == kInvalidOid || role == kPublicGrantee || role == kRootUser) {
      return;
    }
    _visitor(RoleDependency{
      .database = _database,
      .classid = classid,
      .objid = objid,
      .objsubid = objsubid,
      .role = role,
      .deptype = deptype,
    });
  }

  absl::FunctionRef<void(const RoleDependency&)> _visitor;
  duckdb::idx_t _database;
};

void VisitDatabase(duckdb::ClientContext& context,
                   catalog::SereneDBCatalog& database, Emitter& emitter) {
  std::vector<duckdb::idx_t> schemas;
  VisitSchemas(context, database, [&](duckdb::SchemaCatalogEntry& schema) {
    schemas.push_back(schema.oid);
    emitter.Entry(PgNamespace::kId, schema);
    schema.Scan(context, duckdb::CatalogType::TABLE_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  emitter.Entry(PgClass::kId, entry);
                  if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
                    return;
                  }
                  const auto& table = entry.Cast<duckdb::TableCatalogEntry>();
                  for (const auto& column : table.GetColumns().Logical()) {
                    emitter.Acl(
                      PgClass::kId, entry.oid,
                      static_cast<int32_t>(column.Logical().index + 1),
                      column.Acl());
                  }
                });
    schema.Scan(
      context, duckdb::CatalogType::SEQUENCE_ENTRY,
      [&](duckdb::CatalogEntry& entry) { emitter.Entry(PgClass::kId, entry); });
    schema.Scan(
      context, duckdb::CatalogType::TYPE_ENTRY,
      [&](duckdb::CatalogEntry& entry) { emitter.Entry(PgType::kId, entry); });
    schema.Scan(
      context, duckdb::CatalogType::MACRO_ENTRY,
      [&](duckdb::CatalogEntry& entry) { emitter.Entry(PgProc::kId, entry); });
    schema.Scan(context, duckdb::CatalogType::TOKENIZER_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  emitter.Entry(PgTsDict::kId, entry);
                });
  });
  database.GetCatalogSet(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
    .Scan(database.GetCatalogTransaction(context),
          [&](duckdb::CatalogEntry& entry) {
            emitter.Entry(PgForeignServer::kId, entry);
          });
  auto& cluster = catalog::ClusterOf(context);
  auto entry =
    cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
      .GetEntry(cluster.GetCatalogTransaction(context), database.GetName());
  if (!entry) {
    return;
  }
  for (const auto& row : entry->permissions.defaults) {
    if (row.scope == kInvalidOid || std::ranges::contains(schemas, row.scope)) {
      emitter.Defaults(row);
    }
  }
}

}  // namespace

void VisitRoleDependencies(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(const RoleDependency&)> visitor) {
  for (auto& attached : duckdb::DatabaseManager::Get(context).GetDatabases()) {
    auto& catalog = attached->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    Emitter emitter{visitor, attached->oid};
    VisitDatabase(context, catalog.Cast<catalog::SereneDBCatalog>(), emitter);
  }
  auto& cluster = catalog::ClusterOf(context);
  Emitter emitter{visitor, 0};
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.GetCatalogTransaction(context),
          [&](duckdb::CatalogEntry& entry) {
            emitter.Entry(PgDatabase::kId, entry);
          });
}

}  // namespace sdb::pg
