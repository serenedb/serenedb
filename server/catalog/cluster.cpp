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

#include "catalog/cluster.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/storage/checkpoint_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/database_directory.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/types.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";
constexpr duckdb::idx_t kCompactionFloor = duckdb::idx_t{1} << 20;

}  // namespace

duckdb::Catalog& ClusterCatalog::ReplayUseCatalog(
  duckdb::ClientContext& context, duckdb::idx_t catalog_oid) {
  duckdb::optional_ptr<duckdb::CatalogEntry> database;
  GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(GetCatalogTransaction(context), [&](duckdb::CatalogEntry& entry) {
      if (entry.oid == catalog_oid) {
        database = entry;
      }
    });
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("the catalog log names database ", catalog_oid,
                            ", which it never created"));
  }
  return AttachDatabaseCatalog(context, database->name, catalog_oid);
}

duckdb::idx_t ClusterCatalog::DefaultSchemaOid() const {
  return pg::kPgMainSchema;
}

void ClusterCatalog::OpenCatalogLog(
  duckdb::unique_ptr<duckdb::WriteAheadLog> log, bool compactable) {
  {
    std::lock_guard guard{_log_mutex};
    _catalog_log = std::move(log);
  }
  _compactable = compactable;
  _live_bytes.store(GetAttached().GetStorageManager().GetWALSize(),
                    std::memory_order_relaxed);
}

void ClusterCatalog::OnCatalogLogPrepared() {
  SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
  SDB_IF_FAILURE("catalog_append_fails") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("catalog log: could not append the transaction"));
  }
}

void ClusterCatalog::OnCatalogLogDecided() {
  SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  SDB_WAIT_ON_FAILURE("pause_after_catalog_decision");
}

void ClusterCatalog::MaybeCompactCatalogLog() {
  if (!_compactable) {
    return;
  }
  bool force = false;
  SDB_IF_FAILURE("compact_inside_ddl") { force = true; }
  SDB_IF_FAILURE("compact_inside_drop") { force = true; }
  auto& storage = GetAttached().GetStorageManager();
  const auto threshold = [&] {
    return force ? 0
                 : std::max<duckdb::idx_t>(
                     kCompactionFloor,
                     2 * _live_bytes.load(std::memory_order_relaxed));
  };
  if (storage.GetWALSize() < threshold()) {
    return;
  }
  auto lock = storage.GetCommitLock();
  if (storage.GetWALSize() < threshold()) {
    return;
  }
  auto commits = _commit_lock.TryGetExclusiveLock();
  if (!commits) {
    return;
  }
  std::vector<duckdb::unique_ptr<duckdb::StorageLockKey>> quiescent;
  auto quiesce = [&](duckdb::AttachedDatabase& db) {
    auto key = duckdb::DuckTransactionManager::Get(db).TryGetCheckpointLock();
    if (!key) {
      return false;
    }
    quiescent.push_back(std::move(key));
    return true;
  };
  if (!quiesce(GetAttached())) {
    return;
  }
  for (const auto& db :
       duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
    if (db->GetCatalog().GetCatalogType() == SereneDBCatalog::kStorageType &&
        !quiesce(*db)) {
      return;
    }
  }
  try {
    CompactCatalogLog();
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "catalog log rewrite failed: ", e.what());
  }
}

void ClusterCatalog::CompactCatalogLog() {
  auto& storage = GetAttached().GetStorageManager();
  auto& fs = duckdb::FileSystem::Get(GetAttached());
  const auto path = _catalog_log->GetPath();
  const auto rewrite_path = path + ".rewrite";
  fs.TryRemoveFile(rewrite_path);
  duckdb::idx_t size = 0;
  {
    duckdb::WriteAheadLog rewrite{storage, rewrite_path};
    duckdb::WriteCatalogEntries(rewrite, *this);
    for (const auto& db :
         duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
      auto& catalog = db->GetCatalog();
      if (catalog.GetCatalogType() == SereneDBCatalog::kStorageType) {
        duckdb::WriteCatalogEntries(rewrite,
                                    catalog.Cast<duckdb::DuckCatalog>());
      }
    }
    duckdb::DatabaseManager::Get(GetDatabase())
      .RetainPrepared(
        [&](const duckdb::hugeint_t& txid,
            const duckdb::vector<std::pair<duckdb::idx_t, duckdb::idx_t>>&
              participants) {
          const bool pending =
            absl::c_any_of(participants, [&](const auto& participant) {
              return HoldsPreparedBatch(participant.first, participant.second);
            });
          if (pending) {
            rewrite.WriteCommitPrepared(txid, participants);
          }
          return pending;
        });
    rewrite.Flush();
    size = rewrite.GetTotalWritten();
  }
  fs.MoveFile(rewrite_path, path);
  {
    std::lock_guard guard{_log_mutex};
    _catalog_log = duckdb::make_shared_ptr<duckdb::WriteAheadLog>(
      storage, path, size, duckdb::WALInitState::UNINITIALIZED);
  }
  _live_bytes.store(size, std::memory_order_relaxed);
  SyncDirectory(std::filesystem::path{path}.parent_path());
}

bool ClusterCatalog::HoldsPreparedBatch(duckdb::idx_t oid,
                                        duckdb::idx_t generation) {
  bool exists = false;
  GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan([&](duckdb::CatalogEntry& entry) {
      exists = exists || entry.oid == oid;
    });
  if (!exists) {
    return false;
  }
  for (const auto& db :
       duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
    if (db->oid != oid || !db->HasStorageManager() ||
        db->GetStorageManager().InMemory()) {
      continue;
    }
    return db->GetStorageManager().GetBlockManager().GetCheckpointIteration() <=
           generation;
  }
  return true;
}

void ClusterCatalog::Bootstrap(duckdb::ClientContext& context) {
  const auto transaction = GetCatalogTransaction(context);
  const duckdb::Identifier root{kRootRole};
  if (!GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
         .GetEntry(transaction, root)) {
    duckdb::CreateRoleInfo info;
    info.SetName(root);
    info.oid = pg::kRootUser;
    info.options = RoleOption::Superuser | RoleOption::Inherit |
                   RoleOption::CreateRole | RoleOption::CreateDb |
                   RoleOption::Login | RoleOption::Replication |
                   RoleOption::BypassRls;
    if (const char* password = std::getenv("POSTGRES_PASSWORD");
        password && *password) {
      info.password = password;
      SDB_INFO(GENERAL, "bootstrap: initial password set for role '", kRootRole,
               "' from POSTGRES_PASSWORD");
    }
    CreateRole(transaction, info);
  }
  const duckdb::Identifier postgres{irs::StaticStrings::kDefaultDatabase};
  if (!GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
         .GetEntry(transaction, postgres)) {
    duckdb::CreateDatabaseInfo info;
    info.SetName(postgres);
    info.oid = pg::kPgPostgresDatabase;
    info.permissions.owner = pg::kRootUser;
    CreateDatabase(transaction, info);
  }
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeRoleEntry(
  duckdb::CreateRoleInfo& info) {
  RequireUnreservedRoleName(info.GetQualifiedName().Name());
  if (!info.password.empty()) {
    info.password = StoredPassword(info.password);
  }
  return duckdb::make_uniq<RoleCatalogEntry>(*this, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  DeclareModified(transaction, *this);
  auto role = duckdb::DuckCatalog::CreateRole(transaction, info);
  const auto grant = [&](const duckdb::Identifier& member,
                         const duckdb::Identifier& granted, bool admin) {
    duckdb::AlterRoleInfo alter{member};
    alter.grant_role = granted.GetIdentifierName();
    alter.admin_option = admin;
    Alter(transaction, alter);
  };
  for (const auto& name : info.in_roles) {
    grant(role->name, name, false);
  }
  for (const auto& name : info.role_members) {
    grant(name, role->name, false);
  }
  for (const auto& name : info.admin_members) {
    grant(name, role->name, true);
  }
  auto& context = transaction.GetContext();
  if (const auto creator = GrantorOfMembership(context);
      creator != pg::kRootUser) {
    duckdb::AlterRoleInfo alter{
      duckdb::Identifier{auth::RolesOf(&context)->NameOf(creator)}};
    alter.grant_role = role->name.GetIdentifierName();
    alter.grantor_id = pg::kRootUser;
    alter.admin_option = true;
    alter.inherit_option = false;
    alter.set_option = false;
    Alter(transaction, alter);
  }
  return role;
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  auto& roles = GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY);
  if (auto role = roles.GetEntry(transaction, info.GetQualifiedName().Name())) {
    auto* session = connector::GetSereneDBContextPtr(transaction.GetContext());
    if (session && (role->oid == session->GetRoleId() ||
                    role->oid == session->GetSessionRoleId() ||
                    role->oid == session->GetLoginRoleId())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("current user cannot be dropped"));
    }
    if (role->oid == pg::kRootUser) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_IN_USE),
        ERR_MSG("cannot drop role \"", role->name.GetIdentifierName(),
                "\" because it is required by the database system"));
    }
    std::vector<duckdb::Identifier> members;
    roles.Scan(transaction, [&](duckdb::CatalogEntry& other) {
      if (absl::c_any_of(other.Cast<RoleCatalogEntry>().MemberOf(),
                         [&](const duckdb::Membership& membership) {
                           return membership.role == role->oid;
                         })) {
        members.emplace_back(other.name);
      }
    });
    for (const auto& member : members) {
      duckdb::AlterRoleInfo alter{member};
      alter.grant_role_id = role->oid;
      alter.revoke = true;
      Alter(transaction, alter);
    }
  }
  duckdb::DuckCatalog::DropRole(transaction, info);
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  DeclareModified(transaction, *this);
  const auto type = info.GetCatalogType();
  const auto& name = info.GetQualifiedName().Name();
  if (!GetCatalogSet(type).AlterEntry(transaction, name, info)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(duckdb::CatalogTypeToString(type), " with name ",
                            name.GetIdentifierName(), " does not exist!"));
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  DeclareModified(transaction, *this);
  auto entry = duckdb::DuckCatalog::CreateDatabase(transaction, info);
  if (entry) {
    entry->Cast<DatabaseCatalogEntry>().Directory()->Create();
  }
  return entry;
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropDatabase(transaction, info);
  auto& context = transaction.GetContext();
  duckdb::DatabaseManager::Get(context).DetachDatabase(
    context, info.GetQualifiedName().Name(),
    duckdb::OnEntryNotFound::RETURN_NULL);
}

ClusterCatalog& ClusterOf(duckdb::ClientContext& context) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  return duckdb::Catalog::GetCatalog(context, name).Cast<ClusterCatalog>();
}

// Not Catalog::GetCatalog(DatabaseInstance&, name): the pin declares that
// overload and never defines it. This is what it would have done.
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  auto attached = duckdb::DatabaseManager::Get(db).GetDatabase(name);
  if (!attached) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("the cluster catalog is not attached"));
  }
  return attached->GetCatalog().Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(irs::DuckDBEngine::Instance().instance());
}

}  // namespace sdb::catalog
