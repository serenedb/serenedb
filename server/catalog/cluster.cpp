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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/default/default_generator.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/storage/checkpoint_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <filesystem>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcode_for_file_access.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "auth/role_closure.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/database_directory.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";
constexpr duckdb::idx_t kCompactionFloor = duckdb::idx_t{1} << 20;

std::string NormalizeLocation(std::string_view location) {
  const auto parsed = duckdb::Path::FromString(location);
  if (parsed.IsRemote()) {
    if (parsed.GetScheme() == duckdb::MemoryFileSystem::PREFIX) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
        ERR_MSG("tablespace location cannot be in memory"),
        ERR_DETAIL("Files under ", duckdb::MemoryFileSystem::PREFIX,
                   " are lost when the server stops."));
    }
    if (!parsed.HasAuthority()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
        ERR_MSG("tablespace location must name a bucket or container"));
    }
  } else if (!location.starts_with('/')) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                    ERR_MSG("tablespace location must be an absolute path"));
  }
  auto result = parsed.ToString();
  if (result.size() > 1 && result.back() == '/') {
    result.pop_back();
  }
  if (absl::c_linear_search(parsed.GetPathSegments(), kEngineDir)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                    ERR_MSG("tablespace location \"", result,
                            "\" is inside the files of a tablespace"));
  }
  return result;
}

class BuiltinTablespaces final : public duckdb::DefaultGenerator {
 public:
  BuiltinTablespaces(duckdb::Catalog& catalog,
                     std::shared_ptr<TablespaceDirectory> default_directory)
    : duckdb::DefaultGenerator{catalog},
      _default_directory{std::move(default_directory)} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction, const duckdb::Identifier& name) final {
    duckdb::CreateTablespaceInfo info;
    info.SetName(name);
    info.internal = true;
    info.permissions.owner = pg::kRootUser;
    if (name == kDefault) {
      info.oid = pg::kPgDefaultTablespace;
      return duckdb::make_uniq<TablespaceCatalogEntry>(catalog, info,
                                                       _default_directory);
    }
    if (name == kGlobal) {
      info.oid = pg::kPgGlobalTablespace;
      return duckdb::make_uniq<TablespaceCatalogEntry>(catalog, info, nullptr);
    }
    return nullptr;
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() final {
    return {duckdb::Identifier{kDefault}, duckdb::Identifier{kGlobal}};
  }

  bool LockDuringCreate() const final { return true; }

 private:
  static constexpr const char* kDefault = "pg_default";
  static constexpr const char* kGlobal = "pg_global";

  std::shared_ptr<TablespaceDirectory> _default_directory;
};

}  // namespace

void ClusterCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
  _default_directory = std::make_shared<TablespaceDirectory>(
    ClusterLayout(GetAttached()).EngineDir().string());
  GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
    .SetDefaultGenerator(
      duckdb::make_uniq<BuiltinTablespaces>(*this, _default_directory));
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeDatabaseEntry(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  if (info.oid == 0) {
    info.oid = GetDatabase().GetDatabaseManager().NextOid();
  }
  auto tablespace = TablespaceDirectoryOf(transaction, info.tablespace_oid);
  std::filesystem::path path =
    irs::StorageKindOf(tablespace->Path()) == irs::StorageKind::Remote
      ? ClusterLayout(GetAttached()).DatabaseDir(info.oid)
      : std::filesystem::path{tablespace->Path()} / absl::StrCat(info.oid);
  return duckdb::make_uniq<DatabaseCatalogEntry>(
    *this, info,
    std::make_shared<DatabaseDirectory>(info.oid, std::move(path),
                                        std::move(tablespace)));
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeTablespaceEntry(
  duckdb::CreateTablespaceInfo& info) {
  if (info.oid == 0) {
    info.oid = GetDatabase().GetDatabaseManager().NextOid();
  }
  return duckdb::make_uniq<TablespaceCatalogEntry>(
    *this, info,
    std::make_shared<TablespaceDirectory>(absl::StrCat(
      info.location, "/", kEngineDir, "/",
      backup::PositionSource::Instance().SystemIdentifier(), "-", info.oid)));
}

TablespaceCatalogEntry& ClusterCatalog::Tablespace(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  const auto own =
    transaction.HasContext()
      ? GetCatalogTransaction(transaction.GetContext())
      : duckdb::CatalogTransaction::GetCommittedTransaction(GetDatabase());
  if (auto entry = EntryById(own, duckdb::CatalogType::TABLESPACE_ENTRY, oid)) {
    return entry->Cast<TablespaceCatalogEntry>();
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("tablespace with OID ", static_cast<int64_t>(oid),
                          " does not exist"));
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::EntryById(
  duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
  duckdb::idx_t oid) {
  if (auto entry = GetOidIndex().GetVisible(oid, transaction.view)) {
    return entry->type == type ? entry : nullptr;
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> internal;
  GetCatalogSet(type).Scan(transaction, [&](duckdb::CatalogEntry& entry) {
    if (entry.internal && entry.oid == oid) {
      internal = entry;
    }
  });
  return internal;
}

std::shared_ptr<TablespaceDirectory> ClusterCatalog::TablespaceDirectoryOf(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  if (oid == pg::kPgDefaultTablespace) {
    return _default_directory;
  }
  return Tablespace(transaction, oid).Directory();
}

std::vector<std::string> ClusterCatalog::TablespaceDirs(
  duckdb::CatalogTransaction transaction) {
  std::vector<std::string> directories;
  GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      if (const auto& directory =
            entry.Cast<TablespaceCatalogEntry>().Directory()) {
        directories.push_back(directory->Path());
      }
    });
  return directories;
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateTablespace(
  duckdb::CatalogTransaction transaction, duckdb::CreateTablespaceInfo& info) {
  DeclareModified(transaction, *this);
  const auto& name = info.GetQualifiedName().Name().GetIdentifierName();
  RequireUnreservedTablespaceName(name);
  auto& tablespaces = GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY);
  if (tablespaces.GetEntry(transaction, info.GetQualifiedName().Name())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("tablespace \"", name, "\" already exists"));
  }
  info.location = NormalizeLocation(info.location);
  const auto created = duckdb::DuckCatalog::CreateTablespace(transaction, info);
  if (created) {
    const auto& directory =
      created->Cast<TablespaceCatalogEntry>().Directory()->Path();
    if (irs::StorageKindOf(directory) == irs::StorageKind::Remote) {
      StorageFileSystem().ListFiles(directory, [](const std::string&, bool) {});
    } else {
      CreateDirectories(directory);
    }
  }
  return created;
}

void ClusterCatalog::DropTablespace(duckdb::CatalogTransaction transaction,
                                    duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  const auto& name = info.GetQualifiedName().Name();
  auto entry = GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
                 .GetEntry(transaction, name);
  if (!entry) {
    if (info.if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("tablespace \"", name.GetIdentifierName(),
                              "\" does not exist"));
    }
    return;
  }
  auto& tablespace = entry->Cast<TablespaceCatalogEntry>();
  if (tablespace.internal) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied for tablespace ", name.GetIdentifierName()));
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_tablespace_drop");
  bool placed = false;
  GetDependencyManager()->ScanDependentEntries(
    transaction, tablespace, [&](duckdb::CatalogEntry&) { placed = true; });
  if (placed) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("tablespace \"", name.GetIdentifierName(), "\" is not empty"));
  }
  if (const auto unattached = UnattachedDatabase(transaction)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("tablespace \"", name.GetIdentifierName(),
              "\" cannot be dropped while database \"",
              unattached->GetIdentifierName(), "\" is not attached"),
      ERR_HINT("Restore the files of the database and restart the server, or "
               "drop the database."));
  }
  duckdb::DuckCatalog::DropTablespace(transaction, info);
}

std::optional<duckdb::Identifier> ClusterCatalog::UnattachedDatabase(
  duckdb::CatalogTransaction transaction) {
  std::optional<duckdb::Identifier> unattached;
  if (_unattached.empty()) {
    return unattached;
  }
  GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      if (!unattached && _unattached.contains(entry.oid)) {
        unattached = entry.name;
      }
    });
  return unattached;
}

duckdb::Catalog& ClusterCatalog::ReplayUseCatalog(
  duckdb::ClientContext& context, duckdb::idx_t catalog_oid) {
  const auto database =
    EntryById(GetCatalogTransaction(context),
              duckdb::CatalogType::DATABASE_ENTRY, catalog_oid);
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
  log->Initialize();
  {
    std::lock_guard guard{_log_mutex};
    _catalog_log = std::move(log);
  }
  _compactable = compactable;
  _live_bytes.store(GetAttached().GetStorageManager().GetWALSize(),
                    std::memory_order_relaxed);
  auth::PublishRoles(*this);
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

void ClusterCatalog::EndCatalogLogCommit() { auth::PublishRoles(*this); }

void ClusterCatalog::MaybeCompactCatalogLog() {
  if (!_compactable || TablespaceRemovalsPending()) {
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
       SereneDBDatabases(duckdb::DatabaseManager::Get(GetDatabase()))) {
    if (!quiesce(*db)) {
      return;
    }
  }
  try {
    CompactCatalogLog();
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL,
             "catalog log rewrite failed: ", duckdb::ErrorData{e}.RawMessage());
  }
}

void ClusterCatalog::CompactCatalogLog() {
  auto& storage = GetAttached().GetStorageManager();
  auto& fs = duckdb::FileSystem::Get(GetAttached());
  const auto path = ClusterLayout(GetAttached()).CatalogLogFile();
  const auto rewrite_path = path + ".rewrite";
  fs.TryRemoveFile(rewrite_path);
  auto rewrite =
    duckdb::make_shared_ptr<duckdb::WriteAheadLog>(storage, rewrite_path);
  rewrite->Initialize();
  duckdb::WriteCatalogEntries(*rewrite, *this);
  for (const auto& db :
       SereneDBDatabases(duckdb::DatabaseManager::Get(GetDatabase()))) {
    duckdb::WriteCatalogEntries(*rewrite,
                                db->GetCatalog().Cast<duckdb::DuckCatalog>());
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
          rewrite->WriteCommitPrepared(txid, participants);
        }
        return pending;
      });
  rewrite->Flush();
  const auto size = rewrite->GetTotalWritten();
  fs.MoveFile(rewrite_path, path);
  {
    std::lock_guard guard{_log_mutex};
    _catalog_log = std::move(rewrite);
  }
  _live_bytes.store(size, std::memory_order_relaxed);
  try {
    StorageFileSystem().SyncDirectory(
      std::filesystem::path{path}.parent_path().string());
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL,
              "catalog log rewrite: ", duckdb::ErrorData{e}.RawMessage());
  }
}

duckdb::idx_t ClusterCatalog::SnapshotCatalogLog(
  const std::filesystem::path& link) {
  auto& fs = StorageFileSystem();
  duckdb::idx_t size = 0;
  {
    auto lock = GetAttached().GetStorageManager().GetCommitLock();
    const auto log = CatalogLog();
    SDB_ASSERT(log);
    std::error_code ec;
    std::filesystem::create_hard_link(log->GetPath(), link, ec);
    if (ec) {
      THROW_SQL_ERROR(ERR_CODE(irs::pg::ErrcodeForFileAccess(ec.value())),
                      ERR_MSG("could not snapshot the catalog log as \"",
                              link.string(), "\": ", ec.message()));
    }
    size = fs.OpenFile(link.string(), duckdb::FileFlags::FILE_FLAGS_READ)
             ->GetFileSize();
  }
  fs.SyncDirectory(link.parent_path().string());
  return size;
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
    return db->GetStorageManager().GetOldestWALGeneration() <= generation;
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
      SDB_INFO(STARTUP, "bootstrap: initial password set for role '", kRootRole,
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
    info.tablespace_oid = pg::kPgDefaultTablespace;
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

bool ClusterCatalog::IsPinnedEntry(const duckdb::CatalogEntry& entry) const {
  return entry.type == duckdb::CatalogType::ROLE_ENTRY &&
         entry.oid == pg::kRootUser;
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
  const auto entry = GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                       .GetEntry(transaction, info.GetQualifiedName().Name());
  const auto directory =
    entry ? entry->Cast<DatabaseCatalogEntry>().Directory() : nullptr;
  duckdb::DuckCatalog::DropDatabase(transaction, info);
  if (!directory) {
    return;
  }
  directory->SetTablespaces(TablespaceDirs(transaction));
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
