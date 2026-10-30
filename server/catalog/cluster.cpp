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
#include <absl/strings/str_split.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/default/default_generator.hpp>
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
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/database_directory.h"
#include "catalog/entry/database.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/role.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";
constexpr duckdb::idx_t kCompactionFloor = duckdb::idx_t{1} << 20;

std::string NormalizeLocation(const std::string& location) {
  const auto parsed = duckdb::Path::FromString(location);
  std::string result;
  if (parsed.IsRemote()) {
    result = parsed.ToString();
    if (result.starts_with(duckdb::MemoryFileSystem::PREFIX)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
        ERR_MSG("tablespace location cannot be in memory"),
        ERR_DETAIL("Files under ", duckdb::MemoryFileSystem::PREFIX,
                   " are lost when the server stops."));
    }
  } else {
    const std::filesystem::path path{location};
    if (!path.is_absolute()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                      ERR_MSG("tablespace location must be an absolute path"));
    }
    result = path.lexically_normal().string();
  }
  while (result.size() > 1 && result.back() == '/') {
    result.pop_back();
  }
  return result;
}

bool Nested(std::string_view outer, std::string_view inner) {
  return inner.starts_with(outer) &&
         (inner.size() == outer.size() || inner[outer.size()] == '/');
}

bool UsesTablespace(duckdb::DuckCatalog& catalog, duckdb::idx_t tablespace) {
  const auto in_tablespace = [&](const auto& entry) {
    return entry.get().Tablespace() == tablespace;
  };
  return absl::c_any_of(InvertedIndexEntries(catalog), in_tablespace) ||
         absl::c_any_of(SearchTableEntries(catalog), in_tablespace);
}

class BuiltinTablespaces final : public duckdb::DefaultGenerator {
 public:
  BuiltinTablespaces(duckdb::Catalog& catalog,
                     std::shared_ptr<TablespaceRoot> default_root)
    : duckdb::DefaultGenerator{catalog},
      _default_root{std::move(default_root)} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction, const duckdb::Identifier& name) final {
    duckdb::CreateTablespaceInfo info;
    info.SetName(name);
    info.internal = true;
    info.permissions.owner = pg::kRootUser;
    if (name == kDefault) {
      info.oid = pg::kPgDefaultTablespace;
      return duckdb::make_uniq<TablespaceCatalogEntry>(catalog, info,
                                                       _default_root);
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

  std::shared_ptr<TablespaceRoot> _default_root;
};

}  // namespace

void ClusterCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
  _default_root =
    std::make_shared<TablespaceRoot>(ClusterLayout(GetAttached()).EngineDir());
  GetCatalogSet(duckdb::CatalogType::TABLESPACE_ENTRY)
    .SetDefaultGenerator(
      duckdb::make_uniq<BuiltinTablespaces>(*this, _default_root));
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeDatabaseEntry(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  return duckdb::make_uniq<DatabaseCatalogEntry>(transaction, *this, info);
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeTablespaceEntry(
  duckdb::CreateTablespaceInfo& info) {
  return duckdb::make_uniq<TablespaceCatalogEntry>(
    *this, info, std::make_shared<TablespaceRoot>(EngineDirOf(info.location)));
}

std::shared_ptr<TablespaceRoot> ClusterCatalog::TablespaceRootOf(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  if (oid == pg::kPgDefaultTablespace) {
    return _default_root;
  }
  const auto view =
    transaction.HasContext()
      ? GetCatalogTransaction(transaction.GetContext()).view
      : duckdb::CatalogTransaction::GetCommittedTransaction(GetDatabase()).view;
  if (const auto entry = GetOidIndex().GetVisible(oid, view);
      entry && entry->type == duckdb::CatalogType::TABLESPACE_ENTRY) {
    return entry->Cast<TablespaceCatalogEntry>().Root();
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("tablespace with OID ", oid, " does not exist"));
}

std::shared_ptr<DatabaseDirectory> ClusterCatalog::MakeDatabaseDirectory(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid,
  duckdb::idx_t tablespace) {
  auto root = TablespaceRootOf(transaction, tablespace);
  std::filesystem::path path =
    (root->Remote() ? _default_root : root)->DatabaseDir(oid);
  return std::make_shared<DatabaseDirectory>(oid, std::move(root),
                                             std::move(path));
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
  const auto root = EngineDirOf(info.location);
  std::optional<duckdb::Identifier> overlapping;
  tablespaces.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
    const auto& other = entry.Cast<TablespaceCatalogEntry>().Root();
    if (other && (Nested(other->Path(), root) || Nested(root, other->Path()))) {
      overlapping = entry.name;
    }
  });
  if (overlapping) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("tablespace location \"", info.location,
                            "\" overlaps tablespace \"",
                            overlapping->GetIdentifierName(), "\""));
  }
  if (absl::c_linear_search(absl::StrSplit(info.location, '/'), kEngineDir)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                    ERR_MSG("tablespace location \"", info.location,
                            "\" is inside the files of a tablespace"));
  }
  auto entry = duckdb::DuckCatalog::CreateTablespace(transaction, info);
  if (entry) {
    entry->Cast<TablespaceCatalogEntry>().Root()->Claim();
  }
  return entry;
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
  {
    const auto& tablespace = entry->Cast<TablespaceCatalogEntry>();
    if (tablespace.internal) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for tablespace ", name.GetIdentifierName()));
    }
    auto drop = tablespace.Root()->BeginDrop();
    if (!drop) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("tablespace \"", name.GetIdentifierName(),
                              "\" is being used by another transaction"));
    }
    if (HoldsTablespaceObjects(tablespace)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ERR_MSG("tablespace \"", name.GetIdentifierName(), "\" is not empty"));
    }
    if (auto* connection =
          connector::GetSereneDBContextPtr(transaction.GetContext())) {
      connection->HoldUntilEnd(std::move(drop));
    }
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_tablespace_drop");
  duckdb::DuckCatalog::DropTablespace(transaction, info);
}

bool ClusterCatalog::HoldsTablespaceObjects(
  const TablespaceCatalogEntry& tablespace) {
  bool placed = false;
  irs::containers::FlatHashSet<duckdb::idx_t> unattached;
  GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan([&](duckdb::CatalogEntry& entry) {
      placed = placed || entry.Cast<DatabaseCatalogEntry>().Tablespace() ==
                           tablespace.oid;
      unattached.insert(entry.oid);
    });
  if (placed) {
    return true;
  }
  for (const auto& db :
       duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() != SereneDBCatalog::kStorageType) {
      continue;
    }
    if (UsesTablespace(catalog.Cast<duckdb::DuckCatalog>(), tablespace.oid)) {
      return true;
    }
    unattached.erase(db->oid);
  }
  return absl::c_any_of(unattached, [&](duckdb::idx_t oid) {
    return !ListStorages(tablespace.Root()->DatabaseDir(oid)).empty();
  });
}

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
       duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() == SereneDBCatalog::kStorageType) {
      duckdb::WriteCatalogEntries(*rewrite,
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
  duckdb::DuckCatalog::DropDatabase(transaction, info);
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

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterEntry(
  duckdb::ClientContext& context, duckdb::CatalogType type,
  std::string_view name) {
  auto& cluster = ClusterOf(context);
  return cluster.GetCatalogSet(type).GetEntry(
    cluster.GetCatalogTransaction(context), duckdb::Identifier{name});
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterEntryById(
  duckdb::ClientContext& context, duckdb::CatalogType type, duckdb::idx_t oid) {
  auto& cluster = ClusterOf(context);
  duckdb::optional_ptr<duckdb::CatalogEntry> result;
  cluster.GetCatalogSet(type).Scan(cluster.GetCatalogTransaction(context),
                                   [&](duckdb::CatalogEntry& entry) {
                                     if (!result && entry.oid == oid) {
                                       result = &entry;
                                     }
                                   });
  return result;
}

}  // namespace sdb::catalog
