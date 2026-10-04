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
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/storage/checkpoint_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string_view>

#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "network/credentials.h"
#include "pg/pg_types.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";
constexpr duckdb::idx_t kCompactionFloor = duckdb::idx_t{1} << 20;

void SyncDirectory(const std::string& directory) {
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  const bool synced = fd >= 0 && ::fsync(fd) == 0;
  const int error = errno;
  if (fd >= 0) {
    ::close(fd);
  }
  if (!synced) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not fsync directory \"", directory,
                            "\": ", std::strerror(error)));
  }
}

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

ClusterCatalog::~ClusterCatalog() {
  {
    absl::MutexLock lock{&_sync_mutex};
    _sync_stop = true;
  }
  if (_sync_thread.joinable()) {
    _sync_thread.join();
  }
}

duckdb::idx_t ClusterCatalog::DefaultSchemaOid() const {
  return pg::kPgMainSchema;
}

void ClusterCatalog::RequestCatalogLogSync(
  duckdb::shared_ptr<duckdb::WriteAheadLog> log, duckdb::idx_t offset) {
  absl::MutexLock lock{&_sync_mutex};
  if (_sync_stop) {
    return;
  }
  if (_sync_log != log || offset > _sync_offset) {
    _sync_log = std::move(log);
    _sync_offset = offset;
  }
  if (!_sync_thread.joinable()) {
    _sync_thread = std::thread{[this] { SyncCatalogLogLoop(); }};
  }
}

void ClusterCatalog::SyncCatalogLogLoop() {
  while (true) {
    duckdb::shared_ptr<duckdb::WriteAheadLog> log;
    duckdb::idx_t offset = 0;
    {
      absl::MutexLock lock{&_sync_mutex};
      _sync_mutex.Await(absl::Condition(this, &ClusterCatalog::SyncPending));
      if (_sync_stop) {
        return;
      }
      log = std::move(_sync_log);
      offset = _sync_offset;
    }
    SDB_WAIT_ON_FAILURE("pause_catalog_log_sync");
    try {
      log->SyncUpTo(offset);
    } catch (...) {
    }
  }
}

void ClusterCatalog::OpenCatalogLog(
  duckdb::unique_ptr<duckdb::WriteAheadLog> log, bool compactable) {
  {
    std::lock_guard guard{_log_mutex};
    _catalog_log = std::move(log);
  }
  _compactable = compactable;
  _live_bytes = GetAttached().GetStorageManager().GetWALSize();
  _catalog_generation.fetch_add(1, std::memory_order_acq_rel);
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
}

void ClusterCatalog::BeginCatalogLogCommit() {
  _commits_in_flight.fetch_add(1, std::memory_order_acq_rel);
}

void ClusterCatalog::EndCatalogLogCommit() {
  const auto version = duckdb::DuckTransactionManager::Get(GetAttached())
                         .GetLastCommittedCatalogVersion();
  if (_generation_version.exchange(version, std::memory_order_acq_rel) !=
      version) {
    _catalog_generation.fetch_add(1, std::memory_order_acq_rel);
  }
  _commits_in_flight.fetch_sub(1, std::memory_order_acq_rel);
}

void ClusterCatalog::MaybeCompactCatalogLog() {
  if (!_compactable || !CatalogLog()) {
    return;
  }
  bool force = false;
  SDB_IF_FAILURE("compact_inside_ddl") { force = true; }
  SDB_IF_FAILURE("compact_inside_drop") { force = true; }
  auto& storage = GetAttached().GetStorageManager();
  const auto threshold =
    force ? 0 : std::max<duckdb::idx_t>(kCompactionFloor, 2 * _live_bytes);
  if (storage.GetWALSize() < threshold) {
    return;
  }
  auto lock = storage.GetCommitLock();
  if (storage.GetWALSize() < threshold ||
      _commits_in_flight.load(std::memory_order_acquire) > 0) {
    return;
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
    {
      const std::filesystem::path root{ClusterLayout(GetAttached()).directory};
      std::lock_guard guard{_artifacts_mutex};
      std::erase_if(_artifacts, [&](const Artifact& artifact) {
        if (!artifact.drop && IsLive(artifact)) {
          return true;
        }
        return absl::c_none_of(artifact.paths, [&](const std::string& path) {
          std::error_code ec;
          return std::filesystem::exists(root / path, ec) ||
                 std::filesystem::exists(
                   search::DroppedStoragePath(root / path), ec);
        });
      });
      for (const auto& artifact : _artifacts) {
        rewrite.WriteArtifact(artifact.type, artifact.catalog_oid, artifact.oid,
                              artifact.paths);
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
  _live_bytes = size;
  SyncDirectory(std::filesystem::path{path}.parent_path().string());
}

namespace {

duckdb::vector<std::string> RelativePaths(
  const std::filesystem::path& root,
  const std::vector<std::filesystem::path>& paths) {
  duckdb::vector<std::string> relative;
  for (const auto& path : paths) {
    relative.push_back(path.lexically_relative(root).string());
  }
  return relative;
}

}  // namespace

void ClusterCatalog::NoteDroppedArtifact(
  duckdb::CatalogType type, duckdb::idx_t catalog_oid, duckdb::idx_t oid,
  const std::vector<std::filesystem::path>& paths) {
  const std::filesystem::path root{ClusterLayout(GetAttached()).directory};
  std::lock_guard guard{_artifacts_mutex};
  if (!CatalogLog()) {
    _replayed_drops.insert(oid);
    return;
  }
  _artifacts.push_back(
    {type, catalog_oid, oid, RelativePaths(root, paths), true});
}

void ClusterCatalog::LogArtifact(
  duckdb::CatalogType type, duckdb::idx_t catalog_oid, duckdb::idx_t oid,
  const std::vector<std::filesystem::path>& paths, bool drop) {
  if (!CatalogLog()) {
    return;
  }
  const std::filesystem::path root{ClusterLayout(GetAttached()).directory};
  auto relative = RelativePaths(root, paths);
  {
    auto lock = GetAttached().GetStorageManager().GetCommitLock();
    _catalog_log->WriteArtifact(type, catalog_oid, oid, relative);
    _catalog_log->SyncUpTo(_catalog_log->FlushMarker());
  }
  std::lock_guard guard{_artifacts_mutex};
  _artifacts.push_back({type, catalog_oid, oid, std::move(relative), drop});
}

void ClusterCatalog::ReplayArtifact(duckdb::CatalogType type,
                                    duckdb::idx_t catalog_oid,
                                    duckdb::idx_t oid,
                                    duckdb::vector<std::string> paths) {
  GetDatabase().GetDatabaseManager().ClaimOid(oid);
  std::lock_guard guard{_artifacts_mutex};
  _artifacts.push_back({type, catalog_oid, oid, std::move(paths), true});
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

bool ClusterCatalog::IsLive(const Artifact& artifact) {
  bool live = false;
  if (artifact.type == duckdb::CatalogType::DATABASE_ENTRY) {
    GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
      .Scan([&](duckdb::CatalogEntry& entry) {
        live = live || entry.oid == artifact.oid;
      });
    return live;
  }
  bool attached = false;
  for (const auto& db :
       duckdb::DatabaseManager::Get(GetDatabase()).GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (db->oid != artifact.catalog_oid ||
        catalog.GetCatalogType() != SereneDBCatalog::kStorageType) {
      continue;
    }
    attached = true;
    live = live || catalog.Cast<SereneDBCatalog>().FindEntryById(
                     nullptr, artifact.type, artifact.oid);
  }
  return live || !attached;
}

void ClusterCatalog::ResolveArtifacts() {
  const std::filesystem::path root{ClusterLayout(GetAttached()).directory};
  std::lock_guard guard{_artifacts_mutex};
  for (const auto& artifact : _artifacts) {
    for (const auto& path : artifact.paths) {
      std::error_code ec;
      std::filesystem::remove_all(search::DroppedStoragePath(root / path), ec);
    }
    if (_replayed_drops.contains(artifact.oid) || IsLive(artifact)) {
      continue;
    }
    for (const auto& path : artifact.paths) {
      std::error_code ec;
      std::filesystem::remove_all(root / path, ec);
      if (ec) {
        SDB_WARN(STARTUP, "could not remove '", (root / path).string(),
                 "' of dropped object ", artifact.oid, ": ", ec.message());
      }
    }
  }
  _artifacts.clear();
  _replayed_drops.clear();
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
      auto verifier = network::BuildScramVerifierString(password);
      if (!verifier) {
        SDB_FATAL(GENERAL,
                  "could not derive a password verifier from "
                  "POSTGRES_PASSWORD");
      }
      info.password = std::move(*verifier);
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

namespace {

void RequireUnreservedRoleName(const duckdb::Identifier& name) {
  if (name.GetIdentifierName().starts_with("pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("role name \"", name.GetIdentifierName(), "\" is reserved"),
      ERR_DETAIL("Role names starting with \"pg_\" are reserved."));
  }
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  RequireUnreservedRoleName(info.GetQualifiedName().Name());
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateRole(transaction, info);
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropRole(transaction, info);
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  if (info.type == duckdb::AlterType::ALTER_ROLE) {
    const auto& new_name = info.Cast<duckdb::AlterRoleInfo>().new_name;
    if (!new_name.empty()) {
      RequireUnreservedRoleName(new_name);
    }
  }
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
  return duckdb::DuckCatalog::CreateDatabase(transaction, info);
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  if (auto entry = GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                     .GetEntry(transaction, info.GetQualifiedName().Name())) {
    LogArtifact(duckdb::CatalogType::DATABASE_ENTRY, GetAttached().oid,
                entry->oid, DatabaseArtifacts(GetAttached(), entry->oid), true);
  }
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

}  // namespace sdb::catalog
