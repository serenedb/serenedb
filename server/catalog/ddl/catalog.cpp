////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/ddl/catalog.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <duckdb/main/database_manager.hpp>
#include <filesystem>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "basics/system-compiler.h"
#include "catalog/database.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/persistence/role.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/role.h"
#include "catalog/sequence.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_storage_extension.h"
#include "network/credentials.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

// A database file that is gone or will not open is not a state the server
// reaches on its own: somebody removed or corrupted it. Refusing is the default
// because silently recreating an empty database is the one outcome that turns a
// recoverable accident into data loss.
ABSL_FLAG(std::string, missing_database, "refuse",
          "What boot does with a database whose data file is missing or will "
          "not open: 'refuse' (default) stops the server, 'skip' leaves it "
          "unattached, 'drop' removes it from the catalog.");

namespace sdb::catalog {

AccessContext ActingAs(duckdb::ClientContext& context) {
  return {connector::GetSereneDBContext(context).GetRoleId(), &context};
}

void ThrowConcurrentlyDropped(duckdb::CatalogType type, std::string_view name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent delete of ",
            pg::ToPgObjectTypeName(type), " \"", name, "\""));
}

void ThrowConcurrentlyDropped(ObjectId /*id*/) {
  // The id is deliberately out of the message -- it is an internal oid, and it
  // would make the error unassertable.
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent delete of a "
            "referenced object"));
}

void ThrowDuplicateName(NameKind kind, std::string_view name) {
  switch (kind) {
    case NameKind::Relation:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                      ERR_MSG("relation \"", name, "\" already exists"));
    case NameKind::Type:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("type \"", name, "\" already exists"));
    case NameKind::Role:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("role \"", name, "\" already exists"));
  }
  SDB_UNREACHABLE();
}

void Catalog::RecordSequenceSeed(duckdb::ClientContext* /*context*/,
                                 ObjectId id, uint64_t seed) {
  GetCatalogStore().PutSequenceValue(id, seed);
}

void RequireDatabaseAccess(duckdb::ClientContext* context, ObjectId role,
                           const catalog::SereneDBDatabaseEntry* database,
                           AclMode need) {
  if (database == nullptr || auth::ClosureFor(context, role)
                               ->Can(duckdb::CatalogType::DATABASE_ENTRY,
                                     database->permissions, need)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for database ",
                          database->name.GetIdentifierName()));
}

void RequireCreateOn(duckdb::ClientContext* context, ObjectId role,
                     ObjectId parent_id) {
  const auto* schema = catalog::FindSchema(context, parent_id);
  if (schema == nullptr || auth::ClosureFor(context, role)
                             ->Can(duckdb::CatalogType::SCHEMA_ENTRY,
                                   schema->permissions, AclMode::Create)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for schema ", schema->name.GetIdentifierName()));
}

void RequireOwner(duckdb::ClientContext* context, ObjectId role,
                  const Permissions& perm, std::string_view noun,
                  std::string_view name) {
  if (auth::ClosureFor(context, role)->Owns(ObjectId{perm.owner})) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("must be owner of ", noun, " ", name));
}

void RequireOwnerTransfer(const AccessContext& ax, ObjectId schema_id,
                          const Permissions& perm, ObjectId new_owner,
                          std::string_view new_owner_name,
                          std::string_view noun, std::string_view name) {
  if (auth::ClosureFor(ax.context, ax.role)->is_superuser) {
    return;
  }
  RequireOwner(ax.context, ax.role, perm, noun, name);
  if (!auth::ComputeSetRoleClosure(*auth::RolesOf(ax.context), ax.role)
         .contains(new_owner)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be able to SET ROLE \"", new_owner_name, "\""));
  }
  // A schema has no schema above it, so an unset parent skips the last check
  // rather than resolving nothing.
  if (!schema_id.isSet() || new_owner == ObjectId{perm.owner}) {
    return;
  }
  const auto* schema = catalog::FindSchema(ax.context, schema_id);
  if (schema != nullptr && !auth::ClosureFor(ax.context, new_owner)
                              ->Can(duckdb::CatalogType::SCHEMA_ENTRY,
                                    schema->permissions, AclMode::Create)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for schema ",
                            schema->name.GetIdentifierName()));
  }
}

void Catalog::DropResolved(duckdb::ClientContext* context, ObjectId parent_id,
                           duckdb::CatalogType type, ObjectId id,
                           std::string_view name, bool cascade) {
  if (type == duckdb::CatalogType::INDEX_ENTRY) {
    // The definition outlives the entry: the artifact half reads it.
    if (const auto* entry =
          catalog::Find<SereneDBIndexEntry>(context, parent_id, id)) {
      const auto index = entry->GetInfo();
      DropIndexResolved(context, catalog::SchemaDatabaseId(context, parent_id),
                        index->Cast<catalog::CreateIndexInfo>(),
                        entry->GetInvertedData(), cascade);
      return;
    }
  }
  // The set drop first, whole: duckdb's dependency walk refuses a RESTRICT
  // with dependents there, before any store or artifact work below, and takes
  // the indexes on a relation with it.
  catalog::DropEntryOfKind(context, type, parent_id, name, cascade);
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    GetCatalogStore().DropSequence(id);
  }
}

namespace {

// Binds the durable state that hangs off a definition rather than living in it:
// an inverted index's iresearch segments, a search table's shards, and a
// sequence's counter, which replay could only read once the whole log was in.
// Runs over what survived the log, so nothing is opened for an object a later
// record dropped.
void OpenBootStorage() {
  std::vector<ObjectId> databases;
  catalog::VisitDatabases(nullptr,
                          [&](const catalog::SereneDBDatabaseEntry& ref) {
                            databases.push_back(catalog::IdOf(ref));
                          });
  for (const auto database_id : databases) {
    // The shard a search table's rows live in. Off the sets, like everything
    // else here: what survived the log is the version the entries hold.
    std::vector<const catalog::SereneDBTableEntry*> tables;
    catalog::Visit<SereneDBTableEntry>(
      nullptr, database_id, [&](const catalog::SereneDBTableEntry& table) {
        if (table.IsSearchTable()) {
          tables.push_back(&table);
        }
      });
    for (const auto* table : tables) {
      // The declared key columns are term-indexed under their own ids, so the
      // shard needs them to build its merged config.
      std::vector<ColumnId> pk_columns;
      const auto& columns = table->GetColumns();
      for (const auto index : table->GetPKColumnIndexes()) {
        pk_columns.emplace_back(columns.GetColumn(index).CatalogOid());
      }
      table->SetSearchData(search::SearchTable::Create(
        database_id, ObjectId{table->ParentSchema().oid}, ObjectId{table->oid},
        /*is_new=*/false, table->SearchOptions(), std::move(pk_columns)));
    }
    // The sequences: replay read each definition mid-log, where the counter
    // records after it had not been folded in yet.
    for (const auto* sequence :
         catalog::DatabaseSequences(nullptr, database_id)) {
      sequence->Counter()->ReloadDurable();
    }
    // Before the databases attach: their data WAL replays into the index
    // through GetData(), so the segments have to be open by then.
    for (const auto* index :
         catalog::DatabaseInvertedIndexes(nullptr, database_id)) {
      const auto& inverted = InvertedInfo(index->Definition());
      // A Search-table index shares the table's own store: it stays
      // storage-less and folds its columns into the shard's merged config.
      const auto* relation = catalog::FindIn<SereneDBTableEntry>(
        nullptr, database_id, index->GetRelationId());
      if (relation != nullptr && relation->IsSearchTable()) {
        relation->GetSearchData()->MergeIndexConfig(inverted);
        continue;
      }
      index->SetInvertedData(search::InvertedIndexStorage::Create(
        database_id, inverted, /*is_new=*/false));
    }
  }
}

}  // namespace

void Catalog::FinalizeLoad() { OpenBootStorage(); }

namespace {

std::optional<uint64_t> NumericName(const std::filesystem::path& path) {
  uint64_t id = 0;
  if (absl::SimpleAtoi(path.filename().string(), &id)) {
    return id;
  }
  return std::nullopt;
}

void RemoveOrphanDir(const std::filesystem::path& path) {
  SDB_INFO(STARTUP, "reclaiming orphaned search artifacts at '", path.string(),
           "'");
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    SDB_WARN(STARTUP, "could not remove '", path.string(), "': ", ec.message());
  }
}

// Search artifacts whose ids no committed record names are garbage from a
// crashed drop sweep or create; ids are never reissued, so an unresolvable id
// can only mean a dropped object. Every level of the layout is checked:
// <db>/<schema>/<relation>/<index> and <db>/wal/chunks/<relation>.
void ReclaimOrphanSearchArtifacts() {
  namespace fs = std::filesystem;
  const auto root =
    search::GetSearchEngine().GetPersistedPath(ObjectId{1}).parent_path();
  std::error_code ec;
  for (const auto& db_dir : fs::directory_iterator{root, ec}) {
    const auto db = NumericName(db_dir.path());
    if (!db || !db_dir.is_directory(ec)) {
      continue;
    }
    const ObjectId db_id{*db};
    if (!catalog::FindDatabase(nullptr, db_id)) {
      RemoveOrphanDir(db_dir.path());
      continue;
    }
    const auto resolves = [&](uint64_t id) {
      return catalog::LookupEntryIn(nullptr, db_id, ObjectId{id}) != nullptr;
    };
    for (const auto& schema_dir : fs::directory_iterator{db_dir.path(), ec}) {
      if (!schema_dir.is_directory(ec)) {
        continue;
      }
      if (schema_dir.path().filename() == "wal") {
        for (const auto& chunk_dir :
             fs::directory_iterator{schema_dir.path() / "chunks", ec}) {
          const auto relation = NumericName(chunk_dir.path());
          if (relation && !resolves(*relation)) {
            RemoveOrphanDir(chunk_dir.path());
          }
        }
        continue;
      }
      const auto schema = NumericName(schema_dir.path());
      if (!schema) {
        continue;
      }
      if (!resolves(*schema)) {
        RemoveOrphanDir(schema_dir.path());
        continue;
      }
      for (const auto& table_dir :
           fs::directory_iterator{schema_dir.path(), ec}) {
        const auto relation = NumericName(table_dir.path());
        if (!relation || !table_dir.is_directory(ec)) {
          continue;
        }
        if (!resolves(*relation)) {
          RemoveOrphanDir(table_dir.path());
          continue;
        }
        for (const auto& index_dir :
             fs::directory_iterator{table_dir.path(), ec}) {
          const auto index = NumericName(index_dir.path());
          if (index && index_dir.is_directory(ec) && !resolves(*index)) {
            RemoveOrphanDir(index_dir.path());
          }
        }
      }
    }
  }
}

// The same reconciliation for the counters, which live beside the definition
// tree rather than in it: a crash between a drop's removal records and its
// counter wipe leaves the value behind, and compaction would carry it forever.
void ReclaimOrphanSequenceCounters() {
  auto& store = GetCatalogStore();
  std::vector<ObjectId> databases;
  catalog::VisitDatabases(nullptr,
                          [&](const catalog::SereneDBDatabaseEntry& db) {
                            databases.push_back(catalog::IdOf(db));
                          });
  for (const auto id : store.SequenceIds()) {
    const auto resolves = absl::c_any_of(databases, [&](ObjectId db_id) {
      return catalog::LookupEntryIn(nullptr, db_id, id) != nullptr;
    });
    if (!resolves) {
      SDB_INFO(STARTUP, "reclaiming orphaned sequence counter ", id.id());
      store.DropSequence(id);
    }
  }
}

// The first boot of a data directory: the log is empty, so what everything else
// hangs off has to be written before anything can be created. Not through the
// mutators -- there is no role to check a privilege against yet, and the ids
// are fixed rather than allocated. No statement is behind these, so no commit
// walk records them: the record is written here.
void BootstrapEntry(duckdb::unique_ptr<duckdb::CreateInfo> info,
                    const Permissions& perm) {
  WriteBootstrapEntry(*info, perm);
  ReplayCatalogRecord(std::move(info), perm, /*dropped=*/false);
}

void EnsureSystemDatabase() {
  if (catalog::FindDatabase(nullptr, id::kSystemDB)) {
    SDB_TRACE(STARTUP, "Found system database");
    return;
  }
  // The database every connection defaults to. Its public schema is not a
  // record of its own -- opening the catalog makes it, from the id this record
  // states.
  BootstrapEntry(duckdb::make_uniq<CreateDatabaseInfo>(
                   id::kSystemDB, StaticStrings::kDefaultDatabase, NextId()),
                 Permissions{id::kRootUser});
}

}  // namespace
namespace {

std::shared_ptr<Catalog> gCatalog;

enum class MissingDatabase : uint8_t {
  Refuse,
  Skip,
  Drop,
};

MissingDatabase ParseMissingDatabasePolicy() {
  const auto value = absl::GetFlag(FLAGS_missing_database);
  if (value == "refuse") {
    return MissingDatabase::Refuse;
  }
  if (value == "skip") {
    return MissingDatabase::Skip;
  }
  if (value == "drop") {
    return MissingDatabase::Drop;
  }
  SDB_FATAL(STARTUP, "--missing_database must be refuse, skip or drop, not '",
            value, "'");
}

void ReportUnusableDatabase(const catalog::SereneDBDatabaseEntry& db,
                            std::string_view reason, MissingDatabase policy) {
  switch (policy) {
    case MissingDatabase::Refuse:
      SDB_FATAL(STARTUP, "database '", db.name.GetIdentifierName(), "' (id ",
                catalog::IdOf(db).id(), ") cannot be opened: ", reason,
                ". Pass --missing_database=skip to leave it unattached or "
                "--missing_database=drop to remove it from the catalog.");
    case MissingDatabase::Skip:
      SDB_WARN(STARTUP, "database '", db.name.GetIdentifierName(),
               "' is not attached: ", reason);
      return;
    case MissingDatabase::Drop:
      SDB_WARN(STARTUP, "dropping database '", db.name.GetIdentifierName(),
               "' from the catalog: ", reason);
      return;
  }
}

// A missing file is a loss only when the catalog says there was something in
// it. An empty database whose file has not been created yet is the ordinary
// first-boot shape, and recreating it loses nothing.
bool DatabaseFileUsable(const catalog::SereneDBDatabaseEntry& db,
                        MissingDatabase policy) {
  const auto path = CatalogStore::DatabaseFilePath(catalog::IdOf(db));
  std::error_code ec;
  if (std::filesystem::exists(path, ec)) {
    return true;
  }
  bool has_content = false;
  catalog::Visit<SereneDBTableEntry>(
    nullptr, catalog::IdOf(db),
    [&](const SereneDBTableEntry&) { has_content = true; });
  catalog::Visit<SereneDBViewEntry>(
    nullptr, catalog::IdOf(db),
    [&](const duckdb::ViewCatalogEntry&) { has_content = true; });
  if (!has_content) {
    return true;
  }
  ReportUnusableDatabase(db, absl::StrCat("'", path, "' does not exist"),
                         policy);
  return false;
}

// The ARTs a database's rows are missing. An ART has no duckdb entry, so the
// only thing that writes one down is the checkpoint that captures the rows it
// covers; one built after the last checkpoint is gone after a crash while the
// definition calling for it survives in the catalog log.
void RebuildMissingIndexes() {
  const auto begin = std::chrono::steady_clock::now();
  // Ids first: a rebuild resolves the database it is for, and doing that from
  // inside the walk re-enters the very set the walk holds.
  std::vector<ObjectId> database_ids;
  catalog::VisitDatabases(nullptr,
                          [&](const catalog::SereneDBDatabaseEntry& db) {
                            database_ids.push_back(catalog::IdOf(db));
                          });
  for (const auto database_id : database_ids) {
    GetDataStore().RebuildMissingIndexes(database_id);
  }
  const auto databases = database_ids.size();
  SDB_INFO(STARTUP, "indexes rebuilt for ", databases, " database(s) in ",
           absl::FormatDuration(
             absl::FromChrono(std::chrono::steady_clock::now() - begin)));
  GetCatalogStore().TryCompact();
}

}  // namespace

void InitCatalog() {
  catalog::RegisterForeignCreateInfoDeserializer();
  gCatalog = std::make_shared<Catalog>();

  // Before the roles are read and long before any database is attached: a
  // cluster-global write must never run without the log it goes to.
  catalog::InitClusterCatalogWal();

  EnsureSystemDatabase();

  bool has_roles = false;
  catalog::VisitRoles(nullptr,
                      [&](const SereneDBRoleEntry&) { has_roles = true; });
  if (!has_roles) {
    std::string initial_verifier;
    if (const char* pw = std::getenv("POSTGRES_PASSWORD");
        pw != nullptr && *pw != '\0') {
      auto verifier = network::BuildScramVerifierString(pw);
      if (!verifier) {
        SDB_FATAL(GENERAL,
                  "could not derive a password verifier from "
                  "POSTGRES_PASSWORD");
      }
      initial_verifier = std::move(*verifier);
      SDB_INFO(GENERAL, "bootstrap: initial password set for role '",
               StaticStrings::kDefaultUser, "' from POSTGRES_PASSWORD");
    }
    auto root = duckdb::make_uniq<CreateRoleInfo>(
      id::kRootUser, persistence::RoleData{
                       .name = std::string{StaticStrings::kDefaultUser},
                       .options = static_cast<uint32_t>(RoleOption::All),
                       .conn_limit = CreateRoleInfo::kNoConnLimit,
                       .valid_until = CreateRoleInfo::kNoValidUntil,
                       .password = {std::move(initial_verifier)},
                     });
    BootstrapEntry(std::move(root), Permissions{});
  }

  GetCatalog().FinalizeLoad();

  if (!catalog::GetDatabaseId(StaticStrings::kDefaultDatabase).isSet()) {
    SDB_FATAL(GENERAL, "No ", StaticStrings::kDefaultDatabase,
              " database found in database directory");
  }

  // A data file whose id no committed catalog record names is garbage: the
  // create crashed between the file operation and the catalog append, or the
  // drop crashed after it. Ids are never reissued, so the file can only ever
  // be unreachable. Reclaim before attaching, so nothing opens one.
  {
    for (const auto id : CatalogStore::DatabaseFileIds()) {
      if (catalog::FindDatabase(nullptr, id)) {
        continue;
      }
      const auto path = CatalogStore::DatabaseFilePath(id);
      SDB_INFO(STARTUP, "reclaiming orphaned database file '", path, "'");
      for (const auto& name : {path, path + ".wal"}) {
        std::error_code ec;
        std::filesystem::remove(name, ec);
        if (ec) {
          SDB_WARN(STARTUP, "could not remove '", name, "': ", ec.message());
        }
      }
    }
  }

  ReclaimOrphanSearchArtifacts();
  ReclaimOrphanSequenceCounters();

  // The data half of every attachment: the catalog log already made each one
  // and filled its sets, so this opens the file and replays the data WAL into
  // inverted indexes OpenBootStorage has already injected.
  {
    const auto attach_begin = std::chrono::steady_clock::now();
    const auto missing_policy = ParseMissingDatabasePolicy();
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    std::vector<const catalog::SereneDBDatabaseEntry*> databases;
    catalog::VisitDatabases(nullptr,
                            [&](const catalog::SereneDBDatabaseEntry& db) {
                              databases.push_back(&db);
                            });
    std::vector<const catalog::SereneDBDatabaseEntry*> unusable;
    for (const auto* db : databases) {
      if (!DatabaseFileUsable(*db, missing_policy)) {
        unusable.push_back(db);
        continue;
      }
      try {
        connector::LoadDatabaseStorage(db->name.GetIdentifierName());
      } catch (const std::exception& e) {
        ReportUnusableDatabase(*db, e.what(), missing_policy);
        unusable.push_back(db);
      }
    }
    for (const auto* db : unusable) {
      // The attachment goes whatever the policy is: nothing may reach a
      // database whose rows could not be opened.
      connector::DiscardDatabaseAttachment(db->name.GetIdentifierName());
      if (missing_policy == MissingDatabase::Drop) {
        GetCatalog().DropDatabase(
          NoAccessCheck(), std::string{db->name.GetIdentifierName()}, nullptr);
      }
    }
    // The main database is the cluster-global catalog, which no connection may
    // resolve into: a connection with no search path gets the default database.
    duckdb::DatabaseManager::Get(*conn->context)
      .SetDefaultDatabase(*conn->context,
                          std::string{StaticStrings::kDefaultDatabase});
    SDB_INFO(STARTUP, "database storage loaded in ",
             absl::FormatDuration(absl::FromChrono(
               std::chrono::steady_clock::now() - attach_begin)));
  }
  GetDataStore().MarkReady();

  // After MarkReady: a replayed CREATE INDEX builds its store-side index
  // through the bind contexts, which are gated on the store being up.
  RebuildMissingIndexes();

  // Re-attach persisted foreign servers (external DBs: clickhouse/postgres) so
  // they survive restart, the same way the databases above do. Unlike a local
  // database, a remote being unreachable must NOT abort startup -- warn and
  // continue; the server stays defined and a later access will surface it.
  {
    // Collected before anything is attached: the ATTACH runs a whole statement
    // on a fresh connection, and the walk that found these is holding the set
    // it came out of.
    std::vector<duckdb::unique_ptr<duckdb::CreateInfo>> servers;
    std::vector<ObjectId> databases;
    catalog::VisitDatabases(nullptr,
                            [&](const catalog::SereneDBDatabaseEntry& db) {
                              databases.push_back(catalog::IdOf(db));
                            });
    for (const auto database_id : databases) {
      catalog::VisitForeignServers(
        nullptr, database_id,
        [&](const catalog::SereneDBForeignServerEntry& server) {
          servers.push_back(server.GetInfo());
        });
    }
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    for (const auto& info : servers) {
      const auto& server =
        basics::downCast<const catalog::CreateForeignServerInfo>(*info);
      auto res = RunForeignServerAttach(*conn, server);
      if (res.status == ForeignServerAttachResult::Status::Failed) {
        SDB_WARN(GENERAL, "Failed to re-attach foreign server ",
                 server.GetName(), ": ", res.error);
      }
    }
  }
}

void ShutdownCatalog() {
  // The log goes with the catalog it belongs to: it holds duckdb's own writer,
  // and nothing of duckdb's may outlive the instance it was built over.
  catalog::CloseClusterCatalogWal();
  gCatalog.reset();
}

ObjectId GetDatabaseId(std::string_view name) {
  return catalog::FindDatabaseId(nullptr, name);
}

Catalog& GetCatalog() {
  SDB_ASSERT(gCatalog, "Catalog is not initialized");
  return *gCatalog;
}

Catalog* TryGetCatalog() { return gCatalog.get(); }

}  // namespace sdb::catalog
