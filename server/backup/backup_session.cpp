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

#include "backup/backup_session.h"

#include <absl/base/no_destructor.h>
#include <absl/cleanup/cleanup.h>

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <utility>

#include "backup/commit_gate.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_storage_extension.h"
#include "search/inverted_index_storage.h"
#include "search/search_db_wal.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::backup {
namespace {

duckdb::SingleFileBlockManager& BlockManagerOf(duckdb::AttachedDatabase& db) {
  return irs::utils::downCast<duckdb::SingleFileBlockManager>(
    db.GetStorageManager().GetBlockManager());
}

uint64_t GenerationOf(const irs::DirectoryReader& reader) {
  return reader ? reader.Meta().index_meta.gen : 0;
}

void CollectStores(catalog::SereneDBCatalog& catalog, duckdb::idx_t database,
                   std::vector<StoreCut>& stores) {
  std::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
  catalog.ScanSchemas(
    [&](duckdb::SchemaCatalogEntry& schema) { schemas.emplace_back(schema); });
  for (auto& schema : schemas) {
    schema.get().Scan(
      duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry& entry) {
        const auto* table = dynamic_cast<catalog::SearchTableEntry*>(&entry);
        if (table && table->Storage()) {
          stores.push_back(
            {database, entry.oid,
             GenerationOf(table->Storage()->GetDirectoryReader())});
        }
      });
    schema.get().Scan(
      duckdb::CatalogType::INDEX_ENTRY, [&](duckdb::CatalogEntry& entry) {
        const auto* index = dynamic_cast<catalog::InvertedIndexEntry*>(&entry);
        if (index && index->Storage()) {
          stores.push_back(
            {database, entry.oid,
             GenerationOf(
               index->Storage()->GetInvertedIndexSnapshot()->reader)});
        }
      });
  }
}

[[noreturn]] void ThrowCancelled() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                  ERR_MSG("canceled while starting a backup"));
}

void ThrowIoError(std::string_view what, const std::filesystem::path& path,
                  const std::error_code& ec) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_IO_ERROR),
    ERR_MSG("could not ", what, " \"", path.string(), "\": ", ec.message()));
}

Cut RecordCut(
  const std::filesystem::path& backup_dir,
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> pins,
  std::span<duckdb::SingleFileBlockManager* const> block_managers,
  absl::FunctionRef<bool()> cancelled) {
  Cut cut;
  auto& gate = CommitGate::Instance();
  if (!gate.Close(cancelled)) {
    ThrowCancelled();
  }
  absl::Cleanup open = [&gate] { gate.Open(); };
  cut.catalog_link = backup_dir / kCatalogLink;
  cut.catalog_log_size =
    catalog::ClusterOf().SnapshotCatalogLog(cut.catalog_link);
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& storage = pins[i]->GetStorageManager();
    auto& block_manager = *block_managers[i];
    auto& entry = cut.databases.emplace_back();
    entry.oid = pins[i]->oid;
    auto commit_lock = storage.GetCommitLock();
    entry.wal_size = storage.GetWALSize();
    entry.checkpoint_iteration = block_manager.GetCheckpointIteration();
    entry.used_size =
      duckdb::SingleFileBlockManager::BLOCK_START +
      block_manager.TotalBlocks() * block_manager.GetBlockAllocSize();
  }
  for (size_t i = 0; i < pins.size(); ++i) {
    auto& db_catalog = pins[i]->GetCatalog().Cast<catalog::SereneDBCatalog>();
    if (auto* wal = db_catalog.Directory()->TryWal()) {
      cut.databases[i].search_tick = wal->SealAndCut();
    }
  }
  for (const auto& db : pins) {
    CollectStores(db->GetCatalog().Cast<catalog::SereneDBCatalog>(), db->oid,
                  cut.stores);
  }
  return cut;
}

void Resume(
  std::span<duckdb::SingleFileBlockManager* const> block_managers) noexcept {
  search::GetSearchEngine().EndBackup();
  catalog::ResumeRemovals();
  for (auto* block_manager : block_managers) {
    block_manager->SetDeferBlockReuse(false);
  }
}

void RemoveBackupDir(const std::filesystem::path& backup_dir) noexcept {
  std::error_code ec;
  std::filesystem::remove_all(backup_dir, ec);
  if (ec) {
    SDB_WARN(GENERAL, "could not remove '", backup_dir.string(),
             "': ", ec.message());
  }
}

}  // namespace

BackupSession& BackupSession::Instance() {
  static absl::NoDestructor<BackupSession> gSession;
  return *gSession;
}

void BackupSession::Begin(absl::FunctionRef<bool()> cancelled) {
  absl::MutexLock lock{&_mutex};
  if (_active) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("a backup is already in progress"));
  }
  std::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> pins;
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  for (auto& db : manager.GetDatabases()) {
    auto& db_catalog = db->GetCatalog();
    if (db_catalog.GetCatalogType() == catalog::SereneDBCatalog::kStorageType &&
        !db_catalog.InMemory()) {
      pins.push_back(std::move(db));
    }
  }
  std::vector<duckdb::unique_ptr<duckdb::StorageLockKey>> checkpoint_keys;
  std::vector<duckdb::SingleFileBlockManager*> block_managers;
  for (auto& db : pins) {
    checkpoint_keys.push_back(
      duckdb::DuckTransactionManager::Get(*db).SharedCheckpointLock());
    block_managers.push_back(&BlockManagerOf(*db));
  }
  for (auto& db : pins) {
    for (const auto& storage : connector::BoundStorages(*db)) {
      if (!storage->HasPendingCheckpoint()) {
        continue;
      }
      if (duckdb::ValidChecker::IsInvalidated(*db)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
          ERR_MSG("cannot start a backup: database \"",
                  db->GetName().GetIdentifierName(), "\" is invalidated"),
          ERR_HINT("Re-attach the database, then retry."));
      }
      SDB_ENSURE(false, "cannot start a backup: inverted index ",
                 storage->GetId(), " in database \"",
                 db->GetName().GetIdentifierName(),
                 "\" has an unfinished checkpoint save");
    }
  }
  auto& cluster = catalog::ClusterOf();
  auto backup_dir =
    catalog::ClusterLayout(cluster.GetAttached()).EngineDir() / kBackupDir;
  std::error_code ec;
  std::filesystem::remove_all(backup_dir, ec);
  if (ec) {
    ThrowIoError("remove directory", backup_dir, ec);
  }
  if (!std::filesystem::create_directory(backup_dir, ec)) {
    ThrowIoError("create directory", backup_dir, ec);
  }
  catalog::SyncDirectory(backup_dir.parent_path());

  for (auto* block_manager : block_managers) {
    block_manager->SetDeferBlockReuse(true);
  }
  catalog::DeferRemovals();
  absl::Cleanup undo = [&]() noexcept {
    Resume(block_managers);
    RemoveBackupDir(backup_dir);
  };
  if (!search::GetSearchEngine().BeginBackup(cancelled)) {
    ThrowCancelled();
  }
  auto cut = RecordCut(backup_dir, pins, block_managers, cancelled);
  std::move(undo).Cancel();

  // Noexcept committing of backup start
  _pins = std::move(pins);
  _checkpoint_keys = std::move(checkpoint_keys);
  _block_managers = std::move(block_managers);
  _backup_dir = std::move(backup_dir);
  _cut = std::move(cut);
  _active = true;
}

void BackupSession::End() noexcept {
  absl::MutexLock lock{&_mutex};
  if (_active) {
    Stop();
  }
}

void BackupSession::Stop() noexcept {
  Resume(_block_managers);
  _block_managers.clear();
  _checkpoint_keys.clear();
  _pins.clear();
  RemoveBackupDir(_backup_dir);
  _backup_dir.clear();
  _cut = {};
  _active = false;
}

bool BackupSession::IsActive() {
  absl::MutexLock lock{&_mutex};
  return _active;
}

}  // namespace sdb::backup
