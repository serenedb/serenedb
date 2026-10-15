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
#include <absl/time/clock.h>

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

#include "backup/backup_label.h"
#include "backup/commit_gate.h"
#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_storage_extension.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"
#include "utils/file_utils.h"

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

std::optional<Cut> RecordCut(
  const std::filesystem::path& backup_dir,
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> pins,
  absl::FunctionRef<bool()> cancelled) {
  Cut cut;
  auto& gate = CommitGate();
  if (!gate.Close(cancelled)) {
    return std::nullopt;
  }
  absl::Cleanup open = [&gate] { gate.Open(); };
  cut.catalog_link = backup_dir / kCatalogLink;
  cut.catalog_log_size =
    catalog::ClusterOf().SnapshotCatalogLog(cut.catalog_link);
  for (const auto& db : pins) {
    auto& storage = db->GetStorageManager();
    auto& entry = cut.databases.emplace_back();
    entry.oid = db->oid;
    auto commit_lock = storage.GetCommitLock();
    entry.wal_size = storage.GetWALSize();
    entry.checkpoint_iteration =
      storage.GetBlockManager().GetCheckpointIteration();
  }
  for (const auto& db : pins) {
    CollectStores(db->GetCatalog().Cast<catalog::SereneDBCatalog>(), db->oid,
                  cut.stores);
  }
  return cut;
}

void Resume(
  std::span<duckdb::SingleFileBlockManager* const> block_managers) noexcept {
  search::GetSearchEngine().Maintenance().Open();
  catalog::ResumeRemovals();
  for (auto* block_manager : block_managers) {
    block_manager->SetDeferBlockReuse(false);
  }
}

}  // namespace

BackupSession& BackupSession::Instance() {
  static absl::NoDestructor<BackupSession> gSession;
  return *gSession;
}

std::optional<uint64_t> BackupSession::Begin(
  BackupOwner owner, std::string_view label,
  absl::FunctionRef<bool()> cancelled) {
  absl::MutexLock lock{&_mutex};
  if (_active && _owner.session == owner.session) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("a backup is already in progress in this session"));
  }
  if (_active) {
    const auto holder = _owner.pid;
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("a backup is already in progress"),
      ERR_DETAIL("It is held by the session with PID ", holder, "."));
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
  const auto& layout =
    catalog::ClusterLayout(catalog::ClusterOf().GetAttached());
  auto backup_dir = layout.EngineDir() / kBackupDir;
  std::error_code ec;
  std::filesystem::remove_all(backup_dir, ec);
  if (ec) {
    utils::file_utils::ThrowFileError("remove directory", backup_dir.string(),
                                      ec);
  }
  catalog::CreateDirectory(backup_dir);

  for (auto* block_manager : block_managers) {
    block_manager->SetDeferBlockReuse(true);
  }
  catalog::DeferRemovals();
  absl::Cleanup undo = [&]() noexcept {
    Resume(block_managers);
    catalog::RemoveTree(backup_dir);
  };
  if (!search::GetSearchEngine().Maintenance().Close(cancelled)) {
    return std::nullopt;
  }
  auto cut = RecordCut(backup_dir, pins, cancelled);
  if (!cut) {
    return std::nullopt;
  }
  auto& positions = PositionSource::Instance();
  const auto start_lsn = positions.Issue();
  cut->catalog_link = cut->catalog_link.lexically_relative(layout.directory);
  auto label_file = BuildBackupLabel(
    label, start_lsn, positions.SystemIdentifier(), absl::Now(), *cut);
  std::move(undo).Cancel();

  // Noexcept committing of backup start
  _pins = std::move(pins);
  _checkpoint_keys = std::move(checkpoint_keys);
  _block_managers = std::move(block_managers);
  _backup_dir = std::move(backup_dir);
  _owner = owner;
  _label_file = std::move(label_file);
  _active = true;
  return start_lsn;
}

FinishedBackup BackupSession::Finish(const void* session) {
  absl::MutexLock lock{&_mutex};
  if (!_active || _owner.session != session) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("backup is not in progress"),
                    ERR_HINT("Did you call pg_backup_start()?"));
  }
  FinishedBackup finished{PositionSource::Instance().Issue(),
                          std::move(_label_file)};
  Stop();
  return finished;
}

void BackupSession::AbortIfOwnedBy(const void* session) noexcept {
  absl::MutexLock lock{&_mutex};
  if (_active && _owner.session == session) {
    SDB_WARN(GENERAL, "aborting the backup of the closed session with PID ",
             _owner.pid);
    Stop();
  }
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
  catalog::RemoveTree(_backup_dir);
  _backup_dir.clear();
  _owner = {};
  _label_file.clear();
  _active = false;
}

bool BackupSession::IsActive() {
  absl::MutexLock lock{&_mutex};
  return _active;
}

}  // namespace sdb::backup
