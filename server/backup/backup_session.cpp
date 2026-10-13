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

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/database_directory.h"
#include "connector/duckdb_storage_extension.h"
#include "search/inverted_index_storage.h"
#include "storage_engine/search_engine.h"

namespace sdb::backup {
namespace {

duckdb::SingleFileBlockManager& BlockManagerOf(duckdb::AttachedDatabase& db) {
  return irs::utils::downCast<duckdb::SingleFileBlockManager>(
    db.GetStorageManager().GetBlockManager());
}

}  // namespace

BackupSession& BackupSession::Instance() {
  static absl::NoDestructor<BackupSession> gSession;
  return *gSession;
}

void BackupSession::Begin() {
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

  // Noexcept committing of backup start
  for (auto* block_manager : block_managers) {
    block_manager->SetDeferBlockReuse(true);
  }
  catalog::DeferRemovals();
  search::GetSearchEngine().BeginBackup();
  _pins = std::move(pins);
  _checkpoint_keys = std::move(checkpoint_keys);
  _block_managers = std::move(block_managers);
  _active = true;
}

void BackupSession::End() noexcept {
  absl::MutexLock lock{&_mutex};
  if (!_active) {
    return;
  }
  search::GetSearchEngine().EndBackup();
  catalog::ResumeRemovals();
  for (auto* block_manager : _block_managers) {
    block_manager->SetDeferBlockReuse(false);
  }
  _block_managers.clear();
  _checkpoint_keys.clear();
  _pins.clear();
  _active = false;
}

bool BackupSession::IsActive() {
  absl::MutexLock lock{&_mutex};
  return _active;
}

}  // namespace sdb::backup
