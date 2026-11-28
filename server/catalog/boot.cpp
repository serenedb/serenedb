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

#include "catalog/boot.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backup/pg_control.h"
#include "backup/restore.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/log.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "pg/catalog/oids.h"
#include "search/store.h"
#include "search/wal_recovery.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kLogDir = "wal";

struct LiveStorage {
  search::Store* storage;
  const duckdb::CatalogEntry* entry;
};

struct StoreListing {
  std::string directory;
  std::vector<std::string> files;
  uint64_t top_segment = 0;
};

std::vector<StoreListing> ListStores(std::vector<std::string> directories) {
  std::vector<StoreListing> listings(directories.size());
  irs::ForEach(
    irs::DuckDBEngine::Instance().instance(),
    absl::c_any_of(directories,
                   [](std::string_view directory) {
                     return duckdb::Path::IsRemote(directory);
                   }),
    directories.size(), [&](size_t i) {
      auto& listing = listings[i];
      listing.directory = std::move(directories[i]);
      try {
        StorageFileSystem().ListFiles(
          listing.directory,
          [&](duckdb::OpenFileInfo& info) {
            uint64_t id = 0;
            if (!duckdb::FileSystem::IsDirectory(info) &&
                irs::ParseSegmentId(info.path, id)) {
              listing.top_segment = std::max(listing.top_segment, id);
              listing.files.push_back(std::move(info.path));
            }
          },
          nullptr, {.strict = true});
      } catch (const std::exception& e) {
        SDB_FATAL(STARTUP, "cannot list directory '", listing.directory,
                  "': ", duckdb::ErrorData{e}.RawMessage());
      }
    });
  for (const auto& listing : listings) {
    irs::ReserveSegmentId(*SegmentIds(), listing.top_segment);
  }
  return listings;
}

bool RemoveOrphans(std::span<const StoreListing> listings,
                   std::span<const LiveStorage> live) {
  irs::containers::FlatHashMap<std::string_view,
                               irs::containers::FlatHashSet<std::string>>
    owned;
  for (const auto& one : live) {
    one.storage->CollectFiles(owned[one.storage->Path()]);
  }
  bool removed = true;
  for (const auto& listing : listings) {
    const auto& keep = owned[listing.directory];
    std::vector<std::string> orphans;
    for (const auto& file : listing.files) {
      if (!keep.contains(file)) {
        orphans.push_back(file);
      }
    }
    removed =
      irs::RemoveIndexFiles(StorageFileSystem(), listing.directory, orphans) &&
      removed;
  }
  return removed;
}

void OpenStorages(SereneDBCatalog& catalog, std::vector<LiveStorage>& live) {
  const auto open = [&](search::Store& storage,
                        const duckdb::CatalogEntry& entry) {
    if (!storage.Opened()) {
      storage.Open();
    }
    live.push_back({&storage, &entry});
  };
  ForEachSearchStorage(
    catalog, [&](SearchTableEntry& table) { open(*table.Storage(), table); },
    [&](InvertedIndexEntry& index) { open(*index.Storage(), index); });
}

void RequirePresent(std::span<const LiveStorage> live) {
  irs::containers::FlatHashSet<std::string_view> seen;
  std::vector<const LiveStorage*> first;
  for (const auto& one : live) {
    if (seen.insert(one.storage->Path()).second) {
      first.push_back(&one);
    }
  }
  std::vector<uint8_t> absent(first.size());
  irs::ForEach(
    irs::DuckDBEngine::Instance().instance(),
    absl::c_any_of(
      first, [](const LiveStorage* one) { return one->storage->Remote(); }),
    first.size(), [&](size_t i) { absent[i] = first[i]->storage->Absent(); });
  for (size_t i = 0; i != first.size(); ++i) {
    if (absent[i]) {
      SDB_FATAL(STARTUP, "the directory '", first[i]->storage->Path(), "' of ",
                first[i]->entry->name.GetIdentifierName(), " (oid ",
                first[i]->entry->oid, ") is missing");
    }
  }
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)},
    engine{directory.empty()
             ? std::string{}
             : duckdb::FileSystem::JoinPath(directory, kEngineDir)} {}

std::string DataDirectory::LogDir() const {
  return duckdb::FileSystem::JoinPath(engine, kLogDir);
}

std::string DataDirectory::StoreDir() const { return StoreDirOf(engine); }

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AccessMode access_mode,
            duckdb::AttachVisibility visibility) {
  duckdb::AttachOptions options{info.options, access_mode};
  options.db_type.assign(type);
  options.visibility = visibility;
  duckdb::DatabaseManager::Get(context).AttachDatabase(context, info, options);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>,
  duckdb::AttachedDatabase& db, duckdb::Catalog&) {
  return duckdb::make_uniq<TransactionManager>(db);
}

duckdb::InternalCatalog CreateInternalCatalog(duckdb::AttachedDatabase& db,
                                              const DataDirectory& layout) {
  duckdb::unique_ptr<duckdb::Catalog> catalog;
  if (db.IsSystem()) {
    catalog = duckdb::make_uniq<ClusterCatalog>(db, layout);
  } else {
    catalog = duckdb::make_uniq<SereneDBCatalog>(
      db, std::make_shared<DatabaseDirectory>(std::string{}), false);
  }
  return {.catalog = std::move(catalog),
          .transaction_manager = duckdb::make_uniq<TransactionManager>(db)};
}

void InitCatalog(bool default_database, bool initialized) {
  auto& instance = irs::DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  const auto& layout = cluster.Layout();
  if (!layout.InMemory()) {
    if (Log::IsEarlierFormat(StorageFileSystem(), layout.LogDir())) {
      SDB_FATAL(STARTUP, "'", layout.directory,
                "' was written with an earlier layout of the data directory, "
                "which this build cannot read");
    }
    BeginStorageWork();
    StorageFileSystem().CreateDirectoryExtended(
      layout.StoreDir(),
      {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
    backup::PrepareDataDirectory(layout);
    if (initialized && !Log::Exists(StorageFileSystem(), layout.LogDir())) {
      SDB_FATAL(STARTUP, "'", layout.directory, "' holds ",
                backup::kPgVersionFile, " but '", layout.LogDir(),
                "' holds no catalog log: refusing to start an empty catalog "
                "in it");
    }
  }
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  auto& transactions = Transactions::Get();
  bool clean = false;
  if (!layout.InMemory()) {
    auto log = std::make_unique<Log>(StorageFileSystem(), layout.LogDir());
    const auto target = [&](duckdb::idx_t id) {
      return InstanceCatalog(context, id);
    };
    transactions.SetReplaying(true);
    log->Replay(
      [&](std::span<uint8_t> payload) {
        context.RunFunctionInTransaction(
          [&] { clean = ReplayRecord(context, target, payload); });
      },
      &CatalogPart);
    if (const auto oids = log->SnapshotOids(); oids != 0) {
      duckdb::DatabaseManager::Get(instance).ClaimOid(oids - 1);
    }
    transactions.SetReplaying(false);
    if (clean) {
      log->Append(StartupRecord());
    }
    transactions.Open(std::move(log));
  }
  context.RunFunctionInTransaction(
    [&] { cluster.Bootstrap(context, default_database); });
  auth::PublishRoles(cluster);
  if (layout.InMemory()) {
    return;
  }
  if (!initialized) {
    backup::PublishPgVersion(layout.directory);
  }
  const auto attached =
    SereneDBDatabases(duckdb::DatabaseManager::Get(instance));
  std::vector<LiveStorage> stores;
  for (const auto& db : attached) {
    OpenStorages(db->GetCatalog().Cast<SereneDBCatalog>(), stores);
  }
  RequirePresent(stores);
  std::vector<StoreListing> listings;
  if (!clean) {
    std::vector<std::string> directories;
    context.RunFunctionInTransaction([&] {
      for (const auto& tablespace :
           cluster.TablespaceDirs(cluster.GetCatalogTransaction(context))) {
        directories.push_back(StoreDirOf(tablespace));
      }
    });
    listings = ListStores(std::move(directories));
  }
  const bool recovered = search::RecoverSearchStorages(context, attached);
  const bool removed = clean || RemoveOrphans(listings, stores);
  EndStorageWork(removed && recovered);
  std::vector<duckdb::reference<ForeignServerCatalogEntry>> servers;
  for (const auto& db : attached) {
    db->GetCatalog()
      .Cast<SereneDBCatalog>()
      .Set(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
      .ScanCommitted([&](duckdb::CatalogEntry& entry) {
        servers.emplace_back(entry.Cast<ForeignServerCatalogEntry>());
      });
  }
  for (auto& server : servers) {
    try {
      context.RunFunctionInTransaction([&] { server.get().Attach(context); });
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "failed to re-attach foreign server '",
               server.get().name.GetIdentifierName(),
               "': ", duckdb::ErrorData{e}.RawMessage());
    }
  }
}

void CloseLog() {
  auto& transactions = Transactions::Get();
  if (auto* log = transactions.GetLog(); log && StorageTidy()) {
    log->Append(ShutdownRecord());
  }
  transactions.Close();
}

}  // namespace sdb::catalog
