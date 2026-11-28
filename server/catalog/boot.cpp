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
#include <duckdb/storage/temporary_file_manager.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <optional>
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
#include "catalog/pins.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "pg/catalog/oids.h"
#include "search/store.h"
#include "search/wal_recovery.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kWalDir = "wal";
constexpr std::string_view kTempDir = "temp";
constexpr std::string_view kSnapshotFile = "snapshot";
constexpr std::string_view kBackupsDir = "backups";

std::optional<UniqueId> ReadServerOwner(duckdb::FileSystem& fs,
                                        const ServerDirectory& layout) {
  const auto path = fs.JoinPath(layout.directory, kOwnerFile);
  if (!fs.FileExists(path)) {
    return std::nullopt;
  }
  auto owner = UniqueId::FromBytes(
    irs::ReadWhole<std::string>(duckdb::QueryContext{}, fs, path));
  if (!owner) {
    SDB_FATAL(STARTUP, "'", path, "' is damaged");
  }
  return owner;
}

void TakeServerOwner(duckdb::FileSystem& fs, const ServerDirectory& layout,
                     const UniqueId& wal_id, bool replace) {
  const auto path = fs.JoinPath(layout.directory, kOwnerFile);
  try {
    if (replace) {
      irs::WriteWhole(fs, path, wal_id.Bytes(), true);
      return;
    }
    const auto file =
      fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                          duckdb::FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
    const auto bytes = wal_id.Bytes();
    file->Write(duckdb::QueryContext{}, const_cast<char*>(bytes.data()),
                bytes.size(), 0);
    file->Close();
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "could not take the server directory '",
              layout.directory, "' for the WAL directory ", wal_id.Hex(), ": ",
              duckdb::ErrorData{e}.RawMessage());
  }
}

UniqueId ClaimWal(LockedOwner& owner, const ServerDirectory& layout,
                  const UniqueId& server_id) {
  if (owner.content.empty()) {
    const auto wal_id = UniqueId::New();
    WriteOwner(owner, absl::StrCat(server_id.Bytes(), wal_id.Bytes()));
    return wal_id;
  }
  const auto server = UniqueId::FromBytes(
    std::string_view{owner.content}.substr(0, UniqueId::kBytes));
  const auto wal =
    owner.content.size() == 2 * UniqueId::kBytes
      ? UniqueId::FromBytes(
          std::string_view{owner.content}.substr(UniqueId::kBytes))
      : std::nullopt;
  if (!server || !wal) {
    SDB_FATAL(STARTUP, "'", owner.path, "' is damaged");
  }
  if (*server != server_id) {
    SDB_FATAL(STARTUP, "the WAL directory '", layout.WalDir(),
              "' belongs to the server ", server->Hex(), ", and '",
              layout.directory, "' holds the server ", server_id.Hex());
  }
  return *wal;
}

void ClaimTemp(LockedOwner& owner, const ServerDirectory& layout,
               const UniqueId& server_id) {
  const auto self = std::string{server_id.Bytes()};
  const bool own = owner.content == self;
  if (!own && !owner.content.empty()) {
    SDB_INFO(STARTUP, "removing what another server left in the temporary "
                      "directory '",
             layout.TempDir(), "'");
  }
  duckdb::TemporaryDirectoryHandle::RemoveLeftovers(
    irs::DuckDBEngine::Instance().instance(), layout.TempDir(), own);
  if (!own) {
    WriteOwner(owner, self);
  }
}

Log::Identity NewIdentity() {
  return {.server_id = UniqueId::New(), .mock_nonce = backup::NewMockNonce()};
}

void AdoptIdentity(const Log::Identity& identity, const ServerDirectory& layout,
                   bool fresh) {
  try {
    backup::PositionSource::Instance().Adopt(
      layout.Remote() ? std::string{} : layout.directory, identity.server_id,
      identity.mock_nonce, fresh);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, duckdb::ErrorData{e}.RawMessage());
  }
}

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
  auto& pins = Pins::Get();
  bool removed = true;
  for (const auto& listing : listings) {
    const auto& keep = owned[listing.directory];
    std::vector<std::string> orphans;
    for (const auto& file : listing.files) {
      uint64_t id = 0;
      if (keep.contains(file) ||
          (irs::ParseSegmentId(file, id) &&
           pins.ParkIfHeld(listing.directory, id, std::span{&file, 1}))) {
        continue;
      }
      orphans.push_back(file);
    }
    removed =
      irs::RemoveIndexFiles(StorageFileSystem(), listing.directory, orphans) &&
      removed;
  }
  return removed;
}

bool HoldsFiles(const std::string& directory) {
  auto& fs = StorageFileSystem();
  if (!fs.DirectoryExists(directory)) {
    return false;
  }
  bool found = false;
  fs.ListFiles(directory, [&](duckdb::OpenFileInfo&) { found = true; }, nullptr,
               {.strict = true});
  return found;
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

ServerDirectory::ServerDirectory(std::string directory_p, std::string wal_p,
                                 std::string temp_p)
  : directory{std::move(directory_p)},
    wal{std::move(wal_p)},
    temp{std::move(temp_p)} {}

std::string ServerDirectory::DefaultWal(std::string_view directory) {
  return duckdb::FileSystem::JoinPath(std::string{directory}, kWalDir);
}

std::string ServerDirectory::DefaultTemp(std::string_view directory) {
  return duckdb::FileSystem::JoinPath(std::string{directory}, kTempDir);
}

bool ServerDirectory::Remote() const {
  return !InMemory() && duckdb::Path::IsRemote(directory);
}

bool ServerDirectory::WalHasOwner() const {
  return !InMemory() && (Remote() || wal != DefaultWal(directory));
}

bool ServerDirectory::TempHasOwner() const {
  return !InMemory() && !temp.empty() &&
         (Remote() || temp != DefaultTemp(directory));
}

std::string ServerDirectory::SnapshotPath() const {
  return duckdb::FileSystem::JoinPath(directory, kSnapshotFile);
}

std::string ServerDirectory::StoreDir() const { return StoreDirOf(directory); }

std::string ServerDirectory::BackupsDir() const {
  return duckdb::FileSystem::JoinPath(directory, kBackupsDir);
}

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
                                              const ServerDirectory& layout) {
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

void InitCatalog(const CatalogStart& start) {
  auto& instance = irs::DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  const auto& layout = cluster.Layout();
  auto& fs = StorageFileSystem();
  bool created = false;
  const bool remote = layout.Remote();
  Log::Identity identity;
  UniqueId wal_id;
  std::optional<UniqueId> owner;
  bool abandoned = false;
  if (!layout.InMemory()) {
    BeginStorageWork();
    auto restore = backup::FindRestore(layout);
    if (!remote) {
      backup::PrepareServerDirectory(layout, restore);
      restore.reset();
    }
    std::optional<Log::Identity> stored;
    try {
      stored = restore ? restore->identity
                       : Log::ReadIdentity(fs, layout.SnapshotPath());
    } catch (const std::exception& e) {
      SDB_FATAL(STARTUP, duckdb::ErrorData{e}.RawMessage());
    }
    created = !stored;
    if (created) {
      if (start.require_existing) {
        SDB_FATAL(STARTUP, "'", layout.directory,
                  "' holds no server, and --require_existing refuses to "
                  "create one");
      }
      if (HoldsFiles(layout.StoreDir())) {
        SDB_FATAL(STARTUP, "'", layout.directory,
                  "' holds segments but no snapshot: refusing to start an "
                  "empty server over them");
      }
    }
    identity = created ? NewIdentity() : *stored;
    try {
      if (start.wal_owner) {
        wal_id = ClaimWal(*start.wal_owner, layout, identity.server_id);
      }
      if (start.temp_owner) {
        ClaimTemp(*start.temp_owner, layout, identity.server_id);
      } else if (!layout.TempDir().empty()) {
        // inside the server directory, whose lock keeps every other server out
        duckdb::TemporaryDirectoryHandle::RemoveLeftovers(
          irs::DuckDBEngine::Instance().instance(), layout.TempDir(), true);
      }
    } catch (const std::exception& e) {
      SDB_FATAL(STARTUP, duckdb::ErrorData{e}.RawMessage());
    }
    if (remote) {
      owner = ReadServerOwner(fs, layout);
      const bool foreign = owner && *owner != wal_id;
      abandoned = foreign && (restore || owner->Hex() == start.abandon_wal);
      if (foreign && !abandoned) {
        SDB_FATAL(STARTUP, "the server directory '", layout.directory,
                  "' belongs to the WAL directory ", owner->Hex(),
                  ", which may hold commits its snapshot does not have: "
                  "start the server with that WAL directory, or, when it is "
                  "lost, start with --abandon_wal=",
                  owner->Hex(), " and lose those commits");
      }
      if (abandoned && !restore) {
        SDB_WARN(STARTUP, "starting '", layout.directory,
                 "' from its snapshot without the WAL directory ", owner->Hex(),
                 ": the commits only that WAL holds are lost");
      }
      backup::PrepareServerDirectory(layout, restore);
    }
  }
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  auto& transactions = Transactions::Get();
  bool clean = false;
  bool initialized = true;
  if (!layout.InMemory()) {
    auto log =
      std::make_unique<Log>(fs, layout.WalDir(), layout.SnapshotPath());
    const bool takeover = remote && (!owner || abandoned);
    if (abandoned && !created) {
      Log::Discard(fs, layout.WalDir(), layout.SnapshotPath());
    } else if (created || takeover) {
      log->Clear();
    }
    const auto target = [&](duckdb::idx_t id) {
      return InstanceCatalog(context, id);
    };
    transactions.SetReplaying(true);
    const auto replayed = log->Replay(
      [&](std::span<uint8_t> payload) {
        context.RunFunctionInTransaction(
          [&] { clean = ReplayRecord(context, target, payload); });
      },
      &CatalogPart);
    transactions.SetReplaying(false);
    initialized = replayed.snapshot;
    clean = !abandoned && (clean || replayed.clean);
    AdoptIdentity(identity, layout, created);
    try {
      Pins::Get().Load(fs, layout.BackupsDir());
    } catch (const std::exception& e) {
      SDB_FATAL(STARTUP, "cannot read the backups of '", layout.directory,
                "': ", duckdb::ErrorData{e}.RawMessage());
    }
    log->SetIdentity(identity);
    if (takeover) {
      TakeServerOwner(fs, layout, wal_id, abandoned);
    }
    fs.CreateDirectoryExtended(
      layout.StoreDir(),
      {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
    if (clean) {
      log->Append(StartupRecord());
    }
    transactions.Open(std::move(log));
  }
  context.RunFunctionInTransaction(
    [&] { cluster.Bootstrap(context, start.default_database); });
  auth::PublishRoles(cluster);
  if (layout.InMemory()) {
    return;
  }
  if (!initialized) {
    if (const auto status = transactions.TruncateLog(); !status.ok()) {
      SDB_FATAL(STARTUP, "cannot write the first snapshot of '",
                layout.directory, "': ", status.message());
    }
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
    std::vector<std::string> locations;
    context.RunFunctionInTransaction([&] {
      for (auto& tablespace :
           cluster.TablespaceDirs(cluster.GetCatalogTransaction(context))) {
        directories.push_back(StoreDirOf(tablespace));
        if (tablespace != layout.directory) {
          locations.push_back(std::move(tablespace));
        }
      }
    });
    for (const auto& location : locations) {
      try {
        ClaimLocation(location);
      } catch (const std::exception& e) {
        SDB_FATAL(STARTUP, duckdb::ErrorData{e}.RawMessage());
      }
    }
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
