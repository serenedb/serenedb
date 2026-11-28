////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "storage/storage.h"

#include <absl/flags/flag.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/main/database.hpp>
#include <exception>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <string>
#include <utility>

#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "catalog/database_directory.h"
#include "catalog/transaction.h"
#include "network/pg/hba.h"
#include "runtime/runtime.h"
#include "server/utils/lifecycle.h"

ABSL_FLAG(std::string, server_directory, "serenedb-data",
          "Path to the server directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <server directory>/pg_hba.conf; a relative path is "
          "resolved against the server directory.");

ABSL_FLAG(std::string, wal_directory, "",
          "Local directory for the write-ahead log (WAL) of the commits since "
          "the last snapshot. Empty uses <server directory>/wal; a server "
          "directory on an object store needs it.");

ABSL_FLAG(std::string, temp_directory, "",
          "Local directory for the temporary files of queries that spill to "
          "disk and the copies of evicted blocks of object store files, which "
          "the server reads again after a restart. Empty uses <server "
          "directory>/temp; a server directory on an object store needs it.");

ABSL_FLAG(std::string, max_temp_directory_size, "",
          "The most disk space the temporary directory may take, such as "
          "100GiB; its copies of evicted blocks take half of it at most. Empty "
          "is 90% of the disk space available where it lies.");

ABSL_FLAG(bool, require_existing, false,
          "Refuse to start when the server directory holds no server, "
          "instead of creating a new one.");

ABSL_FLAG(std::string, abandon_wal, "",
          "The id of a lost WAL directory that owns a server directory on an "
          "object store: the server starts from its last snapshot, and the "
          "commits that only that WAL holds are lost.");

namespace sdb {
namespace {

duckdb::FileSystem& DataFileSystem() {
  return irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
}

void RequireLocal(std::string_view flag, std::string_view value,
                  std::string_view directory) {
  if (value.empty()) {
    SDB_FATAL(GENERAL, "the server directory '", directory,
              "' is on an object store, so --", flag,
              " has to name a local directory");
  }
  if (duckdb::Path::IsRemote(value)) {
    SDB_FATAL(GENERAL, "--", flag, " '", value,
              "' has to name a local directory");
  }
}

}  // namespace

catalog::ServerDirectory ReadServerDirectory(std::string directory) {
  if (directory.empty()) {
    return {};
  }
  auto wal = absl::GetFlag(FLAGS_wal_directory);
  if (wal.empty()) {
    wal = catalog::ServerDirectory::DefaultWal(directory);
  }
  auto temp = ReadTempDirectory(directory);
  return {std::move(directory), std::move(wal), std::move(temp)};
}

std::string ReadTempDirectory(const std::string& directory) {
  auto temp = absl::GetFlag(FLAGS_temp_directory);
  if (temp.empty() && !directory.empty()) {
    temp = catalog::ServerDirectory::DefaultTemp(directory);
  }
  return temp;
}

StorageConfig ReadStorageConfig() {
  auto& fs = DataFileSystem();
  StorageConfig config{
    .layout = ReadServerDirectory(
      lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory))),
    .require_existing = absl::GetFlag(FLAGS_require_existing),
    .abandon_wal = absl::GetFlag(FLAGS_abandon_wal),
  };
  const auto& directory = config.layout.directory;
  if (duckdb::Path::IsRemote(directory)) {
    RequireLocal("wal_directory", absl::GetFlag(FLAGS_wal_directory),
                 directory);
    RequireLocal("temp_directory", absl::GetFlag(FLAGS_temp_directory),
                 directory);
  } else if (duckdb::Path::IsRemote(config.layout.WalDir())) {
    RequireLocal("wal_directory", config.layout.WalDir(), directory);
  }
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    config.hba_config = fs.JoinPath(directory, "pg_hba.conf");
  } else if (fs.IsPathAbsolute(hba)) {
    config.hba_config = hba;
  } else {
    config.hba_config = fs.JoinPath(directory, hba);
  }
  return config;
}

Storage::Storage(Runtime& runtime, const StorageConfig& config)
  : _runtime{runtime}, _search{runtime} {
  if (!config.layout.InMemory()) {
    Lock(config.layout);
    network::pg::hba::SetHbaConfig(config.hba_config);
  }
  try {
    catalog::InitCatalog({.default_database = config.default_database,
                          .require_existing = config.require_existing,
                          .abandon_wal = config.abandon_wal,
                          .wal_owner = _wal_owner ? &*_wal_owner : nullptr,
                          .temp_owner = _temp_owner ? &*_temp_owner : nullptr});
    _search.Start();
  } catch (...) {
    Close();
    throw;
  }
  _remote = duckdb::Path::IsRemote(config.layout.directory);
  _directory = config.layout.directory;
  _started = true;
}

Storage::~Storage() { Close(); }

void Storage::Lock(const catalog::ServerDirectory& layout) {
  auto& fs = DataFileSystem();
  const auto& directory = layout.directory;
  const auto create = [&](const std::string& path) {
    try {
      return fs.CreateDirectoryExtended(
        path, {.mode = duckdb::CreateDirectoryMode::RECURSIVE});
    } catch (const std::exception& e) {
      SDB_FATAL(GENERAL, "Unable to create directory '", path,
                "': ", duckdb::ErrorData{e}.RawMessage());
    }
  };
  const auto take = [](const std::string& path) {
    auto lock = FileLock::Take(path, FileLock::Mode::Exclusive);
    if (!lock.ok()) {
      SDB_FATAL(GENERAL, lock.status().message());
    }
    return std::move(*lock);
  };
  const bool remote = duckdb::Path::IsRemote(directory);
  if (!remote) {
    if (create(directory)) {
      SDB_INFO(STARTUP, "Created server directory: ", directory);
    }
    try {
      _lock = take(backup::OpenPgVersion(directory));
    } catch (const std::exception& e) {
      SDB_FATAL(GENERAL, "Unable to open '",
                fs.JoinPath(directory, backup::kPgVersionFile),
                "': ", duckdb::ErrorData{e}.RawMessage());
    }
  }
  const auto lock_owner = [&](const std::string& path) {
    create(path);
    try {
      return catalog::LockOwner(path);
    } catch (const std::exception& e) {
      SDB_FATAL(GENERAL, duckdb::ErrorData{e}.RawMessage());
    }
  };
  if (layout.WalHasOwner()) {
    _wal_owner = lock_owner(layout.WalDir());
  } else {
    create(layout.WalDir());
  }
  if (layout.TempHasOwner()) {
    _temp_owner = lock_owner(layout.TempDir());
  }
}

void Storage::Close() noexcept {
  _runtime.RequestStop();
  _search.Stop();
  _runtime.Background().Drain();
  auto& transactions = catalog::Transactions::Get();
  std::optional<catalog::Transactions::FinalCheckpoint> final;
  if (_started && lifecycle::HandoverRequested()) {
    try {
      final = transactions.CaptureFinalCheckpoint();
    } catch (const std::exception& ex) {
      SDB_ERROR(GENERAL, "the checkpoint of the stop failed: ",
                duckdb::ErrorData{ex}.RawMessage());
      lifecycle::FailShutdown();
    }
  }
  try {
    irs::DuckDBEngine::Instance().CloseDatabases();
    if (final) {
      const bool clean = transactions.WriteFinalCheckpoint(
        std::move(*final), catalog::StorageTidy());
      transactions.Close();
      if (clean && _remote) {
        auto& fs = DataFileSystem();
        fs.TryRemoveFile(fs.JoinPath(_directory, catalog::kOwnerFile));
      }
    } else {
      catalog::CloseLog();
    }
  } catch (const std::exception& ex) {
    SDB_ERROR(GENERAL, "closing the databases failed: ",
              duckdb::ErrorData{ex}.RawMessage());
    lifecycle::FailShutdown();
    transactions.Close();
  }
}

}  // namespace sdb
