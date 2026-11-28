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
#include <duckdb/main/database.hpp>
#include <exception>
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
          "Path to the database directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <datadir>/pg_hba.conf; a relative path is resolved "
          "against the data directory.");

ABSL_FLAG(std::string, log_directory, "",
          "Local directory for the log of the commits since the last "
          "snapshot. Empty uses <server directory>/logs.");

ABSL_FLAG(bool, require_existing, false,
          "Refuse to start when the server directory holds no server, "
          "instead of creating a new one.");

ABSL_FLAG(bool, checkpoint_at_stop, false,
          "End every clean stop with a checkpoint and an empty log directory, "
          "so the server can start anywhere from its server directory alone.");

namespace sdb {
namespace {

constexpr std::string_view kLockFile = "LOCK";

duckdb::FileSystem& DataFileSystem() {
  return irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
}

}  // namespace

catalog::ServerDirectory ReadServerDirectory(std::string directory) {
  if (directory.empty()) {
    return {};
  }
  auto log = absl::GetFlag(FLAGS_log_directory);
  if (log.empty()) {
    log = catalog::ServerDirectory::DefaultLog(directory);
  }
  return {std::move(directory), std::move(log)};
}

StorageConfig ReadStorageConfig() {
  auto& fs = DataFileSystem();
  StorageConfig config{
    .layout = ReadServerDirectory(
      lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory))),
    .require_existing = absl::GetFlag(FLAGS_require_existing),
  };
  const auto& directory = config.layout.directory;
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
    catalog::InitCatalog(config.default_database, config.require_existing);
    _search.Start();
  } catch (...) {
    Close();
    throw;
  }
}

Storage::~Storage() { Close(); }

void Storage::Lock(const catalog::ServerDirectory& layout) {
  auto& fs = DataFileSystem();
  const auto& directory = layout.directory;
  try {
    if (fs.CreateDirectoryExtended(
          directory, {.mode = duckdb::CreateDirectoryMode::RECURSIVE})) {
      SDB_INFO(STARTUP, "Created server directory: ", directory);
    }
    fs.CreateDirectoryExtended(
      layout.LogDir(), {.mode = duckdb::CreateDirectoryMode::RECURSIVE});
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to create server directory '", directory,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
  const auto take = [](const std::string& path) {
    auto lock = FileLock::Take(path, FileLock::Mode::Exclusive);
    if (!lock.ok()) {
      SDB_FATAL(GENERAL, lock.status().message());
    }
    return std::move(*lock);
  };
  try {
    _lock = take(backup::OpenPgVersion(directory));
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to open '",
              fs.JoinPath(directory, backup::kPgVersionFile),
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
  if (layout.LogDir() != catalog::ServerDirectory::DefaultLog(directory)) {
    _log_lock = take(fs.JoinPath(layout.LogDir(), kLockFile));
  }
}

void Storage::Close() noexcept {
  _runtime.RequestStop();
  _search.Stop();
  _runtime.Background().Drain();
  auto& transactions = catalog::Transactions::Get();
  std::optional<catalog::Transactions::FinalCheckpoint> final;
  if (absl::GetFlag(FLAGS_checkpoint_at_stop)) {
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
      transactions.WriteFinalCheckpoint(std::move(*final),
                                        catalog::StorageTidy());
      transactions.Close();
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
