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

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>

#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <exception>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <utility>

#include "backup/pg_control.h"
#include "catalog/boot.h"
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

namespace sdb {
namespace {

duckdb::FileSystem& DataFileSystem() {
  return irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
}

}  // namespace

StorageConfig ReadStorageConfig() {
  auto& fs = DataFileSystem();
  StorageConfig config{.directory = lifecycle::ResolveDataDir(
                         absl::GetFlag(FLAGS_server_directory))};
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    config.hba_config = fs.JoinPath(config.directory, "pg_hba.conf");
  } else if (fs.IsPathAbsolute(hba)) {
    config.hba_config = hba;
  } else {
    config.hba_config = fs.JoinPath(config.directory, hba);
  }
  return config;
}

Storage::Storage(Runtime& runtime, const StorageConfig& config)
  : _runtime{runtime}, _search{runtime} {
  bool initialized = false;
  if (!config.directory.empty()) {
    initialized = Lock(config.directory);
    network::pg::hba::SetHbaConfig(config.hba_config);
  }
  absl::Cleanup close = [&] { Close(); };
  try {
    catalog::InitCatalog(config.default_database, initialized);
    _search.Start();
  } catch (const duckdb::OutOfMemoryException& e) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OUT_OF_MEMORY),
      ERR_MSG("replaying the logs at startup does not fit in memory_limit: ",
              duckdb::ErrorData{e}.RawMessage()),
      ERR_HINT("Raise memory_limit in the server configuration."));
  }
  std::move(close).Cancel();
}

Storage::~Storage() { Close(); }

bool Storage::Lock(const std::string& directory) {
  auto& fs = DataFileSystem();
  try {
    if (fs.CreateDirectoryExtended(
          directory, {.mode = duckdb::CreateDirectoryMode::RECURSIVE})) {
      SDB_INFO(STARTUP, "Created database directory: ", directory);
    }
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to create database directory '", directory,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
  const auto version = fs.JoinPath(directory, backup::kPgVersionFile);
  bool initialized = false;
  std::string locked;
  try {
    initialized = fs.FileExists(version);
    locked = initialized ? version : backup::PendingPgVersion(directory);
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to check '", version,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
  auto lock = FileLock::Take(locked, FileLock::Mode::Exclusive);
  if (!lock.ok()) {
    SDB_FATAL(GENERAL, lock.status().message());
  }
  _lock = std::move(*lock);
  return initialized;
}

void Storage::Close() noexcept {
  _runtime.RequestStop();
  _search.Stop();
  _runtime.Background().Drain();
  try {
    irs::DuckDBEngine::Instance().CloseDatabases();
    catalog::CloseLog();
  } catch (const std::exception& ex) {
    SDB_ERROR(GENERAL, "closing the databases failed: ",
              duckdb::ErrorData{ex}.RawMessage());
  }
}

}  // namespace sdb
