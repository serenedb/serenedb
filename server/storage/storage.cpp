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

#include "storage/storage.h"

#include <absl/flags/flag.h>

#include <filesystem>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <system_error>
#include <utility>

#include "backup/pg_control.h"
#include "catalog/boot.h"
#include "network/pg/hba.h"
#include "server/utils/file_utils.h"
#include "server/utils/lifecycle.h"

ABSL_FLAG(std::string, server_directory, "serenedb-data",
          "Path to the database directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <datadir>/pg_hba.conf; a relative path is resolved "
          "against the data directory.");

namespace sdb {

StorageConfig ReadStorageConfig() {
  StorageConfig config{.directory = lifecycle::ResolveDataDir(
                         absl::GetFlag(FLAGS_server_directory))};
  // Resolve the HBA config path against the (now absolute) data directory.
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    config.hba_config =
      utils::file_utils::BuildFilename(config.directory, "pg_hba.conf");
  } else if (std::filesystem::path{hba}.is_absolute()) {
    config.hba_config = hba;
  } else {
    config.hba_config = utils::file_utils::BuildFilename(config.directory, hba);
  }
  return config;
}

Storage::Storage(Runtime& runtime, const StorageConfig& config)
  : _runtime{runtime} {
  bool initialized = false;
  if (!config.directory.empty()) {
    std::error_code ec;
    if (!std::filesystem::is_directory(config.directory, ec)) {
      std::filesystem::create_directories(config.directory, ec);
      if (!ec) {
        SDB_INFO(STARTUP, "Created database directory: ", config.directory);
      } else {
        SDB_FATAL(GENERAL, "Unable to create database directory '",
                  config.directory, "': ", ec.message());
      }
    }
    const std::filesystem::path data_dir{config.directory};
    const auto version = data_dir / backup::kPgVersionFile;
    initialized = std::filesystem::exists(version, ec);
    if (ec) {
      SDB_FATAL(GENERAL, "Unable to check '", version.string(),
                "': ", ec.message());
    }
    auto lock = FileLock::Take(
      (initialized ? version : backup::PendingPgVersion(data_dir)).string(),
      FileLock::Mode::Exclusive);
    if (!lock.ok()) {
      SDB_FATAL(GENERAL, lock.status().message());
    }
    _lock = std::move(*lock);
    network::pg::hba::SetHbaConfig(config.hba_config);
  }
  catalog::InitCatalog(config.default_database, initialized);
}

Storage::~Storage() {
  _runtime.RequestStop();
  // The shutdown checkpoint of every attached database, taken here rather than
  // left to the DuckDB destructor in main(): an inverted index vetoes it unless
  // it can read its definition.
  StopLogged("storage", [] {
    irs::DuckDBEngine::Instance().CloseDatabases();
    catalog::CloseLog();
  });
}

}  // namespace sdb
