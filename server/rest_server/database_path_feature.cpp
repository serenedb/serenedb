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

#include "database_path_feature.h"

#include <absl/flags/flag.h>

#include <filesystem>
#include <iresearch/utils/application_exit.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/operating_system.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <system_error>

#include "backup/pg_control.h"
#include "server/utils/file_utils.h"
#include "server/utils/lifecycle.h"
#include "server/utils/lockfile.h"

ABSL_FLAG(std::string, server_directory, "serenedb-data",
          "Path to the database directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <datadir>/pg_hba.conf; a relative path is resolved "
          "against the data directory.");

namespace sdb {

DatabasePathFeature::DatabasePathFeature()
  : _directory(
      lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory))) {
  std::error_code ec;
  // Resolve the HBA config path against the (now absolute) data directory.
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    _hba_config_file =
      utils::file_utils::BuildFilename(_directory, "pg_hba.conf");
  } else if (std::filesystem::path{hba}.is_absolute()) {
    _hba_config_file = hba;
  } else {
    _hba_config_file = utils::file_utils::BuildFilename(_directory, hba);
  }

  if (!std::filesystem::is_directory(_directory, ec)) {
    std::filesystem::create_directories(_directory, ec);
    if (!ec) {
      SDB_INFO(STARTUP, "Created database directory: ", _directory);
    } else {
      SDB_FATAL(GENERAL, "Unable to create database directory '", _directory,
                "': ", ec.message());
    }
  }

  backup::EnsurePgVersion(_directory);
  if (const auto status = LockFile(
        (std::filesystem::path{_directory} / backup::kPgVersionFile).string());
      !status.ok()) {
    SDB_FATAL(GENERAL, status.message());
  }

  gInstance = this;
}

DatabasePathFeature::~DatabasePathFeature() { gInstance = nullptr; }

}  // namespace sdb
