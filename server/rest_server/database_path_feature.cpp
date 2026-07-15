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
#include <system_error>

#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/lifecycle.h"
#include "basics/lockfile.h"
#include "basics/log.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"

ABSL_FLAG(std::string, server_directory, "serenedb-data",
          "Path to the database directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <datadir>/pg_hba.conf; a relative path is resolved "
          "against the data directory.");

using namespace sdb::basics;

namespace sdb {

DatabasePathFeature::DatabasePathFeature()
  : _directory(absl::GetFlag(FLAGS_server_directory)) {
  // Positional arg wins over the flag if given (size-check ran in
  // AppServer::parseOptions).
  if (auto p = lifecycle::DataDirArg(); !p.empty()) {
    _directory = std::string(p);
  }

  // strip trailing separators and make the path absolute so log lines and
  // error messages aren't ambiguous about which directory we're touching.
  _directory.erase(_directory.find_last_not_of(SERENEDB_DIR_SEPARATOR_STR) + 1);
  std::error_code ec;
  if (auto abs = std::filesystem::absolute(_directory, ec); !ec) {
    _directory = abs.string();
  }

  // Resolve the HBA config path against the (now absolute) data directory.
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    _hba_config_file =
      basics::file_utils::BuildFilename(_directory, "pg_hba.conf");
  } else if (std::filesystem::path{hba}.is_absolute()) {
    _hba_config_file = hba;
  } else {
    _hba_config_file = basics::file_utils::BuildFilename(_directory, hba);
  }

  if (!std::filesystem::is_directory(_directory, ec)) {
    std::filesystem::create_directories(_directory, ec);
    if (!ec) {
      SDB_INFO(GENERAL, "Created database directory: ", _directory);
    } else {
      SDB_FATAL(GENERAL, "Unable to create database directory '", _directory,
                "': ", ec.message());
    }
  }

  // Acquire the LOCK file in the data directory. The lockfile guards against
  // a second serened starting on the same data dir. No explicit shutdown hook
  // needed: libs/basics/lockfile.cpp owns a LockfileRemover whose static dtor
  // runs on normal exit and releases / unlinks every lockfile we acquired.
  // Fatal paths (_exit / abort) skip static dtors; the stale lockfile that
  // remains is handled by VerifyLockFile on the next start.
  std::string lock_filename =
    basics::file_utils::BuildFilename(_directory, "LOCK");
  if (!VerifyLockFile(lock_filename.c_str())) {
    std::string other_pid;
    try {
      other_pid = basics::file_utils::Slurp(lock_filename);
    } catch (...) {
    }
    if (other_pid.empty()) {
      SDB_FATAL_EXIT_CODE(
        GENERAL, EXIT_COULD_NOT_LOCK,
        "failed to read/write lockfile, please check the file permissions "
        "of the lockfile '",
        lock_filename, "'");
    } else {
      SDB_FATAL_EXIT_CODE(
        GENERAL, EXIT_COULD_NOT_LOCK, "database is locked by process ",
        other_pid, "; please stop it first and check that the lockfile '",
        lock_filename, "' goes away.");
    }
  }
  if (std::filesystem::exists(lock_filename, ec) && !ec) {
    std::filesystem::remove(lock_filename, ec);
    if (ec) {
      SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
                          "failed to remove an abandoned lockfile '",
                          lock_filename, "': ", ec.message());
    }
  }
  if (!CreateLockFile(lock_filename.c_str())) {
    SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
                        "failed to lock the database directory using '",
                        lock_filename, "'");
  }

  gInstance = this;
}

DatabasePathFeature::~DatabasePathFeature() { gInstance = nullptr; }

}  // namespace sdb
