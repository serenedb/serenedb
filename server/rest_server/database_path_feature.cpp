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
#include "basics/cleanup_functions.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/lifecycle.h"
#include "basics/lockfile.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"

ABSL_FLAG(std::string, database_directory, "",
          "Path to the database directory. Overrides any positional arg.");

using namespace sdb::basics;

namespace sdb {

DatabasePathFeature::DatabasePathFeature()
  : _directory(absl::GetFlag(FLAGS_database_directory)) {
  // Positional arg wins over the flag if given (size-check ran in
  // AppServer::parseOptions).
  if (auto p = lifecycle::DataDirArg(); !p.empty()) {
    _directory = std::string(p);
  }

  if (_directory.empty()) {
    _directory = "serenedb-data";
    SDB_INFO(GENERAL, "no database path has been supplied, using default '",
             _directory, "'");
  }

  // strip trailing separators and make the path absolute so log lines and
  // error messages aren't ambiguous about which directory we're touching.
  _directory =
    basics::string_utils::RTrim(_directory, SERENEDB_DIR_SEPARATOR_STR);
  basics::file_utils::MakePathAbsolute(_directory);

  gInstance = this;
}

DatabasePathFeature::~DatabasePathFeature() { gInstance = nullptr; }

void DatabasePathFeature::start() {
  // Create base directory if it does not exist.
  if (!basics::file_utils::IsDirectory(_directory)) {
    std::error_code ec;
    std::filesystem::create_directories(_directory, ec);
    if (!ec) {
      SDB_INFO(GENERAL, "Created database directory: ", _directory);
    } else {
      SDB_FATAL(GENERAL, "Unable to create database directory '", _directory,
                "': ", ec.message());
    }
  }

  // Acquire the LOCK file in the data directory. The lockfile guards against
  // a second serened starting on the same data dir.
  std::string lock_filename =
    basics::file_utils::BuildFilename(_directory, "LOCK");
  auto res = VerifyLockFile(lock_filename.c_str());
  if (res != ERROR_OK) {
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
  {
    std::error_code ec;
    if (std::filesystem::exists(lock_filename, ec) && !ec) {
      std::filesystem::remove(lock_filename, ec);
      if (ec) {
        SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
                            "failed to remove an abandoned lockfile '",
                            lock_filename, "': ", ec.message());
      }
    }
  }
  res = CreateLockFile(lock_filename.c_str());
  if (res != ERROR_OK) {
    SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
                        "failed to lock the database directory using '",
                        lock_filename, "': ", GetErrorStr(res));
  }
  basics::CleanupFunctions::registerFunction(
    std::make_unique<basics::CleanupFunctions::CleanupFunction>(
      [lock_filename](int, void*) {
        std::ignore = DestroyLockFile(lock_filename.c_str());
      }));
}

std::string DatabasePathFeature::subdirectoryName(
  std::string_view sub_directory) const {
  SDB_ASSERT(!_directory.empty());
  return basics::file_utils::BuildFilename(_directory, sub_directory);
}

}  // namespace sdb
