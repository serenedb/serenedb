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

#include <absl/strings/str_join.h>

#include "app/app_server.h"
#include "app/global_context.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

DatabasePathFeature::DatabasePathFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

void DatabasePathFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options
    ->addOption("--database.directory", "The path to the database directory.",
                new StringParameter(&_directory))
    .setLongDescription(R"(This defines the location where all data of a
server is stored.

Make sure the directory is writable by the serened process. You should further
not use a database directory which is provided by a network filesystem such as
NFS. The reason is that networked filesystems might cause inconsistencies when
there are multiple parallel readers or writers or they lack features required by
serened, e.g. `flock()`.)");

  // --database.required-directory-state: dropped; CI scripts can stat
  // the dir themselves.
}

void DatabasePathFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  const auto& positionals = options->processingResult().positionals;

  if (1 == positionals.size()) {
    _directory = positionals[0];
  } else if (1 < positionals.size()) {
    SDB_FATAL(GENERAL,
              "expected at most one database directory, got '",
              absl::StrJoin(positionals, ","), "'");
  }

  if (_directory.empty()) {
    _directory = "serenedb-data";
    SDB_INFO(GENERAL,
             "no database path has been supplied, using default '", _directory,
             "'");
  }

  // strip trailing separators
  _directory =
    basics::string_utils::RTrim(_directory, SERENEDB_DIR_SEPARATOR_STR);

  auto ctx = GlobalContext::gContext;

  if (ctx == nullptr) {
    SDB_FATAL(GENERAL, "failed to get global context.");
  }

  ctx->normalizePath(_directory, "database.directory", false);
}

void DatabasePathFeature::prepare() {
  // TempPath collision check removed with the feature; the system temp
  // path is typically /tmp which never overlaps with the data dir.
}

void DatabasePathFeature::start() {
  // create base directory if it does not exist
  if (!basics::file_utils::IsDirectory(_directory)) {
    std::string system_error_str;
    long error_no;

    const auto res =
      SdbCreateRecursiveDirectory(_directory, error_no, system_error_str);

    if (res == ERROR_OK) {
      SDB_INFO(GENERAL,
               "Created database directory: ", _directory);
    } else {
      SDB_FATAL(GENERAL,
                "Unable to create database directory '", _directory,
                "': ", system_error_str);
    }
  }
}

std::string DatabasePathFeature::subdirectoryName(
  std::string_view sub_directory) const {
  SDB_ASSERT(!_directory.empty());
  return basics::file_utils::BuildFilename(_directory, sub_directory);
}

}  // namespace sdb
