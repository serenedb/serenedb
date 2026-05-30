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
#include "app/temp_path.h"
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
  // check if temporary directory and database directory are identical
  {
    std::string directory_copy = _directory;
    basics::file_utils::MakePathAbsolute(directory_copy);

    if (server().hasFeature<TempPath>()) {
      auto& tf = server().getFeature<TempPath>();
      // the feature is not present in unit tests, so make the execution depend
      // on whether the feature is available
      std::string temp_path_copy = tf.path();
      basics::file_utils::MakePathAbsolute(temp_path_copy);
      temp_path_copy =
        basics::string_utils::RTrim(temp_path_copy, SERENEDB_DIR_SEPARATOR_STR);

      if (directory_copy == temp_path_copy) {
        SDB_FATAL(GENERAL, "database directory '", directory_copy,
          "' is identical to the temporary directory. ",
          "This can cause follow-up problems, including data loss. Please "
          "review your setup!");
      }
    }
  }


  // all good here
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
