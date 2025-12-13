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
  : SerenedFeature{server, name()}, _required_directory_state("any") {
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

  options->addOption(
    "--database.required-directory-state",
    "The required state of the database directory at startup "
    "(non-existing: the database directory must not exist, existing: the"
    "database directory must exist, empty: the database directory must exist "
    "but be empty, populated: the database directory must exist and contain "
    "specific files already, any: any state is allowed)",
    new DiscreteValuesParameter<StringParameter>(&_required_directory_state,
                                                 {
                                                   "any",
                                                   "non-existing",
                                                   "existing",
                                                   "empty",
                                                   "populated",
                                                 }));
}

void DatabasePathFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  const auto& positionals = options->processingResult().positionals;

  if (1 == positionals.size()) {
    _directory = positionals[0];
  } else if (1 < positionals.size()) {
    SDB_FATAL("xxxxx", sdb::Logger::FIXME,
              "expected at most one database directory, got '",
              absl::StrJoin(positionals, ","), "'");
  }

  if (_directory.empty()) {
    SDB_FATAL("xxxxx", sdb::Logger::FIXME,
              "no database path has been supplied, giving up, please use "
              "the '--database.directory' option");
  }

  // strip trailing separators
  _directory =
    basics::string_utils::RTrim(_directory, SERENEDB_DIR_SEPARATOR_STR);

  auto ctx = GlobalContext::gContext;

  if (ctx == nullptr) {
    SDB_FATAL("xxxxx", sdb::Logger::FIXME, "failed to get global context.");
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
        SDB_FATAL(
          "xxxxx", sdb::Logger::FIXME, "database directory '", directory_copy,
          "' is identical to the temporary directory. ",
          "This can cause follow-up problems, including data loss. Please "
          "review your setup!");
      }
    }
  }

  if (_required_directory_state == "any") {
    // database directory can have any state. this is the default
    return;
  }

  if (_required_directory_state == "non-existing") {
    if (basics::file_utils::IsDirectory(_directory)) {
      SDB_FATAL(
        "xxxxx", sdb::Logger::STARTUP, "database directory '", _directory,
        "' already exists, but option "
        "'--database.required-directory-state' was set to 'non-existing'");
    }
    return;
  }

  // existing, empty, populated when we get here
  if (!basics::file_utils::IsDirectory(_directory)) {
    SDB_FATAL(
      "xxxxx", sdb::Logger::STARTUP, "database directory '", _directory,
      "' does not exist, but option '--database.required-directory-state' "
      "was set to '",
      _required_directory_state, "'");
  }

  if (_required_directory_state == "existing") {
    // directory exists. all good
    return;
  }

  std::vector<std::string> files;
  for (const auto& it : basics::file_utils::ListFiles(_directory)) {
    if (it.empty() || basics::file_utils::IsDirectory(it)) {
      continue;
    }

    // we are interested in just the filenames
    files.emplace_back(SdbBasename(it));
  }

  if (_required_directory_state == "empty" && !files.empty()) {
    SDB_FATAL(
      "xxxxx", sdb::Logger::STARTUP, "database directory '", _directory,
      "' is not empty, but option '--database.required-directory-state' "
      "was set to '",
      _required_directory_state, "'");
  }

  if (_required_directory_state == "populated" &&
      (std::find(files.begin(), files.end(), "ENGINE") == files.end() ||
       std::find(files.begin(), files.end(), "SERVER") == files.end())) {
    SDB_FATAL("xxxxx", sdb::Logger::STARTUP, "database directory '", _directory,
              "' is not properly populated, but option "
              "'--database.required-directory-state' was set to '",
              _required_directory_state, "'");
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
      SDB_INFO("xxxxx", sdb::Logger::FIXME, "created database directory '",
               _directory, "'");
    } else {
      SDB_FATAL("xxxxx", sdb::Logger::FIXME,
                "unable to create database directory '", _directory,
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
