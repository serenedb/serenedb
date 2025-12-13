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

#include "init_database_feature.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/terminal-utils.h"
#include "general_server/state.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/environment_feature.h"

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

InitDatabaseFeature::InitDatabaseFeature(
  Server& server, std::span<const size_t> non_server_features)
  : SerenedFeature{server, name()}, _non_server_features(non_server_features) {
  setOptional(false);
}

void InitDatabaseFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options->addOption(
    "--database.init-database", "Initialize an empty database.",
    new BooleanParameter(&_init_database),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon,
                                   sdb::options::Flags::Command));

  options->addOption(
    "--database.restore-admin", "Reset the admin users and set a new password.",
    new BooleanParameter(&_restore_admin),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon,
                                   sdb::options::Flags::Command));

  options->addOption(
    "--database.password", "The initial password of the root user.",
    new StringParameter(&_password),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));
}

void InitDatabaseFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  const ProgramOptions::ProcessingResult& result = options->processingResult();
  _seen_password = result.touched("database.password");

  if (_init_database || _restore_admin) {
    server().forceDisableFeatures(_non_server_features);
    ServerState::instance()->SetRole(ServerState::Role::Single);
  }
}

void InitDatabaseFeature::prepare() {
  if (!_seen_password) {
    if (SdbGETENV("SERENEDB_DEFAULT_ROOT_PASSWORD", _password)) {
      _seen_password = true;
    }
  }

  if (!_init_database && !_restore_admin) {
    return;
  }

  if (_init_database) {
    checkEmptyDatabase();
  }

  if (!_seen_password) {
    while (true) {
      std::string password1 =
        readPassword("Please enter a new password for the SereneDB root user");

      if (!password1.empty()) {
        std::string password2 = readPassword("Repeat password");

        if (password1 == password2) {
          _password = password1;
          break;
        }
        SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                  "passwords do not match, please repeat");
      } else {
        SDB_FATAL("xxxxx", sdb::Logger::FIXME,
                  "initialization aborted by user");
      }
    }
  }
}

std::string InitDatabaseFeature::readPassword(const std::string& message) {
  std::string password;

  sdb::log::Flush();
  // Wait for the logger thread to flush eventually existing output.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  std::cerr << std::flush;
  std::cout << message << ": " << std::flush;
#ifdef SERENEDB_HAVE_TERMIOS_H
  terminal_utils::SetStdinVisibility(false);
  std::getline(std::cin, password);
  terminal_utils::SetStdinVisibility(true);
#else
  std::getline(std::cin, password);
#endif
  std::cout << std::endl;

  return password;
}

void InitDatabaseFeature::checkEmptyDatabase() {
  auto& database = server().getFeature<DatabasePathFeature>();
  std::string path = database.directory();
  std::string server_file = database.subdirectoryName("SERVER");

  bool empty = false;
  std::string message;
  int code = EXIT_CODE_RESOLVING_FAILED;

  auto do_exit = [&] {
    SDB_FATAL("xxxxx", sdb::Logger::FIXME, message);

    auto& logger = server().getFeature<LoggerFeature>();
    logger.unprepare();

    gExitFunction(code, nullptr);
    exit(code);
  };

  if (file_utils::Exists(path)) {
    if (!file_utils::IsDirectory(path)) {
      message = "database path '" + path + "' is not a directory";
      code = EXIT_FAILURE;
      do_exit();
    }

    if (file_utils::Exists(server_file)) {
      if (file_utils::IsDirectory(server_file)) {
        message = "database SERVER '" + server_file + "' is not a file";
        code = EXIT_FAILURE;
        do_exit();
      }
    } else {
      empty = true;
    }
  } else {
    empty = true;
  }

  if (!empty) {
    message = "database already initialized, refusing to initialize it again";
    code = EXIT_DB_NOT_EMPTY;
    do_exit();
  }
}

}  // namespace sdb
