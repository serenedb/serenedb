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

#include "check_version_feature.h"

#include <thread>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "catalog/catalog.h"
#include "database/methods/version.h"
#include "general_server/server_options_feature.h"
#include "general_server/state.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/environment_feature.h"
#include "rest_server/server_id_feature.h"

#ifdef SDB_CLUSTER
#include "replication/replication_feature.h"
#endif

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

CheckVersionFeature::CheckVersionFeature(
  Server& server, int* result, std::span<const size_t> non_server_features)
  : SerenedFeature{server, name()},
    _result(result),
    _non_server_features(non_server_features) {
  setOptional(false);
}

void CheckVersionFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options->addOption(
    "--database.check-version", "Check the version of the database and exit.",
    new BooleanParameter(&_check_version),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon,
                                   sdb::options::Flags::Command));
}

void CheckVersionFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  if (!_check_version) {
    return;
  }

  // hard-code our role to a single server instance, because
  // noone else will set our role
  ServerState::instance()->SetRole(ServerState::Role::Single);

  server().forceDisableFeatures(_non_server_features);

  LoggerFeature& logger = server().getFeature<LoggerFeature>();
  logger.disableThreaded();

#ifdef SDB_CLUSTER
  server().getFeature<ReplicationFeature>().disableReplicationApplier();
#endif
}

void CheckVersionFeature::start() {
  if (!_check_version) {
    return;
  }

  // check the version
  if (server().getFeature<ServerIdFeature>().GetIsInitiallyEmpty()) {
    SDB_TRACE("xxxxx", sdb::Logger::STARTUP,
              "skipping version check because database directory was initially "
              "empty");
    *_result = EXIT_SUCCESS;
  } else {
    checkVersion();
  }

  // and force shutdown
  server().beginShutdown();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  gExitFunction(EXIT_SUCCESS, nullptr);
}

void CheckVersionFeature::checkVersion() {
  *_result = 1;

  // run version check
  SDB_TRACE("xxxxx", sdb::Logger::STARTUP, "starting version check");

  SDB_TRACE("xxxxx", sdb::Logger::STARTUP, "database path is: '",
            server().getFeature<DatabasePathFeature>().directory(), "'");

  const bool ignore_datafile_errors =
    GetServerOptions().database_ignore_datafile_errors;

  // iterate over all databases
  for (const auto& database :
       server().getFeature<catalog::CatalogFeature>().Global().GetDatabases()) {
    methods::VersionResult res = methods::Version::check(database->GetId());

    if (res.status == methods::VersionResult::kCannotParseVersionFile ||
        res.status == methods::VersionResult::kCannotReadVersionFile) {
      if (ignore_datafile_errors) {
        // try to install a fresh new, empty VERSION file instead
        if (methods::Version::write(database->GetId(),
                                    std::map<std::string, bool>(), true)
              .ok()) {
          // give it another try
          res = methods::Version::check(database->GetId());
        }
      } else {
        SDB_WARN("xxxxx", Logger::STARTUP,
                 "in order to automatically fix the VERSION file on startup, ",
                 "please start the server with option "
                 "`--database.ignore-datafile-errors true`");
      }
    } else if (res.status == methods::VersionResult::kNoVersionFile) {
      // try to install a fresh new, empty VERSION file instead
      if (methods::Version::write(database->GetId(),
                                  std::map<std::string, bool>(), true)
            .ok()) {
        // give it another try
        res = methods::Version::check(database->GetId());
      }
    }

    SDB_DEBUG("xxxxx", Logger::STARTUP, "version check return status ",
              res.status);
    if (res.status < 0) {
      SDB_FATAL_EXIT_CODE(
        "xxxxx", sdb::Logger::FIXME, EXIT_VERSION_CHECK_FAILED,
        "Database version check failed for '", database->GetName(),
        "'. Please inspect the logs for any errors. If there are no "
        "obvious issues in the logs, please retry with option "
        "`--log.level startup=trace`");
    } else if (res.status == methods::VersionResult::kDowngradeNeeded) {
      // this is safe to do even if further databases will be checked
      // because we will never set the status back to success
      *_result = 3;
      SDB_WARN("xxxxx", sdb::Logger::FIXME,
               "Database version check failed for '", database->GetName(),
               "': downgrade needed");
    } else if (res.status == methods::VersionResult::kUpgradeNeeded &&
               *_result == 1) {
      // this is safe to do even if further databases will be checked
      // because we will never set the status back to success
      *_result = 2;
      SDB_WARN("xxxxx", sdb::Logger::FIXME,
               "Database version check failed for '", database->GetName(),
               "': upgrade needed");
    }
  }

  SDB_DEBUG("xxxxx", Logger::STARTUP,
            "final result of version check: ", *_result);

  if (*_result == 1) {
    *_result = EXIT_SUCCESS;
  } else if (*_result > 1) {
    if (*_result == 3) {
      // downgrade needed
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::FIXME, EXIT_DOWNGRADE_REQUIRED,
                          "Database version check failed: downgrade needed");
    } else if (*_result == 2) {
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::FIXME, EXIT_UPGRADE_REQUIRED,
                          "Database version check failed: upgrade needed");
    } else {
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::FIXME, EXIT_VERSION_CHECK_FAILED,
                          "Database version check failed");
    }
  }
}

}  // namespace sdb
