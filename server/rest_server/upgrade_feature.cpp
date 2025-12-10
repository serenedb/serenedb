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

#include "upgrade_feature.h"

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "auth/role_utils.h"
#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "database/methods/upgrade.h"
#include "general_server/authentication_feature.h"
#include "general_server/server_options_feature.h"
#include "general_server/state.h"
#include "rest_server/check_version_feature.h"
#include "rest_server/init_database_feature.h"
#include "rest_server/restart_action.h"

#ifdef SDB_CLUSTER
#include "cluster/backup_feature.h"
#include "replication/replication_feature.h"
#endif

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

UpgradeFeature::UpgradeFeature(Server& server, int* result,
                               std::span<const size_t> non_server_features)
  : SerenedFeature{server, name()},
    _upgrade(false),
    _upgrade_check(true),
    _result(result),
    _non_server_features(non_server_features) {
  setOptional(false);
}

void UpgradeFeature::addTask(methods::Upgrade::Task&& task) {
  _tasks.push_back(std::move(task));
}

void UpgradeFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options
    ->addOption("--database.auto-upgrade",
                "Perform a database upgrade if necessary.",
                new BooleanParameter(&_upgrade))
    .setLongDescription(R"(If you specify this option, then the server
performs a database upgrade instead of starting normally.

A database upgrade first compares the version number stored in the `VERSION`
file in the database directory with the current server version.

If the version number found in the database directory is higher than that of the
server, the server considers this is an unintentional downgrade and warns about
this. Using the server in these conditions is neither recommended nor supported.

If the version number found in the database directory is lower than that of the
server, the server checks whether there are any upgrade tasks to perform.
It then executes all required upgrade tasks and prints the status. If one of the
upgrade tasks fails, the server exits with an error. Re-starting the server with
the upgrade option again triggers the upgrade check and execution until the
problem is fixed.

Whether or not you specify this option, the server always perform a version
check on startup. If you running the server with a non-matching version number
in the `VERSION` file, the server refuses to start.)");

  options->addOption(
    "--database.upgrade-check", "Skip the database upgrade if set to false.",
    new BooleanParameter(&_upgrade_check),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));
}

static int UpgradeRestart() {
  unsetenv(StaticStrings::kUpgradeEnvName.c_str());
  return 0;
}

void UpgradeFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
  // The following environment variable is another way to run a database
  // upgrade. If the environment variable is set, the system does a database
  // upgrade and then restarts itself without the environment variable.
  // This is used in backup if a restore to a backup happens which is from
  // an older database version. The restore process sets the environment
  // variable at runtime and then does a restore. After the restart (with
  // the old data) the database upgrade is run and another restart is
  // happening afterwards with the environment variable being cleared.
  char* upgrade = getenv(StaticStrings::kUpgradeEnvName.c_str());
  if (upgrade != nullptr) {
    _upgrade = true;
    gRestartAction = new std::function<int()>();
    *gRestartAction = UpgradeRestart;
    SDB_INFO("xxxxx", Logger::STARTUP, "Detected environment variable ",
             StaticStrings::kUpgradeEnvName, " with value ", upgrade,
             " will perform database auto-upgrade and immediately restart.");
  }

  if (_upgrade && !_upgrade_check) {
    SDB_FATAL_EXIT_CODE(
      "xxxxx", sdb::Logger::FIXME, EXIT_INVALID_OPTION_VALUE,
      "cannot specify both '--database.auto-upgrade true' and "
      "'--database.upgrade-check false'");
  }

  if (_upgrade &&
      server().getFeature<CheckVersionFeature>().GetCheckVersion()) {
    SDB_FATAL("xxxxx", Logger::FIXME,
              "cannot specify both '--database.check-version' and "
              "'--database.auto-upgrade'");
  }

  if (!_upgrade) {
    SDB_TRACE("xxxxx", sdb::Logger::FIXME,
              "executing upgrade check: not disabling server features");
    return;
  }

  SDB_INFO("xxxxx", sdb::Logger::FIXME,
           "executing upgrade procedure: disabling server features");

  // if we run the upgrade, we need to disable a few features that may get
  // in the way...
  if (ServerState::instance()->IsCoordinator()) {
    auto disable_daemon_and_supervisor = [&]() {
      if constexpr (Server::contains<DaemonFeature>()) {
        server().forceDisableFeatures({Server::id<DaemonFeature>()});
      }
      if constexpr (Server::contains<SupervisorFeature>()) {
        server().forceDisableFeatures({Server::id<SupervisorFeature>()});
      }
    };

    server().forceDisableFeatures({Server::id<GreetingsFeature>()});
    disable_daemon_and_supervisor();
  } else {
    server().forceDisableFeatures(_non_server_features);
    server().forceDisableFeatures(
      {Server::id<BootstrapFeature>(), Server::id<HttpEndpointProvider>()});
  }

#ifdef SDB_CLUSTER
  server().getFeature<ReplicationFeature>().disableReplicationApplier();
  server().getFeature<BackupFeature>().forceDisable();
#endif
}

void UpgradeFeature::prepare() {
  // need to register tasks before creating any database
  methods::Upgrade::registerTasks(*this);
}

void UpgradeFeature::start() {
  auto& init = server().getFeature<InitDatabaseFeature>();

  // upgrade the database
  if (_upgrade_check) {
    if (!ServerState::instance()->IsCoordinator()) {
      // no need to run local upgrades in the coordinator
      upgradeLocalDatabase();
    }

    if (ServerState::instance()->IsClientNode()) {
      if (!ServerState::instance()->IsCoordinator() && !init.restoreAdmin() &&
          !init.defaultPassword().empty()) {
        // this method sets the root password in case on non-coordinators.
        // on coordinators, we cannot execute it here, because the roles
        // is not yet present (was true when roles was collection)?
        // for coordinators, the default password will be installed by the
        // BootstrapFeature later.
        if (auto res = auth::CreateRootRole(false); res.fail()) {
          SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                    "failed to set default password: ", res.errorMessage());
          *_result = EXIT_FAILURE;
        }
      }
    }

    // change admin user
    if (init.restoreAdmin() && ServerState::instance()->IsClientNode()) {
      auto r = auth::RemoveAllRoles();
      if (!r.ok()) {
        SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                  "failed to clear users: ", r.errorMessage());
        *_result = EXIT_FAILURE;
        return;
      }

      r = auth::CreateRootRole(false);

      if (!r.ok()) {
        SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                  "failed to create root user: ", r.errorMessage());
        *_result = EXIT_FAILURE;
        return;
      }
      auto old_level = sdb::Logger::FIXME.GetLevel();
      sdb::Logger::FIXME.SetLevel(sdb::LogLevel::INFO);
      SDB_INFO("xxxxx", sdb::Logger::FIXME, "Password changed.");
      sdb::Logger::FIXME.SetLevel(old_level);
      *_result = EXIT_SUCCESS;
    }
  }

  // and force shutdown
  if (_upgrade || init.isInitDatabase() || init.restoreAdmin()) {
    if (init.isInitDatabase()) {
      *_result = EXIT_SUCCESS;
    }

    if (!ServerState::instance()->IsCoordinator() || !_upgrade) {
      SDB_INFO(
        "xxxxx", sdb::Logger::STARTUP,
        "server will now shut down due to upgrade, database initialization "
        "or admin restoration.");

      // in the non-coordinator case, we are already done now and will shut
      // down. in the coordinator case, the actual upgrade is performed by the
      // ClusterUpgradeFeature, which is way later in the startup sequence.
      server().beginShutdown();
    }
  }
}

void UpgradeFeature::upgradeLocalDatabase() {
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, "starting database init/upgrade");

  const bool ignore_datafile_errors =
    GetServerOptions().database_ignore_datafile_errors;

  for (auto& database :
       server().getFeature<catalog::CatalogFeature>().Local().GetDatabases()) {
    auto res =
      methods::Upgrade::startup(*database, _upgrade, ignore_datafile_errors);

    if (res.result.fail()) {
      std::string_view type_name = "initialization";
      int exit_code = EXIT_FAILURE;

      if (res.type == methods::VersionResult::kUpgradeNeeded) {
        type_name = "upgrade";  // an upgrade failed or is required

        if (!_upgrade) {
          exit_code = EXIT_UPGRADE_REQUIRED;
          SDB_ERROR("xxxxx", sdb::Logger::FIXME, "Database '",
                    database->GetName(), "' needs upgrade. ",
                    "Please start the server with --database.auto-upgrade");
        } else {
          exit_code = EXIT_UPGRADE_FAILED;
        }
      } else if (res.type == methods::VersionResult::kDowngradeNeeded) {
        exit_code = EXIT_DOWNGRADE_REQUIRED;
      } else if (res.type == methods::VersionResult::kCannotParseVersionFile ||
                 res.type == methods::VersionResult::kCannotReadVersionFile) {
        exit_code = EXIT_VERSION_CHECK_FAILED;
      }

      SDB_FATAL_EXIT_CODE("xxxxx", sdb::Logger::FIXME, exit_code, "Database '",
                          database->GetName(), "' ", type_name, " failed (",
                          res.result.errorMessage(),
                          "). Please inspect the logs from the ", type_name,
                          " procedure and try starting the server again.");
    }
  }

  if (_upgrade) {
    *_result = EXIT_SUCCESS;
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "database upgrade passed");
  }

  // and return from the context
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, "finished database init/upgrade");
}

}  // namespace sdb
