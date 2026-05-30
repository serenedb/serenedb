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

#include "upgrade.h"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/slice.h>

#include "app/app_server.h"
#include "basics/logger/logger.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "database/methods/version.h"
#include "general_server/state.h"
#include "rest_server/upgrade_feature.h"
#include "utils/exec_context.h"

namespace sdb {

using namespace sdb::methods;

UpgradeResult Upgrade::clusterBootstrap(catalog::Database& system) {
  uint64_t cc = Version::current();  // not actually used here
  VersionResult vinfo = {VersionResult::kVersionMatch, cc, cc, {}};
  uint32_t cluster_flag = Flags::kClusterCoordinatorGlobal;
  if (ServerState::instance()->IsDBServer()) {
    cluster_flag = Flags::kClusterDbServerLocal;
  }
  SDB_ASSERT(ServerState::instance()->IsClusterNode());

  vpack::Slice params = vpack::Slice::emptyObjectSlice();
  return runTasks(system, vinfo, params, cluster_flag,
                  Upgrade::Flags::kDatabaseInit);
}

UpgradeResult Upgrade::startup(catalog::Database& database, bool is_upgrade,
                               bool ignore_file_errors) {
  if (ServerState::instance()->IsCoordinator()) {
    // coordinators do not have any persistent data, so there is no VERSION file
    // available. We don't know the previous version we are upgrading from, so
    // we need to pretend no upgrade is necessary
    return UpgradeResult(ERROR_OK, methods::VersionResult::kVersionMatch);
  }

  uint32_t cluster_flag = 0;

  if (ServerState::instance()->IsSingle()) {
    cluster_flag = Flags::kClusterNone;
  } else {
    cluster_flag = Flags::kClusterLocal;
  }

  uint32_t dbflag = Flags::kDatabaseExisting;
  ObjectId database_id = database.GetId();
  VersionResult vinfo = Version::check(database_id);

  if (vinfo.status == methods::VersionResult::kCannotParseVersionFile ||
      vinfo.status == methods::VersionResult::kCannotReadVersionFile) {
    if (ignore_file_errors) {
      // try to install a fresh new, empty VERSION file instead
      if (methods::Version::write(database_id, std::map<std::string, bool>(),
                                  true)
            .ok()) {
        // give it another try
        SDB_WARN(STARTUP,
                 "overwriting unparsable VERSION file with default value ",
                 "because option `--database.ignore-datafile-errors` is set");
        vinfo = methods::Version::check(database_id);
      }
    } else {
      SDB_WARN(STARTUP,
               "in order to automatically fix the VERSION file on startup, ",
               "please start the server with option "
               "`--database.ignore-datafile-errors true`");
    }
  }

  switch (vinfo.status) {
    case VersionResult::kInvalid:
      SDB_ASSERT(false);  // never returned by Version::check
      break;
    case VersionResult::kVersionMatch:
      if (is_upgrade) {
        dbflag = Flags::kDatabaseUpgrade;  // forcing the upgrade as server is
                                           // in upgrade state with some
                                           // features disabled
      }
      break;  // just run tasks that weren't run yet
    case VersionResult::kUpgradeNeeded: {
      if (!is_upgrade) {
        // we do not perform upgrades without being told so during startup
        SDB_ERROR(STARTUP, "Database directory version (",
                  vinfo.database_version,
                  ") is lower than current executable version (",
                  vinfo.server_version, ").");

        SDB_ERROR(STARTUP,
          "---------------------------------------------------------------"
          "-------");
        SDB_ERROR(STARTUP,
                  "It seems like you have upgraded the SereneDB executable.");
        SDB_ERROR(STARTUP,
                  "If this is what you wanted to do, please restart with the");
        SDB_ERROR(STARTUP, "  --database.auto-upgrade true");
        SDB_ERROR(STARTUP,
                  "option to upgrade the data in the database directory.");
        SDB_ERROR(STARTUP,
          "---------------------------------------------------------------"
          "-------");
        return UpgradeResult(ERROR_BAD_PARAMETER, vinfo.status);
      }
      // do perform the upgrade
      dbflag = Flags::kDatabaseUpgrade;
      break;
    }
    case VersionResult::kDowngradeNeeded: {
      // we do not support downgrades, just error out
      SDB_ERROR(STARTUP, "Database directory version (",
                vinfo.database_version,
                ") is higher than current executable version (",
                vinfo.server_version, ").");
      SDB_ERROR(STARTUP,
        "It seems like you are running SereneDB on a database directory",
        " that was created with a newer version of SereneDB. Maybe this",
        " is what you wanted but it is not supported by SereneDB.");
      return {.type = vinfo.status};
    }
    case VersionResult::kCannotParseVersionFile:
    case VersionResult::kCannotReadVersionFile:
    case VersionResult::kNoServerVersion: {
      SDB_DEBUG(STARTUP, "Error reading version file");
      return UpgradeResult{
        .result = {ERROR_INTERNAL, "error during ",
                   (is_upgrade ? "upgrade" : "startup")},
        .type = vinfo.status,
      };
    }
    case VersionResult::kNoVersionFile:
      SDB_DEBUG(STARTUP, "No VERSION file found");
      // VERSION file does not exist, we are running on a new database
      dbflag = kDatabaseInit;
      break;
  }

  // should not do anything on VERSION_MATCH, and init the database
  // with all tasks if they were not executed yet. Tasks not listed
  // in the "tasks" attribute will be executed automatically
  const vpack::Slice params = vpack::Slice::emptyObjectSlice();
  return runTasks(database, vinfo, params, cluster_flag, dbflag);
}

UpgradeResult methods::Upgrade::startupCoordinator(
  catalog::Database& database) {
  SDB_ASSERT(ServerState::instance()->IsCoordinator());

  // this will return a hard-coded version result
  VersionResult vinfo = Version::check(database.GetId());

  const vpack::Slice params = vpack::Slice::emptyObjectSlice();
  return runTasks(database, vinfo, params, Flags::kClusterCoordinatorGlobal,
                  Flags::kDatabaseUpgrade);
}

void methods::Upgrade::registerTasks(sdb::UpgradeFeature& upgrade_feature) {}

UpgradeResult methods::Upgrade::runTasks(catalog::Database& database,
                                         VersionResult& vinfo,
                                         vpack::Slice params,
                                         uint32_t cluster_flag,
                                         uint32_t db_flag) {
  auto& upgrade_feature =
    SerenedServer::Instance().getFeature<sdb::UpgradeFeature>();
  auto& tasks = upgrade_feature._tasks;

  SDB_ASSERT(cluster_flag != 0 && db_flag != 0);
  // only local should actually write a VERSION file
  bool is_local = cluster_flag == kClusterNone ||
                  cluster_flag == kClusterLocal ||
                  cluster_flag == kClusterDbServerLocal;

  bool ran_once = false;
  // execute all tasks
  for (const Task& t : tasks) {
    // check for system database
    if (t.system_flag == kDatabaseSystem && database.GetId() != id::kSystemDB) {
      SDB_DEBUG(STARTUP, "Upgrade: DB not system, skipping ",
                t.name);
      continue;
    }
    if (t.system_flag == kDatabaseExceptSystem &&
        database.GetId() == id::kSystemDB) {
      SDB_DEBUG(STARTUP, "Upgrade: DB system, skipping ",
                t.name);
      continue;
    }

    // check that the cluster occurs in the cluster list
    if (!(t.cluster_flags & cluster_flag)) {
      SDB_DEBUG(STARTUP,
                "Upgrade: cluster mismatch, skipping ", t.name);
      continue;
    }

    const auto& it = vinfo.tasks.find(t.name);
    if (it != vinfo.tasks.end()) {
      if (it->second) {
        SDB_DEBUG(STARTUP,
                  "Upgrade: already executed, skipping ", t.name);
        continue;
      }
      vinfo.tasks.erase(it);  // in case we encounter false
    }

    // check that the database occurs in the database list
    if (!(t.database_flags & db_flag)) {
      // special optimization: for local server and new database,
      // a one-shot task can be viewed as executed.
      if (is_local && db_flag == kDatabaseInit &&
          (t.database_flags & kDatabaseOnlyOnce)) {
        vinfo.tasks.try_emplace(t.name, true);
      }
      SDB_DEBUG(STARTUP,
                "Upgrade: db flag mismatch, skipping ", t.name);
      continue;
    }

    SDB_DEBUG(STARTUP, "Upgrade: executing ", t.name);
    try {
      Result res = t.action(database, params);
      if (res.fail()) {
        std::string msg =
          absl::StrCat("executing ", t.name, " (", t.description,
                       ") failed: ", res.errorMessage());
        SDB_ERROR(STARTUP, msg,
                  " aborting upgrade procedure.");
        return {
          .result = {ERROR_INTERNAL, std::move(msg)},
          .type = vinfo.status,
        };
      }
    } catch (const std::exception& e) {
      SDB_ERROR(STARTUP, "executing ", t.name, " (",
                t.description, ") failed with error: ", e.what(),
                ". aborting upgrade procedure.");
      return {
        .result = {ERROR_FAILED, e.what()},
        .type = vinfo.status,
      };
    }

    // remember we already executed this one
    vinfo.tasks.try_emplace(t.name, true);

    if (is_local) {  // save after every task for resilience
      auto res = methods::Version::write(database.GetId(), vinfo.tasks,
                                         /*sync*/ false);

      if (res.fail()) {
        return {
          .result = std::move(res),
          .type = vinfo.status,
        };
      }

      ran_once = true;
    }
  }

  if (is_local) {  // no need to write this for cluster bootstrap
    // save even if no tasks were executed
    SDB_DEBUG(STARTUP, "Upgrade: writing VERSION file");
    auto res = methods::Version::write(database.GetId(), vinfo.tasks,
                                       /*sync*/ ran_once);

    if (res.fail()) {
      return {
        .result = std::move(res),
        .type = vinfo.status,
      };
    }
  }

  return {.type = vinfo.status};
}

}  // namespace sdb
