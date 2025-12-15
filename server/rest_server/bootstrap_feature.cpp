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

#include "rest_server/bootstrap_feature.h"

#include <vpack/iterator.h>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "auth/role_utils.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "general_server/state.h"
#include "rest/version.h"
#include "rest_server/serened.h"
#include "rest_server/upgrade_feature.h"

#ifdef SDB_CLUSTER
#include "agency/agency_comm.h"
#include "agency/async_agency_comm.h"
#include "aql/query_registry_feature.h"
#include "cluster/cluster_feature.h"
#include "cluster/cluster_info.h"
#include "cluster/cluster_upgrade_feature.h"
#include "database/methods/upgrade.h"
#endif

namespace sdb {
namespace aql {
class Query;
}
}  // namespace sdb

using namespace sdb;
using namespace sdb::options;

#ifdef SDB_CLUSTER
namespace {

static const std::string kBootstrapKey = "Bootstrap";
static const std::string kHealthKey = "Supervision/Health";

/// Initialize certain agency entries, like Plan, system collections
/// and various similar things. Only runs through on a SINGLE coordinator.
/// must only return if we are bootstrap lead or bootstrap is done.
void RaceForClusterBootstrap(BootstrapFeature& feature) {
  AgencyComm agency(feature.server());
  auto& ci = feature.server().getFeature<ClusterFeature>().clusterInfo();
  while (true) {
    auto result = agency.getValues(::kBootstrapKey);
    if (!result.successful()) {
      // Error in communication, note that value not found is not an error
      SDB_TRACE("xxxxx", Logger::STARTUP,
                "raceForClusterBootstrap: no agency communication");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    vpack::Slice value = result.slice().at(0).get(
      std::vector<std::string>({AgencyCommHelper::path(), ::kBootstrapKey}));
    if (value.isString()) {
      // key was found and is a string
      auto bootstrap_val = value.stringView();
      if (bootstrap_val.find("done") != std::string::npos) {
        // all done, let's get out of here:
        SDB_TRACE("xxxxx", Logger::STARTUP,
                  "raceForClusterBootstrap: bootstrap already done");
        return;
      } else if (bootstrap_val == ServerState::instance()->GetId()) {
        std::ignore = agency.removeValues(::kBootstrapKey, false);
      }
      SDB_DEBUG("xxxxx", Logger::STARTUP,
                "raceForClusterBootstrap: somebody else does the bootstrap");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    // No value set, we try to do the bootstrap ourselves:
    vpack::Builder b;
    b.add(sdb::ServerState::instance()->GetId());
    result = agency.casValue(::kBootstrapKey, b.slice(), false, 300, 15);
    if (!result.successful()) {
      SDB_DEBUG(
        "xxxxx", Logger::STARTUP,
        "raceForClusterBootstrap: lost race, somebody else will bootstrap");
      // Cannot get foot into the door, try again later:
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    // OK, we handle things now
    SDB_DEBUG("xxxxx", Logger::STARTUP,
              "raceForClusterBootstrap: race won, we do the bootstrap");

    // let's see whether a DBserver is there:
    ci.loadCurrentDBServers();

    auto dbservers = ci.getCurrentDBServers();

    if (dbservers.size() == 0) {
      SDB_TRACE("xxxxx", Logger::STARTUP,
                "raceForClusterBootstrap: no DBservers, waiting");
      std::ignore = agency.removeValues(::kBootstrapKey, false);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    auto database = catalog::GetDatabase(id::kSystemDB);
    auto upgrade_res = database
                         ? methods::Upgrade::clusterBootstrap(**database).result
                         : sdb::Result(ERROR_SERVER_DATABASE_NOT_FOUND);

    if (upgrade_res.fail()) {
      SDB_ERROR("xxxxx", Logger::STARTUP,
                "Problems with cluster bootstrap, marking as not successful.");
      std::ignore = agency.removeValues(::kBootstrapKey, false);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    std::ignore = agency.increment("Current/Version");

    SDB_DEBUG("xxxxx", Logger::STARTUP, "Creating the root user");
    if (ServerState::instance()->IsClientNode()) {
      if (auto r = auth::CreateRootRole(true); !r.ok()) {
        SDB_ERROR("xxxxx", Logger::AUTHENTICATION, "unable to create user \"",
                  auth::kRootUserName, "\": ", r.errorMessage());
      }
    }

    SDB_DEBUG("xxxxx", Logger::STARTUP,
              "raceForClusterBootstrap: bootstrap done");

    b.clear();
    b.add(sdb::ServerState::instance()->GetId() + ": done");
    result = agency.setValue(::kBootstrapKey, b.slice(), 0);
    if (result.successful()) {
      // store current version number in agency to avoid unnecessary upgrades
      // to the same version
      if (feature.server().hasFeature<ClusterUpgradeFeature>()) {
        ClusterUpgradeFeature& cluster_upgrade_feature =
          feature.server().getFeature<ClusterUpgradeFeature>();
        cluster_upgrade_feature.setBootstrapVersion();
      }
      return;
    }

    SDB_TRACE("xxxxx", Logger::STARTUP,
              "raceForClusterBootstrap: could not indicate success");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void waitForHealthEntry(SerenedServer& server) {
  SDB_DEBUG("xxxxx", sdb::Logger::CLUSTER,
            "waiting for our health entry to appear in Supervision/Health");
  bool found = false;
  AgencyComm agency(server);
  int tries = 0;
  while (++tries < 30) {
    auto result = agency.getValues(::kHealthKey);
    if (result.successful()) {
      vpack::Slice value = result.slice().at(0).get(std::vector<std::string>(
        {AgencyCommHelper::path(), "Supervision", "Health",
         ServerState::instance()->GetId(), "Status"}));
      if (value.isString() && !value.stringView().empty()) {
        found = true;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  if (found) {
    SDB_DEBUG("xxxxx", sdb::Logger::CLUSTER,
              "found our health entry in Supervision/Health");
  } else {
    SDB_INFO("xxxxx", sdb::Logger::CLUSTER,
             "did not find our health entry after 15 s in Supervision/Health");
  }
}

void waitForDatabases(SerenedServer& server) {
  auto& ci = server.getFeature<ClusterFeature>().clusterInfo();

  uint64_t iterations = 0;
  while (ci.databases().empty() && !server.isStopping()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));

    if (++iterations % 2000 == 0) {
      // log every few seconds that we are waiting here
      SDB_INFO("xxxxx", Logger::CLUSTER, "waiting for databases to appear...");
    }
  }
}

void killRunningQueries() {
  for (auto list : GetQueryLists()) {
    list->kill([](aql::Query&) { return true; }, true);
  }
}

}  // namespace

#endif

BootstrapFeature::BootstrapFeature(Server& server)
  : SerenedFeature{server, name()}, _is_ready(false) {}

bool BootstrapFeature::isReady() const {
  SDB_IF_FAILURE("BootstrapFeature_not_ready") { return false; }
  return _is_ready;
}

void BootstrapFeature::start() {
  auto database = catalog::GetDatabase(id::kSystemDB);
  SDB_ENSURE(database, ERROR_SERVER_DATABASE_NOT_FOUND);

  ServerState::Role role = ServerState::instance()->GetRole();

  if (ServerState::IsClusterNode(role)) {
#ifdef SDB_CLUSTER
    // the coordinators will race to perform the cluster initialization.
    // The coordinatpr who does it will create system collections and
    // the root user
    if (ServerState::IsCoordinator(role)) {
      SDB_DEBUG("xxxxx", Logger::STARTUP, "Racing for cluster bootstrap...");
      // note: this may create the _system database in Plan!
      RaceForClusterBootstrap(*this);

      // wait until at least one database appears. this is an indication that
      // both Plan and Current have been populated successfully
      waitForDatabases(server());
    } else if (ServerState::IsDBServer(role)) {
      // don't wait for databases in Current here, as we are a DB server and may
      // be the one responsible to create it. blocking here is thus no option!

      SDB_DEBUG("xxxxx", Logger::STARTUP, "Running bootstrap");

      auto upgrade_res = methods::Upgrade::clusterBootstrap(**database);

      if (upgrade_res.result.fail()) {
        SDB_ERROR("xxxxx", Logger::STARTUP, "Problem during startup: ",
                  upgrade_res.result.errorMessage());
      }
    } else {
      SDB_ASSERT(false);
    }
#endif
  } else {
    if (ServerState::instance()->IsClientNode()) {
      // only creates root user if it does not exist, will be overwritten on
      // slaves
      if (auto r = auth::CreateRootRole(true); !r.ok()) {
        SDB_ERROR("xxxxx", Logger::AUTHENTICATION, "unable to create user \"",
                  auth::kRootUserName, "\": ", r.errorMessage());
      }
    }
  }

#ifdef SDB_CLUSTER
  if (ServerState::IsClusterNode(role)) {
    waitForHealthEntry(server());
  }
#endif

  // Start service properly:
  ServerState::instance()->SetMode(ServerState::Mode::Default);

  if (!server().getFeature<UpgradeFeature>().upgrading()) {
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "SereneDB (version ",
             SERENEDB_VERSION_FULL, ") is ready for business. Have fun!");
  }

  _is_ready = true;
}

void BootstrapFeature::stop() {
#ifdef SDB_CLUSTER
  // notify all currently running queries about the shutdown
  killRunningQueries();
#endif
}

void BootstrapFeature::unprepare() {
#ifdef SDB_CLUSTER
  // notify all currently running queries about the shutdown
  killRunningQueries();
#endif
}
