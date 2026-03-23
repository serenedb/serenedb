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

#include "app/app_server.h"
#include "auth/role_utils.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "general_server/state.h"
#include "rest/version.h"
#include "rest_server/serened.h"
#include "rest_server/upgrade_feature.h"

using namespace sdb;
using namespace sdb::options;

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

  if (!ServerState::IsClusterNode(role)) {
    if (ServerState::instance()->IsClientNode()) {
      // only creates root user if it does not exist, will be overwritten on
      // slaves
      if (auto r = auth::CreateRootRole(true); !r.ok()) {
        SDB_ERROR("xxxxx", Logger::AUTHENTICATION, "unable to create user \"",
                  auth::kRootUserName, "\": ", r.errorMessage());
      }
    }
  }

  // Start service properly:
  ServerState::instance()->SetMode(ServerState::Mode::Default);

  if (!server().getFeature<UpgradeFeature>().upgrading()) {
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "SereneDB (version ",
             SERENEDB_VERSION_FULL, ") is ready for business. Have fun!");
  }

  _is_ready = true;
}

void BootstrapFeature::stop() {}

void BootstrapFeature::unprepare() {}
