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

#include "server_feature.h"

#include <chrono>
#include <thread>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "app/shutdown.h"
#include "basics/application-exit.h"
#include "basics/logger/logger.h"
#include "basics/process-utils.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "rest_server/upgrade_feature.h"
#include "statistics/statistics_feature.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "cluster/cluster_feature.h"
#include "cluster/heartbeat_thread.h"
#include "replication/replication_feature.h"
#endif

using namespace sdb::app;
using namespace sdb::options;
using namespace sdb::rest;

namespace sdb {

ServerFeature::ServerFeature(Server& server, int* res)
  : SerenedFeature{server, name()}, _result{res} {
  setOptional(true);
}

void ServerFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options->addSection("server", "server features");

  options->addOption(
    "--server.rest-server", "Start a REST server.",
    new BooleanParameter(&_rest_server),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

  options->addOption(
    "--server.validate-utf8-strings",
    "Perform UTF-8 string validation for incoming JSON and VPack "
    "data.",
    new BooleanParameter(&_validate_utf8_strings),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));
}

void ServerFeature::validateOptions(std::shared_ptr<ProgramOptions> options) {
  if (!_rest_server && !server().getFeature<UpgradeFeature>().upgrading() &&
      !options->processingResult().touched("rocksdb.verify-sst")) {
    SDB_FATAL("xxxxx", sdb::Logger::FIXME,
              "restServer disabled only for upgrade");
  }

  auto disable_deamon_and_supervisor = [&]() {
    if constexpr (Server::contains<DaemonFeature>()) {
      server().disableFeatures({Server::id<DaemonFeature>()});
    }
    if constexpr (Server::contains<SupervisorFeature>()) {
      server().disableFeatures({Server::id<SupervisorFeature>()});
    }
  };

  if (!_rest_server) {
    server().disableFeatures({
      Server::id<HttpEndpointProvider>(),
      Server::id<GeneralServerFeature>(),
      Server::id<SslServerFeature>(),
      Server::id<StatisticsFeature>(),
    });
    disable_deamon_and_supervisor();

#ifdef SDB_CLUSTER
    if (!options->processingResult().touched("replication.auto-start")) {
      // turn off replication applier when we do not have a rest server
      // but only if the config option is not explicitly set (the recovery
      // test want the applier to be enabled for testing it)
      ReplicationFeature& replication_feature =
        server().getFeature<ReplicationFeature>();
      replication_feature.disableReplicationApplier();
    }
#endif
  }
  server().getFeature<ShutdownFeature>().disable();
}

void ServerFeature::prepare() {
  // adjust global settings for UTF-8 string validation
  basics::VPackHelper::gStrictRequestValidationOptions.validate_utf8_strings =
    _validate_utf8_strings;
}

void ServerFeature::start() {
  waitForHeartbeat();

  *_result = EXIT_SUCCESS;

  // flush all log output before we go on... this is sensible because any
  // of the following options may print or prompt, and pending log entries
  // might overwrite that
  log::Flush();

  // install CTRL-C handlers
  server().registerStartupCallback([this]() {
    server().getFeature<SchedulerFeature>().buildControlCHandler();
  });
}

void ServerFeature::beginShutdown() { _is_stopping = true; }

void ServerFeature::waitForHeartbeat() {
  if (!ServerState::instance()->IsCoordinator()) {
    // waiting for the heartbeart thread is necessary on coordinator only
    return;
  }

#ifdef SDB_CLUSTER
  if (!server().hasFeature<ClusterFeature>()) {
    return;
  }

  auto& cf = server().getFeature<ClusterFeature>();
  while (true) {
    auto heartbeat_thread = cf.heartbeatThread();
    SDB_ASSERT(heartbeat_thread != nullptr);
    if (heartbeat_thread == nullptr || heartbeat_thread->hasRunOnce()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
#endif
}

}  // namespace sdb
