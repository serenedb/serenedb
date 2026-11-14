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

#include <absl/cleanup/cleanup.h>

#include <cstdlib>
#include <duckdb/common/error_data.hpp>
#include <exception>
#include <iresearch/utils/crash_handler.hpp>
#include <iresearch/utils/log.hpp>

#include "app/process.h"
#include "docs/docs_bootstrap.h"
#include "jobs/scheduler.h"
#include "network/server.h"
#include "replication/subscription_engine.h"
#include "runtime/runtime.h"
#include "search/engine.h"
#include "server/utils/signals.h"
#include "storage/storage.h"

namespace {

using namespace sdb;

int RunServer() {
  try {
    irs::CrashHandler::installCrashHandler();
    signals::InstallShutdownHandlers();

    irs::CrashHandler::SetState("starting");
    absl::Cleanup shut_down = [] {
      SDB_INFO(GENERAL, "SereneDB is shut down");
    };
    Runtime runtime{ReadRuntimeConfig()};
    search::SearchEngine search;
    Storage storage{runtime, ReadStorageConfig()};
    search::SearchTasks search_tasks{runtime, search};
    JobScheduler jobs{runtime};
    if (const auto bootstrapped = docs::RunDocsBootstrap()) {
      return *bootstrapped;
    }
    replication::SubscriptionEngine subscriptions{runtime};
    Server network{runtime};

    SDB_INFO(STARTUP, "SereneDB is ready for business. Have fun!");

    irs::CrashHandler::SetState("running");
    app::WaitForShutdown();
    irs::CrashHandler::SetState("stopping");
    runtime.RequestStop();
    return EXIT_SUCCESS;
  } catch (const std::exception& ex) {
    // gLogger is still up (the DuckDB engine shuts down after RunServer
    // returns), but go through SDB_ERROR so the line lands in the same
    // duckdb_logs() store as everything else.
    SDB_ERROR(GENERAL, "serened terminated because of an exception: ",
              duckdb::ErrorData{ex}.RawMessage());
  } catch (...) {
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception of unknown type");
  }
  return EXIT_FAILURE;
}

}  // namespace

int main(int argc, char* argv[]) {
  sdb::app::PrepareProcess(argv[0]);
  if (const auto rc = sdb::app::RunShellCommand(argc, argv)) {
    return *rc;
  }
  const sdb::app::ServerProcess process{argc, argv};
  return RunServer();
}
