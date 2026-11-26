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

#include "app/server.h"

#include <absl/flags/flag.h>

#include <cstddef>
#include <cstdlib>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <exception>
#include <iresearch/utils/crash_handler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/remap_executable.hpp>
#include <utility>

#include "app/command.h"
#include "app/process.h"
#include "connector/duckdb_copy_filesystem.h"
#include "jobs/scheduler.h"
#include "network/server.h"
#include "query/server_engine.h"
#include "replication/subscription_engine.h"
#include "runtime/runtime.h"
#include "server/utils/build.h"
#include "server/utils/init.h"
#include "server/utils/number_of_cores.h"
#include "server/utils/signals.h"
#include "storage/storage.h"

ABSL_FLAG(bool, remap_executable, SDB_REMAP_EXECUTABLE,
          "Copy the server's machine code into anonymous huge-page memory at "
          "startup, so the first queries after a cold start pay no code page "
          "faults and the code runs from huge pages. Off, the machine code is "
          "locked in memory instead, or pre-read where locking is not "
          "permitted, and perf can symbolize it, which it cannot for "
          "anonymous code. Read-only data is pre-read either way. On by "
          "default in Release builds, and off in the others.");

namespace sdb::app {
namespace {

int RunServer(int argc, char* argv[]) {
  return RunWithEngine(argc, argv, Serve);
}

SDB_COMMAND kServer{"server", &RunServer};

}  // namespace

int Serve() {
  auto server_config = ReadServerConfig();
  Runtime runtime{ReadIoThreads()};
  Storage storage{runtime, ReadStorageConfig()};
  replication::SubscriptionEngine subscriptions{runtime.IoPool()};
  JobScheduler jobs{runtime};
  Server network{runtime.IoPool(), std::move(server_config)};
  SDB_INFO(STARTUP, "SereneDB is ready for business. Have fun!");
  irs::CrashHandler::SetState("running");
  WaitForShutdown();
  irs::CrashHandler::SetState("stopping");
  return EXIT_SUCCESS;
}

int RunWithEngine(int argc, char* argv[], absl::FunctionRef<int()> body) {
  ParseOptions(argc, argv);
  const auto remap =
    irs::RemapExecutable(absl::GetFlag(FLAGS_remap_executable),
                         static_cast<size_t>(CountLogicalCores()));
  auto& engine = irs::DuckDBEngine::Instance();
  engine.Initialize(&server::query::ConfigureServerDBConfig);
  SDB_INFO(STARTUP, "starting SereneDB ", SERENEDB_VERSION);
  server::query::RegisterServerExtensions(engine.instance());
  duckdb::FileSystem::GetFileSystem(engine.instance())
    .RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  InitProcess(argv[0]);
  SDB_DEBUG(STARTUP, "executable memory: ", remap.remapped >> 20,
            " MB of machine code remapped into anonymous huge-page memory, ",
            remap.locked >> 20, " MB locked, ", remap.populated >> 20,
            " MB pre-read",
            remap.skipped.empty() ? "" : "; remap skipped: ", remap.skipped);
  auto rc = EXIT_FAILURE;
  try {
    irs::CrashHandler::installCrashHandler();
    signals::InstallShutdownHandlers();
    irs::CrashHandler::SetState("starting");
    rc = body();
  } catch (const std::exception& ex) {
    SDB_ERROR(GENERAL, "serened terminated because of an exception: ",
              duckdb::ErrorData{ex}.RawMessage());
  } catch (...) {
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception of unknown type");
  }
  SDB_INFO(GENERAL, "SereneDB is shut down");
  engine.Shutdown();
  return rc;
}

}  // namespace sdb::app
