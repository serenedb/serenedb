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
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <utility>

#include "app/app_server.h"
#include "app/init.h"
#include "basics/crash_handler.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "duckdb_shell.hpp"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler_feature.h"
#include "general_server/ssl_server_feature.h"
#include "pg/pg_feature.h"
#include "query/server_engine.h"
#include "rest_server/database_path_feature.h"
#include "storage_engine/search_engine.h"

namespace {

using namespace sdb;
using namespace sdb::app;

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

int RunServer(int argc, char** argv) {
  try {
    CrashHandler::installCrashHandler();

    AppServer server;

    // 1. Parse CLI before constructing any feature (ctors read flags).
    //    parseOptions can SDB_FATAL on bad CLI -- DuckDB is already up
    //    by the time we get here (Initialize ran in main(), see below).
    server.parseOptions(argc, argv);

    // 2. Construct features in dependency order. Each ctor reads its
    //    own flags, runs validation, and sets its static gInstance
    //    pointer; Feature::instance() works from here on.
    SslServerFeature ssl;
    DatabasePathFeature db_path;
    catalog::CatalogStore store;
    SchedulerFeature scheduler;
    search::SearchEngine search;
    GeneralServerFeature general;
    pg::PostgresFeature pg;

    // Lifecycle is two explicit, flat lists: bring features UP in dependency
    // order, then take them DOWN in a dependency order that is deliberately
    // NOT the reverse of startup. The non-obvious edge: the scheduler must
    // drain (it runs drop tasks that touch search) before search goes down.
    // DuckDBEngine brackets all of this from main(). The up_* flags let DOWN
    // skip whatever never came UP (start() threw).
    bool up_ssl = false, up_store = false, up_scheduler = false,
         up_catalog = false, up_search = false, up_general = false;

    absl::Cleanup down = [&]() noexcept {
      CrashHandler::SetState("stopping");
      auto stop = [](const char* what, auto&& fn) noexcept {
        try {
          fn();
        } catch (const std::exception& ex) {
          SDB_ERROR(GENERAL, "exception stopping ", what, ": ", ex.what());
        } catch (...) {
          SDB_ERROR(GENERAL, "unknown exception stopping ", what);
        }
      };
      if (up_general) {
        stop("general", [&] { general.stop(); });
      }
      if (up_scheduler) {
        stop("scheduler", [&] { scheduler.stop(); });
      }
      if (up_search) {
        stop("search", [&] { search.stop(); });
      }
      if (up_catalog) {
        stop("catalog", [&] { catalog::ShutdownCatalog(); });
      }
      if (up_store) {
        stop("store", [&] { store.Shutdown(); });  // detach + checkpoint
      }
      if (up_ssl) {
        stop("ssl", [&] { ssl.stop(); });
      }
    };

    CrashHandler::SetState("starting");
    ssl.start();
    up_ssl = true;
    store.Initialize(db_path.directory());
    up_store = true;
    scheduler.start();
    up_scheduler = true;
    catalog::InitCatalog();
    up_catalog = true;
    search.start();
    up_search = true;
    general.start();
    up_general = true;
    pg.start();

    SDB_INFO(GENERAL, "SereneDB is ready for business. Have fun!");

    CrashHandler::SetState("running");
    server.wait();
    return EXIT_SUCCESS;
  } catch (const std::exception& ex) {
    // gLogger is still up (Shutdown runs in main() after RunServer
    // returns), but go through SDB_ERROR so the line lands in the same
    // duckdb_logs() store as everything else.
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception: ", ex.what());
  } catch (...) {
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception of unknown type");
  }
  // Return non-zero -- main() will run DuckDBEngine::Shutdown() before exit.
  return EXIT_FAILURE;
}

// Trim argv in place: overwrite argv[1] with argv[0] and hand the shell
// `argv + 1`. The shell sees {<binary>, <user args>...} with no copy.
int RunSubcommand(int argc, char* argv[],
                  duckdb_shell::ShellSubcommand subcommand) {
  argv[1] = argv[0];
  return duckdb_shell::Run(argc - 1, argv + 1, subcommand);
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc >= 2 && std::strcmp(argv[1], "shell") == 0) {
    // Pure duckdb shell -- manages its own DuckDB instance, no SDB_*.
    return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
  }
  if (argc >= 2 && std::strcmp(argv[1], "psql") == 0) {
    return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
  }

  // DuckDBEngine must be up BEFORE the first SDB_* fires. VPackHelper::
  // initialize() below does SDB_TRACE, so it has to run after the engine
  // is initialized. Bracket the whole RunServer lifetime so the contract
  // holds.
  //
  // Two-step boot: the lite Initialize (serene_base) installs the storage
  // extension via the ConfigureServerDBConfig mutator BEFORE the duckdb
  // ctor runs (storage extensions must be registered pre-construct), then
  // RegisterServerExtensions fills connector/pg/index types on the live
  // DatabaseInstance.
  auto& engine = sdb::DuckDBEngine::Instance();
  engine.Initialize(&server::query::ConfigureServerDBConfig);
  server::query::RegisterServerExtensions(engine.instance());

  sdb::app::InitProcess(argv[0]);
  int rc = RunServer(argc, argv);
  sdb::app::ShutdownGlobals();

  engine.Shutdown();
  return rc;
}
