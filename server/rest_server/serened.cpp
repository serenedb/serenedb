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
#include "network/server.h"
#include "query/server_engine.h"
#include "rest_server/database_path_feature.h"
#include "scheduler/background_scheduler.h"
#include "storage_engine/search_engine.h"

namespace {

using namespace sdb;
using namespace sdb::app;

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

int RunServer(int argc, char** argv) {
  try {
    CrashHandler::installCrashHandler();

    AppServer server;

    // 1. CLI is already parsed (in main(), before engine.Initialize, so the
    //    pool-sizing flags like --cpu_threads are visible while DuckDB is
    //    built). Feature ctors below read their flags from the parsed state.

    // 2. Construct features in dependency order. Each ctor reads its
    //    own flags, runs validation, and sets its static gInstance
    //    pointer; Feature::instance() works from here on.
    DatabasePathFeature db_path;
    catalog::CatalogStore store;
    BackgroundScheduler background;
    search::SearchEngine search;
    Server network;

    // Lifecycle is two explicit, flat lists: bring features UP in dependency
    // order, then take them DOWN in a dependency order that is deliberately
    // NOT the reverse of startup. The non-obvious edge: the search loops run ON
    // the background pool, so SearchEngine::stop() must join them (it sets
    // _stopping and Waits the loop Futures, which resume on the still-live
    // pool) BEFORE BackgroundScheduler::stop() tears that pool down -- else a
    // loop is stranded mid-resume. search.stop() does not destroy the engine
    // (its dtor runs at end of scope), so drop tasks draining on the pool
    // afterwards still see a live SearchEngine. DuckDBEngine brackets all of
    // this from main(). The up_* flags let DOWN skip whatever never came UP
    // (start() threw).
    bool up_store = false, up_background = false, up_catalog = false,
         up_search = false, up_network = false;

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
      if (up_search) {
        // Signal the search loops to stop BEFORE the IoPool is torn down, so
        // their Delay()s (which complete instantly once the pool is gone) break
        // immediately instead of spinning until search.stop() below.
        search.RequestStop();
      }
      if (up_network) {
        stop("network", [&] { network.stop(); });
      }
      if (up_search) {
        stop("search", [&] { search.stop(); });
      }
      if (up_background) {
        stop("background", [&] { background.stop(); });
      }
      if (up_catalog) {
        stop("catalog", [&] { catalog::ShutdownCatalog(); });
      }
      if (up_store) {
        stop("store", [&] { store.Shutdown(); });  // detach + checkpoint
      }
    };

    CrashHandler::SetState("starting");
    store.Initialize(db_path.directory());
    up_store = true;
    background.start();
    up_background = true;
    catalog::InitCatalog();
    up_catalog = true;
    // The io pool must be up before search.start(): the per-index refresh /
    // compaction loops co_await BackgroundScheduler::Delay(), which hosts its
    // timers on the io pool. Without it Delay() returns instantly and the loops
    // busy-spin every core, starving startup so no listener ever binds.
    network.StartIoPool();
    up_network = true;
    search.start();
    up_search = true;
    // Accept connections only once the indexes are loaded and loops are
    // running.
    network.StartListeners();

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

extern "C" void json_object_seed(size_t seed);

int main(int argc, char* argv[]) {
  json_object_seed(0);
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
  //
  // CLI flags are parsed FIRST: ConfigureServerDBConfig reads --cpu_threads to
  // size the DuckDB pool at construction, so the flags must be live before
  // Initialize (parseOptions is SDB_*-free precisely so it can run this early).
  sdb::app::AppServer::parseOptions(argc, argv);
  auto& engine = sdb::DuckDBEngine::Instance();
  engine.Initialize(&server::query::ConfigureServerDBConfig);
  server::query::RegisterServerExtensions(engine.instance());

  sdb::app::InitProcess(argv[0]);
  int rc = RunServer(argc, argv);
  sdb::app::ShutdownGlobals();

  engine.Shutdown();
  return rc;
}
