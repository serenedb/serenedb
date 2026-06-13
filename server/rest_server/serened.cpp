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
#include "duckdb_shell.hpp"
#include "network/network_server_feature.h"
#include "query/server_engine.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/flush_feature.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "scheduler/background_scheduler.h"
#include "storage_engine/engine_feature.h"
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
    DatabasePathFeature db_path;
    BackgroundScheduler background;
    RocksDBOptionFeature rocksdb_opt;
    EngineFeature engine;
    FlushFeature flush;
    RocksDBRecoveryManager recovery;
    catalog::CatalogFeature catalog;
    search::SearchEngine search;
    NetworkServerFeature network;

    // --------------------------------------------------------------
    // Two-method lifecycle: start() does non-IO setup + spin-up of
    // threads / sockets; stop() drains long-running threads then
    // tears down. Construction order doubles as boot order; shutdown
    // is strict LIFO. `stoppers` holds the stop closures of every
    // feature that successfully started; the absl::Cleanup below
    // drains them on scope exit -- whether that's a normal return
    // after wait() OR an exception thrown mid-start. Either path
    // unwinds exactly the already-started subset in reverse order.
    // --------------------------------------------------------------
    std::deque<std::function<void()>> stoppers;
    absl::Cleanup drain = [&]() noexcept {
      while (!stoppers.empty()) {
        try {
          stoppers.back()();
        } catch (const std::exception& ex) {
          SDB_ERROR(GENERAL, "exception during stop: ", ex.what());
        } catch (...) {
          SDB_ERROR(GENERAL, "exception of unknown type during stop");
        }
        stoppers.pop_back();
      }
    };
    auto start_one = [&](auto& feature, std::function<void()> stop_fn) {
      feature.start();
      stoppers.push_back(std::move(stop_fn));
    };

    // 3. start -- after a successful pass the server is fully
    //    accepting clients. The CrashHandler state strings get
    //    stamped into /tmp/crash bundles so a fatal signal during
    //    boot lands with a phase tag.
    CrashHandler::SetState("starting");
    start_one(background, [&] { background.stop(); });
    start_one(engine, [&] { engine.stop(); });
    // flush has no start() work; ctor already registered metrics + gInstance,
    // but stop() clears subscriptions so it needs a LIFO slot here.
    stoppers.push_back([&] { flush.stop(); });
    // recovery only has start() work (no stop counterpart): WAL replay. No
    // teardown needed.
    recovery.start();
    start_one(catalog, [&] { catalog.stop(); });
    start_one(search, [&] { search.stop(); });
    start_one(network, [&] { network.stop(); });

    // Boot completed; emit a recognisable banner so operators tailing
    // logs (and the sdb_log smoke test) can confirm the server is up.
    SDB_INFO(GENERAL, "SereneDB is ready for business. Have fun!");

    // 4. wait for SIGTERM/SIGINT.
    CrashHandler::SetState("running");
    server.wait();

    // 5. stop -- the absl::Cleanup runs stoppers in strict LIFO on
    //    scope exit; the SetState below paints the bucket so a fatal
    //    signal during shutdown tags the right phase. DuckDBEngine
    //    Initialize/Shutdown brackets this whole function from main().
    CrashHandler::SetState("stopping");
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
