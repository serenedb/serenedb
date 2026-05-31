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

#include "rest_server/serened.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <utility>
#include <vector>

#include "duckdb_shell.hpp"
#include "rest_server/serened_includes.h"

namespace {

using namespace sdb;
using namespace sdb::app;

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

int RunServer(int argc, char** argv, GlobalContext& context) {
  try {
    CrashHandler::installCrashHandler();
    std::string name = context.binaryName();
    SdbSetApplicationName(name);
    log::Initialize();

    int ret{EXIT_FAILURE};
    SerenedServer server;
    ServerState state;
    // SereneDB is single-node; the cluster topology that the
    // ArangoDB ServerState modeled never applied.
    state.SetRole(ServerState::Role::Single);

    server.addReporter({[&](SerenedServer::State /*state*/) {
      CrashHandler::SetState(magic_enum::enum_name(server.state()));
    }});

    // 1. Parse CLI before constructing any feature (ctors read flags).
    server.parseOptions(argc, argv);

    // 2. Construct features in dependency order. Each ctor reads its
    //    own flags, runs validation, and sets its static gInstance
    //    pointer; SerenedFeature::instance() works from here on.
    SslServerFeature ssl;
    DatabasePathFeature db_path;
    SchedulerFeature scheduler;
    RocksDBOptionFeature rocksdb_opt;
    EngineFeature engine{server};
    FlushFeature flush;
    RocksDBRecoveryManager recovery;
    catalog::CatalogFeature catalog;
    search::SearchEngine search;
    EndpointFeature endpoint;
    GeneralServerFeature general;
    pg::PostgresFeature pg;

    // ----------------------------------------------------------------
    // Two-method lifecycle: start() does non-IO setup + spin-up of
    // threads / sockets; stop() drains long-running threads then
    // tears down. Construction order doubles as boot order; shutdown
    // is strict LIFO. A `stoppers` stack tracks every feature that
    // successfully started so a throw mid-start unwinds exactly the
    // already-started subset; on the happy path we drain it after
    // wait() in the same reverse order.
    // ----------------------------------------------------------------
    std::vector<std::function<void()>> stoppers;
    stoppers.reserve(12);
    auto start_one = [&](auto& feature, std::function<void()> stop_fn) {
      feature.start();
      stoppers.push_back(std::move(stop_fn));
    };
    auto drain_stoppers = [&]() noexcept {
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

    try {
      // 3. start -- after a successful pass the server is fully
      //    accepting clients.
      server.setState(SerenedServer::State::InStart);
      start_one(ssl, [&] { ssl.stop(); });
      start_one(db_path, [&] { db_path.stop(); });
      start_one(scheduler, [&] { scheduler.stop(); });
      start_one(rocksdb_opt, [&] { rocksdb_opt.stop(); });
      start_one(engine, [&] { engine.stop(); });
      start_one(flush, [&] { flush.stop(); });
      start_one(recovery, [&] { recovery.stop(); });
      start_one(catalog, [&] { catalog.stop(); });
      start_one(search, [&] { search.stop(); });
      start_one(endpoint, [&] { endpoint.stop(); });
      start_one(general, [&] { general.stop(); });
      start_one(pg, [&] { pg.stop(); });

      // 4. wait for SIGTERM/SIGINT.
      server.setState(SerenedServer::State::InWait);
      server.wait();

      // 5. stop -- strict LIFO. Each feature's stop() first nudges any
      //    long-running threads to exit, then blocks on the joins and
      //    finishes its own teardown.
      server.setState(SerenedServer::State::InStop);
      drain_stoppers();
      server.setState(SerenedServer::State::Stopped);

      ret = EXIT_SUCCESS;
    } catch (const std::exception& ex) {
      SDB_ERROR(GENERAL,
                "serened terminated because of an exception: ", ex.what());
      drain_stoppers();
      ret = EXIT_FAILURE;
    } catch (...) {
      SDB_ERROR(GENERAL,
                "serened terminated because of an exception of "
                "unknown type");
      drain_stoppers();
      ret = EXIT_FAILURE;
    }
    log::Shutdown();
    return context.exit(ret);
  } catch (const std::exception& ex) {
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception: ", ex.what());
  } catch (...) {
    SDB_ERROR(GENERAL,
              "serened terminated because of an exception of "
              "unknown type");
  }
  exit(EXIT_FAILURE);
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
    return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
  }
  if (argc >= 2 && std::strcmp(argv[1], "psql") == 0) {
    return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
  }

  GlobalContext context(argc, argv, BIN_DIRECTORY);
  return RunServer(argc, argv, context);
}
