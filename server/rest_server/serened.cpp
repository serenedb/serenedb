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

#include <absl/functional/overload.h>

#include <cstdlib>
#include <cstring>

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
    SerenedServer server{BIN_DIRECTORY};
    ServerState state;
    // SereneDB is single-node; the cluster topology that the
    // ArangoDB ServerState modeled never applied.
    state.SetRole(ServerState::Role::Single);

    server.addReporter(
      {[&](SerenedServer::State /*state*/) {
         CrashHandler::SetState(
           magic_enum::enum_name(server.state()));
       },
       {}});

    // 1. Parse CLI before constructing any feature (some ctors read flags).
    server.parseOptions(argc, argv);

    // 2. Construct features in dependency order. Each ctor sets its
    //    static gInstance pointer; SerenedFeature::instance() works from
    //    here on.
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

    try {
      // ----------------------------------------------------------------
      // Explicit boot / shutdown ordering. Each feature's lifecycle
      // method is invoked here directly; AppServer no longer dispatches.
      // Construction order above doubles as boot order; shutdown is
      // strict LIFO.
      // ----------------------------------------------------------------

      // 3. validateOptions -- pull absl::GetFlag values into fields,
      //    fail-fast on bad combinations.
      server.setState(SerenedServer::State::InValidateOptions);
      ssl.validateOptions();
      db_path.validateOptions();
      scheduler.validateOptions();
      rocksdb_opt.validateOptions();
      engine.validateOptions();
      flush.validateOptions();
      recovery.validateOptions();
      catalog.validateOptions();
      search.validateOptions();
      endpoint.validateOptions();
      general.validateOptions();
      pg.validateOptions();

      // 4. prepare -- non-thread-creating init.
      server.setState(SerenedServer::State::InPrepare);
      ssl.prepare();
      db_path.prepare();
      scheduler.prepare();
      rocksdb_opt.prepare();
      engine.prepare();
      flush.prepare();
      recovery.prepare();
      catalog.prepare();
      search.prepare();
      endpoint.prepare();
      general.prepare();
      pg.prepare();

      // 5. start -- spin up threads, open sockets. AFTER this returns
      //    successfully, the server is fully accepting clients.
      server.setState(SerenedServer::State::InStart);
      ssl.start();
      db_path.start();
      scheduler.start();
      rocksdb_opt.start();
      engine.start();
      flush.start();
      recovery.start();
      catalog.start();
      search.start();
      endpoint.start();
      general.start();
      pg.start();

      // 6. wait for SIGTERM/SIGINT.
      server.setState(SerenedServer::State::InWait);
      server.wait();

      // 7. beginShutdown -- LIFO; lets long-running threads (general
      //    server listeners, engine background, search thread pools)
      //    notice they should exit before stop() blocks on them.
      general.beginShutdown();
      search.beginShutdown();
      engine.beginShutdown();

      // 8. stop -- strict LIFO of step 5. Threads must exit before
      //    the call returns.
      server.setState(SerenedServer::State::InStop);
      pg.stop();
      general.stop();
      endpoint.stop();
      search.stop();
      catalog.stop();
      recovery.stop();
      flush.stop();
      engine.stop();
      rocksdb_opt.stop();
      scheduler.stop();
      db_path.stop();
      ssl.stop();

      // 8. unprepare -- final teardown, also LIFO.
      server.setState(SerenedServer::State::InUnprepare);
      pg.unprepare();
      general.unprepare();
      endpoint.unprepare();
      search.unprepare();
      catalog.unprepare();
      recovery.unprepare();
      flush.unprepare();
      engine.unprepare();
      rocksdb_opt.unprepare();
      scheduler.unprepare();
      db_path.unprepare();
      ssl.unprepare();
      server.setState(SerenedServer::State::Stopped);

      ret = EXIT_SUCCESS;
    } catch (const std::exception& ex) {
      SDB_ERROR(GENERAL,
                "serened terminated because of an exception: ", ex.what());
      ret = EXIT_FAILURE;
    } catch (...) {
      SDB_ERROR(GENERAL,
                "serened terminated because of an exception of "
                "unknown type");
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
