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

    server.addFeatures(absl::Overload{
      []<typename T>(auto& server, type::Tag<T>) {
        return std::make_unique<T>(server);
      },
      [](auto& server, type::Tag<SslServerFeature>) {
        return std::make_unique<SslServerFeature>(server);
      },
    });

    try {
      // ----------------------------------------------------------------
      // Explicit boot / shutdown ordering. Each feature's lifecycle
      // method is invoked here directly; AppServer no longer dispatches.
      // Boot order matches kSerenedFeatures; shutdown is strict LIFO.
      // ----------------------------------------------------------------

      // 1. Parse CLI (absl::ParseCommandLine + positional argv stash).
      server.parseOptions(argc, argv);

      // 2. validateOptions -- pull absl::GetFlag values into fields,
      //    fail-fast on bad combinations. Order does not matter; we use
      //    the kSerenedFeatures declaration order.
      server.setState(SerenedServer::State::InValidateOptions);
      server.getFeature<SslServerFeature>().validateOptions();
      server.getFeature<DatabasePathFeature>().validateOptions();
      server.getFeature<SchedulerFeature>().validateOptions();
      server.getFeature<RocksDBOptionFeature>().validateOptions();
      server.getFeature<EngineFeature>().validateOptions();
      server.getFeature<FlushFeature>().validateOptions();
      server.getFeature<RocksDBRecoveryManager>().validateOptions();
      server.getFeature<catalog::CatalogFeature>().validateOptions();
      server.getFeature<search::SearchEngine>().validateOptions();
      server.getFeature<EndpointFeature>().validateOptions();
      server.getFeature<GeneralServerFeature>().validateOptions();
      server.getFeature<pg::PostgresFeature>().validateOptions();

      // 3. prepare -- non-thread-creating init.
      server.setState(SerenedServer::State::InPrepare);
      server.getFeature<SslServerFeature>().prepare();
      server.getFeature<DatabasePathFeature>().prepare();
      server.getFeature<SchedulerFeature>().prepare();
      server.getFeature<RocksDBOptionFeature>().prepare();
      server.getFeature<EngineFeature>().prepare();
      server.getFeature<FlushFeature>().prepare();
      server.getFeature<RocksDBRecoveryManager>().prepare();
      server.getFeature<catalog::CatalogFeature>().prepare();
      server.getFeature<search::SearchEngine>().prepare();
      server.getFeature<EndpointFeature>().prepare();
      server.getFeature<GeneralServerFeature>().prepare();
      server.getFeature<pg::PostgresFeature>().prepare();

      // 4. start -- spin up threads, open sockets. AFTER this returns
      //    successfully, the server is fully accepting clients.
      server.setState(SerenedServer::State::InStart);
      server.getFeature<SslServerFeature>().start();
      server.getFeature<DatabasePathFeature>().start();
      server.getFeature<SchedulerFeature>().start();
      server.getFeature<RocksDBOptionFeature>().start();
      server.getFeature<EngineFeature>().start();
      server.getFeature<FlushFeature>().start();
      server.getFeature<RocksDBRecoveryManager>().start();
      server.getFeature<catalog::CatalogFeature>().start();
      server.getFeature<search::SearchEngine>().start();
      server.getFeature<EndpointFeature>().start();
      server.getFeature<GeneralServerFeature>().start();
      server.getFeature<pg::PostgresFeature>().start();

      // 5. wait for SIGTERM/SIGINT.
      server.setState(SerenedServer::State::InWait);
      server.wait();

      // 6. stop -- strict LIFO of step 4. Threads must exit before
      //    the call returns.
      server.setState(SerenedServer::State::InStop);
      server.getFeature<pg::PostgresFeature>().stop();
      server.getFeature<GeneralServerFeature>().stop();
      server.getFeature<EndpointFeature>().stop();
      server.getFeature<search::SearchEngine>().stop();
      server.getFeature<catalog::CatalogFeature>().stop();
      server.getFeature<RocksDBRecoveryManager>().stop();
      server.getFeature<FlushFeature>().stop();
      server.getFeature<EngineFeature>().stop();
      server.getFeature<RocksDBOptionFeature>().stop();
      server.getFeature<SchedulerFeature>().stop();
      server.getFeature<DatabasePathFeature>().stop();
      server.getFeature<SslServerFeature>().stop();

      // 7. unprepare -- final teardown, also LIFO.
      server.setState(SerenedServer::State::InUnprepare);
      server.getFeature<pg::PostgresFeature>().unprepare();
      server.getFeature<GeneralServerFeature>().unprepare();
      server.getFeature<EndpointFeature>().unprepare();
      server.getFeature<search::SearchEngine>().unprepare();
      server.getFeature<catalog::CatalogFeature>().unprepare();
      server.getFeature<RocksDBRecoveryManager>().unprepare();
      server.getFeature<FlushFeature>().unprepare();
      server.getFeature<EngineFeature>().unprepare();
      server.getFeature<RocksDBOptionFeature>().unprepare();
      server.getFeature<SchedulerFeature>().unprepare();
      server.getFeature<DatabasePathFeature>().unprepare();
      server.getFeature<SslServerFeature>().unprepare();
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
