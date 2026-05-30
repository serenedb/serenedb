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
      SslServerFeature::instance().validateOptions();
      DatabasePathFeature::instance().validateOptions();
      SchedulerFeature::instance().validateOptions();
      RocksDBOptionFeature::instance().validateOptions();
      EngineFeature::instance().validateOptions();
      FlushFeature::instance().validateOptions();
      RocksDBRecoveryManager::instance().validateOptions();
      catalog::CatalogFeature::instance().validateOptions();
      search::SearchEngine::instance().validateOptions();
      EndpointFeature::instance().validateOptions();
      GeneralServerFeature::instance().validateOptions();
      pg::PostgresFeature::instance().validateOptions();

      // 3. prepare -- non-thread-creating init.
      server.setState(SerenedServer::State::InPrepare);
      SslServerFeature::instance().prepare();
      DatabasePathFeature::instance().prepare();
      SchedulerFeature::instance().prepare();
      RocksDBOptionFeature::instance().prepare();
      EngineFeature::instance().prepare();
      FlushFeature::instance().prepare();
      RocksDBRecoveryManager::instance().prepare();
      catalog::CatalogFeature::instance().prepare();
      search::SearchEngine::instance().prepare();
      EndpointFeature::instance().prepare();
      GeneralServerFeature::instance().prepare();
      pg::PostgresFeature::instance().prepare();

      // 4. start -- spin up threads, open sockets. AFTER this returns
      //    successfully, the server is fully accepting clients.
      server.setState(SerenedServer::State::InStart);
      SslServerFeature::instance().start();
      DatabasePathFeature::instance().start();
      SchedulerFeature::instance().start();
      RocksDBOptionFeature::instance().start();
      EngineFeature::instance().start();
      FlushFeature::instance().start();
      RocksDBRecoveryManager::instance().start();
      catalog::CatalogFeature::instance().start();
      search::SearchEngine::instance().start();
      EndpointFeature::instance().start();
      GeneralServerFeature::instance().start();
      pg::PostgresFeature::instance().start();

      // 5. wait for SIGTERM/SIGINT.
      server.setState(SerenedServer::State::InWait);
      server.wait();

      // 6. stop -- strict LIFO of step 4. Threads must exit before
      //    the call returns.
      server.setState(SerenedServer::State::InStop);
      pg::PostgresFeature::instance().stop();
      GeneralServerFeature::instance().stop();
      EndpointFeature::instance().stop();
      search::SearchEngine::instance().stop();
      catalog::CatalogFeature::instance().stop();
      RocksDBRecoveryManager::instance().stop();
      FlushFeature::instance().stop();
      EngineFeature::instance().stop();
      RocksDBOptionFeature::instance().stop();
      SchedulerFeature::instance().stop();
      DatabasePathFeature::instance().stop();
      SslServerFeature::instance().stop();

      // 7. unprepare -- final teardown, also LIFO.
      server.setState(SerenedServer::State::InUnprepare);
      pg::PostgresFeature::instance().unprepare();
      GeneralServerFeature::instance().unprepare();
      EndpointFeature::instance().unprepare();
      search::SearchEngine::instance().unprepare();
      catalog::CatalogFeature::instance().unprepare();
      RocksDBRecoveryManager::instance().unprepare();
      FlushFeature::instance().unprepare();
      EngineFeature::instance().unprepare();
      RocksDBOptionFeature::instance().unprepare();
      SchedulerFeature::instance().unprepare();
      DatabasePathFeature::instance().unprepare();
      SslServerFeature::instance().unprepare();
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
