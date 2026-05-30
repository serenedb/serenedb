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

    auto options = std::make_shared<sdb::options::ProgramOptions>(
      argv[0], "Usage: " + name + " [<options>]",
      "For more information use:", BIN_DIRECTORY);

    int ret{EXIT_FAILURE};
    SerenedServer server{options, BIN_DIRECTORY};
    ServerState state;
    // SereneDB is single-node; the cluster topology that the
    // ArangoDB ServerState modeled never applied.
    state.SetRole(ServerState::Role::Single);

    server.addReporter(
      {[&](SerenedServer::State state) {
         CrashHandler::SetState(magic_enum::enum_name(state));
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
      server.run(argc, argv);
      if (server.helpShown()) {
        // --help was displayed
        ret = EXIT_SUCCESS;
      } else {
        ret = EXIT_SUCCESS;
      }
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
