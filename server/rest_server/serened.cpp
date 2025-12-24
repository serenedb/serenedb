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

#include "general_server/server_options_feature.h"
#include "rest_server/serened_includes.h"

using namespace sdb;
using namespace sdb::app;

static_assert(SerenedServer::id<LoggerFeature>() == 0);

constexpr auto kNonServerFeatures = std::array{
#ifdef SDB_CLUSTER
  SerenedServer::id<AgencyFeature>(),
  SerenedServer::id<ClusterFeature>(),
#endif
#ifdef SERENEDB_HAVE_FORK
  SerenedServer::id<SupervisorFeature>(),
  SerenedServer::id<DaemonFeature>(),
#endif
  SerenedServer::id<GeneralServerFeature>(),
  SerenedServer::id<HttpEndpointProvider>(),
  SerenedServer::id<LogBufferFeature>(),
  SerenedServer::id<ServerFeature>(),
  SerenedServer::id<SslServerFeature>(),
  SerenedServer::id<StatisticsFeature>(),
};

static const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

static int RunServer(int argc, char** argv, GlobalContext& context) {
  try {
    CrashHandler::installCrashHandler();
    std::string name = context.binaryName();

    auto options = std::make_shared<sdb::options::ProgramOptions>(
      argv[0], "Usage: " + name + " [<options>]",
      "For more information use:", SBIN_DIRECTORY);

    int ret{EXIT_FAILURE};
    SerenedServer server{options, SBIN_DIRECTORY};
    ServerState state;

    server.addReporter(
      {[&](SerenedServer::State state) {
         CrashHandler::SetState(magic_enum::enum_name(state));

         if (state == SerenedServer::State::InStart) {
           // drop priveleges before starting features
           server.getFeature<PrivilegeFeature>().dropPrivilegesPermanently();
         }
       },
       {}});

    server.addFeatures(absl::Overload{
      []<typename T>(auto& server, type::Tag<T>) {
        return std::make_unique<T>(server);
      },
      [&ret](auto& server, type::Tag<CheckVersionFeature>) {
        return std::make_unique<CheckVersionFeature>(server, &ret,
                                                     kNonServerFeatures);
      },
      [&name](auto& server, type::Tag<ConfigFeature>) {
        return std::make_unique<ConfigFeature>(
          server, name, [] { return GetServerOptions().app_print_version; });
      },
      [](auto& server, type::Tag<InitDatabaseFeature>) {
        return std::make_unique<InitDatabaseFeature>(server,
                                                     kNonServerFeatures);
      },
      [](auto& server, type::Tag<LoggerFeature>) {
        return std::make_unique<LoggerFeature>(server, true);
      },
      [](auto& server, type::Tag<NetworkFeature>) {
        auto& metrics = server.template getFeature<metrics::MetricsFeature>();
        return std::make_unique<NetworkFeature>(
          server, metrics, network::ConnectionPool::Config{metrics});
      },
      [&ret](auto& server, type::Tag<ServerFeature>) {
        return std::make_unique<ServerFeature>(server, &ret);
      },
      [&name](auto& server, type::Tag<TempPath>) {
        return std::make_unique<TempPath>(server, name);
      },
      [](auto& server, type::Tag<SslServerFeature>) {
        return std::make_unique<SslServerFeature>(server);
      },
      [&ret](auto& server, type::Tag<UpgradeFeature>) {
        return std::make_unique<UpgradeFeature>(server, &ret,
                                                kNonServerFeatures);
      },
      [](auto& server, type::Tag<HttpEndpointProvider>) {
        return std::make_unique<EndpointFeature>(server);
      },
    });

    try {
      server.run(argc, argv);
      if (server.helpShown()) {
        // --help was displayed
        ret = EXIT_SUCCESS;
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "serened terminated because of an exception: ", ex.what());
      ret = EXIT_FAILURE;
    } catch (...) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "serened terminated because of an exception of "
                "unknown type");
      ret = EXIT_FAILURE;
    }
    log::Flush();
    return context.exit(ret);
  } catch (const std::exception& ex) {
    SDB_ERROR("xxxxx", Logger::FIXME,
              "serened terminated because of an exception: ", ex.what());
  } catch (...) {
    SDB_ERROR("xxxxx", Logger::FIXME,
              "serened terminated because of an exception of "
              "unknown type");
  }
  exit(EXIT_FAILURE);
}

int main(int argc, char* argv[]) {
  std::string workdir(basics::file_utils::CurrentDirectory().result());

  GlobalContext context(argc, argv, SBIN_DIRECTORY);

  gRestartAction = nullptr;

  int res = RunServer(argc, argv, context);
  if (res != 0) {
    return res;
  }
  if (gRestartAction == nullptr) {
    return 0;
  }
  try {
    res = (*gRestartAction)();
  } catch (...) {
    res = -1;
  }
  delete gRestartAction;
  if (res != 0) {
    std::cerr << "FATAL: RestartAction returned non-zero exit status: " << res
              << ", giving up." << std::endl;
    return res;
  }
  // It is not clear if we want to do the following under Linux and OSX,
  // it is a clean way to restart from scratch with the same process ID,
  // so the process does not have to be terminated. On Windows, we have
  // to do this because the solution below is not possible. In these
  // cases, we need outside help to get the process restarted.
#if defined(__linux__)
  res = chdir(workdir.c_str());
  if (res != 0) {
    std::cerr << "WARNING: could not change into directory '" << workdir << "'"
              << std::endl;
  }
  if (execvp(argv[0], argv) == -1) {
    std::cerr << "WARNING: could not execvp ourselves, restore will not work!"
              << std::endl;
  }
#endif
}
