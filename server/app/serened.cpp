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

#include <absl/base/config.h>
#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <pthread.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/logging/log_manager.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <exception>
#include <filesystem>
#include <functional>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/utils/cpu_features.hpp>
#include <iresearch/utils/crash_handler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/remap_executable.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "app/process.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "connector/duckdb_copy_filesystem.h"
#include "docs/docs_bootstrap.h"
#include "docs/docs_shell_backend.h"
#include "duckdb_shell.hpp"
#include "jobs/scheduler.h"
#include "network/server.h"
#include "pg/catalog/oids.h"
#include "query/config.h"
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

namespace {

using namespace sdb;
using namespace sdb::app;

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

void SetLargeThreadStackSize() {
#if defined(__GLIBC__) &&                  \
  (defined(ABSL_HAVE_ADDRESS_SANITIZER) || \
   defined(ABSL_HAVE_THREAD_SANITIZER) ||  \
   defined(ABSL_HAVE_MEMORY_SANITIZER) || !defined(__OPTIMIZE__))
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, size_t{64} << 20);
  pthread_setattr_default_np(&attr);
  pthread_attr_destroy(&attr);
#endif
}

int Serve() {
  auto server_config = ReadServerConfig();
  Runtime runtime{ReadIoThreads()};
  Storage storage{runtime, ReadStorageConfig()};
  replication::SubscriptionEngine subscriptions{runtime.IoPool()};
  JobScheduler jobs{runtime};
  Server network{runtime.IoPool(), std::move(server_config)};
  SDB_INFO(STARTUP, "SereneDB is ready for business. Have fun!");
  irs::CrashHandler::SetState("running");
  app::WaitForShutdown();
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

struct ShellServices {
  explicit ShellServices(bool client)
    : storage{runtime, {.default_database = !client}} {}

  Runtime runtime{ReadIoThreads()};
  Storage storage;
  JobScheduler jobs{runtime};
};

std::optional<ShellServices> gShellServices;
duckdb_shell::ShellSubcommand gShellSubcommand =
  duckdb_shell::ShellSubcommand::SHELL;

duckdb::shared_ptr<duckdb::DatabaseInstance> OpenShellEngine(
  const std::string& path) {
  auto& engine = irs::DuckDBEngine::Instance();
  const bool client = gShellSubcommand == duckdb_shell::ShellSubcommand::PSQL;
  if (!gShellServices) {
    engine.Initialize(&server::query::ConfigureShellDBConfig);
    server::query::RegisterServerExtensions(engine.instance());
    gShellServices.emplace(client);
  }
  auto& db = engine.instance();
  auto& manager = duckdb::DatabaseManager::Get(db);
  if (path.empty() || path == IN_MEMORY_PATH) {
    if (!client) {
      manager.SetDefaultDatabase(
        duckdb::Identifier{irs::StaticStrings::kDefaultDatabase});
    }
    return db.shared_from_this();
  }
  auto conn = engine.CreateConnection();
  auto& context = *conn->context;
  duckdb::AttachInfo info;
  info.path = path;
  info.name = duckdb::AttachedDatabase::ExtractDatabaseName(
    path, duckdb::FileSystem::GetFileSystem(db));
  context.RunFunctionInTransaction([&] {
    catalog::Attach(context, info, "duckdb", duckdb::AccessMode::READ_WRITE,
                    duckdb::AttachVisibility::SHOWN);
  });
  manager.SetDefaultDatabase(info.name);
  return db.shared_from_this();
}

void CloseShellEngine() {
  if (!gShellServices) {
    return;
  }
  gShellServices.reset();
  irs::DuckDBEngine::Instance().Shutdown();
}

int RunCLI(int argc, char* argv[], duckdb_shell::ShellSubcommand subcommand) {
  argv[1] = argv[0];
  gShellSubcommand = subcommand;
  docs::RegisterShellDocsBackend();
  return duckdb_shell::Run(
    argc - 1, argv + 1, subcommand,
    {.open = &OpenShellEngine, .close = &CloseShellEngine});
}

int RunDocsBuilder(int argc, char* argv[]) {
  if (argc != 4) {
    std::fputs("usage: serened docs-index <corpus> <index-dir>\n", stderr);
    return EXIT_FAILURE;
  }
  const std::filesystem::path corpus{argv[2]};
  const std::filesystem::path index{argv[3]};
  std::error_code ec;
  const auto temp = std::filesystem::temp_directory_path(ec);
  auto directory = (temp / "serened-docs-XXXXXX").string();
  if (ec || ::mkdtemp(directory.data()) == nullptr) {
    std::fputs(
      absl::StrCat("serened: cannot create a data directory in ", temp.string(),
                   ": ", ec ? ec.message() : std::strerror(errno), "\n")
        .c_str(),
      stderr);
    return EXIT_FAILURE;
  }
  absl::Cleanup remove = [&] {
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
  };
  char* args[] = {argv[0], directory.data()};
  return RunWithEngine(2, args, [&] {
    Runtime runtime{ReadIoThreads()};
    Storage storage{runtime, ReadStorageConfig()};
    return docs::BuildDocsIndex(corpus, index) ? EXIT_SUCCESS : EXIT_FAILURE;
  });
}

int RunServer(int argc, char* argv[]) {
  return RunWithEngine(argc, argv, Serve);
}

}  // namespace

extern "C" void json_object_seed(size_t seed);

int main(int argc, char* argv[]) {
  irs::CheckCpuFeatures(argv[0]);
  SetLargeThreadStackSize();
  json_object_seed(0);
  irs::InitOptimizeRules();
  const std::string_view command = argc > 1 ? argv[1] : "";
  if (command == "shell") {
    return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
  }
  if (command == "psql") {
    return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
  }
  if (command == "docs-index") {
    return RunDocsBuilder(argc, argv);
  }
  return RunServer(argc, argv);
}
