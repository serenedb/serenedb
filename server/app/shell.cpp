////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/logging/log_manager.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "app/process.h"
#include "catalog/boot.h"
#include "docs/docs_shell_backend.h"
#include "duckdb_shell.hpp"
#include "jobs/scheduler.h"
#include "query/server_engine.h"
#include "runtime/runtime.h"
#include "search/engine.h"
#include "storage/storage.h"

namespace sdb::app {
namespace {

struct ShellServices {
  explicit ShellServices(bool client)
    : storage{runtime, {.default_database = !client}} {}

  Runtime runtime{ReadRuntimeConfig()};
  search::SearchEngine search;
  Storage storage;
  search::SearchTasks search_tasks{runtime, search};
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

// Trim argv in place: overwrite argv[1] with argv[0] and hand the shell
// `argv + 1`. The shell sees {<binary>, <user args>...} with no copy.
int RunSubcommand(int argc, char* argv[],
                  duckdb_shell::ShellSubcommand subcommand) {
  argv[1] = argv[0];
  gShellSubcommand = subcommand;
  docs::RegisterShellDocsBackend();
  return duckdb_shell::Run(
    argc - 1, argv + 1, subcommand,
    {.open = &OpenShellEngine, .close = &CloseShellEngine});
}

int RunShell(int argc, char* argv[]) {
  return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
}

int RunPsql(int argc, char* argv[]) {
  return RunSubcommand(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
}

std::vector<Command>& Registry() {
  static std::vector<Command> commands;
  return commands;
}

}  // namespace

void RegisterCommand(const Command& command) { Registry().push_back(command); }

std::span<const Command> Commands() { return Registry(); }

std::optional<int> RunCommand(int argc, char* argv[]) {
  if (argc < 2) {
    return std::nullopt;
  }
  for (const auto& command : Registry()) {
    if (command.name == argv[1]) {
      return command.run(argc, argv);
    }
  }
  return std::nullopt;
}

void RegisterShellCommands() {
  RegisterCommand(
    {.name = "shell", .usage = "[duckdb-shell args ...]", .run = &RunShell});
  RegisterCommand(
    {.name = "psql", .usage = "[psql args ...]", .run = &RunPsql});
}

}  // namespace sdb::app
