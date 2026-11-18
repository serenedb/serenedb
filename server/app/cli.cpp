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

#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string>

#include "app/command.h"
#include "catalog/boot.h"
#include "docs/docs_shell_backend.h"
#include "duckdb_shell.hpp"
#include "jobs/scheduler.h"
#include "query/server_engine.h"
#include "runtime/runtime.h"
#include "storage/storage.h"

namespace sdb::app {
namespace {

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
  gShellSubcommand = subcommand;
  docs::RegisterShellDocsBackend();
  return duckdb_shell::Run(
    argc, argv, subcommand,
    {.open = &OpenShellEngine, .close = &CloseShellEngine});
}

int RunShell(int argc, char* argv[]) {
  return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
}

int RunPsql(int argc, char* argv[]) {
  return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
}

const RegisterCommand kShell{"shell", &RunShell};
const RegisterCommand kPsql{"psql", &RunPsql};

}  // namespace
}  // namespace sdb::app
