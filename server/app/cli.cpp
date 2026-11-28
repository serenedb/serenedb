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

#include <absl/base/internal/endian.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
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
#include "pg/serialize.h"
#include "query/server_engine.h"
#include "runtime/runtime.h"
#include "server/utils/message_buffer.h"
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
duckdb::Identifier gShellFile;
duckdb_shell::ShellSubcommand gShellSubcommand =
  duckdb_shell::ShellSubcommand::SHELL;

duckdb::shared_ptr<duckdb::DatabaseInstance> OpenShellEngine(
  const std::string& path, bool read_only) {
  auto& engine = irs::DuckDBEngine::Instance();
  const bool client = gShellSubcommand == duckdb_shell::ShellSubcommand::PSQL;
  if (!gShellServices) {
    engine.Initialize(&server::query::ConfigureShellDBConfig);
    server::query::RegisterServerExtensions(engine.instance());
    gShellServices.emplace(client);
  }
  auto& db = engine.instance();
  auto& manager = duckdb::DatabaseManager::Get(db);
  if (const auto opened = std::exchange(gShellFile, {}); !opened.empty()) {
    const auto conn = engine.CreateConnection();
    auto& context = *conn->context;
    context.RunFunctionInTransaction([&] {
      manager.DetachDatabase(context, opened,
                             duckdb::OnEntryNotFound::RETURN_NULL, true);
    });
  }
  if (path.empty() || path == IN_MEMORY_PATH) {
    if (!client) {
      manager.SetDefaultDatabase(
        duckdb::Identifier{irs::StaticStrings::kDefaultDatabase});
    }
    return db.shared_from_this();
  }
  duckdb::AttachInfo info;
  info.path = path;
  info.name = duckdb::AttachedDatabase::ExtractDatabaseName(db, path);
  if (info.name == irs::StaticStrings::kDefaultDatabase) {
    info.name =
      duckdb::Identifier{absl::StrCat(info.name.GetIdentifierName(), "_db")};
  }
  const auto conn = engine.CreateConnection();
  auto& context = *conn->context;
  context.RunFunctionInTransaction([&] {
    catalog::Attach(context, info, "duckdb",
                    read_only ? duckdb::AccessMode::READ_ONLY
                              : duckdb::AccessMode::READ_WRITE,
                    duckdb::AttachVisibility::SHOWN);
  });
  manager.SetDefaultDatabase(info.name);
  gShellFile = info.name;
  return db.shared_from_this();
}

void CloseShellEngine() {
  if (!gShellServices) {
    return;
  }
  gShellServices.reset();
  irs::DuckDBEngine::Instance().Shutdown();
}

void RenderShellText(duckdb::ClientContext& client, duckdb::DataChunk& input,
                     duckdb::DataChunk& output) {
  constexpr size_t kChunkMin = 4 << 10;
  constexpr size_t kChunkMax = 1 << 20;
  pg::SerializationContext context;
  pg::FillContext(client, context);
  const auto rows = input.size();
  message::Buffer buffer{kChunkMin, kChunkMax};
  std::string bytes;
  duckdb::RecursiveUnifiedVectorFormat values;
  for (duckdb::idx_t column = 0; column < input.ColumnCount(); ++column) {
    const auto serialize = pg::GetSerialization(input.data[column].GetType(),
                                                pg::VarFormat::Text, context);
    duckdb::Vector::RecursiveToUnifiedFormat(input.data[column], rows, values);
    for (duckdb::idx_t row = 0; row < rows; ++row) {
      message::Writer writer{buffer};
      context.writer = &writer;
      serialize(context, values, row);
      writer.Commit(false);
    }
    const auto chain = buffer.ReleaseChain();
    bytes.clear();
    for (auto* chunk = chain.head; chunk; chunk = chunk->Next()) {
      const auto data = chunk->Data(chunk->GetEnd());
      bytes.append(reinterpret_cast<const char*>(data.data()), data.size());
    }
    auto text =
      duckdb::FlatVector::Writer<duckdb::string_t>(output.data[column], rows);
    size_t at = 0;
    for (duckdb::idx_t row = 0; row < rows; ++row) {
      const auto length =
        static_cast<int32_t>(absl::big_endian::Load32(bytes.data() + at));
      at += sizeof(int32_t);
      if (length < 0) {
        text.WriteNull();
        continue;
      }
      text.WriteValue(
        duckdb::string_t{bytes.data() + at, static_cast<uint32_t>(length)});
      at += static_cast<size_t>(length);
    }
  }
}

int RunCLI(int argc, char* argv[], duckdb_shell::ShellSubcommand subcommand) {
  gShellSubcommand = subcommand;
  docs::RegisterShellDocsBackend();
  return duckdb_shell::Run(argc, argv, subcommand,
                           {.open = &OpenShellEngine,
                            .close = &CloseShellEngine,
                            .name = "sdb",
                            .render_text = &RenderShellText});
}

int RunShell(int argc, char* argv[]) {
  return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::SHELL);
}

int RunPsql(int argc, char* argv[]) {
  return RunCLI(argc, argv, duckdb_shell::ShellSubcommand::PSQL);
}

SDB_COMMAND kShell{"shell", &RunShell};
SDB_COMMAND kPsql{"psql", &RunPsql};

}  // namespace
}  // namespace sdb::app
