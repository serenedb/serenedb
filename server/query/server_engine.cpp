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

#include "query/server_engine.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include <duckdb.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <functional>

#include "auth/enforce.h"
#include "catalog/boot.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/secret_store.h"
#include "connector/duckdb_pg_binary_copy.h"
#include "connector/duckdb_pg_text_copy.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_subscription.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/ai/ai.h"
#include "connector/functions/array.h"
#include "connector/functions/backup.h"
#include "connector/functions/catalog_introspect.h"
#include "connector/functions/duckdb_aliases.h"
#include "connector/functions/encode_key.h"
#include "connector/functions/es.h"
#include "connector/functions/formatting.h"
#include "connector/functions/hash.h"
#include "connector/functions/jobs.h"
#include "connector/functions/json.h"
#include "connector/functions/markdown_render.h"
#include "connector/functions/math.h"
#include "connector/functions/metrics.h"
#include "connector/functions/otel.h"
#include "connector/functions/search.h"
#include "connector/functions/string.h"
#include "connector/iresearch_replacement_scan.h"
#include "connector/scan/scan_function.h"
#include "docs/docs_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/scan_function.h"
#include "pg/catalog/functions/functions.h"
#include "pg/connection_context.h"
#include "pg/types/register.h"
#include "quack_storage.hpp"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "replication/functions.h"
#include "replication/repl_source.h"
#include "scheduler/background_scheduler.h"
#include "server/utils/file_utils.h"
#include "server/utils/lifecycle.h"
#include "server/utils/number_of_cores.h"

extern "C" const duckdb::DefaultType* duckdb_external_types(
  duckdb::idx_t* count) {
  const auto types = sdb::pg::ExternalTypes();
  *count = types.size();
  return types.data();
}

ABSL_FLAG(uint64_t, cpu_threads, 0,
          "Executor pool size at process start. 0 = let server "
          "auto-detect from cpu_count. `SET GLOBAL threads = N` resizes the "
          "pool at runtime.");

ABSL_DECLARE_FLAG(std::string, server_directory);

namespace sdb::server::query {
namespace {

void ConfigureEngineDBConfig(duckdb::DBConfig& config,
                             const std::string& datadir) {
  duckdb::FunctionExpression::SetBuiltinPrefix({});
  config.create_internal_catalog =
    [layout = catalog::DataDirectory{datadir}](duckdb::AttachedDatabase& db) {
      return catalog::CreateInternalCatalog(db, layout);
    };
  config.create_client_context = CreateClientContext;
  config.create_secret_storage = catalog::CreateSecretStorage;
  connector::RegisterSereneDBStorage(config);
  connector::RegisterConfigVariables(config);
  config.AddExtensionOption(
    duckdb::Identifier{"data_directory"}, "Shows the data directory.",
    duckdb::LogicalType::VARCHAR, duckdb::Value{datadir}, nullptr, nullptr,
    duckdb::SetScope::AUTOMATIC, true);
  connector::RegisterIResearchReplacementScan(config);
}

}  // namespace

void ConfigureShellDBConfig(duckdb::DBConfig& config) {
  ConfigureEngineDBConfig(config, {});
}

void ConfigureServerDBConfig(duckdb::DBConfig& config) {
  // Server-mode DuckDB state lives under the datadir, never in cwd-relative
  // temp files or ~/.duckdb fallbacks.
  const auto datadir =
    lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory));
  ConfigureEngineDBConfig(config, datadir);
  config.SetOptionByName(
    "temp_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "tmp")});
  config.SetOptionByName(
    "secret_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "secrets")});
  config.SetOptionByName(
    "extension_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "extensions")});
  // DuckDB's own auto-detect uses std::thread::hardware_concurrency(), which
  // ignores cgroup CPU limits and would over-thread in a container. Pin it to
  // our cgroup-aware logical core count when unset, and publish the resolved
  // value into the flag.
  auto threads = absl::GetFlag(FLAGS_cpu_threads);
  if (threads == 0) {
    threads = CountLogicalCores();
    absl::SetFlag(&FLAGS_cpu_threads, threads);
  }
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(threads));
  // serenedb runs every query on the internal pool (sessions are scheduled as
  // tasks; the driver is itself a pool worker), so there is no external thread
  // feeding the scheduler. Default external_threads=1 would over-count
  // parallelism by one and make `threads`/`cpu_threads` resolve to N-1 internal
  // workers; zero makes the count exact and `threads=1` a true single worker.
  // Sessions run as tasks on that pool, so it must never be left empty; the
  // value is refused for the lifetime of the process (IsUnchangeableSetting),
  // which is what keeps `threads -
  // external_threads` from ever resolving to zero internal workers.
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
}

namespace {

void RegisterEngineModules(duckdb::DatabaseInstance& db) {
  connector::RegisterPgMathFunctions(db);

  connector::RegisterPgFormattingFunctions(db);

  connector::RegisterHashFunctions(db);

  connector::RegisterKeyEncodingFunctions(db);

  pg::RegisterTypes(db);

  connector::RegisterPgStringFunctions(db);

  connector::RegisterPgArrayFunctions(db);

  connector::RegisterPgJsonFunctions(db);

  connector::RegisterMetricsFunctions(db);

  connector::RegisterMarkdownRenderFunctions(db);

  docs::RegisterDocsFunctions(db);

  connector::RegisterDuckDBAliases(db);

  connector::RegisterAIFunctions(db);
}

}  // namespace

void RegisterServerExtensions(duckdb::DatabaseInstance& db) {
  RegisterEngineModules(db);

  connector::RegisterSubscriptionPragma(db);

  pg::RegisterFunctions(db);

  connector::RegisterEsFunctions(db);

  connector::RegisterOtelFunctions(db);

  connector::RegisterBackupFunctions(db);

  connector::RegisterCatalogIntrospectFunctions(db);

  connector::RegisterJobFunctions(db);

  connector::RegisterVacuumFunction(db);

  connector::RegisterReindexFunction(db);

  connector::RegisterPgBinaryCopyFunction(db);

  connector::RegisterPgTextCopyFunction(db);
  replication::RegisterReplicationSourceFunction(db);
  replication::RegisterReplicationFunctions(db);

  connector::RegisterSearchFunctions(db);

  connector::RegisterIResearchScanFunction(db);

  pg::RegisterSystemTableScanFunction(db);

  connector::RegisterSereneDBOptimizers(db);

  auth::InstallQuackAccessCheck(db);
  duckdb::QuackStorageExtensionInfo::GetState(db).scheduler =
    [](duckdb::milliseconds delay, std::function<void()> task) {
      if (auto* scheduler = BackgroundScheduler::gInstance) {
        scheduler->RunAfter(delay, std::move(task));
      }
    };

  duckdb::IndexType inverted;
  inverted.name = catalog::kInvertedIndexTypeName;
  inverted.create_plan = &connector::SereneDBCreateIndexPlan;
  db.config.GetIndexTypes().RegisterIndexType(inverted);

  // Parse and cache system functions/views for serving from our attached
  // catalog.
  auto parser = duckdb::Parser::GetBuiltinParser();
  pg::InitSystemTables();
  pg::InitSystemFunctions(db, parser);
  pg::InitSystemViews(parser);
}

}  // namespace sdb::server::query
