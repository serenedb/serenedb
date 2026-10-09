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

#include "catalog/boot.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_pg_binary_copy.h"
#include "connector/duckdb_pg_text_copy.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/ai/ai.h"
#include "connector/functions/array.h"
#include "connector/functions/catalog_introspect.h"
#include "connector/functions/duckdb_aliases.h"
#include "connector/functions/encode_key.h"
#include "connector/functions/es.h"
#include "connector/functions/inout.h"
#include "connector/functions/jobs.h"
#include "connector/functions/json.h"
#include "connector/functions/markdown_render.h"
#include "connector/functions/math.h"
#include "connector/functions/otel.h"
#include "connector/functions/search.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/inverted_store_index.h"
#include "connector/iresearch_replacement_scan.h"
#include "connector/pg_logical_types.h"
#include "connector/scan/scan_function.h"
#include "docs/docs_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/scan_function.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/ruleutils.h"
#include "query/config.h"
#include "query/config_variable_names.h"
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

ABSL_FLAG(uint32_t, recovery_replay_depth, 0,
          "Maximum WAL chunks in flight per inverted index during recovery "
          "replay (the prefetch window; bounds replay memory). 0 = auto "
          "(4 x cpu threads).");

ABSL_FLAG(bool, skip_search_recovery, false,
          "Do not replay the search-table WAL at startup; search tables come "
          "up with what their last refresh made durable.");

ABSL_DECLARE_FLAG(std::string, server_directory);

namespace sdb::server::query {

void ConfigureServerDBConfig(duckdb::DBConfig& config) {
  // Server-mode DuckDB state lives under the datadir, never in cwd-relative
  // temp files or ~/.duckdb fallbacks (shell/psql subcommands return before
  // this mutator runs and keep DuckDB defaults).
  const auto datadir =
    lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory));
  auto layout = duckdb::make_shared_ptr<catalog::DataDirectory>(datadir);
  connector::RegisterSereneDBStorage(config, layout);
  catalog::RegisterClusterStorage(config, std::move(layout));
  connector::RegisterConfigVariables(config);
  connector::RegisterIResearchReplacementScan(config);
  config.SetOptionByName(
    "temp_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "tmp")});
  config.SetOptionByName(
    "secret_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "secrets")});
  config.SetOptionByName(
    "extension_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "extensions")});
  // Dependency edges are built from what the binder resolved
  // (CreateInfo::dependencies), so the collection must be on for every bind.
  config.SetOptionByName("enable_view_dependencies",
                         duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("enable_macro_dependencies",
                         duckdb::Value::BOOLEAN(true));
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
  if (const auto depth = absl::GetFlag(FLAGS_recovery_replay_depth);
      depth != 0) {
    config.SetOptionByName(duckdb::Identifier{kRecoveryReplayDepthSetting},
                           duckdb::Value::UINTEGER(depth));
  }
  // serenedb runs every query on the internal pool (sessions are scheduled as
  // tasks; the driver is itself a pool worker), so there is no external thread
  // feeding the scheduler. Default external_threads=1 would over-count
  // parallelism by one and make `threads`/`cpu_threads` resolve to N-1 internal
  // workers; zero makes the count exact and `threads=1` a true single worker.
  // Sessions run as tasks on that pool, so it must never be left empty; the
  // value is refused for the lifetime of the process (kUnchangeableSettings in
  // connector/duckdb_client_state.cpp), which is what keeps `threads -
  // external_threads` from ever resolving to zero internal workers.
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
  config.SetOptionByName("scheduler_process_partial",
                         duckdb::Value::BOOLEAN(true));
  // PostgreSQL's COPY ... TO writes no CSV header unless HEADER is given;
  // DuckDB's writer defaults it on.
  config.SetOptionByName("copy_csv_header_default",
                         duckdb::Value::BOOLEAN(false));
  // `/` between two integers truncates in PostgreSQL, where DuckDB produces a
  // DOUBLE. A client that wants DuckDB's reading can still SET this back per
  // session.
  config.SetOptionByName("integer_division", duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("show_behavior", duckdb::Value("SETTING"));
  config.SetOptionByName("autoinstall_known_extensions",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("autoload_known_extensions",
                         duckdb::Value::BOOLEAN(false));
}

void RegisterServerExtensions(duckdb::DatabaseInstance& db) {
  connector::RegisterPgMathFunctions(db);

  connector::RegisterKeyEncodingFunctions(db);

  connector::RegisterPgSystemFunctions(db);

  connector::RegisterRuleutilsFunctions(db);

  connector::RegisterPgInOutFunctions(db);

  connector::RegisterPgStringFunctions(db);

  connector::RegisterPgArrayFunctions(db);

  connector::RegisterPgJsonFunctions(db);

  connector::RegisterEsFunctions(db);

  connector::RegisterOtelFunctions(db);

  connector::RegisterCatalogIntrospectFunctions(db);

  connector::RegisterMarkdownRenderFunctions(db);

  docs::RegisterDocsFunctions(db);

  connector::RegisterJobFunctions(db);

  connector::RegisterDuckDBAliases(db);

  connector::RegisterVacuumFunction(db);

  connector::RegisterReindexFunction(db);

  connector::RegisterPgBinaryCopyFunction(db);

  connector::RegisterPgTextCopyFunction(db);

  connector::RegisterSearchFunctions(db);

  connector::RegisterIResearchScanFunction(db);

  connector::RegisterSystemTableScanFunction(db);

  connector::RegisterAIFunctions(db);

  connector::RegisterSereneDBOptimizers(db);

  // The inverted index: its build plan and its store-side instance are
  // serenedb's. A plain CREATE INDEX is duckdb's ART end to end -- the bind
  // normalizes every non-inverted spelling to it.
  auto& index_types = db.config.GetIndexTypes();
  index_types.RegisterIndexType(
    connector::InvertedStoreIndex::GetInvertedIndexType());

  // Register filesystem for COPY FROM STDIN support.
  // Intercepts "/dev/stdin" and reads from PG CopyData messages.
  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  fs.RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  // Parse and cache system functions/views for serving from our attached
  // catalog.
  auto parser = duckdb::Parser::GetBuiltinParser();
  pg::InitSystemTables();
  pg::InitSystemFunctions(parser);
  pg::InitSystemViews(parser);
}

}  // namespace sdb::server::query
