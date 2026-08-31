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

#include "basics/file_utils.h"
#include "basics/lifecycle.h"
#include "basics/number_of_cores.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_foreign_server_function.h"
#include "connector/duckdb_pg_binary_copy.h"
#include "connector/duckdb_pg_text_copy.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_rbac_function.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_table_function.h"
#include "connector/duckdb_tokenizer_function.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/array.h"
#include "connector/functions/catalog_introspect.h"
#include "connector/functions/duckdb_aliases.h"
#include "connector/functions/embedding/embedding.h"
#include "connector/functions/encode_key.h"
#include "connector/functions/es.h"
#include "connector/functions/inout.h"
#include "connector/functions/json.h"
#include "connector/functions/math.h"
#include "connector/functions/search.h"
#include "connector/functions/sequence.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/functions/vector.h"
#include "connector/inverted_store_index.h"
#include "connector/pg_logical_types.h"
#include "pg/pg_catalog/pg_statistic.h"
#include "pg/system_catalog.h"
#include "pg/system_table.h"
#include "query/config.h"
#include "query/config_variable_names.h"

extern "C" const duckdb::DefaultType* duckdb_external_types(
  duckdb::idx_t* count) {
  // Lazy-initialized to avoid static initialization order issues
  // (LogicalType has shared_ptr that needs heap allocation).
  static const duckdb::DefaultType kExternalTypes[] = {
    // reg* types -- aliased BIGINT with input functions for catalog lookups
    {
      "regclass",
      sdb::pg::REGCLASS(),
      nullptr,
    },
    {
      "regtype",
      sdb::pg::REGTYPE(),
      nullptr,
    },
    {
      "regnamespace",
      sdb::pg::REGNAMESPACE(),
      nullptr,
    },
    {
      "regproc",
      sdb::pg::REGPROC(),
      nullptr,
    },
    {
      "regoper",
      sdb::pg::REGOPER(),
      nullptr,
    },
    {
      "regoperator",
      sdb::pg::REGOPERATOR(),
      nullptr,
    },
    {
      "regprocedure",
      sdb::pg::REGPROCEDURE(),
      nullptr,
    },
    {
      "regrole",
      sdb::pg::REGROLE(),
      nullptr,
    },
    {
      "regconfig",
      sdb::pg::REGCONFIG(),
      nullptr,
    },
    {
      "regdictionary",
      sdb::pg::REGDICTIONARY(),
      nullptr,
    },
    {
      "regcollation",
      sdb::pg::REGCOLLATION(),
      nullptr,
    },
    // oid -- overrides DuckDB builtin (plain BIGINT) with aliased BIGINT
    {
      "oid",
      sdb::pg::OID(),
      nullptr,
    },
    // System identifier types -- aliased BIGINT
    {
      "tid",
      sdb::pg::TID(),
      nullptr,
    },
    {
      "cid",
      sdb::pg::CID(),
      nullptr,
    },
    {
      "xid",
      sdb::pg::XID(),
      nullptr,
    },
    {
      "xid8",
      sdb::pg::XID8(),
      nullptr,
    },
    // PG name type
    {
      "name",
      sdb::pg::NAME(),
      nullptr,
    },
    // PG composite type used as cast target in pg_stats_ext_exprs view.
    {
      "pg_statistic",
      [] {
        auto t = sdb::pg::SystemTable<sdb::pg::PgStatistic>{}.RowType();
        t.SetAlias("pg_statistic");
        return t;
      }(),
      nullptr,
    },
    // information_schema types, TODO(mbkkt) move this to namespace
    {
      "cardinal_number",
      sdb::pg::CARDINALNUMBER(),
      nullptr,
    },
    {
      "character_data",
      sdb::pg::CHARACTERDATA(),
      nullptr,
    },
    {
      "sql_identifier",
      sdb::pg::SQLIDENTIFIER(),
      nullptr,
    },
    {
      "time_stamp",
      sdb::pg::TIMESTAMP(),
      nullptr,
    },
    {
      "yes_or_no",
      sdb::pg::YESORNO(),
      nullptr,
    },
    // pseudo-types for SERIAL types.
    {
      "serial",
      sdb::pg::SERIAL(),
      nullptr,
    },
    {
      "bigserial",
      sdb::pg::BIGSERIAL(),
      nullptr,
    },
    {
      "smallserial",
      sdb::pg::SMALLSERIAL(),
      nullptr,
    },
    // PG pseudo-type used in RETURNS VOID; backed by SQLNULL.
    {
      "void",
      sdb::pg::VOID(),
      nullptr,
    },
  };
  *count = std::size(kExternalTypes);
  return kExternalTypes;
}

ABSL_FLAG(uint64_t, cpu_threads, 0,
          "Executor pool size at process start. 0 = let server "
          "auto-detect from cpu_count. The SQL-level `SET threads = N` "
          "continues to win at runtime.");

ABSL_FLAG(uint32_t, recovery_replay_depth, 0,
          "Maximum WAL chunks in flight per inverted index during recovery "
          "replay (the prefetch window; bounds replay memory). 0 = auto "
          "(4 x cpu threads).");

ABSL_DECLARE_FLAG(std::string, server_directory);

namespace sdb::server::query {

void ConfigureServerDBConfig(duckdb::DBConfig& config) {
  connector::RegisterSereneDBStorage(config);
  catalog::RegisterSereneDBGlobalStorage(config);
  connector::RegisterConfigVariables(config);
  // Roles and the database list are the first thing that has to exist, and
  // they belong to no database, so the cluster-global catalog is duckdb's main
  // database rather than something attached next to a throwaway in-memory one.
  config.options.database_type = std::string{catalog::kGlobalStorageType};
  config.options.database_name = std::string{catalog::kGlobalDatabaseName};
  config.options.database_hidden = true;
  // Server-mode DuckDB state lives under the datadir, never in cwd-relative
  // temp files or ~/.duckdb fallbacks (shell/psql subcommands return before
  // this mutator runs and keep DuckDB defaults).
  const auto datadir =
    lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory));
  config.SetOptionByName(
    "temp_directory",
    duckdb::Value{basics::file_utils::BuildFilename(datadir, "tmp")});
  config.SetOptionByName(
    "secret_directory",
    duckdb::Value{basics::file_utils::BuildFilename(datadir, "secrets")});
  config.SetOptionByName(
    "extension_directory",
    duckdb::Value{basics::file_utils::BuildFilename(datadir, "extensions")});
  // Dependency edges are built from what the binder resolved
  // (CreateInfo::dependencies), so the collection must be on for every bind.
  config.SetOptionByName("enable_view_dependencies",
                         duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("enable_macro_dependencies",
                         duckdb::Value::BOOLEAN(true));
  // DuckDB's own auto-detect uses std::thread::hardware_concurrency(), which
  // ignores cgroup CPU limits and would over-thread in a container. Pin it to
  // our cgroup-aware logical core count when unset (SET threads=N still wins at
  // runtime), and publish the resolved value into the flag.
  auto threads = absl::GetFlag(FLAGS_cpu_threads);
  if (threads == 0) {
    threads = CountLogicalCores();
    absl::SetFlag(&FLAGS_cpu_threads, threads);
  }
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(threads));
  if (const auto depth = absl::GetFlag(FLAGS_recovery_replay_depth);
      depth != 0) {
    config.SetOptionByName(std::string{kRecoveryReplayDepthSetting},
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
  // `SELECT ... FROM index_name`: an index is a relation in postgres, but it
  // keeps no scannable catalog entry -- the name resolves here instead.
  config.replacement_scans.emplace_back(connector::IndexScanReplacement);
  // PostgreSQL's COPY ... TO writes no CSV header unless HEADER is given;
  // DuckDB's writer defaults it on.
  config.SetOptionByName("copy_csv_header_default",
                         duckdb::Value::BOOLEAN(false));
  // `/` between two integers truncates in PostgreSQL, where DuckDB produces a
  // DOUBLE. A client that wants DuckDB's reading can still SET this back per
  // session.
  config.SetOptionByName("integer_division", duckdb::Value::BOOLEAN(true));
}

void RegisterServerExtensions(duckdb::DatabaseInstance& db) {
  // On the live config: the pre-construct mutator's copy does not carry
  // plain function-pointer members into the instance.
  duckdb::DBConfig::GetConfig(db).external_index_provider =
    connector::InjectExternalIndexes;
  duckdb::DBConfig::GetConfig(db).host_table_provider = catalog::HostTableEntry;
  duckdb::DBConfig::GetConfig(db).external_range_replay =
    connector::InvertedStoreIndex::ReplayExternalRange;
  duckdb::DBConfig::GetConfig(db).external_local_append =
    connector::InvertedStoreIndex::AppendLocalRange;

  connector::RegisterTokenizerPragma(db);

  connector::RegisterForeignServerPragma(db);

  connector::RegisterPgMathFunctions(db);

  connector::RegisterKeyEncodingFunctions(db);

  connector::RegisterPgSystemFunctions(db);

  connector::RegisterSequenceFunctions(db);

  connector::RegisterPgInOutFunctions(db);

  connector::RegisterRbacPragmas(db);

  connector::RegisterPgStringFunctions(db);

  connector::RegisterPgArrayFunctions(db);

  connector::RegisterPgJsonFunctions(db);

  connector::RegisterEsFunctions(db);

  connector::RegisterCatalogIntrospectFunctions(db);

  connector::RegisterDuckDBAliases(db);

  connector::RegisterVacuumFunction(db);

  connector::RegisterReindexFunction(db);

  connector::RegisterPgBinaryCopyFunction(db);

  connector::RegisterPgTextCopyFunction(db);

  connector::RegisterSearchFunctions(db);

  connector::RegisterIResearchScanFunction(db);

  connector::RegisterVectorFunctions(db);

  connector::RegisterEmbeddingFunctions(db);

  connector::RegisterSereneDBOptimizers(db);

  // The inverted index: its build plan and its store-side instance are
  // serenedb's. A plain CREATE INDEX is duckdb's ART end to end -- the bind
  // normalizes every non-inverted spelling to it.
  auto& index_types = db.config.GetIndexTypes();
  duckdb::IndexType inverted;
  inverted.name = connector::InvertedStoreIndex::kTypeName;
  inverted.create_plan = &connector::SereneDBCreateIndexPlan;
  inverted.create_instance = &connector::CreateInvertedInstance;
  index_types.RegisterIndexType(inverted);

  // Register filesystem for COPY FROM STDIN support.
  // Intercepts "/dev/stdin" and reads from PG CopyData messages.
  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  fs.RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  // Parse and cache system functions/views for serving from our attached
  // catalog. Route through the database parser cache so the PEG matcher built
  // here is the one reused by every connection -- a bare Parser uses a
  // throwaway local cache.
  duckdb::ParserOptions parser_options;
  parser_options.parser_cache = &db.GetParserCache();
  duckdb::Parser parser{parser_options};
  pg::InitSystemFunctions(parser);
  pg::InitSystemViews(parser);
}

}  // namespace sdb::server::query
