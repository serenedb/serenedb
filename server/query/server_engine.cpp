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

#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_pg_binary_copy.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_tokenizer_function.h"
#include "connector/duckdb_truncate_function.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/array.h"
#include "connector/functions/cast.h"
#include "connector/functions/embedding/embedding.h"
#include "connector/functions/inout.h"
#include "connector/functions/json.h"
#include "connector/functions/math.h"
#include "connector/functions/search.h"
#include "connector/functions/sequence.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/functions/vector.h"
#include "connector/pg_logical_types.h"
#include "pg/pg_catalog/pg_statistic.h"
#include "pg/system_catalog.h"
#include "pg/system_table.h"
#include "query/config.h"

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

ABSL_DECLARE_FLAG(uint64_t, server_cpu_threads);

namespace sdb::server::query {

void ConfigureServerDBConfig(duckdb::DBConfig& config) {
  connector::RegisterSereneDBStorage(config);
  connector::RegisterConfigVariables(config);
  if (const auto t = absl::GetFlag(FLAGS_server_cpu_threads); t > 0) {
    config.SetOptionByName("threads", duckdb::Value::UBIGINT(t));
  }
}

void RegisterServerExtensions(duckdb::DatabaseInstance& db) {
  connector::RegisterTokenizerPragma(db);

  connector::RegisterPgCasts(db);

  connector::RegisterPgMathFunctions(db);

  connector::RegisterPgSystemFunctions(db);

  connector::RegisterSequenceFunctions(db);

  connector::RegisterPgInOutFunctions(db);

  connector::RegisterPgStringFunctions(db);

  connector::RegisterPgArrayFunctions(db);

  connector::RegisterPgJsonFunctions(db);

  connector::RegisterVacuumFunction(db);

  connector::RegisterPgBinaryCopyFunction(db);

  connector::RegisterTruncateFunction(db);

  connector::RegisterSearchFunctions(db);

  connector::RegisterVectorFunctions(db);

  connector::RegisterEmbeddingFunctions(db);

  connector::RegisterSereneDBOptimizers(db);

  // Register SereneDB index types.
  // These provide create_plan callbacks that bypass DuckDB's native
  // PhysicalCreateIndex (which requires DuckTableEntry).
  auto& index_types = db.config.GetIndexTypes();
  for (auto& name : {"secondary", "btree", "inverted"}) {
    index_types.RegisterIndexType({
      .name = name,
      .create_plan = &connector::SereneDBCreateIndexPlan,
    });
  }

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
