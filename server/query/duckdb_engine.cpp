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

#include "query/duckdb_engine.h"

#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>
#include <iostream>

#include "basics/assert.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_search_functions.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_tokenizer_function.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/functions/array.h"
#include "connector/functions/cast.h"
#include "connector/functions/inout.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/pg_logical_types.h"
#include "pg/system_functions.h"
#include "pg/system_views.h"

// PG types not built into DuckDB.
// Types with aliases preserve them through the binder for proper cast dispatch.
// input_function: if set, used by registered casts for catalog lookups.
static const duckdb::DefaultType kExternalTypes[] = {
  // clang-format off
  // reg* types -- aliased BIGINT with input functions for catalog lookups
  {"regclass",       sdb::pg::REGCLASS(),       nullptr},
  {"regtype",        sdb::pg::REGTYPE(),        nullptr},
  {"regnamespace",   sdb::pg::REGNAMESPACE(),   nullptr},
  {"regproc",        sdb::pg::REGPROC(),        nullptr},
  {"regoper",        sdb::pg::REGOPER(),        nullptr},
  {"regoperator",    sdb::pg::REGOPERATOR(),    nullptr},
  {"regprocedure",   sdb::pg::REGPROCEDURE(),   nullptr},
  {"regrole",        sdb::pg::REGROLE(),        nullptr},
  {"regconfig",      sdb::pg::REGCONFIG(),      nullptr},
  {"regdictionary",  sdb::pg::REGDICTIONARY(),  nullptr},
  {"regcollation",   sdb::pg::REGCOLLATION(),   nullptr},
  // System identifier types -- aliased BIGINT
  {"tid",  sdb::pg::TID(),  nullptr},
  {"cid",  sdb::pg::CID(),  nullptr},
  {"xid",  sdb::pg::XID(),  nullptr},
  {"xid8", sdb::pg::XID8(), nullptr},
  // PG name type -- aliased VARCHAR
  {"name", duckdb::LogicalTypeId::VARCHAR, nullptr},
  // clang-format on
};

extern "C" const duckdb::DefaultType* duckdb_external_types(
  duckdb::idx_t* count) {
  *count = std::size(kExternalTypes);
  return kExternalTypes;
}

extern "C" const duckdb::DefaultMacro* duckdb_external_macros(
  duckdb::idx_t* count) {
  *count = std::size(sdb::pg::kExternalMacros);
  return sdb::pg::kExternalMacros;
}

extern "C" const duckdb::DefaultView* duckdb_external_views(
  duckdb::idx_t* count) {
  *count = std::size(sdb::pg::kExternalViews);
  return sdb::pg::kExternalViews;
}

namespace sdb::query {

DuckDBEngine& DuckDBEngine::Instance() {
  static DuckDBEngine gInstance;
  return gInstance;
}

void DuckDBEngine::Initialize() {
  SDB_ASSERT(!_db);
  duckdb::DBConfig config;
  config.SetOptionByName("threads", duckdb::Value::INTEGER(1));
  // PG folds unquoted identifiers to lowercase
  config.SetOptionByName("preserve_identifier_case", duckdb::Value(false));
  // Register SereneDB storage extension before creating the DB
  connector::RegisterSereneDBStorage(config);

  // Register PG-compatible settings that DuckDB doesn't have natively.
  // DuckDB already has: search_path, timezone (via icu), threads, etc.
  // We only add SereneDB/PG-specific ones that are missing.
  config.AddExtensionOption("extra_float_digits",
                            "Extra digits for float display",
                            duckdb::LogicalType::INTEGER, duckdb::Value(1));
  config.AddExtensionOption("bytea_output", "Output format for bytea",
                            duckdb::LogicalType::VARCHAR, duckdb::Value("hex"));
  config.AddExtensionOption(
    "default_transaction_isolation", "Default transaction isolation level",
    duckdb::LogicalType::VARCHAR, duckdb::Value("read committed"));
  config.AddExtensionOption(
    "transaction_isolation", "Current transaction isolation level",
    duckdb::LogicalType::VARCHAR, duckdb::Value("read committed"));
  config.AddExtensionOption("client_encoding", "Client encoding",
                            duckdb::LogicalType::VARCHAR,
                            duckdb::Value("UTF8"));
  config.AddExtensionOption("server_encoding", "Server encoding",
                            duckdb::LogicalType::VARCHAR,
                            duckdb::Value("UTF8"));
  config.AddExtensionOption("server_version", "Server version string",
                            duckdb::LogicalType::VARCHAR,
                            duckdb::Value("18.3"));
  config.AddExtensionOption("standard_conforming_strings",
                            "Standard conforming strings",
                            duckdb::LogicalType::VARCHAR, duckdb::Value("on"));
  config.AddExtensionOption("DateStyle", "Date display style",
                            duckdb::LogicalType::VARCHAR,
                            duckdb::Value("ISO, MDY"));
  config.AddExtensionOption("IntervalStyle", "Interval display style",
                            duckdb::LogicalType::VARCHAR,
                            duckdb::Value("postgres"));
  config.AddExtensionOption("application_name", "Application name",
                            duckdb::LogicalType::VARCHAR, duckdb::Value(""));
  config.AddExtensionOption("integer_datetimes", "Integer datetimes",
                            duckdb::LogicalType::VARCHAR, duckdb::Value("on"));

  _db = std::make_unique<duckdb::DuckDB>(nullptr, &config);

  connector::RegisterTokenizerPragma(*_db->instance);

  connector::RegisterPgCasts(*_db->instance);

  connector::RegisterPgSystemFunctions(*_db->instance);

  connector::RegisterPgInOutFunctions(*_db->instance);

  connector::RegisterPgStringFunctions(*_db->instance);

  connector::RegisterPgArrayFunctions(*_db->instance);

  connector::RegisterVacuumFunction(*_db->instance);

  connector::RegisterSearchFunctions(*_db->instance);

  // Register SereneDB index types.
  // These provide create_plan callbacks that bypass DuckDB's native
  // PhysicalCreateIndex (which requires DuckTableEntry).
  auto& index_types = _db->instance->config.GetIndexTypes();
  {
    duckdb::IndexType secondary;
    secondary.name = "secondary";
    secondary.create_plan = &connector::SereneDBCreateIndexPlan;
    index_types.RegisterIndexType(secondary);
  }
  {
    duckdb::IndexType btree;
    btree.name = "btree";
    btree.create_plan = &connector::SereneDBCreateIndexPlan;
    index_types.RegisterIndexType(btree);
  }
  {
    duckdb::IndexType inverted;
    inverted.name = "inverted";
    inverted.create_plan = &connector::SereneDBCreateIndexPlan;
    index_types.RegisterIndexType(inverted);
  }

  // Register filesystem for COPY FROM STDIN support.
  // Intercepts "/dev/stdin" and reads from PG CopyData messages.
  auto& fs = duckdb::FileSystem::GetFileSystem(*_db->instance);
  fs.RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  {
    size_t _;
    duckdb_external_macros(&_);
    duckdb_external_views(&_);
  }

  std::cerr << "DuckDB engine initialized with SereneDB storage" << std::endl;
}

void DuckDBEngine::Shutdown() { _db.reset(); }

duckdb::unique_ptr<duckdb::Connection> DuckDBEngine::CreateConnection() {
  SDB_ASSERT(_db);
  return duckdb::make_uniq<duckdb::Connection>(*_db);
}

}  // namespace sdb::query
