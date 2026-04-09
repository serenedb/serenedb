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
#include <duckdb/catalog/default/default_views.hpp>
#include <iostream>

#include "basics/assert.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/duckdb_search_functions.h"
#include "connector/duckdb_storage_extension.h"
#include "pg/system_functions.h"
#include "pg/system_views.h"

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
  static DuckDBEngine instance;
  return instance;
}

void DuckDBEngine::Initialize() {
  SDB_ASSERT(!_db);
  duckdb::DBConfig config;
  config.SetOptionByName("threads", duckdb::Value::INTEGER(1));
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
  config.AddExtensionOption("integer_datetimes", "Integer datetimes",
                            duckdb::LogicalType::VARCHAR, duckdb::Value("on"));

  _db = std::make_unique<duckdb::DuckDB>(nullptr, &config);

  // Register search stub functions (sdb_phrase, sdb_term_eq)
  connector::RegisterSearchFunctions(*_db->instance);

  // Register VACUUM function (CALL serenedb_vacuum(...))
  connector::RegisterVacuumFunction(*_db->instance);

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
