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

#include <iostream>

#include "basics/assert.h"
#include "connector/duckdb_storage_extension.h"

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
  // Attach SereneDB as the default database
  auto conn = duckdb::make_uniq<duckdb::Connection>(*_db);
  auto r1 = conn->Query("ATTACH '' AS serenedb (TYPE serenedb)");
  if (r1->HasError()) {
    std::cerr << "DuckDB ATTACH failed: " << r1->GetError() << std::endl;
  }
  auto r2 = conn->Query("USE serenedb");
  if (r2->HasError()) {
    std::cerr << "DuckDB USE failed: " << r2->GetError() << std::endl;
  }
  std::cerr << "DuckDB engine initialized with SereneDB storage" << std::endl;
}

void DuckDBEngine::Shutdown() { _db.reset(); }

duckdb::unique_ptr<duckdb::Connection> DuckDBEngine::CreateConnection() {
  SDB_ASSERT(_db);
  auto conn = duckdb::make_uniq<duckdb::Connection>(*_db);
  auto r = conn->Query("USE serenedb");
  if (r->HasError()) {
    std::cerr << "DuckDB USE serenedb failed: " << r->GetError() << std::endl;
  }
  return conn;
}

}  // namespace sdb::query
