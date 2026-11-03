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

#pragma once

#include <duckdb/main/database.hpp>

#include "query/engine_host.h"

namespace duckdb {

struct DBConfig;

}  // namespace duckdb
namespace sdb::server::query {

void InstallEngineHost(EngineMode mode);

// Pre-construct mutator handed to irs::DuckDBEngine::Initialize.
// Runs AFTER the lite defaults and BEFORE the duckdb::DuckDB ctor.
void ConfigureServerDBConfig(duckdb::DBConfig& config);

void RegisterEngineModules(duckdb::DatabaseInstance& db);

// Post-construct fill on a live DatabaseInstance: every connector
// function/cast registration, the SereneDBCreateIndexPlan
// index-type loop, the SereneDBCopyFileSystem subsystem install, and the
// pg::InitSystemFunctions / pg::InitSystemViews caches. Called from
// serened main() right after DuckDBEngine::Initialize().
void RegisterServerExtensions(duckdb::DatabaseInstance& db);

}  // namespace sdb::server::query
