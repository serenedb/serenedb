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
//
// Glue between the sdb::log shim and duckdb::LogManager. Installs the sink
// that the SDB_* macros dispatch through after the DuckDB instance has been
// constructed, registers the SereneDB-specific log types so they can be
// referenced by SET enabled_log_types/disabled_log_types, and seeds an
// initial LogConfig that mirrors the pre-migration behaviour (HTTP+SSL muted
// by default, everything else at INFO).

#pragma once

#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>

namespace sdb::query {

// Mutate `config.options.log_config` so logging is enabled with the level
// the user picked at startup (or the default INFO), mode = DISABLE_SELECTED
// with HTTP+SSL pre-muted, and storage = stdout. Called from
// `DuckDBEngine::Initialize()` BEFORE constructing duckdb::DuckDB.
void ConfigureLogManagerDefaults(duckdb::DBConfig& config);

// Register the 7 SereneDB log types (Startup, SSL, Storage, Search,
// IResearch, Crash; the existing duckdb HTTPLogType covers HTTP) and
// install the sink so SDB_* writes flow through duckdb::LogManager.
void InstallLogManagerSink(duckdb::DatabaseInstance& db);

// Detach the sink before destroying the DatabaseInstance so any logging
// during shutdown reverts to the stderr fallback.
void UninstallLogManagerSink() noexcept;

}  // namespace sdb::query
