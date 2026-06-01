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

#include <absl/flags/flag.h>

#include <string>

ABSL_FLAG(std::string, log_storage, "stdout",
          "DuckDB log destination at process start: 'stdout' (default; "
          "container log drivers capture it), 'memory' (queryable via "
          "SELECT * FROM duckdb_logs()), or 'file://<path>'. Override at "
          "runtime with `SET logging_storage = '<value>'`.");
ABSL_FLAG(std::string, log_level, "info",
          "DuckDB minimum log severity at process start: trace | debug | "
          "info | warning | error | fatal. Override at runtime with "
          "`SET logging_level = '<value>'`.");
