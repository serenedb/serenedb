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
          "Log destination at process start. Accepted values: 'stdout' "
          "(default; systemd/container log drivers capture it), 'memory' "
          "(queryable via SELECT * FROM duckdb_logs()), or 'file' (uses "
          "--log_path). Override at runtime with "
          "`CALL enable_logging(storage='<value>', storage_path='<path>')`.");
ABSL_FLAG(std::string, log_path, "",
          "Path used by --log_storage, when it takes one. Its meaning is "
          "storage-dependent (for 'file', the log file or directory); ignored "
          "by storages that take no path, such as 'stdout' and 'memory'.");
ABSL_FLAG(std::string, log_level, "info",
          "Minimum log severity at process start: trace | debug | "
          "info | warning | error | fatal. Override at runtime with "
          "`SET logging_level = '<value>'`.");
