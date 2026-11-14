////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <optional>

namespace sdb::app {

void PrepareProcess(char* argv0);

// Parses the CLI (absl flags + the single positional data-dir). Free of SDB_*
// logging so it can run in main() BEFORE the DuckDB engine is brought up --
// ConfigureServerDBConfig reads flags (e.g. --cpu_threads) while sizing the
// pool, which happens during engine init.
void ParseOptions(int argc, char* argv[]);

class ServerProcess final {
 public:
  ServerProcess(int argc, char* argv[]);
  ~ServerProcess();

  ServerProcess(const ServerProcess&) = delete;
  ServerProcess& operator=(const ServerProcess&) = delete;
};

std::optional<int> RunShellCommand(int argc, char* argv[]);

void WaitForShutdown();

}  // namespace sdb::app
