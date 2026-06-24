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

#include "app_server.h"

#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "basics/lifecycle.h"
#include "basics/log.h"

namespace sdb::app {

void AppServer::parseOptions(int argc, char* argv[]) {
  absl::FlagsUsageConfig usage_config;
  usage_config.contains_help_flags = [](std::string_view file) {
    return file.find("third_party/") == std::string_view::npos;
  };
  usage_config.contains_helpshort_flags = usage_config.contains_help_flags;
  usage_config.contains_helppackage_flags = usage_config.contains_help_flags;
  usage_config.normalize_filename = [](std::string_view file) -> std::string {
    struct CategoryRule {
      std::string_view file_suffix;
      std::string_view category;
    };
    static constexpr CategoryRule kRules[] = {
      {"server/rest_server/database_path_feature.cpp", "server"},
      {"server/storage_engine/search_engine.cpp", "server"},
      {"libs/basics/log_flags.cpp", "log"},
    };
    for (const auto& [suffix, category] : kRules) {
      if (file.ends_with(suffix)) {
        return std::string{category};
      }
    }
    constexpr std::string_view kMarker = "/serenedb/";
    if (auto pos = file.rfind(kMarker); pos != std::string_view::npos) {
      return std::string{file.substr(pos + kMarker.size())};
    }
    return std::string{file};
  };
  absl::SetFlagsUsageConfig(usage_config);
  absl::SetProgramUsageMessage(
    "serened -- SereneDB pg-wire server\n\n"
    "Usage:\n"
    "  serened <data-dir> [--flag=value ...]\n"
    "  serened shell [duckdb-shell args ...]\n"
    "  serened psql  [psql args ...]\n\n"
    "Flag sources (absl built-in, full definitions under --helpfull):\n"
    "  --flagfile=path1,path2     Read flags from one or more files (one\n"
    "                             '--flag=value' per line, '#' comments OK).\n"
    "  --fromenv=name1,name2      Read each listed flag from FLAGS_<name>;\n"
    "                             missing env vars are an error.\n"
    "  --tryfromenv=name1,name2   Same as --fromenv but silently skip\n"
    "                             flags whose env var is unset.\n"
    "  --undefok=name1,name2      Tolerate '--name1=...' even when no flag\n"
    "                             with that name exists (useful for shared\n"
    "                             flagfiles across binaries).\n\n"
    "Help variants:\n"
    "  --help                     This filtered banner.\n"
    "  --helpfull                 Every absl-managed flag.\n"
    "  --help=<substring>         Flags whose name or description matches.\n"
    "  --helpshort                Project-defined flags only (no absl/gtest).\n"
    "  --helppackage              Flags grouped by source-tree location.\n"
    "  --version                  Print the build banner and exit.");

  auto positionals = absl::ParseCommandLine(argc, argv);
  if (positionals.size() == 2) {
    lifecycle::SetDataDirArg(positionals[1]);
  } else if (positionals.size() > 2) {
    // Runs before the DuckDB-backed log is up (SDB_* would crash), so report
    // this CLI usage error straight to stderr, like absl's own flag errors.
    std::fputs("serened: expected at most one positional data-dir arg\n",
               stderr);
    FatalErrorExit();
  }
}

void AppServer::wait() {
  lifecycle::WaitForShutdown();
  SDB_INFO(GENERAL, "received shutdown signal, beginning shut down sequence");
}

}  // namespace sdb::app
