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

#include "app/process.h"

#include <absl/base/config.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/str_split.h>
#include <pthread.h>

#include <cstdio>
#include <cstdlib>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/utils/cpu_features.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/remap_executable.hpp>
#include <string>
#include <tuple>

#include "connector/duckdb_copy_filesystem.h"
#include "query/server_engine.h"
#include "server/utils/asio_ns.h"
#include "server/utils/build.h"
#include "server/utils/init.h"
#include "server/utils/lifecycle.h"

ABSL_FLAG(bool, remap_executable, SDB_REMAP_EXECUTABLE,
          "Copy the server's machine code into anonymous huge-page memory at "
          "startup, so the first queries after a cold start pay no code page "
          "faults and the code runs from huge pages. Off, the machine code is "
          "locked in memory instead, or pre-read where locking is not "
          "permitted, and perf can symbolize it, which it cannot for "
          "anonymous code. Read-only data is pre-read either way. On by "
          "default in Release builds, and off in the others.");

extern "C" void json_object_seed(size_t seed);

namespace sdb::app {
namespace {

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

void SetSanitizerThreadStackSize() {
#if defined(__GLIBC__) &&                  \
  (defined(ABSL_HAVE_ADDRESS_SANITIZER) || \
   defined(ABSL_HAVE_THREAD_SANITIZER) || defined(ABSL_HAVE_MEMORY_SANITIZER))
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, size_t{64} << 20);
  pthread_setattr_default_np(&attr);
  pthread_attr_destroy(&attr);
#endif
}

}  // namespace

void PrepareProcess(char* argv0) {
  irs::CheckCpuFeatures(argv0);
  SetSanitizerThreadStackSize();
  json_object_seed(0);
  irs::InitOptimizeRules();
}

ServerProcess::ServerProcess(int argc, char* argv[]) {
  // DuckDBEngine must be up BEFORE the first SDB_* fires. VPackHelper::
  // initialize() below does SDB_TRACE, so it has to run after the engine
  // is initialized. Bracket the whole server lifetime so the contract holds.
  //
  // Two-step boot: the lite Initialize (serene_base) installs the storage
  // extension via the ConfigureServerDBConfig mutator BEFORE the duckdb
  // ctor runs (storage extensions must be registered pre-construct), then
  // RegisterServerExtensions fills connector/pg/index types on the live
  // DatabaseInstance.
  //
  // CLI flags are parsed FIRST: ConfigureServerDBConfig reads --cpu_threads to
  // size the DuckDB pool at construction, so the flags must be live before
  // Initialize (ParseOptions is SDB_*-free precisely so it can run this early).
  ParseOptions(argc, argv);
  const auto remap =
    irs::RemapExecutable(absl::GetFlag(FLAGS_remap_executable));
  auto& engine = irs::DuckDBEngine::Instance();
  engine.Initialize(&server::query::ConfigureServerDBConfig);
  SDB_INFO(STARTUP, "starting SereneDB ", SERENEDB_VERSION);
  server::query::RegisterServerExtensions(engine.instance());
  duckdb::FileSystem::GetFileSystem(engine.instance())
    .RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());

  InitProcess(argv[0]);
  SDB_DEBUG(STARTUP, "executable memory: ", remap.remapped >> 20,
            " MB of machine code remapped into anonymous huge-page memory, ",
            remap.locked >> 20, " MB locked, ", remap.populated >> 20,
            " MB pre-read",
            remap.skipped.empty() ? "" : "; remap skipped: ", remap.skipped);
}

ServerProcess::~ServerProcess() { irs::DuckDBEngine::Instance().Shutdown(); }

void ParseOptions(int argc, char* argv[]) {
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
      {"server/storage/storage.cpp", "server"},
      {"server/app/process.cpp", "server"},
      {"server/network/server.cpp", "server"},
      {"server/query/server_engine.cpp", "server"},
      {"server/runtime/runtime.cpp", "server"},
      {"iresearch/utils/log_flags.cpp", "log"},
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
    irs::FatalErrorExit();
  }
#ifdef SDB_FAULT_INJECTION
  if (const char* faults = std::getenv("SDB_FAULTS")) {
    for (const auto name : absl::StrSplit(faults, ',', absl::SkipEmpty())) {
      std::ignore = duckdb::FaultPoints::Arm(std::string{name});
    }
  }
#endif
}

void WaitForShutdown() {
  lifecycle::WaitForShutdown();
  SDB_INFO(GENERAL, "received shutdown signal, beginning shut down sequence");
#ifdef SDB_FAULT_INJECTION
  SDB_PARK_ONCE_ON_FAILURE("hold_shutdown");
  duckdb::FaultPoints::Clear();
#endif
}

}  // namespace sdb::app
