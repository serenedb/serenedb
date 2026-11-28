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

#include <absl/cleanup/cleanup.h>

#include <cstdio>
#include <cstdlib>
#include <duckdb/common/error_data.hpp>
#include <exception>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>

#include "app/command.h"
#include "docs/docs_bootstrap.h"
#include "query/server_engine.h"
#include "runtime/runtime.h"
#include "storage/storage.h"

namespace sdb::app {
namespace {

int RunDocsBuilder(int argc, char* argv[]) {
  if (argc != 3) {
    std::fputs("usage: serenedb docs-index <corpus> <image>\n", stderr);
    return EXIT_FAILURE;
  }
  auto& engine = irs::DuckDBEngine::Instance();
  engine.Initialize(&server::query::ConfigureShellDBConfig);
  absl::Cleanup shutdown = [&] { engine.Shutdown(); };
  server::query::RegisterServerExtensions(engine.instance());
  try {
    Runtime runtime{ReadIoThreads()};
    Storage storage{runtime, {}};
    return docs::BuildDocsIndex(argv[1], argv[2]) ? EXIT_SUCCESS : EXIT_FAILURE;
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot build the docs index: ",
              duckdb::ErrorData{e}.RawMessage());
    return EXIT_FAILURE;
  }
}

SDB_COMMAND kDocsIndex{"docs-index", &RunDocsBuilder};

}  // namespace
}  // namespace sdb::app
