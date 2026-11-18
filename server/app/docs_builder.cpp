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
#include <absl/strings/str_cat.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>

#include "app/command.h"
#include "app/server.h"
#include "docs/docs_bootstrap.h"
#include "runtime/runtime.h"
#include "storage/storage.h"

namespace sdb::app {
namespace {

int RunDocsBuilder(int argc, char* argv[]) {
  if (argc != 3) {
    std::fputs("usage: serenedb docs-index <corpus> <index-dir>\n", stderr);
    return EXIT_FAILURE;
  }
  const std::filesystem::path corpus{argv[1]};
  const std::filesystem::path index{argv[2]};
  std::error_code ec;
  const auto temp = std::filesystem::temp_directory_path(ec);
  auto directory = (temp / "serenedb-docs-XXXXXX").string();
  if (ec || ::mkdtemp(directory.data()) == nullptr) {
    std::fputs(absl::StrCat("serenedb: cannot create a data directory in ",
                            temp.string(), ": ",
                            ec ? ec.message() : std::strerror(errno), "\n")
                 .c_str(),
               stderr);
    return EXIT_FAILURE;
  }
  absl::Cleanup remove = [&] {
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
  };
  char* args[] = {argv[0], directory.data()};
  return RunWithEngine(2, args, [&] {
    Runtime runtime{ReadIoThreads()};
    Storage storage{runtime, ReadStorageConfig()};
    return docs::BuildDocsIndex(corpus, index) ? EXIT_SUCCESS : EXIT_FAILURE;
  });
}

SDB_COMMAND kDocsIndex{"docs-index", &RunDocsBuilder};

}  // namespace
}  // namespace sdb::app
