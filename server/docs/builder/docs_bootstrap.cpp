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

#include "docs/docs_bootstrap.h"

#include <absl/flags/flag.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iresearch/utils/log.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "docs/builder/docs_loader.h"
#include "utils/file_utils.h"

ABSL_FLAG(std::string, build_docs_index, "",
          "Index the documentation corpus named by --docs_corpus into this "
          "directory and exit, without starting a listener. The build runs it "
          "on the freshly linked binary and writes the index into that "
          "binary; see CONTRIBUTING.md.");

ABSL_FLAG(std::string, docs_corpus, "",
          "The documentation corpus --build_docs_index reads, as written by "
          "scripts/generate_docs.py --corpus.");

namespace sdb::docs {
namespace {

bool ParseCorpus(std::string_view data, std::vector<Doc>& docs) {
  while (!data.empty()) {
    auto& doc = docs.emplace_back();
    for (auto* field : {&doc.path, &doc.title, &doc.breadcrumb, &doc.content}) {
      std::uint32_t size;
      if (data.size() < sizeof(size)) {
        return false;
      }
      std::memcpy(&size, data.data(), sizeof(size));
      data.remove_prefix(sizeof(size));
      if (data.size() < size) {
        return false;
      }
      *field = data.substr(0, size);
      data.remove_prefix(size);
    }
  }
  return true;
}

}  // namespace

std::optional<int> RunDocsBootstrap() {
  const auto out = absl::GetFlag(FLAGS_build_docs_index);
  if (out.empty()) {
    return std::nullopt;
  }
  const auto path = absl::GetFlag(FLAGS_docs_corpus);
  std::string corpus;
  try {
    corpus = utils::file_utils::Slurp(path);
  } catch (const std::exception& e) {
    SDB_ERROR(STARTUP, "cannot read the docs corpus '", path, "': ", e.what());
    return EXIT_FAILURE;
  }
  std::vector<Doc> docs;
  if (!ParseCorpus(corpus, docs)) {
    SDB_ERROR(STARTUP, "the docs corpus '", path, "' is truncated");
    return EXIT_FAILURE;
  }
  return BuildEmbeddedIndex(out, docs) ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace sdb::docs
