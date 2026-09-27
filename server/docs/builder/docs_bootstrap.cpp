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

#include <cstdlib>
#include <string>

#include "docs/docs_loader.h"

ABSL_FLAG(std::string, build_docs_index, "",
          "Index the embedded documentation into this directory and exit, "
          "without starting a listener. The build uses it to produce the "
          "image it then compiles in; see CONTRIBUTING.md.");

namespace sdb::docs {

std::optional<int> RunDocsBootstrap() {
  const auto out = absl::GetFlag(FLAGS_build_docs_index);
  if (out.empty()) {
    return std::nullopt;
  }
  return BuildEmbeddedIndex(out) ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace sdb::docs
