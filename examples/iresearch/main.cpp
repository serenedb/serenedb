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

#include <absl/strings/str_format.h>

#include <cstdio>
#include <string_view>

#include "examples.h"

namespace {

struct Example {
  std::string_view name;
  int (*main)();
};

constexpr Example kExamples[] = {
  {"basic", &BasicMain},
  {"geo", &GeoMain},
  {"text_filters", &TextFiltersMain},
};

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2) {
    for (const auto& example : kExamples) {
      if (example.name == argv[1]) {
        return example.main();
      }
    }
  }
  absl::FPrintF(stderr, "usage: %s <example>\nexamples:", argv[0]);
  for (const auto& example : kExamples) {
    absl::FPrintF(stderr, " %s", example.name);
  }
  std::fputc('\n', stderr);
  return 1;
}
