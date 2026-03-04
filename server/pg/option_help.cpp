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

#include "pg/option_help.h"

#include <absl/strings/str_cat.h>

namespace sdb::pg {

namespace {

void FormatGroup(std::string& out, const OptionGroup& group, int indent) {
  std::string prefix(indent, ' ');

  absl::StrAppend(&out, prefix, group.name, ":\n");

  for (const auto& opt : group.options) {
    absl::StrAppend(&out, prefix, "  ", opt.name, " (", opt.type, ")");
    if (!opt.default_value.empty()) {
      absl::StrAppend(&out, " [default: ", opt.default_value, "]");
    }
    absl::StrAppend(&out, " - ", opt.description, "\n");
  }

  for (const auto& sub : group.subgroups) {
    FormatGroup(out, sub, indent + 2);
  }

  if (group.options.empty() && group.subgroups.empty()) {
    absl::StrAppend(&out, prefix, "  (no additional options)\n");
  }
}

}  // namespace

std::string FormatHelp(std::span<const OptionGroup> groups) {
  std::string result;
  result.reserve(1024);

  for (const auto& group : groups) {
    FormatGroup(result, group, 0);
    absl::StrAppend(&result, "\n");
  }

  return result;
}

}  // namespace sdb::pg
