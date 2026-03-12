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
#include <absl/strings/str_join.h>

namespace sdb::pg {

namespace {

void FormatGroup(std::string& out, const OptionGroup& group, int indent) {
  std::string prefix(indent, ' ');

  absl::StrAppend(&out, prefix, group.name, ":\n");

  for (const auto& opt : group.options) {
    absl::StrAppend(&out, prefix, "  ", opt.name,
                    opt.required ? "[required]" : "", " (", opt.TypeName(),
                    ")");
    if (!opt.default_value.has_value()) {
      if (!opt.required) {
        absl::StrAppend(&out, " [default: none]");
      }
    } else {
      auto value = *opt.default_value;
      switch (opt.type) {
        case OptionInfo::Type::String: {
          auto str = std::get<std::string_view>(value);
          if (!str.empty()) {
            absl::StrAppend(&out, " [default: ", str, "]");
          }
        } break;
        case OptionInfo::Type::Boolean:
          absl::StrAppend(
            &out, " [default: ", std::get<bool>(value) ? "true" : "false", "]");
          break;
        case OptionInfo::Type::Integer:
          absl::StrAppend(&out, " [default: ", std::get<int>(value), "]");
          break;
        case OptionInfo::Type::Character: {
          char c = std::get<char>(value);
          switch (c) {
            case '\t':
              absl::StrAppend(&out, " [default: \\t]");
              break;
            case '\n':
              absl::StrAppend(&out, " [default: \\n]");
              break;
            case '\r':
              absl::StrAppend(&out, " [default: \\r]");
              break;
            case '\\':
              absl::StrAppend(&out, " [default: \\\\]");
              break;
            default:
              absl::StrAppend(&out, " [default: ", std::string_view{&c, 1},
                              "]");
              break;
          }
        } break;
        case OptionInfo::Type::Double:
          absl::StrAppend(&out, " [default: ", std::get<double>(value), "]");
          break;
        case OptionInfo::Type::Enum:
          absl::StrAppend(
            &out, " [default: ", std::get<std::string_view>(value),
            ", values: ", absl::StrJoin(opt.enum_values, ", "), "]");
          break;
      }
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

std::vector<std::string_view> AllOptionNames(
  std::span<const OptionGroup> groups) {
  std::vector<std::string_view> names;
  for (const auto& group : groups) {
    auto group_names = group.FlatNames();
    names.insert(names.end(), group_names.begin(), group_names.end());
  }
  return names;
}

}  // namespace sdb::pg
