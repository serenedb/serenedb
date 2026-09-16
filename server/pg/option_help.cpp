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

#include <absl/strings/internal/damerau_levenshtein_distance.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <variant>

namespace sdb::pg {

void CheckPositiveInt(std::string_view name, int value) {
  if (value <= 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("\"", name, "\" must be a positive integer"));
  }
}

void CheckNonNegativeInt(std::string_view name, int value) {
  if (value < 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("\"", name, "\" must be a non-negative integer"));
  }
}

namespace {

void FormatGroup(std::string& out, const OptionGroup& group, int indent) {
  std::string prefix(indent, ' ');

  absl::StrAppend(&out, prefix, group.name, ":\n");

  for (const auto& opt : group.options) {
    absl::StrAppend(&out, prefix, "  ", opt.name,
                    opt.IsRequired() ? "[required]" : "", " (", opt.TypeName(),
                    ")");
    if (opt.IsRequired()) {
      absl::StrAppend(&out, " [default: none]");
    } else {
      switch (opt.type) {
        case OptionInfo::Type::String:
        case OptionInfo::Type::StringList: {
          auto str = std::get<std::string_view>(opt.default_value);
          if (!str.empty()) {
            absl::StrAppend(&out, " [default: ", str, "]");
          }
        } break;
        case OptionInfo::Type::Boolean:
          absl::StrAppend(&out, " [default: ",
                          std::get<bool>(opt.default_value) ? "true" : "false",
                          "]");
          break;
        case OptionInfo::Type::Integer:
          absl::StrAppend(&out, " [default: ", std::get<int>(opt.default_value),
                          "]");
          break;
        case OptionInfo::Type::Character: {
          char c = std::get<char>(opt.default_value);
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
          absl::StrAppend(
            &out, " [default: ", std::get<double>(opt.default_value), "]");
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

std::string FormatHelp(const OptionGroup& group) {
  std::string result;
  result.reserve(1024);
  FormatGroup(result, group, 0);
  return result;
}

std::string_view FindClosestName(std::span<const std::string_view> known_names,
                                 std::string_view name) {
  const uint8_t max_distance =
    static_cast<uint8_t>(std::min<size_t>(name.size() / 2 + 1, 3));
  std::string_view best;
  uint8_t best_distance = max_distance;

  for (const auto& known : known_names) {
    uint8_t distance = absl::strings_internal::CappedDamerauLevenshteinDistance(
      absl::string_view{name.data(), name.size()},
      absl::string_view{known.data(), known.size()}, best_distance);
    if (distance < best_distance) {
      best_distance = distance;
      best = known;
    }
  }
  return best;
}

}  // namespace sdb::pg
