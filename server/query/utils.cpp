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

#include "query/utils.h"

#include <re2/re2.h>

namespace sdb::query {

constexpr const char* MakeColumnCleanupPattern() {
  static_assert(kColumnSeparator == ":",
                "ColumnSeparator does not match ':', change the pattern");
  return R"((.*?):(\d+))";
}

std::string CleanColumnNames(std::string text) {
  RE2::GlobalReplace(&text, MakeColumnCleanupPattern(), "\\1");
  return text;
}

bool Equals(const axiom::logical_plan::Expr& lhs,
            const axiom::logical_plan::Expr& rhs) {
  return axiom::logical_plan::ExprPrinter::toText(lhs) ==
         axiom::logical_plan::ExprPrinter::toText(rhs);
}

bool Equals(const axiom::logical_plan::Expr* lhs,
            const axiom::logical_plan::Expr* rhs) {
  if (lhs && rhs) {
    return Equals(*lhs, *rhs);
  }
  return !lhs && !rhs;
}

std::string_view ToAlias(std::string_view name) {
  return name.substr(0, name.find_last_of(kColumnSeparator));
}

std::vector<std::string> ToAliases(std::span<const std::string> names) {
  std::vector<std::string> aliases;
  aliases.reserve(names.size());
  for (const auto& name : names) {
    aliases.emplace_back(ToAlias(name));
  }
  return aliases;
}

}  // namespace sdb::query
