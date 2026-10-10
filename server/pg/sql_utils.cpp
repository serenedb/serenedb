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

#include "sql_utils.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>

#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>

namespace sdb::pg {
namespace {

constexpr std::string_view kQuotedKeywords[] = {
#include "pg/catalog/generated/keywords.gen.inc"
};

bool ReservedKeyword(std::string_view ident) {
  if (absl::c_binary_search(kQuotedKeywords, ident)) {
    return true;
  }
  const auto category = duckdb::KeywordHelper::KeywordCategoryType(ident);
  return category != duckdb::KeywordCategory::KEYWORD_NONE &&
         category != duckdb::KeywordCategory::KEYWORD_UNRESERVED;
}

}  // namespace

std::string QuoteIdentifier(std::string_view ident) {
  const bool safe = !ident.empty() && !absl::ascii_isdigit(ident.front()) &&
                    absl::c_all_of(ident,
                                   [](char c) {
                                     return absl::ascii_islower(c) ||
                                            absl::ascii_isdigit(c) || c == '_';
                                   }) &&
                    !ReservedKeyword(ident);
  if (safe) {
    return std::string{ident};
  }
  return duckdb::KeywordHelper::WriteQuotedAndEscaped(ident, '"');
}

std::string MacroBody(const duckdb::MacroFunction& macro) {
  if (macro.type == duckdb::MacroType::TABLE_MACRO) {
    return macro.Cast<duckdb::TableMacroFunction>().query_node->ToString();
  }
  return macro.Cast<duckdb::ScalarMacroFunction>().expression->ToString();
}

std::string MacroParameterName(const duckdb::MacroFunction& macro,
                               duckdb::idx_t index) {
  const auto& name = macro.parameters[index]
                       ->Cast<duckdb::ColumnRefExpression>()
                       .GetColumnName()
                       .GetIdentifierName();
  const bool positional =
    name.size() > 1 && name.front() == '$' &&
    absl::c_all_of(std::string_view{name}.substr(1), absl::ascii_isdigit);
  return positional ? std::string{} : name;
}

}  // namespace sdb::pg
