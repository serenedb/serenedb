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

#include "table_options.h"

#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>

#include "basics/assert.h"

namespace sdb::catalog {

std::optional<size_t> CheckConstraint::IsNotNull(
  std::span<const Column> columns) const noexcept {
  SDB_ASSERT(expr);
  if (!expr->HasExpr()) {
    return std::nullopt;
  }
  // NOT NULL stored as OPERATOR_IS_NOT_NULL(ColumnRefExpression(col_name)).
  auto& parsed = expr->GetExpr();
  if (parsed.GetExpressionType() !=
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
    return std::nullopt;
  }
  auto& op = parsed.Cast<duckdb::OperatorExpression>();
  if (op.children.size() != 1 || op.children[0]->GetExpressionType() !=
                                   duckdb::ExpressionType::COLUMN_REF) {
    return std::nullopt;
  }
  auto name =
    op.children[0]->Cast<duckdb::ColumnRefExpression>().GetColumnName();
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].name == name) {
      return i;
    }
  }
  return std::nullopt;
}

}  // namespace sdb::catalog
