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
#include "basics/serializer.h"

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
    if (columns[i].GetName() == name) {
      return i;
    }
  }
  return std::nullopt;
}

void Column::Serialize(duckdb::Serializer& sink) const {
  // The column's per-column ACL (pg_attribute.attacl analogue) rides last.
  // Columns have no independent owner -- it is the table's owner.
  basics::WriteTuple(
    sink, std::forward_as_tuple(GetId(), type, std::string{GetName()}, expr,
                                generated_type, GetPermissions().acl));
}

Column Column::Deserialize(duckdb::Deserializer& src) {
  std::tuple<ObjectId, duckdb::LogicalType, std::string,
             std::shared_ptr<ColumnExpr>, GeneratedType, Acl>
    tup;
  basics::ReadTuple(src, tup);
  auto& [id, type, name, expr, gt, acl] = tup;
  return Column{ObjectId{},      id, name,          std::move(type),
                std::move(expr), gt, std::move(acl)};
}

void CheckConstraint::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(
    sink, std::forward_as_tuple(GetId(), std::string{GetName()}, expr));
}

CheckConstraint CheckConstraint::Deserialize(duckdb::Deserializer& src) {
  std::tuple<ObjectId, std::string, std::shared_ptr<ColumnExpr>> tup;
  basics::ReadTuple(src, tup);
  auto& [id, name, expr] = tup;
  return CheckConstraint{ObjectId{}, id, name, std::move(expr)};
}

}  // namespace sdb::catalog
