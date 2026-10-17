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

#include "catalog/sort_keys.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/enums/order_type.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/column_id.h"

namespace sdb::catalog {

std::vector<SortKey> ResolveSortKeys(
  const duckdb::ColumnList& columns,
  const duckdb::vector<duckdb::OrderByNode>& orders) {
  std::vector<SortKey> keys;
  keys.reserve(orders.size());
  for (const auto& order : orders) {
    const auto& expr = *order.expression;
    if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF ||
        expr.Cast<duckdb::ColumnRefExpression>().IsQualified()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("SORTED BY takes column names, not \"", expr.ToString(), "\""));
    }
    const auto& name = expr.Cast<duckdb::ColumnRefExpression>().GetColumnName();
    if (!columns.ColumnExists(name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
        ERR_MSG("column \"", name.GetIdentifierName(), "\" does not exist"));
    }
    const auto& column = columns.GetColumn(name);
    if (column.Generated()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("SORTED BY cannot use generated column \"",
                              name.GetIdentifierName(), "\""));
    }
    if (column.Type().id() == duckdb::LogicalTypeId::VARCHAR) {
      if (const auto collation =
            duckdb::StringType::GetCollation(column.Type());
          !collation.empty()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("SORTED BY cannot use column \"", name.GetIdentifierName(),
                  "\" with collation \"", collation, "\""));
      }
    }
    const auto index = column.Logical();
    if (absl::c_any_of(keys,
                       [&](const auto& key) { return key.column == index; })) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
                      ERR_MSG("column \"", name.GetIdentifierName(),
                              "\" appears more than once in SORTED BY"));
    }
    const bool descending = order.type == duckdb::OrderType::DESCENDING;
    const bool nulls_first =
      order.null_order == duckdb::OrderByNullType::ORDER_DEFAULT
        ? descending
        : order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
    keys.push_back(
      {.column = index, .descending = descending, .nulls_first = nulls_first});
  }
  return keys;
}

duckdb::vector<duckdb::OrderByNode> SortOrdersOf(
  std::span<const SortKey> keys, const duckdb::ColumnList& columns) {
  duckdb::vector<duckdb::OrderByNode> orders;
  for (const auto& key : keys) {
    orders.emplace_back(key.descending ? duckdb::OrderType::DESCENDING
                                       : duckdb::OrderType::ASCENDING,
                        key.nulls_first ? duckdb::OrderByNullType::NULLS_FIRST
                                        : duckdb::OrderByNullType::NULLS_LAST,
                        duckdb::make_uniq<duckdb::ColumnRefExpression>(
                          columns.GetColumn(key.column).Name()));
  }
  return orders;
}

irs::SortSpec SortSpecOf(std::span<const SortKey> keys,
                         const duckdb::ColumnList& columns) {
  irs::SortSpec spec;
  for (const auto& key : keys) {
    spec.keys.push_back(
      {.column = static_cast<irs::field_id>(
         connector::TableColumnId(columns.GetColumn(key.column))),
       .descending = key.descending,
       .nulls_first = key.nulls_first});
  }
  return spec;
}

}  // namespace sdb::catalog
