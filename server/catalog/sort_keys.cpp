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
#include <iterator>

#include "connector/column_id.h"

namespace sdb::catalog {

std::vector<SortKey> ResolveSortKeys(
  const duckdb::ColumnList& columns,
  std::span<const duckdb::OrderByNode> orders) {
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
    const bool desc = order.type == duckdb::OrderType::DESCENDING;
    keys.push_back({
      .column = column.Logical(),
      .order =
        desc ? duckdb::OrderType::DESCENDING : duckdb::OrderType::ASCENDING,
      .null_order = order.null_order != duckdb::OrderByNullType::ORDER_DEFAULT
                      ? order.null_order
                    : desc ? duckdb::OrderByNullType::NULLS_FIRST
                           : duckdb::OrderByNullType::NULLS_LAST,
    });
  }
  return keys;
}

duckdb::vector<duckdb::OrderByNode> SortOrdersOf(
  std::span<const SortKey> keys, const duckdb::ColumnList& columns) {
  duckdb::vector<duckdb::OrderByNode> orders;
  orders.reserve(keys.size());
  absl::c_transform(keys, std::back_inserter(orders), [&](const SortKey& key) {
    return duckdb::OrderByNode{key.order, key.null_order,
                               duckdb::make_uniq<duckdb::ColumnRefExpression>(
                                 columns.GetColumn(key.column).Name())};
  });
  return orders;
}

irs::SortSpec SortSpecOf(std::span<const SortKey> keys,
                         const duckdb::ColumnList& columns) {
  irs::SortSpec spec;
  spec.reserve(keys.size());
  absl::c_transform(keys, std::back_inserter(spec), [&](const SortKey& key) {
    return irs::SortKey{
      .column = static_cast<irs::field_id>(
        connector::TableColumnId(columns.GetColumn(key.column))),
      .order = key.order,
      .null_order = key.null_order,
    };
  });
  return spec;
}

}  // namespace sdb::catalog
