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

#include "pg/catalog/utils/columns.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <ranges>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {

duckdb::shared_ptr<duckdb::ViewColumnInfo> ViewColumns(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view) {
  if (auto columns = view.GetColumnInfo()) {
    return columns;
  }
  try {
    view.BindView(context);
  } catch (const std::exception&) {
  }
  return view.GetColumnInfo();
}

bool HasDefault(const duckdb::ColumnDefinition& column) {
  if (!column.HasDefaultValue()) {
    return false;
  }
  const auto* expression = &column.DefaultValue();
  while (expression->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
    expression = &expression->Cast<duckdb::CastExpression>().Child();
  }
  return expression->GetExpressionClass() !=
           duckdb::ExpressionClass::CONSTANT ||
         !expression->Cast<duckdb::ConstantExpression>().GetLiteral().IsNull();
}

int16_t ViewAttnum(duckdb::ClientContext& context,
                   duckdb::ViewCatalogEntry& view,
                   const duckdb::Identifier& name) {
  const auto columns = ViewColumns(context, view);
  for (duckdb::idx_t i = 0; columns && i < columns->types.size(); ++i) {
    if (view.ColumnName(*columns, i) == name) {
      return static_cast<int16_t>(i + 1);
    }
  }
  return 0;
}

std::vector<bool> NotNullColumns(const duckdb::TableCatalogEntry& table) {
  std::vector<bool> not_null(table.GetColumns().LogicalColumnCount());
  for (const auto& constraint :
       table.GetConstraints() | std::views::filter([](const auto& constraint) {
         return constraint->type == duckdb::ConstraintType::NOT_NULL;
       })) {
    not_null[constraint->Cast<duckdb::NotNullConstraint>().index.index] = true;
  }
  return not_null;
}

namespace {

template<typename T, typename F>
std::vector<T> MapIndexExpressions(const duckdb::IndexCatalogEntry& index,
                                   F&& map) {
  std::vector<T> result;
  result.reserve(index.parsed_expressions.size());
  for (const bool included : {false, true}) {
    for (size_t i = 0; i < index.parsed_expressions.size(); ++i) {
      if ((i < index.column_opclasses.size() &&
           index.column_opclasses[i] == catalog::kIncludedKind) == included) {
        result.emplace_back(map(*index.parsed_expressions[i]));
      }
    }
  }
  return result;
}

}  // namespace

std::vector<const duckdb::ParsedExpression*> IndexExpressions(
  const duckdb::IndexCatalogEntry& index) {
  return MapIndexExpressions<const duckdb::ParsedExpression*>(
    index,
    [](const duckdb::ParsedExpression& expression) { return &expression; });
}

std::vector<int16_t> IndexAttnums(duckdb::ClientContext& context,
                                  const duckdb::IndexCatalogEntry& index,
                                  duckdb::CatalogEntry& relation) {
  auto* view = relation.type == duckdb::CatalogType::VIEW_ENTRY
                 ? &relation.Cast<duckdb::ViewCatalogEntry>()
                 : nullptr;
  const auto columns = view ? ViewColumns(context, *view) : nullptr;
  const auto attnum = [&](const duckdb::Identifier& name) -> int16_t {
    if (!view) {
      const auto& table = relation.Cast<duckdb::TableCatalogEntry>();
      return table.ColumnExists(name) ? Attnum(table.GetColumn(name)) : 0;
    }
    if (!columns) {
      return 0;
    }
    const auto it = absl::c_find(columns->names, name);
    return it == columns->names.end()
             ? 0
             : static_cast<int16_t>(it - columns->names.begin() + 1);
  };
  return MapIndexExpressions<int16_t>(
    index, [&](const duckdb::ParsedExpression& expression) -> int16_t {
      return expression.GetExpressionClass() ==
                 duckdb::ExpressionClass::COLUMN_REF
               ? attnum(expression.Cast<duckdb::ColumnRefExpression>()
                          .GetColumnName())
               : 0;
    });
}

std::vector<int16_t> ExpressionAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::ParsedExpression& expression) {
  std::vector<int16_t> attnums;
  duckdb::ParsedExpressionIterator::VisitExpression<
    duckdb::ColumnRefExpression>(
    expression, [&](const duckdb::ColumnRefExpression& column) {
      const auto& name = column.GetColumnName();
      if (!table.ColumnExists(name)) {
        return;
      }
      const auto attnum = Attnum(table.GetColumn(name));
      if (!absl::c_linear_search(attnums, attnum)) {
        attnums.emplace_back(attnum);
      }
    });
  return attnums;
}

std::string ExpressionText(const duckdb::ParsedExpression& expression) {
  auto copy = expression.Copy();
  duckdb::ParsedExpressionIterator::VisitExpressionMutable<
    duckdb::ColumnRefExpression>(*copy, [](duckdb::ColumnRefExpression& ref) {
    auto& names = ref.ColumnNamesMutable();
    if (names.size() > 1) {
      names.erase(names.begin(), names.end() - 1);
    }
  });
  return copy->ToString();
}

duckdb::optional_ptr<const duckdb::TableCatalogEntry> SiblingTable(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& member,
  const duckdb::Identifier& name) {
  auto entry = FindMember(transaction, member.ParentSchema(transaction),
                          duckdb::CatalogType::TABLE_ENTRY, name);
  if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &entry->Cast<duckdb::TableCatalogEntry>();
}

duckdb::optional_ptr<const duckdb::TableCatalogEntry> ReferencedTable(
  duckdb::CatalogTransaction transaction,
  const duckdb::TableCatalogEntry& table,
  const duckdb::ForeignKeyConstraint& fk) {
  if (fk.info.type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
    return &table;
  }
  return SiblingTable(transaction, table, fk.info.table);
}

SystemConcatText ConstraintName(const duckdb::TableCatalogEntry& table,
                                const duckdb::Constraint& constraint) {
  if (!constraint.constraint_name.empty() ||
      constraint.type != duckdb::ConstraintType::NOT_NULL) {
    return {{constraint.constraint_name}};
  }
  return {{table.name.GetIdentifierName(), "_",
           table.GetColumn(constraint.Cast<duckdb::NotNullConstraint>().index)
             .Name()
             .GetIdentifierName(),
           "_not_null"}};
}

bool NumbersRows(const duckdb::CatalogEntry& sequence) {
  return sequence.tags.contains(catalog::kGeneratedPkSequenceTag);
}

std::optional<std::string_view> NextvalArgument(
  const duckdb::ParsedExpression& expression) {
  if (expression.GetExpressionClass() != duckdb::ExpressionClass::FUNCTION) {
    return std::nullopt;
  }
  const auto& function = expression.Cast<duckdb::FunctionExpression>();
  const auto& arguments = function.GetArguments();
  if (function.FunctionName() != std::string_view{"nextval"} ||
      arguments.size() != 1) {
    return std::nullopt;
  }
  const auto* argument = &arguments[0].GetExpression();
  if (argument->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
    argument = &argument->Cast<duckdb::CastExpression>().Child();
  }
  if (argument->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
    return std::nullopt;
  }
  const auto& literal =
    argument->Cast<duckdb::ConstantExpression>().GetLiteral();
  if (literal.kind != duckdb::LiteralKind::STRING) {
    return std::nullopt;
  }
  return std::string_view{literal.text};
}

}  // namespace sdb::pg
