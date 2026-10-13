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

#include <algorithm>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <vector>

namespace sdb::pg {

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         duckdb::idx_t column_id) {
  for (const auto& column : table.GetColumns().Logical()) {
    if (column.Oid() == column_id) {
      return static_cast<int16_t>(column.Logical().index + 1);
    }
  }
  return 0;
}

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint) {
  if (constraint.HasIndex()) {
    return {static_cast<int16_t>(constraint.GetIndex().index + 1)};
  }
  const auto& columns = table.GetColumns();
  std::vector<int16_t> out;
  out.reserve(constraint.GetColumnNames().size());
  for (const auto& name : constraint.GetColumnNames()) {
    // Zero is what postgres writes for a key part this relation does not list.
    out.emplace_back(
      columns.ColumnExists(name)
        ? static_cast<int16_t>(columns.GetColumn(name).Logical().index + 1)
        : 0);
  }
  return out;
}

std::string ConstraintName(const duckdb::TableCatalogEntry& table,
                           const duckdb::Constraint& constraint) {
  if (!constraint.constraint_name.empty() ||
      constraint.type != duckdb::ConstraintType::NOT_NULL) {
    return constraint.constraint_name;
  }
  return table.name.GetIdentifierName() + "_" +
         table.GetColumns()
           .GetColumn(constraint.Cast<duckdb::NotNullConstraint>().index)
           .Name()
           .GetIdentifierName() +
         "_not_null";
}

std::string QuoteIdentifier(std::string_view ident) {
  bool safe =
    !ident.empty() &&
    ((ident.front() >= 'a' && ident.front() <= 'z') || ident.front() == '_');
  for (const auto c : ident) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
      safe = false;
      break;
    }
  }
  if (safe) {
    const auto category = duckdb::KeywordHelper::KeywordCategoryType(ident);
    safe = category == duckdb::KeywordCategory::KEYWORD_NONE ||
           category == duckdb::KeywordCategory::KEYWORD_UNRESERVED;
  }
  if (safe) {
    return std::string{ident};
  }
  std::string out;
  out.reserve(ident.size() + 2);
  out += '"';
  for (const auto c : ident) {
    if (c == '"') {
      out += '"';
    }
    out += c;
  }
  out += '"';
  return out;
}

std::string QuoteLiteral(std::string_view value) {
  std::string out;
  out.reserve(value.size() + 3);
  if (value.find('\\') != std::string_view::npos) {
    out += 'E';
  }
  out += '\'';
  for (const auto c : value) {
    if (c == '\'' || c == '\\') {
      out += c;
    }
    out += c;
  }
  out += '\'';
  return out;
}

namespace {

template<typename Match>
KeyIndex FindKeyIndexIn(duckdb::ClientContext& context,
                        duckdb::SchemaCatalogEntry& schema, Match&& match) {
  KeyIndex found;
  schema.Scan(
    context, duckdb::CatalogType::TABLE_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      if (found.table || entry.type != duckdb::CatalogType::TABLE_ENTRY) {
        return;
      }
      const auto& table = entry.Cast<duckdb::TableCatalogEntry>();
      for (const auto& constraint : table.GetConstraints()) {
        if (constraint->type != duckdb::ConstraintType::UNIQUE) {
          continue;
        }
        const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (match(table, unique)) {
          found = {&table, &unique};
          return;
        }
      }
    });
  return found;
}

}  // namespace

KeyIndex FindKeyIndex(duckdb::ClientContext& context, duckdb::Catalog& database,
                      duckdb::idx_t oid) {
  KeyIndex found;
  for (auto& schema : database.GetSchemas(context)) {
    found = FindKeyIndexIn(context, schema.get(),
                           [&](const duckdb::TableCatalogEntry&,
                               const duckdb::UniqueConstraint& unique) {
                             return unique.index_oid == oid;
                           });
    if (found.table) {
      break;
    }
  }
  return found;
}

KeyIndex FindKeyIndex(duckdb::ClientContext& context,
                      duckdb::SchemaCatalogEntry& schema,
                      std::string_view name) {
  return FindKeyIndexIn(context, schema,
                        [&](const duckdb::TableCatalogEntry& table,
                            const duckdb::UniqueConstraint& unique) {
                          return ConstraintName(table, unique) == name;
                        });
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
  const bool positional = name.size() > 1 && name.front() == '$' &&
                          std::all_of(name.begin() + 1, name.end(), [](char c) {
                            return c >= '0' && c <= '9';
                          });
  return positional ? std::string{} : name;
}

}  // namespace sdb::pg
