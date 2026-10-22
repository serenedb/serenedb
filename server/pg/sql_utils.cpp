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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "pg/pg_types.h"
#include "pg/system_catalog.h"

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

namespace {

// Tables and views share one duckdb set, so three by-id lookups answer for
// every relation kind.
constexpr std::array kRelationSets{duckdb::CatalogType::TABLE_ENTRY,
                                   duckdb::CatalogType::SEQUENCE_ENTRY,
                                   duckdb::CatalogType::INDEX_ENTRY};

duckdb::optional_ptr<duckdb::DuckCatalog> TemporaryCatalog(
  duckdb::ClientContext& context) {
  auto temporary =
    duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier{TEMP_CATALOG});
  if (!temporary || !temporary->IsDuckCatalog()) {
    return nullptr;
  }
  return temporary->Cast<duckdb::DuckCatalog>();
}

duckdb::CatalogEntry* SystemRelationEntry(duckdb::ClientContext& context,
                                          std::string_view schema,
                                          std::string_view name) {
  auto database = catalog::DefaultDuckCatalog(context);
  if (!database) {
    return nullptr;
  }
  return database
    ->GetEntry(context, duckdb::CatalogType::TABLE_ENTRY,
               duckdb::Identifier{schema}, duckdb::Identifier{name},
               duckdb::OnEntryNotFound::RETURN_NULL)
    .get();
}

}  // namespace

Relation RelationByName(duckdb::ClientContext& context,
                        const duckdb::QualifiedName& name) {
  // Every half of the relation namespace, in the order postgres resolves them
  // -- a table and a view share duckdb's set, so the first lookup covers both.
  for (const auto type : kRelationSets) {
    if (auto entry = duckdb::Catalog::GetEntry(
          context, duckdb::EntryLookupInfo{type, name},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      return {.entry = entry.get()};
    }
  }
  const auto& relation = name.Name().GetIdentifierName();
  const auto key_index = [&](const duckdb::Identifier& catalog,
                             const duckdb::Identifier& schema) {
    auto entry = duckdb::Catalog::GetSchema(
      context, catalog, schema, duckdb::OnEntryNotFound::RETURN_NULL);
    return entry ? FindKeyIndex(context, *entry, relation) : KeyIndex{};
  };
  if (!name.Schema().empty()) {
    if (const auto key = key_index(name.Catalog(), name.Schema()); key.table) {
      return {.key = key};
    }
  } else {
    for (const auto& path :
         duckdb::ClientData::Get(context).catalog_search_path->Get()) {
      if (const auto key = key_index(path.GetCatalog(), path.GetSchema());
          key.table) {
        return {.key = key};
      }
    }
  }
  if (!name.Catalog().empty()) {
    return {};
  }
  const auto& schema = name.Schema().GetIdentifierName();
  if (schema.empty() || schema == irs::StaticStrings::kPgCatalogSchema) {
    return {.entry = SystemRelationEntry(
              context, irs::StaticStrings::kPgCatalogSchema, relation)};
  }
  if (schema == irs::StaticStrings::kInformationSchema) {
    return {.entry = SystemRelationEntry(
              context, irs::StaticStrings::kInformationSchema, relation)};
  }
  return {};
}

IndexColumns ColumnsOfIndex(const duckdb::IndexCatalogEntry& index,
                            const duckdb::TableCatalogEntry* table) {
  IndexColumns result;
  const auto& expressions = index.parsed_expressions;
  const auto included = [&](duckdb::idx_t position) {
    return position < index.column_opclasses.size() &&
           index.column_opclasses[position] == catalog::kIncludedKind;
  };
  const auto add = [&](bool include) {
    for (duckdb::idx_t position = 0; position < expressions.size();
         ++position) {
      if (included(position) != include) {
        continue;
      }
      const auto& expression = *expressions[position];
      auto name = expression.GetColumnName();
      if (name == duckdb::BaseExpression::ANONYMOUS_COLUMN_NAME) {
        name = "expr";
      }
      int16_t attnum = 0;
      if (table && expression.GetExpressionClass() ==
                     duckdb::ExpressionClass::COLUMN_REF) {
        const auto& column =
          expression.Cast<duckdb::ColumnRefExpression>().GetColumnName();
        if (table->GetColumns().ColumnExists(column)) {
          attnum = static_cast<int16_t>(
            table->GetColumns().GetColumn(column).Logical().index + 1);
        }
      }
      result.columns.push_back({.name = std::move(name),
                                .expression = &expression,
                                .position = position,
                                .attnum = attnum});
    }
  };
  add(false);
  result.keys = result.columns.size();
  add(true);
  for (size_t i = 0; i < result.columns.size(); ++i) {
    const auto base = result.columns[i].name;
    for (size_t suffix = 1;
         absl::c_any_of(std::span{result.columns.data(), i},
                        [&](const IndexColumn& previous) {
                          return previous.name == result.columns[i].name;
                        });
         ++suffix) {
      result.columns[i].name = absl::StrCat(base, suffix);
    }
  }
  return result;
}

IndexColumns ColumnsOfIndex(const KeyIndex& key) {
  IndexColumns result;
  const auto& columns = key.table->GetColumns();
  const auto attnums = KeyConstraintAttnums(*key.table, *key.constraint);
  for (duckdb::idx_t position = 0; position < attnums.size(); ++position) {
    const auto attnum = attnums[position];
    result.columns.push_back(
      {.name =
         attnum > 0
           ? std::string{columns.GetColumn(duckdb::LogicalIndex(attnum - 1))
                           .Name()
                           .GetIdentifierName()}
           : std::string{key.constraint->GetColumnNames()[position]
                           .GetIdentifierName()},
       .position = position,
       .attnum = attnum});
  }
  result.keys = result.columns.size();
  return result;
}

Relation RelationByOid(duckdb::ClientContext& context, duckdb::idx_t oid) {
  if (oid == kInvalidOid) {
    return {};
  }
  std::array catalogs{catalog::DefaultDuckCatalog(context),
                      TemporaryCatalog(context)};
  for (auto& searched : catalogs) {
    if (!searched) {
      continue;
    }
    for (const auto type : kRelationSets) {
      if (auto entry = searched->FindEntryById(&context, type, oid)) {
        return {.entry = entry.get()};
      }
    }
  }
  if (const auto system = FindSystemRelation(oid); !system.name.empty()) {
    return {.entry = SystemRelationEntry(context, system.schema, system.name)};
  }
  for (auto& searched : catalogs) {
    if (!searched) {
      continue;
    }
    if (const auto key = FindKeyIndex(context, *searched, oid); key.table) {
      return {.key = key};
    }
  }
  return {};
}

std::string MacroBody(const duckdb::MacroFunction& macro) {
  if (!macro.source.empty()) {
    return macro.source;
  }
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
