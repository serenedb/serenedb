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

#include "pg/catalog/functions/ruleutils.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/functions/reg_types.h"
#include "pg/catalog/lookup.h"
#include "pg/sql_utils.h"
#include "pg/types.h"

namespace sdb::connector {
namespace {

using Definition = std::optional<std::string>;

template<typename T>
const T* EntryByOid(const pg::Session& session, duckdb::CatalogType type,
                    int64_t oid) {
  if (oid <= 0) {
    return nullptr;
  }
  auto entry = pg::EntryByOid(session, static_cast<uint64_t>(oid));
  return entry && entry->type == type ? &entry->Cast<T>() : nullptr;
}

template<typename Visitor>
void VisitTables(duckdb::ClientContext& context, Visitor&& visitor) {
  std::vector<duckdb::reference<duckdb::TableCatalogEntry>> tables;
  for (auto& schema : pg::SessionCatalog(context)->GetSchemas(context)) {
    schema.get().Scan(
      context, duckdb::CatalogType::TABLE_ENTRY,
      [&](duckdb::CatalogEntry& entry) {
        if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
          tables.emplace_back(entry.Cast<duckdb::TableCatalogEntry>());
        }
      });
  }
  for (auto& table : tables) {
    visitor(table.get());
  }
}

std::string RelationName(const pg::Session& session,
                         const duckdb::CatalogEntry& relation, bool qualified) {
  const auto parent = relation.ParentSchemaName();
  const auto& schema = parent.GetIdentifierName();
  const auto& name = relation.name.GetIdentifierName();
  if (qualified) {
    return absl::StrCat(pg::QuoteIdentifier(schema), ".",
                        pg::QuoteIdentifier(name));
  }
  return pg::RelationName(session, schema, name);
}

template<typename Index>
std::string ColumnName(const duckdb::TableCatalogEntry& table, Index index) {
  return pg::QuoteIdentifier(
    table.GetColumns().GetColumn(index).Name().GetIdentifierName());
}

std::vector<std::string> KeyColumns(const duckdb::TableCatalogEntry& table,
                                    const duckdb::UniqueConstraint& unique) {
  if (unique.HasIndex()) {
    return {ColumnName(table, unique.GetIndex())};
  }
  std::vector<std::string> columns;
  for (const auto& name : unique.GetColumnNames()) {
    columns.emplace_back(pg::QuoteIdentifier(name.GetIdentifierName()));
  }
  return columns;
}

std::string OptionValue(std::string_view value) {
  if (pg::QuoteIdentifier(value) == value) {
    return std::string{value};
  }
  std::string quoted = "'";
  for (const auto c : value) {
    if (c == '\'') {
      quoted += '\'';
    }
    quoted += c;
  }
  quoted += '\'';
  return quoted;
}

std::string Options(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  std::vector<std::string> names;
  for (const auto& [name, value] : options) {
    if (value.type().id() != duckdb::LogicalTypeId::BLOB) {
      names.emplace_back(name);
    }
  }
  const auto rank = [](std::string_view name) {
    return static_cast<size_t>(
      absl::c_find(catalog::kInvertedIndexSettings, name) -
      catalog::kInvertedIndexSettings.begin());
  };
  absl::c_sort(names, [&](std::string_view lhs, std::string_view rhs) {
    return std::pair{rank(lhs), lhs} < std::pair{rank(rhs), rhs};
  });
  std::vector<std::string> rendered;
  rendered.reserve(names.size());
  for (const auto& name : names) {
    const auto& value = options.find(name)->second;
    rendered.emplace_back(value.IsNull()
                            ? pg::QuoteIdentifier(name)
                            : absl::StrCat(pg::QuoteIdentifier(name), "=",
                                           OptionValue(value.ToString())));
  }
  return absl::StrJoin(rendered, ", ");
}

const duckdb::ViewCatalogEntry* UserView(const pg::Session& session,
                                         int64_t oid) {
  const auto* view = EntryByOid<duckdb::ViewCatalogEntry>(
    session, duckdb::CatalogType::VIEW_ENTRY, oid);
  if (!view) {
    return nullptr;
  }
  const auto parent = view->ParentSchemaName();
  const auto& schema = parent.GetIdentifierName();
  if (view->internal || schema == irs::StaticStrings::kPgCatalogSchema ||
      schema == irs::StaticStrings::kInformationSchema) {
    return nullptr;
  }
  return view;
}

Definition ViewDefinition(const pg::Session& session, int64_t oid) {
  const auto* view = UserView(session, oid);
  if (!view) {
    return std::nullopt;
  }
  return absl::StrCat(view->query->ToString(), ";");
}

Definition RuleDefinition(const pg::Session& session, int64_t oid,
                          bool pretty) {
  const auto* view = UserView(session, oid);
  if (!view) {
    return std::nullopt;
  }
  return absl::StrCat("CREATE RULE \"_RETURN\" AS\n    ON SELECT TO ",
                      RelationName(session, *view, !pretty), " DO INSTEAD  ",
                      view->query->ToString(), ";");
}

std::string ForeignKeyDefinition(const pg::Session& session,
                                 const duckdb::TableCatalogEntry& table,
                                 const duckdb::ForeignKeyConstraint& fk) {
  std::vector<std::string> fk_columns;
  for (const auto key : fk.info.fk_keys) {
    fk_columns.emplace_back(ColumnName(table, key));
  }
  const duckdb::TableCatalogEntry* target = &table;
  if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
    const auto entry = duckdb::Catalog::GetEntry(
      *session.context,
      duckdb::EntryLookupInfo{
        duckdb::CatalogType::TABLE_ENTRY,
        duckdb::QualifiedName{duckdb::Identifier{}, fk.info.schema,
                              fk.info.table}},
      duckdb::OnEntryNotFound::RETURN_NULL);
    target = entry && entry->type == duckdb::CatalogType::TABLE_ENTRY
               ? &entry->Cast<duckdb::TableCatalogEntry>()
               : nullptr;
  }
  std::vector<std::string> pk_columns;
  std::string target_name;
  if (target) {
    for (const auto key : fk.info.pk_keys) {
      pk_columns.emplace_back(ColumnName(*target, key));
    }
    target_name = RelationName(session, *target, false);
  } else {
    for (const auto& name : fk.pk_columns) {
      pk_columns.emplace_back(pg::QuoteIdentifier(name.GetIdentifierName()));
    }
    target_name =
      absl::StrCat(pg::QuoteIdentifier(fk.info.schema.GetIdentifierName()), ".",
                   pg::QuoteIdentifier(fk.info.table.GetIdentifierName()));
  }
  return absl::StrCat("FOREIGN KEY (", absl::StrJoin(fk_columns, ", "),
                      ") REFERENCES ", target_name, "(",
                      absl::StrJoin(pk_columns, ", "), ")");
}

Definition ConstraintDefinition(const pg::Session& session, int64_t oid) {
  if (oid <= 0) {
    return std::nullopt;
  }
  const auto owner = pg::OwnerTable(session, static_cast<uint64_t>(oid));
  if (!owner) {
    return std::nullopt;
  }
  const auto& table = *owner;
  const auto found = absl::c_find_if(
    table.GetConstraints(),
    [&](const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
      return constraint->oid == static_cast<duckdb::idx_t>(oid) &&
             pg::IsPgConstraint(constraint);
    });
  if (found == table.GetConstraints().end()) {
    return std::nullopt;
  }
  const auto& constraint = **found;
  switch (constraint.type) {
    case duckdb::ConstraintType::CHECK:
      return absl::StrCat(
        "CHECK (",
        constraint.Cast<duckdb::CheckConstraint>().expression->ToString(), ")");
    case duckdb::ConstraintType::NOT_NULL:
      return absl::StrCat(
        "NOT NULL ",
        ColumnName(table, constraint.Cast<duckdb::NotNullConstraint>().index));
    case duckdb::ConstraintType::UNIQUE: {
      const auto& unique = constraint.Cast<duckdb::UniqueConstraint>();
      return absl::StrCat(unique.IsPrimaryKey() ? "PRIMARY KEY (" : "UNIQUE (",
                          absl::StrJoin(KeyColumns(table, unique), ", "), ")");
    }
    case duckdb::ConstraintType::FOREIGN_KEY:
      return ForeignKeyDefinition(
        session, table, constraint.Cast<duckdb::ForeignKeyConstraint>());
    case duckdb::ConstraintType::INVALID:
      return std::nullopt;
  }
}

struct IndexElement {
  std::string attribute;
  std::string definition;
};

struct IndexShape {
  std::string name;
  bool unique = false;
  const duckdb::CatalogEntry* relation = nullptr;
  std::string_view method;
  std::vector<IndexElement> keys;
  std::vector<IndexElement> includes;
  std::string options;
  std::string where;
};

std::string IndexAttribute(const duckdb::ParsedExpression& expression) {
  if (expression.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    return pg::QuoteIdentifier(expression.Cast<duckdb::ColumnRefExpression>()
                                 .GetColumnName()
                                 .GetIdentifierName());
  }
  auto text = pg::ExpressionText(expression);
  const bool function_like =
    expression.GetExpressionType() ==
      duckdb::ExpressionType::OPERATOR_COALESCE ||
    (expression.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION &&
     !expression.Cast<duckdb::FunctionExpression>().IsOperator());
  if (function_like) {
    return text;
  }
  return absl::StrCat("(", text, ")");
}

std::string OpclassName(std::string_view opclass) {
  std::vector<std::string> parts;
  for (const auto part : absl::StrSplit(opclass, '.')) {
    parts.emplace_back(pg::QuoteIdentifier(part));
  }
  return absl::StrJoin(parts, ".");
}

IndexShape ShapeOf(const pg::Session& session,
                   const duckdb::IndexCatalogEntry& index) {
  IndexShape shape{
    .name = index.name.GetIdentifierName(),
    .unique = index.IsUnique(),
    .relation = index.GetRelation(*session.transaction).get(),
    .method = dynamic_cast<const catalog::InvertedIndexEntry*>(&index)
                ? std::string_view{catalog::kInvertedIndexTypeName}
                : std::string_view{"secondary"},
    .options = Options(index.options),
  };
  for (duckdb::idx_t i = 0; i < index.parsed_expressions.size(); ++i) {
    const std::string_view opclass =
      i < index.column_opclasses.size()
        ? std::string_view{index.column_opclasses[i]}
        : std::string_view{};
    const auto* options =
      i < index.column_opclass_options.size() && index.column_opclass_options[i]
        ? &*index.column_opclass_options[i]
        : nullptr;
    const bool has_options = options && !options->empty();
    const bool included = opclass == catalog::kIncludedKind;
    IndexElement element;
    element.attribute = IndexAttribute(*index.parsed_expressions[i]);
    element.definition = element.attribute;
    if (!opclass.empty() && (!included || has_options)) {
      absl::StrAppend(&element.definition, " ", OpclassName(opclass));
    }
    if (has_options) {
      absl::StrAppend(&element.definition, " (", Options(*options), ")");
    }
    (included ? shape.includes : shape.keys).emplace_back(std::move(element));
  }
  if (index.where_clause) {
    shape.where = pg::ExpressionText(*index.where_clause);
  }
  return shape;
}

IndexShape ShapeOf(const duckdb::TableCatalogEntry& table,
                   const duckdb::UniqueConstraint& key) {
  IndexShape shape{
    .name = key.constraint_name,
    .unique = true,
    .relation = &table,
    .method = "secondary",
  };
  for (auto& column : KeyColumns(table, key)) {
    shape.keys.emplace_back(
      IndexElement{.attribute = column, .definition = column});
  }
  return shape;
}

Definition IndexDefinition(const pg::Session& session, int64_t oid,
                           int64_t column, bool pretty) {
  std::optional<IndexShape> shape;
  if (const auto* index = EntryByOid<duckdb::IndexCatalogEntry>(
        session, duckdb::CatalogType::INDEX_ENTRY, oid)) {
    shape = ShapeOf(session, *index);
  } else if (auto table = pg::OwnerTable(session, static_cast<uint64_t>(oid))) {
    for (const auto& key : pg::KeyIndexes(*table)) {
      if (key.index_oid == static_cast<duckdb::idx_t>(oid)) {
        shape = ShapeOf(*table, key);
        break;
      }
    }
  }
  if (!shape || !shape->relation) {
    return std::nullopt;
  }
  if (column != 0) {
    if (column < 0) {
      return std::string{};
    }
    auto position = static_cast<size_t>(column - 1);
    if (position < shape->keys.size()) {
      return shape->keys[position].attribute;
    }
    position -= shape->keys.size();
    if (position < shape->includes.size()) {
      return shape->includes[position].attribute;
    }
    return std::string{};
  }
  const auto definitions = [](const std::vector<IndexElement>& elements) {
    std::vector<std::string_view> out;
    out.reserve(elements.size());
    for (const auto& element : elements) {
      out.emplace_back(element.definition);
    }
    return absl::StrJoin(out, ", ");
  };
  auto definition =
    absl::StrCat("CREATE ", shape->unique ? "UNIQUE " : "", "INDEX ",
                 pg::QuoteIdentifier(shape->name), " ON ",
                 RelationName(session, *shape->relation, !pretty), " USING ",
                 shape->method, " (", definitions(shape->keys), ")");
  if (!shape->includes.empty()) {
    absl::StrAppend(&definition, " INCLUDE (", definitions(shape->includes),
                    ")");
  }
  if (!shape->options.empty()) {
    absl::StrAppend(&definition, " WITH (", shape->options, ")");
  }
  if (!shape->where.empty()) {
    absl::StrAppend(&definition, " WHERE ", shape->where);
  }
  return definition;
}

struct FoundTrigger {
  duckdb::TableCatalogEntry* table = nullptr;
  const duckdb::TriggerCatalogEntry* trigger = nullptr;
};

FoundTrigger FindTrigger(const pg::Session& session, int64_t oid) {
  FoundTrigger found;
  if (oid <= 0) {
    return found;
  }
  VisitTables(*session.context, [&](duckdb::TableCatalogEntry& table) {
    if (found.table) {
      return;
    }
    table.ScanTriggers(
      *session.transaction, [&](duckdb::CatalogEntry& trigger) {
        if (!found.table && trigger.oid == static_cast<duckdb::idx_t>(oid)) {
          found = {&table, &trigger.Cast<duckdb::TriggerCatalogEntry>()};
        }
      });
  });
  return found;
}

Definition TriggerDefinition(const pg::Session& session, int64_t oid,
                             bool pretty) {
  const auto found = FindTrigger(session, oid);
  if (!found.table) {
    return std::nullopt;
  }
  const auto& trigger = *found.trigger;
  auto definition = absl::StrCat(
    "CREATE TRIGGER ", pg::QuoteIdentifier(trigger.name.GetIdentifierName()));
  switch (trigger.timing) {
    case duckdb::TriggerTiming::BEFORE:
      absl::StrAppend(&definition, " BEFORE");
      break;
    case duckdb::TriggerTiming::AFTER:
      absl::StrAppend(&definition, " AFTER");
      break;
    case duckdb::TriggerTiming::INSTEAD_OF:
      absl::StrAppend(&definition, " INSTEAD OF");
      break;
  }
  switch (trigger.event_type) {
    case duckdb::TriggerEventType::INSERT_EVENT:
      absl::StrAppend(&definition, " INSERT");
      break;
    case duckdb::TriggerEventType::DELETE_EVENT:
      absl::StrAppend(&definition, " DELETE");
      break;
    case duckdb::TriggerEventType::UPDATE_EVENT:
      absl::StrAppend(&definition, " UPDATE");
      break;
  }
  if (!trigger.columns.empty()) {
    std::vector<std::string> columns;
    for (const auto& column : trigger.columns) {
      columns.emplace_back(pg::QuoteIdentifier(column.GetIdentifierName()));
    }
    absl::StrAppend(&definition, " OF ", absl::StrJoin(columns, ", "));
  }
  absl::StrAppend(&definition, " ON ",
                  RelationName(session, *found.table, !pretty));
  if (!trigger.referencing_old_table.empty() ||
      !trigger.referencing_new_table.empty()) {
    absl::StrAppend(&definition, " REFERENCING");
    if (!trigger.referencing_old_table.empty()) {
      absl::StrAppend(
        &definition, " OLD TABLE AS ",
        pg::QuoteIdentifier(trigger.referencing_old_table.GetIdentifierName()));
    }
    if (!trigger.referencing_new_table.empty()) {
      absl::StrAppend(
        &definition, " NEW TABLE AS ",
        pg::QuoteIdentifier(trigger.referencing_new_table.GetIdentifierName()));
    }
  }
  absl::StrAppend(
    &definition, " FOR EACH ",
    trigger.for_each == duckdb::TriggerForEach::ROW ? "ROW" : "STATEMENT", " ",
    trigger.trigger_action->ToString());
  return definition;
}

const duckdb::MacroCatalogEntry* FindMacro(const pg::Session& session,
                                           int64_t oid) {
  for (const auto type : {duckdb::CatalogType::MACRO_ENTRY,
                          duckdb::CatalogType::TABLE_MACRO_ENTRY}) {
    if (const auto* entry =
          EntryByOid<duckdb::MacroCatalogEntry>(session, type, oid)) {
      return entry->macros.empty() ? nullptr : entry;
    }
  }
  return nullptr;
}

std::string FormatType(const pg::Session& session,
                       const duckdb::LogicalType& type) {
  return std::string{
    pg::RegOut<pg::RegKind::Type>(session, pg::Type2Oid(type))};
}

const duckdb::ParsedExpression* ParameterDefault(
  const duckdb::MacroFunction& macro, std::string_view name) {
  if (name.empty()) {
    return nullptr;
  }
  const auto it = macro.default_parameters.find(duckdb::Identifier{name});
  return it == macro.default_parameters.end() ? nullptr : it->second.get();
}

std::string Arguments(const pg::Session& session,
                      const duckdb::MacroFunction& macro, bool defaults) {
  std::vector<std::string> arguments;
  arguments.reserve(macro.parameters.size());
  for (duckdb::idx_t i = 0; i < macro.parameters.size(); ++i) {
    std::vector<std::string> parts;
    if (macro.is_procedure) {
      parts.emplace_back("IN");
    }
    const auto name = pg::MacroParameterName(macro, i);
    if (!name.empty()) {
      parts.emplace_back(pg::QuoteIdentifier(name));
    }
    if (i < macro.types.size() &&
        macro.types[i].id() != duckdb::LogicalTypeId::UNKNOWN) {
      parts.emplace_back(FormatType(session, macro.types[i]));
    }
    if (const auto* value =
          defaults ? ParameterDefault(macro, name) : nullptr) {
      parts.emplace_back(absl::StrCat("DEFAULT ", value->ToString()));
    }
    arguments.emplace_back(absl::StrJoin(parts, " "));
  }
  return absl::StrJoin(arguments, ", ");
}

Definition FunctionResult(const pg::Session& session,
                          const duckdb::MacroFunction& macro) {
  if (macro.is_procedure || macro.return_types.empty()) {
    return std::nullopt;
  }
  if (macro.type != duckdb::MacroType::TABLE_MACRO) {
    return FormatType(session, macro.return_types.front());
  }
  if (macro.return_names.empty()) {
    return absl::StrCat("SETOF ",
                        FormatType(session, macro.return_types.front()));
  }
  std::vector<std::string> columns;
  columns.reserve(macro.return_types.size());
  for (duckdb::idx_t i = 0; i < macro.return_types.size(); ++i) {
    columns.emplace_back(absl::StrCat(
      pg::QuoteIdentifier(i < macro.return_names.size() ? macro.return_names[i]
                                                        : std::string{}),
      " ", FormatType(session, macro.return_types[i])));
  }
  return absl::StrCat("TABLE(", absl::StrJoin(columns, ", "), ")");
}

Definition FunctionDefinition(const pg::Session& session, int64_t oid) {
  const auto* entry = FindMacro(session, oid);
  if (!entry) {
    return std::nullopt;
  }
  const auto& macro = *entry->macros.front();
  const bool typed =
    absl::c_none_of(macro.types, [](const duckdb::LogicalType& type) {
      return type.id() == duckdb::LogicalTypeId::UNKNOWN;
    });
  if (!typed || (!macro.is_procedure && macro.return_types.empty())) {
    return entry->ToSQL();
  }
  const std::string_view kind = macro.is_procedure ? "PROCEDURE" : "FUNCTION";
  const std::string_view tag =
    macro.is_procedure ? "$procedure$" : "$function$";
  auto definition = absl::StrCat(
    "CREATE OR REPLACE ", kind, " ",
    pg::QuoteIdentifier(entry->ParentSchemaName().GetIdentifierName()), ".",
    pg::QuoteIdentifier(entry->name.GetIdentifierName()), "(",
    Arguments(session, macro, true), ")\n");
  if (const auto result = FunctionResult(session, macro)) {
    absl::StrAppend(&definition, " RETURNS ", *result, "\n");
  }
  absl::StrAppend(&definition, " LANGUAGE sql\nAS ", tag, pg::MacroBody(macro),
                  tag, "\n");
  return definition;
}

Definition BuiltinArguments(const pg::Session& session,
                            const pg::BuiltinFunction& builtin) {
  return absl::StrJoin(
    builtin.argtypes, ", ", [&](std::string* out, duckdb::idx_t type) {
      out->append(pg::RegOut<pg::RegKind::Type>(session, type));
    });
}

Definition BuiltinResult(const pg::Session& session,
                         const pg::BuiltinFunction& builtin) {
  const auto type = pg::RegOut<pg::RegKind::Type>(session, builtin.rettype);
  return builtin.retset ? absl::StrCat("SETOF ", type) : std::string{type};
}

template<typename MacroRenderer, typename BuiltinRenderer>
duckdb::scalar_function_t FunctionInfo(MacroRenderer macro_renderer,
                                       BuiltinRenderer builtin_renderer) {
  return [macro_renderer, builtin_renderer](duckdb::DataChunk& args,
                                            duckdb::ExpressionState& state,
                                            duckdb::Vector& result) {
    const auto session = pg::MakeSession(&state.GetContext());
    const auto oids = args.data[0].Values<int64_t>();
    std::vector<const duckdb::MacroCatalogEntry*> macros(args.size());
    std::shared_ptr<const pg::BuiltinFunctions> builtins;
    for (duckdb::idx_t row = 0; row < args.size(); ++row) {
      const auto oid = oids[row];
      if (!oid.IsValid() || oid.GetValue() <= 0) {
        continue;
      }
      macros[row] = FindMacro(session, oid.GetValue());
      if (!macros[row] && !builtins) {
        builtins = pg::GetBuiltinFunctions(state.GetContext());
      }
    }
    auto writer =
      duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
    for (duckdb::idx_t row = 0; row < args.size(); ++row) {
      const auto oid = oids[row];
      Definition definition;
      if (macros[row]) {
        definition = macro_renderer(session, *macros[row]->macros.front());
      } else if (oid.IsValid() && oid.GetValue() > 0) {
        if (const auto* builtin =
              builtins->Find(static_cast<duckdb::idx_t>(oid.GetValue()))) {
          definition = builtin_renderer(session, *builtin);
        }
      }
      if (!definition) {
        writer.WriteNull();
        continue;
      }
      writer.WriteValue(duckdb::string_t{*definition});
    }
  };
}

Definition FunctionArgDefault(const pg::Session& session, int64_t oid,
                              int64_t argument) {
  const auto* entry = FindMacro(session, oid);
  if (!entry || argument < 1) {
    return std::nullopt;
  }
  const auto& macro = *entry->macros.front();
  const auto index = static_cast<duckdb::idx_t>(argument - 1);
  if (index >= macro.parameters.size()) {
    return std::nullopt;
  }
  const auto* value =
    ParameterDefault(macro, pg::MacroParameterName(macro, index));
  if (!value) {
    return std::nullopt;
  }
  return value->ToString();
}

duckdb::optional<duckdb::string_t> Emit(duckdb::Vector& result,
                                        const Definition& definition) {
  if (!definition) {
    return duckdb::nullopt;
  }
  return duckdb::StringVector::AddString(result, *definition);
}

template<typename Builder>
duckdb::scalar_function_t OidFunction(Builder builder) {
  return [builder](duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
    const auto session = pg::MakeSession(&state.GetContext());
    duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
      args.data[0], result, args.size(),
      [&](int64_t oid) { return Emit(result, builder(session, oid)); });
  };
}

template<typename Arg, typename Builder>
duckdb::scalar_function_t OidArgFunction(Builder builder) {
  return [builder](duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
    const auto session = pg::MakeSession(&state.GetContext());
    duckdb::BinaryExecutor::Execute<int64_t, Arg, duckdb::string_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](int64_t oid, Arg arg) {
        return Emit(result, builder(session, oid, arg));
      });
  };
}

uint64_t ViewOid(duckdb::ClientContext& context, duckdb::string_t name) {
  const auto oid = pg::ResolveRelation(
    context, duckdb::QualifiedName::Parse(name.GetString()));
  if (oid == pg::kInvalidOid) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("relation \"", name.GetString(), "\" does not exist"));
  }
  return oid;
}

void PgGetViewdefByName(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t name) {
      return Emit(result,
                  ViewDefinition(session, ViewOid(*session.context, name)));
    });
}

void PgGetViewdefByNamePretty(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::BinaryExecutor::Execute<duckdb::string_t, bool, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t name, bool) {
      return Emit(result,
                  ViewDefinition(session, ViewOid(*session.context, name)));
    });
}

void PgGetIndexdefColumn(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  const auto columns = args.data[1].Values<int64_t>();
  const auto prettys = args.data[2].Values<bool>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    const auto column = columns[row];
    const auto pretty = prettys[row];
    if (!oid.IsValid() || !column.IsValid() || !pretty.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto definition = IndexDefinition(
      session, oid.GetValue(), column.GetValue(), pretty.GetValue());
    if (!definition) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue(duckdb::string_t{*definition});
  }
}

void NullResult(duckdb::DataChunk&, duckdb::ExpressionState&,
                duckdb::Vector& result) {
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::SetNull(result, true);
}

void Register(duckdb::ExtensionLoader& loader, duckdb::ScalarFunctionSet set) {
  duckdb::CreateScalarFunctionInfo info{std::move(set)};
  info.SetSchema("pg_catalog");
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

}  // namespace

void RegisterRuleutilsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto oid = pg::OID();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto bigint = duckdb::LogicalType::BIGINT;

  {
    duckdb::ScalarFunctionSet set{"pg_get_viewdef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t view) {
        return ViewDefinition(session, view);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, boolean},
      text,
      OidArgFunction<bool>([](const auto& session, int64_t view, bool) {
        return ViewDefinition(session, view);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, bigint},
      text,
      OidArgFunction<int64_t>([](const auto& session, int64_t view, int64_t) {
        return ViewDefinition(session, view);
      })});
    set.AddFunction(duckdb::ScalarFunction{{text}, text, PgGetViewdefByName});
    set.AddFunction(
      duckdb::ScalarFunction{{text, boolean}, text, PgGetViewdefByNamePretty});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_ruledef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t rule) {
        return RuleDefinition(session, rule, false);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, boolean},
      text,
      OidArgFunction<bool>([](const auto& session, int64_t rule, bool pretty) {
        return RuleDefinition(session, rule, pretty);
      })});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_constraintdef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t constraint) {
        return ConstraintDefinition(session, constraint);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, boolean},
      text,
      OidArgFunction<bool>([](const auto& session, int64_t constraint, bool) {
        return ConstraintDefinition(session, constraint);
      })});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_indexdef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t index) {
        return IndexDefinition(session, index, 0, false);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, bigint, boolean}, text, PgGetIndexdefColumn});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_triggerdef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t trigger) {
        return TriggerDefinition(session, trigger, false);
      })});
    set.AddFunction(duckdb::ScalarFunction{
      {oid, boolean},
      text,
      OidArgFunction<bool>(
        [](const auto& session, int64_t trigger, bool pretty) {
          return TriggerDefinition(session, trigger, pretty);
        })});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_functiondef"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, OidFunction([](const auto& session, int64_t function) {
        return FunctionDefinition(session, function);
      })});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_function_arguments"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid},
      text,
      FunctionInfo(
        [](const pg::Session& session, const duckdb::MacroFunction& macro)
          -> Definition { return Arguments(session, macro, true); },
        BuiltinArguments)});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_function_identity_arguments"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid},
      text,
      FunctionInfo(
        [](const pg::Session& session, const duckdb::MacroFunction& macro)
          -> Definition { return Arguments(session, macro, false); },
        BuiltinArguments)});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_function_result"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, text, FunctionInfo(FunctionResult, BuiltinResult)});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_function_arg_default"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid, bigint},
      text,
      OidArgFunction<int64_t>(
        [](const auto& session, int64_t function, int64_t argument) {
          return FunctionArgDefault(session, function, argument);
        })});
    Register(loader, std::move(set));
  }
  for (const auto* name : {"pg_get_partkeydef", "pg_get_statisticsobjdef",
                           "pg_get_statisticsobjdef_columns"}) {
    duckdb::ScalarFunctionSet set{name};
    set.AddFunction(duckdb::ScalarFunction{{oid}, text, NullResult});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_statisticsobjdef_expressions"};
    set.AddFunction(duckdb::ScalarFunction{
      {oid}, duckdb::LogicalType::LIST(text), NullResult});
    Register(loader, std::move(set));
  }
}

}  // namespace sdb::connector
