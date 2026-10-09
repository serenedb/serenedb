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
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

std::string RelationName(const pg::Session& session,
                         const duckdb::CatalogEntry& relation, bool qualified) {
  const auto parent = relation.ParentSchemaName();
  const auto& schema = parent.GetIdentifierName();
  const auto& name = relation.name.GetIdentifierName();
  if (qualified) {
    return pg::QualifiedOutName(schema, name);
  }
  return pg::RelationName(session, schema, name);
}

constexpr auto kQuotedName = [](std::string* out,
                                const duckdb::Identifier& name) {
  out->append(pg::QuoteIdentifier(name.GetIdentifierName()));
};

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
  return duckdb::KeywordHelper::WriteQuoted(value, '\'');
}

std::string Options(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  using Option = duckdb::case_insensitive_map_t<duckdb::Value>::value_type;
  std::vector<const Option*> shown;
  for (const auto& option : options) {
    if (option.second.type().id() != duckdb::LogicalTypeId::BLOB) {
      shown.emplace_back(&option);
    }
  }
  const auto key = [](const Option* option) {
    const std::string_view name = option->first;
    return std::pair{
      static_cast<size_t>(absl::c_find(catalog::kInvertedIndexSettings, name) -
                          catalog::kInvertedIndexSettings.begin()),
      name};
  };
  absl::c_sort(shown, [&](const Option* lhs, const Option* rhs) {
    return key(lhs) < key(rhs);
  });
  return absl::StrJoin(shown, ", ", [](std::string* out, const Option* option) {
    const auto& [name, value] = *option;
    out->append(pg::QuoteIdentifier(name));
    if (!value.IsNull()) {
      absl::StrAppend(out, "=", OptionValue(value.ToString()));
    }
  });
}

const duckdb::ViewCatalogEntry* UserView(const pg::Session& session,
                                         int64_t oid) {
  const auto* view = EntryByOid<duckdb::ViewCatalogEntry>(
    session, duckdb::CatalogType::VIEW_ENTRY, oid);
  if (!view) {
    return nullptr;
  }
  if (pg::FindSystemNamespace(view->ParentSchemaName().GetIdentifierName())) {
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
  const auto columns = [](const duckdb::TableCatalogEntry& owner,
                          std::span<const duckdb::PhysicalIndex> keys) {
    return absl::StrJoin(keys, ", ",
                         [&](std::string* out, duckdb::PhysicalIndex key) {
                           out->append(ColumnName(owner, key));
                         });
  };
  std::string pk_columns;
  std::string target_name;
  if (target) {
    pk_columns = columns(*target, fk.info.pk_keys);
    target_name = RelationName(session, *target, false);
  } else {
    pk_columns = absl::StrJoin(fk.pk_columns, ", ", kQuotedName);
    target_name = pg::QualifiedOutName(fk.info.schema.GetIdentifierName(),
                                       fk.info.table.GetIdentifierName());
  }
  return absl::StrCat("FOREIGN KEY (", columns(table, fk.info.fk_keys),
                      ") REFERENCES ", target_name, "(", pk_columns, ")");
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
  return absl::StrJoin(absl::StrSplit(opclass, '.'), ".",
                       [](std::string* out, std::string_view part) {
                         out->append(pg::QuoteIdentifier(part));
                       });
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
  } else if (const auto key =
               pg::FindKeyIndex(session, static_cast<uint64_t>(oid));
             key.table) {
    shape = ShapeOf(*key.table, *key.key_index);
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
    return absl::StrJoin(elements, ", ",
                         [](std::string* out, const IndexElement& element) {
                           out->append(element.definition);
                         });
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

Definition TriggerDefinition(const pg::Session& session, int64_t oid,
                             bool pretty) {
  const auto* found = EntryByOid<duckdb::TriggerCatalogEntry>(
    session, duckdb::CatalogType::TRIGGER_ENTRY, oid);
  if (!found) {
    return std::nullopt;
  }
  const auto& trigger = *found;
  const auto table = pg::SiblingTable(*session.transaction, trigger,
                                      trigger.base_table->Table());
  if (!table) {
    return std::nullopt;
  }
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
    absl::StrAppend(&definition, " OF ",
                    absl::StrJoin(trigger.columns, ", ", kQuotedName));
  }
  absl::StrAppend(&definition, " ON ", RelationName(session, *table, !pretty));
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
  if (oid <= 0) {
    return nullptr;
  }
  auto entry = pg::EntryByOid(session, static_cast<uint64_t>(oid));
  if (!entry || pg::CatalogClassOid(entry->type) != pg::kPgProcTable) {
    return nullptr;
  }
  const auto& macro = entry->Cast<duckdb::MacroCatalogEntry>();
  return macro.macros.empty() ? nullptr : &macro;
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
    pg::QualifiedOutName(entry->ParentSchemaName().GetIdentifierName(),
                         entry->name.GetIdentifierName()),
    "(", Arguments(session, macro, true), ")\n");
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

duckdb::optional<duckdb::string_t> Emit(duckdb::Vector& result,
                                        const Definition& definition) {
  if (!definition) {
    return duckdb::nullopt;
  }
  return duckdb::StringVector::AddString(result, *definition);
}

template<typename MacroRenderer, typename BuiltinRenderer>
duckdb::scalar_function_t FunctionInfo(MacroRenderer macro_renderer,
                                       BuiltinRenderer builtin_renderer) {
  return [macro_renderer, builtin_renderer](duckdb::DataChunk& args,
                                            duckdb::ExpressionState& state,
                                            duckdb::Vector& result) {
    const auto session = pg::MakeSession(&state.GetContext());
    std::shared_ptr<const pg::BuiltinFunctions> builtins;
    duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
      args.data[0], result, args.size(), [&](int64_t oid) {
        Definition definition;
        if (const auto* macro = FindMacro(session, oid)) {
          definition = macro_renderer(session, *macro->macros.front());
        } else if (oid > 0) {
          if (!builtins) {
            builtins = pg::GetBuiltinFunctions(state.GetContext());
          }
          if (const auto* builtin =
                builtins->Find(static_cast<duckdb::idx_t>(oid))) {
            definition = builtin_renderer(session, *builtin);
          }
        }
        return Emit(result, definition);
      });
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
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int64_t, bool>(
    args, result, [&](int64_t oid, int64_t column, bool pretty) {
      return Emit(result, IndexDefinition(session, oid, column, pretty));
    });
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
    set.AddFunction(
      duckdb::ScalarFunction{{oid}, text, OidFunction(ViewDefinition)});
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
      {oid, boolean}, text, OidArgFunction<bool>(RuleDefinition)});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_constraintdef"};
    set.AddFunction(
      duckdb::ScalarFunction{{oid}, text, OidFunction(ConstraintDefinition)});
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
      {oid, boolean}, text, OidArgFunction<bool>(TriggerDefinition)});
    Register(loader, std::move(set));
  }
  {
    duckdb::ScalarFunctionSet set{"pg_get_functiondef"};
    set.AddFunction(
      duckdb::ScalarFunction{{oid}, text, OidFunction(FunctionDefinition)});
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
      {oid, bigint}, text, OidArgFunction<int64_t>(FunctionArgDefault)});
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
