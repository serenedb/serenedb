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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
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
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/query_node/set_operation_node.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/common.h"
#include "connector/inverted_store_index.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/sql_utils.h"
#include "pg/types/logical.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

using Definition = std::optional<std::string>;

template<typename T>
T* TypedEntry(const Session& session, duckdb::CatalogType type, int64_t oid) {
  auto entry = EntryByOid(session, static_cast<uint64_t>(oid));
  if (!entry) {
    entry = TemporaryEntryByOid(session, static_cast<uint64_t>(oid));
  }
  return entry && entry->type == type ? &entry->Cast<T>() : nullptr;
}

std::string RelationName(const Session& session, std::string_view schema,
                         std::string_view name, bool qualified) {
  if (qualified) {
    return QualifiedOutName(schema, name);
  }
  return std::string{AsView(pg::RelationName(session, schema, name))};
}

std::string RelationName(const Session& session,
                         const duckdb::CatalogEntry& relation, bool qualified) {
  if (relation.ParentCatalog().IsTemporaryCatalog()) {
    const auto& name = relation.name.GetIdentifierName();
    return qualified ? QualifiedOutName(kTemporarySchema, name)
                     : QuoteIdentifier(name);
  }
  const auto [schema, name] = NameOf(*session.transaction, relation);
  return RelationName(session, schema, name, qualified);
}

template<typename Index>
std::string ColumnName(const duckdb::TableCatalogEntry& table, Index index) {
  return QuoteIdentifier(
    table.GetColumns().GetColumn(index).Name().GetIdentifierName());
}

std::vector<std::string> KeyColumns(const duckdb::TableCatalogEntry& table,
                                    const duckdb::UniqueConstraint& unique) {
  if (unique.HasIndex()) {
    return {ColumnName(table, unique.GetIndex())};
  }
  std::vector<std::string> columns;
  for (const auto& name : unique.GetColumnNames()) {
    columns.emplace_back(QuoteIdentifier(name.GetIdentifierName()));
  }
  return columns;
}

std::string OptionValue(std::string_view value) {
  if (!RequiresQuotes(value)) {
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
    out->append(QuoteIdentifier(name));
    if (!value.IsNull()) {
      absl::StrAppend(out, "=", OptionValue(value.ToString()));
    }
  });
}

duckdb::ViewCatalogEntry* UserView(const Session& session, int64_t oid) {
  auto* view = TypedEntry<duckdb::ViewCatalogEntry>(
    session, duckdb::CatalogType::VIEW_ENTRY, oid);
  if (!view) {
    return nullptr;
  }
  if (!view->ParentCatalog().IsTemporaryCatalog() &&
      FindSystemObject(kSystemNamespaces,
                       NameOf(*session.transaction, *view).schema)) {
    return nullptr;
  }
  return view;
}

std::string ViewQuery(const Session& session, duckdb::ViewCatalogEntry& view) {
  if (view.aliases.empty()) {
    return view.query->ToString();
  }
  auto query = view.query->node->Copy();
  auto* node = query.get();
  bool by_name = false;
  while (node->type == duckdb::QueryNodeType::SET_OPERATION_NODE) {
    const auto& set = node->Cast<duckdb::SetOperationNode>();
    by_name |= set.setop_type == duckdb::SetOperationType::UNION_BY_NAME;
    node = set.children[0].get();
  }
  auto& targets = node->Cast<duckdb::SelectNode>().select_list;
  const auto columns = ViewColumns(*session.context, view);
  if (by_name || !columns || targets.size() != columns->names.size() ||
      absl::c_any_of(targets, [](const auto& target) {
        return target->GetExpressionType() == duckdb::ExpressionType::STAR;
      })) {
    return absl::StrCat(
      "SELECT * FROM (", query->ToString(), ") AS ",
      QuoteIdentifier(view.name.GetIdentifierName()), "(",
      absl::StrJoin(view.aliases, ", ",
                    [](std::string* out, const duckdb::Identifier& alias) {
                      out->append(QuoteIdentifier(alias.GetIdentifierName()));
                    }),
      ")");
  }
  for (size_t i = 0; i < view.aliases.size(); ++i) {
    if (targets[i]->GetName().GetIdentifierName() !=
        view.aliases[i].GetIdentifierName()) {
      targets[i]->SetAlias(view.aliases[i]);
    }
  }
  return query->ToString();
}

Definition ViewDefinition(const Session& session, int64_t oid) {
  if (const auto* system = FindSystemView(oid)) {
    return absl::StrCat(system->info->query->ToString(), ";");
  }
  auto* view = UserView(session, oid);
  if (!view) {
    return std::nullopt;
  }
  return absl::StrCat(ViewQuery(session, *view), ";");
}

Definition RuleDefinition(const Session& session, int64_t oid, bool pretty) {
  std::string relation;
  std::string query;
  if (const auto* system = FindSystemView(oid)) {
    relation = RelationName(session, system->schema, system->name, !pretty);
    query = system->info->query->ToString();
  } else if (auto* view = UserView(session, oid)) {
    relation = RelationName(session, *view, !pretty);
    query = ViewQuery(session, *view);
  } else {
    return std::nullopt;
  }
  return absl::StrCat("CREATE RULE \"_RETURN\" AS\n    ON SELECT TO ", relation,
                      " DO INSTEAD  ", query, ";");
}

std::string ForeignKeyDefinition(const Session& session,
                                 const duckdb::TableCatalogEntry& table,
                                 const duckdb::ForeignKeyConstraint& fk) {
  const auto& target = ReferencedTable(*session.transaction, table, fk);
  const auto columns = [](const duckdb::TableCatalogEntry& owner,
                          std::span<const duckdb::PhysicalIndex> keys) {
    return absl::StrJoin(keys, ", ",
                         [&](std::string* out, duckdb::PhysicalIndex key) {
                           out->append(ColumnName(owner, key));
                         });
  };
  return absl::StrCat("FOREIGN KEY (", columns(table, fk.info.fk_keys),
                      ") REFERENCES ", RelationName(session, target, false),
                      "(", columns(target, fk.info.pk_keys), ")");
}

Definition ConstraintDefinition(const Session& session, int64_t oid) {
  const auto owner = OwnerTable(session, static_cast<uint64_t>(oid));
  if (!owner) {
    return std::nullopt;
  }
  const auto& table = *owner;
  const auto found = absl::c_find_if(
    table.GetConstraints(),
    [&](const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
      return constraint->oid == static_cast<duckdb::idx_t>(oid) &&
             IsPgConstraint(constraint);
    });
  if (found == table.GetConstraints().end()) {
    return std::nullopt;
  }
  const auto& constraint = **found;
  switch (constraint.type) {
    case duckdb::ConstraintType::CHECK:
      return absl::StrCat(
        "CHECK (",
        ExpressionText(*constraint.Cast<duckdb::CheckConstraint>().expression),
        ")");
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
  std::string_view name;
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
    return QuoteIdentifier(expression.Cast<duckdb::ColumnRefExpression>()
                             .GetColumnName()
                             .GetIdentifierName());
  }
  auto text = ExpressionText(expression);
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
                         out->append(QuoteIdentifier(part));
                       });
}

IndexShape ShapeOf(const Session& session,
                   const duckdb::IndexCatalogEntry& index) {
  IndexShape shape{
    .name = index.name.GetIdentifierName(),
    .unique = index.IsUnique(),
    .relation =
      index
        .GetRelation(index.catalog.IsTemporaryCatalog()
                       ? index.catalog.GetCatalogTransaction(*session.context)
                       : *session.transaction)
        .get(),
    .method = connector::IsInvertedIndex(index)
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
    shape.where = ExpressionText(*index.where_clause);
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
      IndexElement{.attribute = column, .definition = std::move(column)});
  }
  return shape;
}

Definition IndexDefinition(const Session& session, int64_t oid, int64_t column,
                           bool pretty) {
  std::optional<IndexShape> shape;
  if (const auto* index = TypedEntry<duckdb::IndexCatalogEntry>(
        session, duckdb::CatalogType::INDEX_ENTRY, oid)) {
    shape = ShapeOf(session, *index);
  } else if (const auto key = FindKeyIndex(session, static_cast<uint64_t>(oid));
             key.table) {
    shape = ShapeOf(*key.table, *key.constraint);
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
                 QuoteIdentifier(shape->name), " ON ",
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

Definition TriggerDefinition(const Session& session, int64_t oid, bool pretty) {
  const auto* found = TypedEntry<duckdb::TriggerCatalogEntry>(
    session, duckdb::CatalogType::TRIGGER_ENTRY, oid);
  if (!found) {
    return std::nullopt;
  }
  const auto& trigger = *found;
  const auto& table =
    SiblingTable(*session.transaction, trigger, trigger.base_table->Table());
  auto definition = absl::StrCat(
    "CREATE TRIGGER ", QuoteIdentifier(trigger.name.GetIdentifierName()));
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
    absl::StrAppend(
      &definition, " OF ",
      absl::StrJoin(trigger.columns, ", ",
                    [](std::string* out, const duckdb::Identifier& name) {
                      out->append(QuoteIdentifier(name.GetIdentifierName()));
                    }));
  }
  absl::StrAppend(&definition, " ON ", RelationName(session, table, !pretty));
  if (!trigger.referencing_old_table.empty() ||
      !trigger.referencing_new_table.empty()) {
    absl::StrAppend(&definition, " REFERENCING");
    if (!trigger.referencing_old_table.empty()) {
      absl::StrAppend(
        &definition, " OLD TABLE AS ",
        QuoteIdentifier(trigger.referencing_old_table.GetIdentifierName()));
    }
    if (!trigger.referencing_new_table.empty()) {
      absl::StrAppend(
        &definition, " NEW TABLE AS ",
        QuoteIdentifier(trigger.referencing_new_table.GetIdentifierName()));
    }
  }
  absl::StrAppend(
    &definition, " FOR EACH ",
    trigger.for_each == duckdb::TriggerForEach::ROW ? "ROW" : "STATEMENT", " ",
    trigger.trigger_action->ToString());
  return definition;
}

std::string_view FormatType(const Session& session,
                            const duckdb::LogicalType& type) {
  return RegOut(session, RegKind::Type, CatalogTypeOid(type));
}

const duckdb::ParsedExpression* ParameterDefault(
  const duckdb::MacroFunction& macro, std::string_view name) {
  if (name.empty()) {
    return nullptr;
  }
  const auto it = macro.default_parameters.find(duckdb::Identifier{name});
  return it == macro.default_parameters.end() ? nullptr : it->second.get();
}

std::string Arguments(const Session& session,
                      const duckdb::MacroFunction& macro, bool defaults) {
  const auto& modes = macro.parameter_modes;
  const auto count = modes.empty() ? macro.parameters.size() : modes.size();
  std::vector<std::string> arguments;
  arguments.reserve(count);
  duckdb::idx_t i = 0;
  duckdb::idx_t output = 0;
  for (duckdb::idx_t k = 0; k < count; ++k) {
    const auto mode = modes.empty() ? 'i' : modes[k];
    std::vector<std::string> parts;
    if (mode == 'o') {
      arguments.emplace_back(
        absl::StrCat("OUT ", QuoteIdentifier(macro.return_names[output]), " ",
                     FormatType(session, macro.return_types[output])));
      ++output;
      continue;
    }
    if (mode == 'b') {
      parts.emplace_back("INOUT");
      ++output;
    } else if (macro.is_procedure) {
      parts.emplace_back("IN");
    }
    const auto name = MacroParameterName(macro, i);
    if (!name.empty()) {
      parts.emplace_back(QuoteIdentifier(name));
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
    ++i;
  }
  return absl::StrJoin(arguments, ", ");
}

Definition FunctionResult(const Session& session,
                          const duckdb::MacroFunction& macro) {
  if (macro.is_procedure || macro.return_types.empty()) {
    return std::nullopt;
  }
  const bool set = macro.type == duckdb::MacroType::TABLE_MACRO;
  if (!set || macro.return_names.empty() || !macro.parameter_modes.empty()) {
    const std::string_view type =
      macro.return_types.size() > 1
        ? "record"
        : FormatType(session, macro.return_types.front());
    return set ? absl::StrCat("SETOF ", type) : std::string{type};
  }
  std::vector<std::string> columns;
  columns.reserve(macro.return_types.size());
  for (duckdb::idx_t i = 0; i < macro.return_types.size(); ++i) {
    columns.emplace_back(
      absl::StrCat(QuoteIdentifier(i < macro.return_names.size()
                                     ? std::string_view{macro.return_names[i]}
                                     : std::string_view{}),
                   " ", FormatType(session, macro.return_types[i])));
  }
  return absl::StrCat("TABLE(", absl::StrJoin(columns, ", "), ")");
}

Definition FunctionDefinition(const Session& session, int64_t oid) {
  const auto* entry = FindMacro(session, static_cast<uint64_t>(oid));
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
  const auto [schema, name] = NameOf(*session.transaction, *entry);
  auto definition = absl::StrCat("CREATE OR REPLACE ", kind, " ",
                                 QualifiedOutName(schema, name), "(",
                                 Arguments(session, macro, true), ")\n");
  if (const auto result = FunctionResult(session, macro)) {
    absl::StrAppend(&definition, " RETURNS ", *result, "\n");
  }
  absl::StrAppend(&definition, " LANGUAGE sql\nAS ", tag, MacroBody(macro), tag,
                  "\n");
  return definition;
}

Definition BuiltinArguments(const Session& session,
                            const BuiltinFunction& builtin) {
  return absl::StrJoin(builtin.argtypes, ", ",
                       [&](std::string* out, duckdb::idx_t type) {
                         out->append(RegOut(session, RegKind::Type, type));
                       });
}

Definition BuiltinResult(const Session& session,
                         const BuiltinFunction& builtin) {
  const auto type = RegOut(session, RegKind::Type, builtin.rettype);
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
    const auto& session = SessionOf(state);
    std::shared_ptr<const BuiltinFunctions> builtins;
    duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
      args.data[0], result, args.size(), [&](int64_t oid) {
        Definition definition;
        if (const auto* macro =
              FindMacro(session, static_cast<uint64_t>(oid))) {
          definition = macro_renderer(session, *macro->macros.front());
        } else if (oid > 0) {
          if (!builtins) {
            builtins = GetBuiltinFunctions(state.GetContext());
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

Definition FunctionArgDefault(const Session& session, int64_t oid,
                              int64_t argument) {
  const auto* entry = FindMacro(session, static_cast<uint64_t>(oid));
  if (!entry || argument < 1) {
    return std::nullopt;
  }
  const auto& macro = *entry->macros.front();
  auto index = static_cast<duckdb::idx_t>(argument - 1);
  if (const auto& modes = macro.parameter_modes; !modes.empty()) {
    if (index >= modes.size() || modes[index] == 'o') {
      return std::nullopt;
    }
    index = static_cast<duckdb::idx_t>(absl::c_count_if(
      std::span{modes}.first(index), [](char mode) { return mode != 'o'; }));
  }
  if (index >= macro.parameters.size()) {
    return std::nullopt;
  }
  const auto* value = ParameterDefault(macro, MacroParameterName(macro, index));
  if (!value) {
    return std::nullopt;
  }
  return value->ToString();
}

template<typename Builder>
duckdb::scalar_function_t OidFunction(Builder builder) {
  return [builder](duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
    const auto& session = SessionOf(state);
    duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
      args.data[0], result, args.size(),
      [&](int64_t oid) { return Emit(result, builder(session, oid)); });
  };
}

template<typename Arg, typename Builder>
duckdb::scalar_function_t OidArgFunction(Builder builder) {
  return [builder](duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
    const auto& session = SessionOf(state);
    duckdb::BinaryExecutor::Execute<int64_t, Arg, duckdb::string_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](int64_t oid, Arg arg) {
        return Emit(result, builder(session, oid, arg));
      });
  };
}

template<typename... Pretty>
void PgGetViewdefByName(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                    Pretty...>(
    args, result, [&](duckdb::string_t name, Pretty...) {
      const auto oid =
        *RegIn(session, RegKind::Class, connector::AsView(name), nullptr);
      return Emit(result, ViewDefinition(session, static_cast<int64_t>(oid)));
    });
}

void PgGetIndexdefColumn(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int64_t, bool>(
    args, result, [&](int64_t oid, int64_t column, bool pretty) {
      return Emit(result, IndexDefinition(session, oid, column, pretty));
    });
}

}  // namespace

void RegisterRuleutilsFunctions(duckdb::ExtensionLoader& loader) {
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  const auto oid = OID();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto bigint = duckdb::LogicalType::BIGINT;
  const auto stability = duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY;

  RegisterScalar(
    loader, pg_catalog, "pg_get_viewdef", stability, InitSession,
    {duckdb::ScalarFunction{{oid}, text, OidFunction(ViewDefinition)},
     duckdb::ScalarFunction{
       {oid, boolean},
       text,
       OidArgFunction<bool>([](const auto& session, int64_t view, bool) {
         return ViewDefinition(session, view);
       })},
     duckdb::ScalarFunction{
       {oid, bigint},
       text,
       OidArgFunction<int64_t>([](const auto& session, int64_t view, int64_t) {
         return ViewDefinition(session, view);
       })},
     duckdb::ScalarFunction{{text}, text, PgGetViewdefByName<>},
     duckdb::ScalarFunction{{text, boolean}, text, PgGetViewdefByName<bool>}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_ruledef", stability, InitSession,
    {duckdb::ScalarFunction{
       {oid}, text, OidFunction([](const auto& session, int64_t rule) {
         return RuleDefinition(session, rule, false);
       })},
     duckdb::ScalarFunction{
       {oid, boolean}, text, OidArgFunction<bool>(RuleDefinition)}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_constraintdef", stability, InitSession,
    {duckdb::ScalarFunction{{oid}, text, OidFunction(ConstraintDefinition)},
     duckdb::ScalarFunction{
       {oid, boolean},
       text,
       OidArgFunction<bool>([](const auto& session, int64_t constraint, bool) {
         return ConstraintDefinition(session, constraint);
       })}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_indexdef", stability, InitSession,
    {duckdb::ScalarFunction{
       {oid}, text, OidFunction([](const auto& session, int64_t index) {
         return IndexDefinition(session, index, 0, false);
       })},
     duckdb::ScalarFunction{
       {oid, bigint, boolean}, text, PgGetIndexdefColumn}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_triggerdef", stability, InitSession,
    {duckdb::ScalarFunction{
       {oid}, text, OidFunction([](const auto& session, int64_t trigger) {
         return TriggerDefinition(session, trigger, false);
       })},
     duckdb::ScalarFunction{
       {oid, boolean}, text, OidArgFunction<bool>(TriggerDefinition)}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_functiondef", stability, InitSession,
    {duckdb::ScalarFunction{{oid}, text, OidFunction(FunctionDefinition)}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_function_arguments", stability, InitSession,
    {duckdb::ScalarFunction{
      {oid},
      text,
      FunctionInfo(
        [](const Session& session, const duckdb::MacroFunction& macro)
          -> Definition { return Arguments(session, macro, true); },
        BuiltinArguments)}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_function_identity_arguments", stability,
    InitSession,
    {duckdb::ScalarFunction{
      {oid},
      text,
      FunctionInfo(
        [](const Session& session, const duckdb::MacroFunction& macro)
          -> Definition { return Arguments(session, macro, false); },
        BuiltinArguments)}});
  RegisterScalar(loader, pg_catalog, "pg_get_function_result", stability,
                 InitSession,
                 {duckdb::ScalarFunction{
                   {oid}, text, FunctionInfo(FunctionResult, BuiltinResult)}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_function_arg_default", stability, InitSession,
    {duckdb::ScalarFunction{
      {oid, bigint}, text, OidArgFunction<int64_t>(FunctionArgDefault)}});
  for (const auto name : {"pg_get_partkeydef", "pg_get_partition_constraintdef",
                          "pg_get_function_sqlbody", "pg_get_statisticsobjdef",
                          "pg_get_statisticsobjdef_columns"}) {
    duckdb::ScalarFunction function{{oid}, text, nullptr};
    function.SetBindExpressionCallback(BindNull);
    RegisterScalar(loader, pg_catalog, name, stability, nullptr, {function});
  }
}

}  // namespace sdb::pg
