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

#include "catalog/dependency.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstdint>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/expression/type_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/query_node.hpp>
#include <duckdb/parser/query_node/recursive_cte_node.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/query_node/set_operation_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/schema.h"
#include "pg/catalog/functions/pg_catalog/objectaddress.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

enum class Part : uint8_t {
  Whole,
  Owned,
  Column,
  Default,
  Constraint,
};

struct Edge {
  duckdb::CatalogEntry* dependent;
  duckdb::CatalogEntry* subject;
  Part part;
  duckdb::Identifier name;
};

Part PartOf(duckdb::AlterTableType alter) {
  switch (alter) {
    case duckdb::AlterTableType::SET_DEFAULT:
      return Part::Default;
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      return Part::Constraint;
    default:
      return Part::Column;
  }
}

int32_t Attnum(const duckdb::CatalogEntry& table,
               const duckdb::Identifier& column) {
  const auto& columns = table.Cast<duckdb::TableCatalogEntry>();
  if (!columns.ColumnExists(column)) {
    return 0;
  }
  return static_cast<int32_t>(columns.GetColumn(column).Logical().index + 1);
}

int64_t ConstraintPosition(const duckdb::CatalogEntry& table,
                           const duckdb::Identifier& name) {
  const auto& constraints =
    table.Cast<duckdb::TableCatalogEntry>().GetConstraints();
  return absl::c_find_if(constraints,
                         [&](const auto& constraint) {
                           return constraint->constraint_name ==
                                  name.GetIdentifierName();
                         }) -
         constraints.begin();
}

struct EdgeOrder {
  duckdb::idx_t oid;
  Part part;
  int64_t position;

  auto operator<=>(const EdgeOrder&) const = default;
};

EdgeOrder OrderOf(const Edge& edge) {
  int64_t position = 0;
  switch (edge.part) {
    case Part::Column:
      position = -Attnum(*edge.dependent, edge.name);
      break;
    case Part::Default:
      position = Attnum(*edge.dependent, edge.name);
      break;
    case Part::Constraint:
      position = ConstraintPosition(*edge.dependent, edge.name);
      break;
    case Part::Whole:
    case Part::Owned:
      break;
  }
  return {edge.dependent->oid, edge.part, position};
}

std::vector<Edge> Sorted(std::vector<Edge> edges) {
  absl::c_sort(edges, [](const Edge& lhs, const Edge& rhs) {
    return OrderOf(lhs) < OrderOf(rhs);
  });
  return edges;
}

std::string DescribeDependent(duckdb::ClientContext& context,
                              const Edge& edge) {
  switch (edge.part) {
    case Part::Whole:
    case Part::Owned:
      return pg::DescribeObject(context, *edge.dependent);
    case Part::Column:
      return pg::DescribeObject(context, *edge.dependent,
                                Attnum(*edge.dependent, edge.name));
    case Part::Default:
      return absl::StrCat(
        "default value for ",
        pg::DescribeObject(context, *edge.dependent,
                           Attnum(*edge.dependent, edge.name)));
    case Part::Constraint:
      return absl::StrCat("constraint ", edge.name.GetIdentifierName(), " on ",
                          pg::DescribeObject(context, *edge.dependent));
  }
  return {};
}

void ForEachDependent(duckdb::CatalogTransaction transaction,
                      BaseCatalog& catalog, duckdb::idx_t subject,
                      absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  catalog.Oids().ForEachDependent(
    transaction, subject, [&](duckdb::CatalogEntry& member) {
      if (member.oid != subject &&
          member.type != duckdb::CatalogType::SCHEMA_ENTRY) {
        callback(member);
      }
    });
}

bool OwnedBy(const duckdb::CatalogEntry& entry, duckdb::idx_t owner) {
  return absl::c_any_of(entry.Cast<duckdb::StandardEntry>().dependencies.Set(),
                        [&](const duckdb::LogicalDependency& dependency) {
                          return dependency.owned_by && dependency.oid == owner;
                        });
}

std::vector<Edge> DirectDependents(duckdb::CatalogTransaction transaction,
                                   duckdb::CatalogEntry& subject) {
  std::vector<Edge> direct;
  if (subject.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    subject.Cast<Schema>().ScanMembers(
      transaction, [&](duckdb::CatalogEntry& member) {
        if (!member.internal &&
            member.type != duckdb::CatalogType::INDEX_ENTRY) {
          direct.push_back({&member, &subject, Part::Whole, {}});
        }
      });
    return Sorted(std::move(direct));
  }
  if (subject.type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto& context = transaction.GetContext();
    if (auto database = duckdb::DatabaseManager::Get(context).GetDatabase(
          context, subject.name)) {
      absl::flat_hash_set<duckdb::idx_t> seen;
      database->GetCatalog().Cast<BaseCatalog>().Oids().ForEachForeignDependent(
        transaction, [&](duckdb::CatalogEntry& member) {
          if (seen.insert(member.oid).second) {
            direct.push_back({&member, &subject, Part::Whole, {}});
          }
        });
    }
    return Sorted(std::move(direct));
  }
  auto& catalog = subject.ParentCatalog().Cast<BaseCatalog>();
  if (subject.type == duckdb::CatalogType::TABLE_ENTRY) {
    for (const auto& dependency :
         subject.Cast<duckdb::StandardEntry>().dependencies.Set()) {
      if (!dependency.owned_by) {
        continue;
      }
      if (auto owned = catalog.LookupOid(transaction, dependency.oid)) {
        direct.push_back({owned.get(), &subject, Part::Owned, {}});
      }
    }
  }
  ForEachDependent(
    transaction, catalog, subject.oid, [&](duckdb::CatalogEntry& member) {
      if (member.type == duckdb::CatalogType::INDEX_ENTRY &&
          member.Cast<duckdb::IndexCatalogEntry>().table_oid == subject.oid) {
        direct.push_back({&member, &subject, Part::Owned, {}});
        return;
      }
      const auto& dependencies =
        member.Cast<duckdb::StandardEntry>().dependencies.Set();
      for (const auto& dependency : dependencies) {
        if (dependency.oid != subject.oid) {
          continue;
        }
        if (dependency.owned_by &&
            member.type != duckdb::CatalogType::TABLE_ENTRY) {
          direct.push_back({&member, &subject, Part::Owned, {}});
          continue;
        }
        if (member.type != duckdb::CatalogType::TABLE_ENTRY ||
            (dependency.subdependencies.empty() && !dependency.owned_by)) {
          direct.push_back({&member, &subject, Part::Whole, {}});
          continue;
        }
        for (const auto& part : dependency.subdependencies) {
          direct.push_back({&member, &subject, PartOf(part.alter), part.name});
        }
      }
    });
  return Sorted(std::move(direct));
}

class Closure {
 public:
  Closure(duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& root)
    : _transaction{transaction} {
    _seen.insert(root.oid);
    Visit(root);
    std::erase_if(edges, [&](const Edge& edge) {
      return edge.part != Part::Whole && _seen.contains(edge.dependent->oid);
    });
  }

  std::vector<Edge> edges;
  std::vector<duckdb::CatalogEntry*> drops;

 private:
  void Visit(duckdb::CatalogEntry& subject) {
    for (auto& edge : DirectDependents(_transaction, subject)) {
      if (edge.part != Part::Whole && edge.part != Part::Owned) {
        edges.push_back(edge);
        continue;
      }
      if (!_seen.insert(edge.dependent->oid).second) {
        continue;
      }
      if (edge.part == Part::Whole) {
        edges.push_back(edge);
      }
      Visit(*edge.dependent);
      drops.push_back(edge.dependent);
    }
  }

  duckdb::CatalogTransaction _transaction;
  absl::flat_hash_set<duckdb::idx_t> _seen;
};

enum class Kind : uint8_t {
  Relation,
  Sequence,
  Function,
  Type,
};

std::optional<Kind> KindOf(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::TABLE_ENTRY:
    case duckdb::CatalogType::VIEW_ENTRY:
      return Kind::Relation;
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return Kind::Sequence;
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      return Kind::Function;
    case duckdb::CatalogType::TYPE_ENTRY:
      return Kind::Type;
    default:
      return std::nullopt;
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> Resolve(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& dependent,
  duckdb::CatalogType type, const duckdb::QualifiedName& name) {
  auto& catalog = dependent.ParentCatalog().Cast<BaseCatalog>();
  const duckdb::IdentifierEquality equals{catalog.IsCaseSensitive()};
  if (!name.Catalog().empty() && !equals(name.Catalog(), catalog.GetName())) {
    return nullptr;
  }
  const auto in = [&](duckdb::CatalogEntry* schema)
    -> duckdb::optional_ptr<duckdb::CatalogEntry> {
    if (!schema) {
      return nullptr;
    }
    return schema->Cast<Schema>().Set(type).Get(transaction, name.Name());
  };
  if (!name.Schema().empty()) {
    return in(
      catalog
        .LookupSchema(transaction,
                      duckdb::EntryLookupInfo{duckdb::CatalogType::SCHEMA_ENTRY,
                                              name.Parent()},
                      duckdb::OnEntryNotFound::RETURN_NULL)
        .get());
  }
  if (auto found = in(&dependent.ParentSchema(transaction))) {
    return found;
  }
  auto& context = transaction.GetContext();
  for (const auto& path :
       duckdb::ClientData::Get(context).catalog_search_path->Get()) {
    const auto& path_catalog =
      path.GetCatalog().empty()
        ? duckdb::DatabaseManager::GetDefaultDatabase(context)
        : path.GetCatalog();
    if (!equals(path_catalog, catalog.GetName())) {
      continue;
    }
    if (auto found = in(catalog.Set(duckdb::CatalogType::SCHEMA_ENTRY)
                          .Get(transaction, path.GetSchema())
                          .get())) {
      return found;
    }
  }
  return nullptr;
}

bool IsSequenceFunction(const duckdb::Identifier& name) {
  return name == "nextval" || name == "currval" || name == "setval";
}

class Rewriter {
 public:
  Rewriter(duckdb::CatalogTransaction transaction,
           duckdb::CatalogEntry& dependent, duckdb::CatalogEntry& subject,
           Kind kind, const duckdb::Identifier& new_schema,
           const duckdb::Identifier& new_name)
    : _transaction{transaction},
      _dependent{dependent},
      _subject{subject},
      _kind{kind},
      _equals{subject.ParentCatalog().IsCaseSensitive()},
      _new_schema{new_schema},
      _new_name{new_name} {}

  bool changed = false;

  void Visit(duckdb::QueryNode& node) {
    CollectCtes(node);
    duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node,
      [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
        Visit(*child);
      },
      [&](duckdb::TableRef& ref) { VisitRef(ref); });
  }

  void Visit(duckdb::ParsedExpression& expr) {
    switch (expr.GetExpressionClass()) {
      case duckdb::ExpressionClass::SUBQUERY:
        Visit(*expr.Cast<duckdb::SubqueryExpression>().SubqueryMutable()->node);
        break;
      case duckdb::ExpressionClass::FUNCTION: {
        auto& function = expr.Cast<duckdb::FunctionExpression>();
        if (_kind == Kind::Function) {
          Rename(function.GetQualifiedNameMutable());
        } else if (_kind == Kind::Sequence &&
                   IsSequenceFunction(function.GetQualifiedName().Name())) {
          RenameLiteral(function);
        }
        break;
      }
      case duckdb::ExpressionClass::TYPE:
        if (_kind == Kind::Type) {
          Rename(expr.Cast<duckdb::TypeExpression>().GetQualifiedNameMutable());
        }
        break;
      default:
        break;
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](duckdb::ParsedExpression& child) { Visit(child); });
  }

 private:
  void CollectCtes(duckdb::QueryNode& node) {
    for (const auto& [name, cte] : node.cte_map.map) {
      _ctes.push_back(name);
    }
  }

  void VisitRef(duckdb::TableRef& ref) {
    if (_kind != Kind::Relation ||
        ref.type != duckdb::TableReferenceType::BASE_TABLE) {
      return;
    }
    auto& name = ref.Cast<duckdb::BaseTableRef>().GetQualifiedNameMutable();
    if (name.Schema().empty() &&
        absl::c_any_of(_ctes, [&](const duckdb::Identifier& cte) {
          return _equals(cte, name.Name());
        })) {
      return;
    }
    if (Rename(name) && ref.alias.empty() &&
        !_equals(_new_name, _subject.name)) {
      ref.alias = _subject.name;
    }
  }

  void RenameLiteral(duckdb::FunctionExpression& function) {
    auto& arguments = function.GetArgumentsMutable();
    if (arguments.empty()) {
      return;
    }
    auto& argument = arguments[0].GetExpressionMutable();
    if (!argument ||
        argument->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
      return;
    }
    const auto& literal =
      argument->Cast<duckdb::ConstantExpression>().GetLiteral();
    if (literal.kind != duckdb::LiteralKind::STRING) {
      return;
    }
    auto name = duckdb::QualifiedName::Parse(
      literal.text, duckdb::IdentifierCaseMode::LOWERCASE);
    if (Rename(name)) {
      argument = duckdb::ConstantExpression::String(name.ToString());
    }
  }

  bool Rename(duckdb::QualifiedName& name) {
    if (!_equals(name.Name(), _subject.name)) {
      return false;
    }
    const auto found = Resolve(_transaction, _dependent, _subject.type, name);
    if (!found || found->oid != _subject.oid) {
      return false;
    }
    if (name.Schema().empty()) {
      if (_equals(_new_name, name.Name())) {
        return false;
      }
      name = name.WithName(_new_name);
    } else {
      name = duckdb::QualifiedName{name.Catalog(), _new_schema, _new_name};
    }
    changed = true;
    return true;
  }

  duckdb::CatalogTransaction _transaction;
  duckdb::CatalogEntry& _dependent;
  duckdb::CatalogEntry& _subject;
  Kind _kind;
  duckdb::IdentifierEquality _equals;
  const duckdb::Identifier& _new_schema;
  const duckdb::Identifier& _new_name;
  std::vector<duckdb::Identifier> _ctes;
};

class ColumnRewriter {
 public:
  ColumnRewriter(duckdb::CatalogTransaction transaction,
                 duckdb::CatalogEntry& dependent, duckdb::CatalogEntry& table,
                 const duckdb::Identifier& old_column,
                 const duckdb::Identifier& new_column)
    : _transaction{transaction},
      _dependent{dependent},
      _table{table},
      _equals{table.ParentCatalog().IsCaseSensitive()},
      _old_column{old_column},
      _new_column{new_column} {}

  bool changed = false;

  void Visit(duckdb::QueryNode& node) {
    for (auto& [name, cte] : node.cte_map.map) {
      if (cte->query_node) {
        Visit(*cte->query_node);
      }
    }
    switch (node.type) {
      case duckdb::QueryNodeType::SELECT_NODE:
        VisitSelect(node.Cast<duckdb::SelectNode>());
        break;
      case duckdb::QueryNodeType::SET_OPERATION_NODE:
        for (auto& child : node.Cast<duckdb::SetOperationNode>().children) {
          Visit(*child);
        }
        break;
      case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
        auto& cte = node.Cast<duckdb::RecursiveCTENode>();
        Visit(*cte.left);
        Visit(*cte.right);
        break;
      }
      default:
        break;
    }
  }

 private:
  struct Scope {
    std::vector<duckdb::Identifier> qualifiers;
    std::vector<duckdb::ParsedExpression*> conditions;
    bool exclusive = true;
  };

  void VisitSelect(duckdb::SelectNode& select) {
    Scope scope;
    if (select.from_table) {
      VisitFrom(*select.from_table, scope);
    }
    if (scope.qualifiers.empty()) {
      scope.exclusive = false;
    }
    for (auto* condition : scope.conditions) {
      Rewrite(*condition, scope);
    }
    for (auto& item : select.select_list) {
      if (Rewrite(*item, scope) && !item->HasAlias()) {
        item->SetAlias(_old_column);
      }
    }
    for (auto* expr : {&select.where_clause, &select.having, &select.qualify}) {
      if (*expr) {
        Rewrite(**expr, scope);
      }
    }
    for (auto& group : select.groups.group_expressions) {
      Rewrite(*group, scope);
    }
    duckdb::ParsedExpressionIterator::EnumerateQueryNodeModifiers(
      select, [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
        Rewrite(*child, scope);
      });
  }

  void VisitFrom(duckdb::TableRef& ref, Scope& scope) {
    switch (ref.type) {
      case duckdb::TableReferenceType::BASE_TABLE: {
        const auto& name = ref.Cast<duckdb::BaseTableRef>().GetQualifiedName();
        const auto found = Resolve(_transaction, _dependent,
                                   duckdb::CatalogType::TABLE_ENTRY, name);
        if (found && found->oid == _table.oid) {
          scope.qualifiers.push_back(ref.alias.empty() ? name.Name()
                                                       : ref.alias);
        } else if (!found || found->type != duckdb::CatalogType::TABLE_ENTRY ||
                   found->Cast<duckdb::TableCatalogEntry>().ColumnExists(
                     _old_column)) {
          scope.exclusive = false;
        }
        break;
      }
      case duckdb::TableReferenceType::JOIN: {
        auto& join = ref.Cast<duckdb::JoinRef>();
        VisitFrom(*join.left, scope);
        VisitFrom(*join.right, scope);
        if (join.condition) {
          scope.conditions.push_back(join.condition.get());
        }
        break;
      }
      case duckdb::TableReferenceType::SUBQUERY:
        Visit(*ref.Cast<duckdb::SubqueryRef>().subquery->node);
        scope.exclusive = false;
        break;
      default:
        scope.exclusive = false;
        break;
    }
  }

  bool Rewrite(duckdb::ParsedExpression& expr, const Scope& scope) {
    if (expr.GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      Visit(*expr.Cast<duckdb::SubqueryExpression>().SubqueryMutable()->node);
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](duckdb::ParsedExpression& child) { Rewrite(child, scope); });
    if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
      return false;
    }
    auto& names = expr.Cast<duckdb::ColumnRefExpression>().ColumnNamesMutable();
    if (!_equals(names.back(), _old_column)) {
      return false;
    }
    if (names.size() == 1
          ? !scope.exclusive
          : !absl::c_any_of(scope.qualifiers, [&](const duckdb::Identifier& q) {
              return _equals(q, names[names.size() - 2]);
            })) {
      return false;
    }
    names.back() = _new_column;
    changed = true;
    return true;
  }

  duckdb::CatalogTransaction _transaction;
  duckdb::CatalogEntry& _dependent;
  duckdb::CatalogEntry& _table;
  duckdb::IdentifierEquality _equals;
  const duckdb::Identifier& _old_column;
  const duckdb::Identifier& _new_column;
};

void Rewrite(duckdb::CatalogTransaction transaction, const Edge& edge,
             Kind kind, const duckdb::Identifier& new_schema,
             const duckdb::Identifier& new_name) {
  auto& dependent = *edge.dependent;
  if (dependent.type == duckdb::CatalogType::INDEX_ENTRY &&
      edge.part == Part::Whole) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot rename ",
              absl::AsciiStrToLower(
                duckdb::CatalogTypeToString(edge.subject->type)),
              " \"",
              edge.subject->name.GetIdentifierName(),
              "\" because other objects depend on it"),
      ERR_DETAIL("index \"", dependent.name.GetIdentifierName(),
                 "\" depends on it by name"));
  }
  Rewriter rewriter{transaction, dependent,  *edge.subject,
                    kind,        new_schema, new_name};
  auto& schema = dependent.ParentSchema(transaction).Cast<Schema>();
  if (dependent.type == duckdb::CatalogType::VIEW_ENTRY) {
    auto info = dependent.GetInfo();
    rewriter.Visit(*info->Cast<duckdb::CreateViewInfo>().query->node);
    if (rewriter.changed) {
      schema.Alter(transaction, *info->GetAlterInfo());
    }
    return;
  }
  if (dependent.type == duckdb::CatalogType::MACRO_ENTRY ||
      dependent.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    auto info = dependent.GetInfo();
    for (auto& macro : info->Cast<duckdb::CreateMacroInfo>().macros) {
      if (macro->type == duckdb::MacroType::SCALAR_MACRO) {
        rewriter.Visit(*macro->Cast<duckdb::ScalarMacroFunction>().expression);
      } else {
        rewriter.Visit(*macro->Cast<duckdb::TableMacroFunction>().query_node);
      }
      for (auto& [parameter, value] : macro->default_parameters) {
        rewriter.Visit(*value);
      }
    }
    if (rewriter.changed) {
      schema.Alter(transaction, *info->GetAlterInfo());
    }
    return;
  }
  if (dependent.type != duckdb::CatalogType::TABLE_ENTRY ||
      edge.part != Part::Default) {
    return;
  }
  const auto& table = dependent.Cast<duckdb::TableCatalogEntry>();
  if (!table.ColumnExists(edge.name)) {
    return;
  }
  const auto& column = table.GetColumn(edge.name);
  if (!column.HasDefaultValue()) {
    return;
  }
  auto value = column.DefaultValue().Copy();
  rewriter.Visit(*value);
  if (!rewriter.changed) {
    return;
  }
  duckdb::SetDefaultInfo info{
    duckdb::AlterEntryData{schema.GetQualifiedName(dependent.name),
                           duckdb::OnEntryNotFound::THROW_EXCEPTION},
    duckdb::vector<duckdb::Identifier>{edge.name}, std::move(value)};
  schema.Alter(transaction, info);
}

void Follow(duckdb::CatalogTransaction transaction,
            duckdb::CatalogEntry& subject, const duckdb::Identifier& new_schema,
            const duckdb::Identifier& new_name) {
  const auto kind = KindOf(subject.type);
  if (!kind) {
    return;
  }
  for (const auto& edge : DirectDependents(transaction, subject)) {
    Rewrite(transaction, edge, *kind, new_schema, new_name);
  }
}

duckdb::unique_ptr<duckdb::AlterTableInfo> DropPart(
  duckdb::CatalogTransaction transaction, const Edge& edge) {
  auto& schema = edge.dependent->ParentSchema(transaction);
  const duckdb::AlterEntryData data{
    schema.GetQualifiedName(edge.dependent->name),
    duckdb::OnEntryNotFound::THROW_EXCEPTION};
  switch (edge.part) {
    case Part::Default:
      return duckdb::make_uniq<duckdb::SetDefaultInfo>(
        data, duckdb::vector<duckdb::Identifier>{edge.name}, nullptr);
    case Part::Constraint:
      return duckdb::make_uniq<duckdb::DropConstraintInfo>(
        data, edge.name.GetIdentifierName(), false, true);
    default:
      return duckdb::make_uniq<duckdb::RemoveColumnInfo>(
        data, edge.name.GetIdentifierName(), false, true);
  }
}

}  // namespace

void ScanSubjects(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& dependent, EdgeVisitor visit) {
  if (dependent.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    return;
  }
  auto& catalog = dependent.ParentCatalog().Cast<BaseCatalog>();
  duckdb::idx_t relation_oid = 0;
  if (dependent.type == duckdb::CatalogType::INDEX_ENTRY) {
    relation_oid = dependent.Cast<duckdb::IndexCatalogEntry>().table_oid;
    if (auto relation = catalog.LookupOid(transaction, relation_oid)) {
      visit(*relation, duckdb::DependencyDependentFlags());
    }
  }
  for (const auto& dependency :
       dependent.Cast<duckdb::StandardEntry>().dependencies.Set()) {
    if ((dependency.owned_by &&
         dependent.type != duckdb::CatalogType::TABLE_ENTRY) ||
        (relation_oid != 0 && dependency.oid == relation_oid)) {
      continue;
    }
    auto subject = catalog.LookupOid(transaction, dependency.oid);
    if (subject && !subject->internal &&
        subject->type != duckdb::CatalogType::SCHEMA_ENTRY) {
      auto flags = dependency.flags;
      if (dependency.owned_by) {
        flags.SetOwnedBy();
      }
      visit(*subject, flags);
    }
  }
}

void ScanDependents(duckdb::CatalogTransaction transaction,
                    duckdb::CatalogEntry& subject, EdgeVisitor visit) {
  absl::flat_hash_set<duckdb::idx_t> seen;
  for (const auto& edge : DirectDependents(transaction, subject)) {
    if (!seen.insert(edge.dependent->oid).second) {
      continue;
    }
    auto flags = duckdb::DependencyDependentFlags().SetBlocking();
    if (edge.part == Part::Owned) {
      flags = duckdb::DependencyDependentFlags().SetOwnedBy();
    }
    visit(*edge.dependent, flags);
  }
}

void DropDependents(duckdb::CatalogTransaction transaction,
                    duckdb::CatalogEntry& entry, bool cascade) {
  Closure closure{transaction, entry};
  if (closure.edges.empty() && closure.drops.empty()) {
    return;
  }
  auto& context = transaction.GetContext();
  if (!cascade && !closure.edges.empty()) {
    std::vector<std::string> lines;
    lines.reserve(closure.edges.size());
    for (const auto& edge : closure.edges) {
      lines.push_back(absl::StrCat(DescribeDependent(context, edge),
                                   " depends on ",
                                   pg::DescribeObject(context, *edge.subject)));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop ", pg::DescribeObject(context, entry),
              " because other objects depend on it"),
      ERR_DETAIL(absl::StrJoin(lines, "\n")),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }
  std::vector<std::string> items;
  items.reserve(closure.edges.size());
  for (const auto& edge : closure.edges) {
    items.push_back(
      absl::StrCat("drop cascades to ", DescribeDependent(context, edge)));
  }
  if (!items.empty()) {
    auto& session = ConnectionContext::Get(context);
    if (items.size() == 1) {
      session.AddNotice(SQL_ERROR_DATA(ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
                                       ERR_MSG(items.front())));
    } else {
      session.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
        ERR_MSG("drop cascades to ", items.size(), " other objects"),
        ERR_DETAIL(absl::StrJoin(items, "\n"))));
    }
  }
  for (const auto& edge : closure.edges) {
    if (edge.part == Part::Whole) {
      continue;
    }
    auto info = DropPart(transaction, edge);
    edge.dependent->ParentSchema(transaction)
      .Cast<Schema>()
      .Alter(transaction, *info);
  }
  for (auto* dependent : closure.drops) {
    if (dependent->type == duckdb::CatalogType::SCHEMA_ENTRY) {
      entry.ParentCatalog()
        .Cast<BaseCatalog>()
        .SchemaSet(transaction, dependent->Cast<Schema>())
        .Drop(transaction, dependent->name);
      continue;
    }
    dependent->ParentSchema(transaction)
      .Cast<Schema>()
      .Drop(transaction, *dependent);
  }
}

duckdb::catalog_entry_vector_t OwnedEntries(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owner) {
  duckdb::catalog_entry_vector_t owned;
  if (owner.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    return owned;
  }
  auto& catalog =
    owner.Cast<duckdb::StandardEntry>().catalog.Cast<BaseCatalog>();
  for (const auto& dependency :
       owner.Cast<duckdb::StandardEntry>().dependencies.Set()) {
    if (dependency.owned_by) {
      if (auto entry = catalog.LookupOid(transaction, dependency.oid)) {
        owned.emplace_back(*entry);
      }
    }
  }
  ForEachDependent(transaction, catalog, owner.oid,
                   [&](duckdb::CatalogEntry& member) {
                     if (member.type != duckdb::CatalogType::TABLE_ENTRY &&
                         OwnedBy(member, owner.oid)) {
                       owned.emplace_back(member);
                     }
                   });
  return owned;
}

duckdb::optional_ptr<duckdb::CatalogEntry> OwnerOf(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owned) {
  if (owned.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    return nullptr;
  }
  auto& catalog =
    owned.Cast<duckdb::StandardEntry>().catalog.Cast<BaseCatalog>();
  if (owned.type != duckdb::CatalogType::TABLE_ENTRY) {
    for (const auto& dependency :
         owned.Cast<duckdb::StandardEntry>().dependencies.Set()) {
      if (dependency.owned_by) {
        return catalog.LookupOid(transaction, dependency.oid);
      }
    }
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> owner;
  ForEachDependent(transaction, catalog, owned.oid,
                   [&](duckdb::CatalogEntry& member) {
                     if (member.type == duckdb::CatalogType::TABLE_ENTRY &&
                         OwnedBy(member, owned.oid)) {
                       owner = member;
                     }
                   });
  return owner;
}

void FollowRename(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& entry,
                  const duckdb::Identifier& new_name) {
  Follow(transaction, entry, entry.ParentSchema(transaction).name, new_name);
}

void FollowColumnRename(duckdb::CatalogTransaction transaction,
                        duckdb::CatalogEntry& table,
                        const duckdb::Identifier& old_column,
                        const duckdb::Identifier& new_column) {
  absl::flat_hash_set<duckdb::idx_t> seen;
  for (const auto& edge : DirectDependents(transaction, table)) {
    auto& dependent = *edge.dependent;
    if (!seen.insert(dependent.oid).second) {
      continue;
    }
    ColumnRewriter rewriter{transaction, dependent, table, old_column,
                            new_column};
    auto& schema = dependent.ParentSchema(transaction).Cast<Schema>();
    if (dependent.type == duckdb::CatalogType::VIEW_ENTRY) {
      auto info = dependent.GetInfo();
      auto& view = info->Cast<duckdb::CreateViewInfo>();
      rewriter.Visit(*view.query->node);
      const duckdb::IdentifierEquality equals{
        table.ParentCatalog().IsCaseSensitive()};
      const bool renames_output =
        view.aliases.empty() &&
        absl::c_any_of(view.names, [&](const duckdb::Identifier& name) {
          return equals(name, old_column);
        });
      if (rewriter.changed || renames_output) {
        if (view.aliases.empty()) {
          view.aliases = view.names;
        }
        schema.Alter(transaction, *info->GetAlterInfo());
      }
      continue;
    }
    if (dependent.type != duckdb::CatalogType::TABLE_MACRO_ENTRY) {
      continue;
    }
    auto info = dependent.GetInfo();
    for (auto& macro : info->Cast<duckdb::CreateMacroInfo>().macros) {
      rewriter.Visit(*macro->Cast<duckdb::TableMacroFunction>().query_node);
    }
    if (rewriter.changed) {
      schema.Alter(transaction, *info->GetAlterInfo());
    }
  }
}

void FollowColumnOids(duckdb::CatalogTransaction transaction,
                      const duckdb::TableCatalogEntry& previous,
                      const duckdb::TableCatalogEntry& current) {
  for (auto& owned : OwnedEntries(transaction, current)) {
    if (owned.get().type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      continue;
    }
    const auto& sequence = owned.get().Cast<duckdb::SequenceCatalogEntry>();
    const auto owner = sequence.owner_column;
    if (!owner.IsValid() ||
        absl::c_any_of(current.GetColumns().Logical(), [&](const auto& column) {
          return column.CatalogOid() == owner.GetIndex();
        })) {
      continue;
    }
    for (const auto& column : previous.GetColumns().Logical()) {
      if (column.CatalogOid() != owner.GetIndex() ||
          !current.ColumnExists(column.Name())) {
        continue;
      }
      auto& schema = sequence.ParentSchema(transaction);
      duckdb::ChangeOwnershipInfo ownership{
        duckdb::CatalogType::SEQUENCE_ENTRY,
        duckdb::AlterEntryData{schema.GetQualifiedName(sequence.name),
                               duckdb::OnEntryNotFound::THROW_EXCEPTION},
        duckdb::Identifier{}, duckdb::Identifier{}};
      ownership.owner_path = {current.name, column.Name()};
      schema.Cast<Schema>().Alter(transaction, ownership);
    }
  }
}

void FollowSchemaRename(duckdb::CatalogTransaction transaction,
                        duckdb::CatalogEntry& schema,
                        const duckdb::Identifier& new_name) {
  std::vector<duckdb::CatalogEntry*> members;
  schema.Cast<Schema>().ScanMembers(
    transaction, [&](duckdb::CatalogEntry& member) {
      if (!member.internal &&
          member.type != duckdb::CatalogType::SCHEMA_ENTRY) {
        members.push_back(&member);
      }
    });
  for (auto* member : members) {
    Follow(transaction, *member, new_name, member->name);
  }
}

}  // namespace sdb::catalog
