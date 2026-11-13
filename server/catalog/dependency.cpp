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
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstdint>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/expression/type_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/query_node.hpp>
#include <duckdb/parser/query_node/recursive_cte_node.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/query_node/set_operation_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
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
  duckdb::DependencyDependentFlags flags;
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
      const duckdb::IdentifierEquality same_name{
        database->GetCatalog().IsCaseSensitive()};
      database->GetCatalog().Cast<BaseCatalog>().Oids().ForEachForeignDependent(
        transaction, [&](duckdb::CatalogEntry& member) {
          if (!seen.insert(member.oid).second) {
            return;
          }
          std::vector<Edge> parts;
          bool whole = member.type != duckdb::CatalogType::TABLE_ENTRY;
          for (const auto& dependency :
               member.Cast<duckdb::StandardEntry>().dependencies.Set()) {
            if (whole || !same_name(dependency.catalog, subject.name)) {
              continue;
            }
            if (dependency.subdependencies.empty()) {
              whole = true;
              continue;
            }
            for (const auto& part : dependency.subdependencies) {
              const auto kind = PartOf(part.alter);
              if (absl::c_none_of(parts, [&](const Edge& edge) {
                    return edge.part == kind && edge.name == part.name;
                  })) {
                parts.push_back({&member, &subject, kind, part.name});
              }
            }
          }
          if (whole || parts.empty()) {
            direct.push_back({&member, &subject, Part::Whole, {}});
            return;
          }
          direct.insert(direct.end(), parts.begin(), parts.end());
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
      if (auto owned = catalog.LookupOid(transaction, dependency.oid);
          owned && SequenceOwnedByTable(*owned, subject)) {
        direct.push_back({owned.get(), &subject, Part::Owned, {}});
      }
    }
  }
  const auto triggers = [&](duckdb::CatalogEntry& table, Part part) {
    table.Cast<duckdb::TableCatalogEntry>().ScanTriggers(
      transaction, [&](duckdb::CatalogEntry& trigger) {
        for (const auto& dependency :
             trigger.Cast<duckdb::StandardEntry>().dependencies.Set()) {
          if (dependency.oid == subject.oid) {
            direct.push_back({&trigger, &subject, part, {}, dependency.flags});
          }
        }
      });
  };
  if (subject.type == duckdb::CatalogType::TABLE_ENTRY) {
    triggers(subject, Part::Owned);
  }
  ForEachDependent(
    transaction, catalog, subject.oid, [&](duckdb::CatalogEntry& member) {
      if (member.type == duckdb::CatalogType::INDEX_ENTRY &&
          member.Cast<duckdb::IndexCatalogEntry>().table_oid == subject.oid) {
        direct.push_back({&member, &subject, Part::Owned, {}});
        return;
      }
      if (member.type == duckdb::CatalogType::TABLE_ENTRY) {
        triggers(member, Part::Whole);
      }
      const auto& dependencies =
        member.Cast<duckdb::StandardEntry>().dependencies.Set();
      for (const auto& dependency : dependencies) {
        if (dependency.oid != subject.oid) {
          continue;
        }
        if (dependency.owned_by &&
            member.type != duckdb::CatalogType::TABLE_ENTRY) {
          direct.push_back(
            {&member, &subject, Part::Owned, {}, dependency.flags});
          continue;
        }
        if (member.type != duckdb::CatalogType::TABLE_ENTRY ||
            (dependency.subdependencies.empty() && !dependency.owned_by)) {
          direct.push_back(
            {&member, &subject, Part::Whole, {}, dependency.flags});
          continue;
        }
        for (const auto& part : dependency.subdependencies) {
          direct.push_back({&member, &subject, PartOf(part.alter), part.name,
                            dependency.flags});
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
      if (edge.part == Part::Owned &&
          edge.dependent->type == duckdb::CatalogType::TRIGGER_ENTRY) {
        continue;
      }
      if (edge.part != Part::Whole && edge.part != Part::Owned) {
        if (absl::c_none_of(edges, [&](const Edge& known) {
              return known.dependent->oid == edge.dependent->oid &&
                     known.part == edge.part && known.name == edge.name;
            })) {
          edges.push_back(edge);
        }
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
           const duckdb::Identifier& new_name, bool qualify)
    : _transaction{transaction},
      _dependent{dependent},
      _subject{subject},
      _kind{kind},
      _equals{subject.ParentCatalog().IsCaseSensitive()},
      _new_schema{new_schema},
      _new_name{new_name},
      _qualify{qualify} {}

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
      case duckdb::ExpressionClass::CAST:
        Visit(expr.Cast<duckdb::CastExpression>().TargetTypeMutable());
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
    auto* argument = &arguments[0].GetExpressionMutable();
    if (*argument &&
        (*argument)->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
      auto& cast = (*argument)->Cast<duckdb::CastExpression>();
      if (cast.TargetType().GetQualifiedName().Name() == "regclass") {
        argument = &cast.ChildMutable();
      }
    }
    if (!*argument || (*argument)->GetExpressionClass() !=
                        duckdb::ExpressionClass::CONSTANT) {
      return;
    }
    const auto& literal =
      (*argument)->Cast<duckdb::ConstantExpression>().GetLiteral();
    if (literal.kind != duckdb::LiteralKind::STRING) {
      return;
    }
    auto name = duckdb::QualifiedName::Parse(
      literal.text, duckdb::IdentifierCaseMode::LOWERCASE);
    if (Rename(name)) {
      *argument = duckdb::ConstantExpression::String(name.ToString());
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
    if (name.Schema().empty() && !_qualify) {
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
  bool _qualify;
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
  bool expands_all = false;
  bool uncertain = false;

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
    std::vector<duckdb::Identifier> others;
    std::vector<duckdb::ParsedExpression*> conditions;
    bool known = true;
    bool other_has_column = false;
    bool merged = false;
  };

  void VisitSelect(duckdb::SelectNode& select) {
    Scope scope;
    if (select.from_table) {
      VisitFrom(*select.from_table, scope);
    }
    if (scope.merged) {
      uncertain = true;
    }
    _scopes.push_back(&scope);
    absl::Cleanup pop = [&] { _scopes.pop_back(); };
    for (auto* condition : scope.conditions) {
      Rewrite(*condition);
    }
    for (auto& item : select.select_list) {
      if (item->GetExpressionClass() == duckdb::ExpressionClass::STAR) {
        const auto& relation =
          item->Cast<duckdb::StarExpression>().RelationName();
        expands_all =
          expands_all ||
          (relation.empty() ? !scope.qualifiers.empty()
                            : absl::c_any_of(scope.qualifiers,
                                             [&](const duckdb::Identifier& q) {
                                               return _equals(q, relation);
                                             }));
      }
      if (Rewrite(*item) && !item->HasAlias()) {
        item->SetAlias(_old_column);
      }
    }
    for (auto* expr : {&select.where_clause, &select.having, &select.qualify}) {
      if (*expr) {
        Rewrite(**expr);
      }
    }
    for (auto& group : select.groups.group_expressions) {
      Rewrite(*group);
    }
    duckdb::ParsedExpressionIterator::EnumerateQueryNodeModifiers(
      select, [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
        Rewrite(*child);
      });
  }

  void VisitFrom(duckdb::TableRef& ref, Scope& scope) {
    switch (ref.type) {
      case duckdb::TableReferenceType::BASE_TABLE: {
        const auto& name = ref.Cast<duckdb::BaseTableRef>().GetQualifiedName();
        auto found = Resolve(_transaction, _dependent,
                             duckdb::CatalogType::TABLE_ENTRY, name);
        if (!found && !name.Catalog().empty() && !name.Schema().empty() &&
            _equals(name.Catalog(), _table.ParentCatalog().GetName()) &&
            _equals(name.Schema(), _table.ParentSchema(_transaction).name) &&
            _equals(name.Name(), _table.name)) {
          found = &_table;
        }
        const auto& qualifier = ref.alias.empty() ? name.Name() : ref.alias;
        if (found && found->oid == _table.oid) {
          scope.qualifiers.push_back(qualifier);
          break;
        }
        scope.others.push_back(qualifier);
        if (!found || found->type != duckdb::CatalogType::TABLE_ENTRY) {
          scope.known = false;
        } else if (found->Cast<duckdb::TableCatalogEntry>().ColumnExists(
                     _old_column)) {
          scope.other_has_column = true;
        }
        break;
      }
      case duckdb::TableReferenceType::JOIN: {
        auto& join = ref.Cast<duckdb::JoinRef>();
        const auto ours = scope.qualifiers.size();
        VisitFrom(*join.left, scope);
        VisitFrom(*join.right, scope);
        if (join.condition) {
          scope.conditions.push_back(join.condition.get());
        }
        if (scope.qualifiers.size() != ours &&
            (join.ref_type == duckdb::JoinRefType::NATURAL ||
             absl::c_any_of(join.using_columns, [&](const auto& column) {
               return _equals(column, _old_column);
             }))) {
          scope.merged = true;
        }
        break;
      }
      case duckdb::TableReferenceType::SUBQUERY:
        Visit(*ref.Cast<duckdb::SubqueryRef>().subquery->node);
        scope.others.push_back(ref.alias);
        scope.known = false;
        break;
      case duckdb::TableReferenceType::EMPTY_FROM:
        break;
      default:
        scope.others.push_back(ref.alias);
        scope.known = false;
        break;
    }
  }

  bool Ours(const duckdb::ColumnRefExpression& reference) {
    const auto& names = reference.ColumnNames();
    for (auto i = _scopes.size(); i-- > 0;) {
      const auto& scope = *_scopes[i];
      if (names.size() > 1) {
        const auto& qualifier = names[names.size() - 2];
        const auto named = [&](const duckdb::Identifier& q) {
          return _equals(q, qualifier);
        };
        if (absl::c_any_of(scope.qualifiers, named)) {
          return true;
        }
        if (absl::c_any_of(scope.others, named)) {
          return false;
        }
        continue;
      }
      if (!scope.qualifiers.empty()) {
        return !scope.merged;
      }
      if (scope.other_has_column) {
        return false;
      }
      if (!scope.known) {
        uncertain =
          uncertain ||
          absl::c_any_of(std::span{_scopes}.first(i), [](const Scope* outer) {
            return !outer->qualifiers.empty();
          });
        return false;
      }
    }
    return false;
  }

  bool Rewrite(duckdb::ParsedExpression& expr) {
    if (expr.GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      Visit(*expr.Cast<duckdb::SubqueryExpression>().SubqueryMutable()->node);
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](duckdb::ParsedExpression& child) { Rewrite(child); });
    if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
      return false;
    }
    auto& reference = expr.Cast<duckdb::ColumnRefExpression>();
    auto& names = reference.ColumnNamesMutable();
    if (!_equals(names.back(), _old_column) || !Ours(reference)) {
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
  std::vector<const Scope*> _scopes;
};

void Rewrite(duckdb::CatalogTransaction transaction, const Edge& edge,
             Kind kind, const duckdb::Identifier& new_schema,
             const duckdb::Identifier& new_name, bool qualify,
             std::vector<PendingDefault>& defaults) {
  auto& dependent = *edge.dependent;
  if (dependent.type == duckdb::CatalogType::INDEX_ENTRY &&
      edge.part == Part::Whole) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot rename ",
                            absl::AsciiStrToLower(
                              duckdb::CatalogTypeToString(edge.subject->type)),
                            " \"", edge.subject->name.GetIdentifierName(),
                            "\" because other objects depend on it"),
                    ERR_DETAIL("index \"", dependent.name.GetIdentifierName(),
                               "\" depends on it by name"));
  }
  Rewriter rewriter{transaction, dependent, *edge.subject, kind,
                    new_schema,  new_name,  qualify};
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
  const auto pending =
    absl::c_find_if(defaults, [&](const PendingDefault& candidate) {
      return candidate.table == table.oid && candidate.column == column.Name();
    });
  if (pending != defaults.end()) {
    rewriter.Visit(*pending->value);
    return;
  }
  auto value = column.DefaultValue().Copy();
  rewriter.Visit(*value);
  if (rewriter.changed) {
    defaults.push_back({table.oid, column.Name(), std::move(value)});
  }
}

std::string DependsLine(const duckdb::CatalogEntry& dependent,
                        const duckdb::CatalogEntry& subject) {
  std::string line = absl::StrCat(
    absl::AsciiStrToLower(duckdb::CatalogTypeToString(dependent.type)), " ",
    duckdb::SQLQuotedIdentifier::ToString(dependent.name));
  if (dependent.type == duckdb::CatalogType::TRIGGER_ENTRY) {
    absl::StrAppend(
      &line, " on table ",
      duckdb::SQLQuotedIdentifier::ToString(
        dependent.Cast<duckdb::TriggerCatalogEntry>().base_table->Table()));
  }
  absl::StrAppend(
    &line, " depends on ",
    absl::AsciiStrToLower(duckdb::CatalogTypeToString(subject.type)), " ",
    duckdb::SQLQuotedIdentifier::ToString(subject.name), ".");
  return line;
}

[[noreturn]] void ThrowDependents(const duckdb::CatalogEntry& subject,
                                  std::span<const std::string> lines) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
    ERR_MSG("Cannot alter entry ",
            duckdb::SQLQuotedIdentifier::ToString(subject.name),
            " because there are entries that depend on it.\n",
            absl::StrJoin(lines, "\n"),
            "\nDrop the dependent entries first and recreate them after the "
            "change."));
}

bool Unfollowable(const Edge& edge, bool rewrites) {
  if (edge.dependent->type == duckdb::CatalogType::TRIGGER_ENTRY) {
    return edge.flags.IsAlterBlocking();
  }
  if (edge.part == Part::Owned) {
    return false;
  }
  if (!rewrites) {
    return true;
  }
  switch (edge.dependent->type) {
    case duckdb::CatalogType::VIEW_ENTRY:
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::INDEX_ENTRY:
      return false;
    case duckdb::CatalogType::TABLE_ENTRY:
      if (edge.part == Part::Default) {
        return false;
      }
      return edge.part != Part::Constraint ||
             !absl::c_any_of(
               edge.dependent->Cast<duckdb::TableCatalogEntry>()
                 .GetConstraints(),
               [&](const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
                 return constraint->type ==
                          duckdb::ConstraintType::FOREIGN_KEY &&
                        constraint->constraint_name ==
                          edge.name.GetIdentifierName();
               });
    default:
      return true;
  }
}

void RefuseUnfollowable(const duckdb::CatalogEntry& subject,
                        std::span<const Edge> edges,
                        std::span<const duckdb::idx_t> moving, bool rewrites) {
  std::vector<std::string> lines;
  absl::flat_hash_set<duckdb::idx_t> seen;
  for (const auto& edge : edges) {
    if (Unfollowable(edge, rewrites) &&
        !absl::c_linear_search(moving, edge.dependent->oid) &&
        seen.insert(edge.dependent->oid).second) {
      lines.push_back(DependsLine(*edge.dependent, subject));
    }
  }
  if (!lines.empty()) {
    ThrowDependents(subject, lines);
  }
}

void Follow(duckdb::CatalogTransaction transaction,
            duckdb::CatalogEntry& subject, const duckdb::Identifier& new_schema,
            const duckdb::Identifier& new_name,
            std::vector<PendingDefault>& defaults) {
  const auto kind = KindOf(subject.type);
  if (!kind) {
    return;
  }
  for (const auto& edge : DirectDependents(transaction, subject)) {
    Rewrite(transaction, edge, *kind, new_schema, new_name, false, defaults);
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

void DropClosure(duckdb::CatalogTransaction transaction,
                 duckdb::CatalogEntry& entry, const Closure& closure,
                 bool cascade, std::string_view subject) {
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
      ERR_MSG("cannot drop ", subject, " because other objects depend on it"),
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
    if (dependent->type == duckdb::CatalogType::TRIGGER_ENTRY) {
      auto& catalog = entry.ParentCatalog().Cast<BaseCatalog>();
      if (auto table = catalog.LookupOid(
            transaction, catalog.Oids().SetOwner(dependent->oid))) {
        table->Cast<duckdb::TableCatalogEntry>().DropTrigger(
          transaction, dependent->name, false);
      }
      continue;
    }
    dependent->ParentSchema(transaction)
      .Cast<Schema>()
      .Drop(transaction, *dependent);
  }
}

}  // namespace

bool SequenceOwnedByTable(const duckdb::CatalogEntry& sequence,
                          const duckdb::CatalogEntry& table) {
  if (sequence.type != duckdb::CatalogType::SEQUENCE_ENTRY ||
      table.type != duckdb::CatalogType::TABLE_ENTRY) {
    return true;
  }
  const auto owner = sequence.Cast<duckdb::SequenceCatalogEntry>().owner_column;
  return owner.IsValid() &&
         absl::c_any_of(
           table.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical(),
           [&](const duckdb::ColumnDefinition& column) {
             return column.CatalogOid() == owner.GetIndex();
           });
}

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
      if (dependency.owned_by && SequenceOwnedByTable(*subject, dependent)) {
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
  const Closure closure{transaction, entry};
  DropClosure(transaction, entry, closure, cascade,
              pg::DescribeObject(transaction.GetContext(), entry));
}

std::vector<duckdb::Identifier> DropOwnedSequences(
  duckdb::CatalogTransaction transaction, duckdb::TableCatalogEntry& table,
  std::span<const duckdb::LogicalIndex> columns, bool cascade) {
  const auto& list = table.GetColumns();
  const duckdb::IdentifierEquality same_name{list.IsCaseSensitive()};
  const auto dropped = [&](const duckdb::Identifier& name) {
    return absl::c_any_of(columns, [&](duckdb::LogicalIndex column) {
      return same_name(list.GetColumn(column).Name(), name);
    });
  };
  std::vector<duckdb::Identifier> cleared;
  for (auto& owned : OwnedEntries(transaction, table)) {
    auto& sequence = owned.get();
    if (sequence.type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      continue;
    }
    const auto owner =
      sequence.Cast<duckdb::SequenceCatalogEntry>().owner_column;
    const auto column =
      absl::c_find_if(columns, [&](duckdb::LogicalIndex candidate) {
        const auto oid = list.GetColumn(candidate).CatalogOid();
        return owner.IsValid() && oid && owner.GetIndex() == oid;
      });
    if (column == columns.end()) {
      continue;
    }
    Closure closure{transaction, sequence};
    std::erase_if(closure.edges, [&](const Edge& edge) {
      if (edge.dependent->oid != table.oid || edge.part != Part::Default) {
        return false;
      }
      if (dropped(edge.name)) {
        return true;
      }
      if (cascade) {
        cleared.push_back(edge.name);
        return true;
      }
      return false;
    });
    DropClosure(transaction, sequence, closure, cascade,
                pg::DescribeObject(transaction.GetContext(), table,
                                   static_cast<int32_t>(column->index + 1)));
    sequence.ParentSchema(transaction)
      .Cast<Schema>()
      .Drop(transaction, sequence);
  }
  return cleared;
}

duckdb::catalog_entry_vector_t OwnedEntries(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owner) {
  duckdb::catalog_entry_vector_t owned;
  if (owner.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    return owned;
  }
  auto& catalog =
    owner.Cast<duckdb::StandardEntry>().catalog.Cast<BaseCatalog>();
  const auto add = [&](duckdb::CatalogEntry& entry) {
    if (absl::c_none_of(owned, [&](const auto& seen) {
          return seen.get().oid == entry.oid;
        })) {
      owned.emplace_back(entry);
    }
  };
  for (const auto& dependency :
       owner.Cast<duckdb::StandardEntry>().dependencies.Set()) {
    if (dependency.owned_by) {
      if (auto entry = catalog.LookupOid(transaction, dependency.oid);
          entry && SequenceOwnedByTable(*entry, owner)) {
        add(*entry);
      }
    }
  }
  ForEachDependent(transaction, catalog, owner.oid,
                   [&](duckdb::CatalogEntry& member) {
                     if (member.type != duckdb::CatalogType::TABLE_ENTRY &&
                         OwnedBy(member, owner.oid)) {
                       add(member);
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
  ForEachDependent(
    transaction, catalog, owned.oid, [&](duckdb::CatalogEntry& member) {
      if (member.type == duckdb::CatalogType::TABLE_ENTRY &&
          OwnedBy(member, owned.oid) && SequenceOwnedByTable(owned, member)) {
        owner = member;
      }
    });
  return owner;
}

void RefuseDependents(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& entry,
  absl::FunctionRef<bool(const duckdb::CatalogEntry&)> refuses) {
  std::vector<std::string> lines;
  ScanDependents(transaction, entry,
                 [&](duckdb::CatalogEntry& dependent,
                     const duckdb::DependencyDependentFlags&) {
                   if (refuses(dependent)) {
                     lines.push_back(DependsLine(dependent, entry));
                   }
                 });
  if (!lines.empty()) {
    ThrowDependents(entry, lines);
  }
}

void FollowRename(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& entry,
                  const duckdb::Identifier& new_name,
                  std::vector<PendingDefault>& defaults) {
  const bool rewrites = KindOf(entry.type).has_value();
  const auto edges = DirectDependents(transaction, entry);
  RefuseUnfollowable(entry, edges, {}, rewrites);
  std::vector<std::string> triggers;
  for (const auto& edge : edges) {
    if (edge.dependent->type == duckdb::CatalogType::TRIGGER_ENTRY &&
        edge.part == Part::Owned) {
      triggers.push_back(DependsLine(*edge.dependent, entry));
    }
  }
  if (!triggers.empty()) {
    ThrowDependents(entry, triggers);
  }
  if (rewrites) {
    Follow(transaction, entry, entry.ParentSchema(transaction).name, new_name,
           defaults);
  }
}

void FollowSchemaMove(duckdb::CatalogTransaction transaction,
                      duckdb::CatalogEntry& entry,
                      const duckdb::Identifier& new_schema,
                      std::span<const duckdb::idx_t> moving,
                      std::vector<PendingDefault>& defaults) {
  const auto kind = KindOf(entry.type);
  const auto edges = DirectDependents(transaction, entry);
  RefuseUnfollowable(entry, edges, moving, kind.has_value());
  if (!kind) {
    return;
  }
  for (const auto& edge : edges) {
    if (!absl::c_linear_search(moving, edge.dependent->oid)) {
      Rewrite(transaction, edge, *kind, new_schema, entry.name, true, defaults);
    }
  }
}

void SetDefaults(duckdb::CatalogTransaction transaction,
                 duckdb::Catalog& catalog,
                 std::vector<PendingDefault>& defaults) {
  for (auto& pending : defaults) {
    auto table =
      catalog.Cast<BaseCatalog>().LookupOid(transaction, pending.table);
    SDB_ASSERT(table);
    auto& schema = table->ParentSchema(transaction);
    duckdb::SetDefaultInfo info{
      duckdb::AlterEntryData{schema.GetQualifiedName(table->name),
                             duckdb::OnEntryNotFound::THROW_EXCEPTION},
      duckdb::vector<duckdb::Identifier>{pending.column},
      std::move(pending.value)};
    schema.Alter(transaction, info);
  }
}

void FollowReferences(duckdb::CatalogTransaction transaction,
                      duckdb::CatalogEntry& table,
                      const duckdb::Identifier& old_schema,
                      const duckdb::Identifier& old_name) {
  if (table.type != duckdb::CatalogType::TABLE_ENTRY) {
    return;
  }
  const duckdb::IdentifierEquality equals{
    table.ParentCatalog().IsCaseSensitive()};
  const auto& schema = table.ParentSchema(transaction).name;
  const auto follow = [&](duckdb::CatalogEntry& referencing) {
    const auto type = &referencing == &table
                        ? duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE
                        : duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
    auto info = referencing.GetInfo();
    bool changed = false;
    for (auto& constraint : info->Cast<duckdb::CreateTableInfo>().constraints) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
      if (fk.info.type != type || !equals(fk.info.table, old_name) ||
          (!fk.info.schema.empty() && !equals(fk.info.schema, old_schema))) {
        continue;
      }
      const auto& new_schema =
        fk.info.schema.empty() &&
            type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE
          ? fk.info.schema
          : schema;
      if (fk.info.table.GetIdentifierName() == table.name.GetIdentifierName() &&
          fk.info.schema.GetIdentifierName() ==
            new_schema.GetIdentifierName()) {
        continue;
      }
      fk.info.schema = new_schema;
      fk.info.table = table.name;
      changed = true;
    }
    if (changed) {
      auto& parent = referencing.ParentSchema(transaction);
      info->SetQualifiedName(parent.GetQualifiedName(referencing.name));
      duckdb::ReplaceDefinitionInfo replace{std::move(info)};
      parent.Alter(transaction, replace);
    }
  };
  absl::flat_hash_set<duckdb::idx_t> seen;
  for (const auto& edge : DirectDependents(transaction, table)) {
    if (edge.part == Part::Constraint &&
        edge.dependent->type == duckdb::CatalogType::TABLE_ENTRY &&
        seen.insert(edge.dependent->oid).second) {
      follow(*edge.dependent);
    }
  }
  follow(table);
}

void FollowReferencedColumnRename(duckdb::CatalogTransaction transaction,
                                  duckdb::TableCatalogEntry& table,
                                  const duckdb::Identifier& old_column,
                                  const duckdb::Identifier& new_column) {
  const duckdb::IdentifierEquality equals{
    table.ParentCatalog().IsCaseSensitive()};
  irs::containers::FlatHashSet<duckdb::idx_t> seen;
  for (const auto& edge : DirectDependents(transaction, table)) {
    auto& referencing = *edge.dependent;
    if (edge.part != Part::Constraint ||
        referencing.type != duckdb::CatalogType::TABLE_ENTRY ||
        referencing.oid == table.oid || !seen.insert(referencing.oid).second) {
      continue;
    }
    auto info = referencing.GetInfo();
    bool changed = false;
    for (auto& constraint : info->Cast<duckdb::CreateTableInfo>().constraints) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
      if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
          ReferencedTableOid(referencing.Cast<duckdb::TableCatalogEntry>(),
                             fk) != table.oid) {
        continue;
      }
      for (auto& column : fk.pk_columns) {
        if (equals(column, old_column)) {
          column = new_column;
          changed = true;
        }
      }
    }
    if (changed) {
      auto& parent = referencing.ParentSchema(transaction);
      info->SetQualifiedName(parent.GetQualifiedName(referencing.name));
      duckdb::ReplaceDefinitionInfo replace{std::move(info)};
      parent.Alter(transaction, replace);
    }
  }
}

duckdb::idx_t ReferencedTableOid(const duckdb::TableCatalogEntry& table,
                                 const duckdb::ForeignKeyConstraint& fk) {
  if (fk.info.type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
    return table.oid;
  }
  const duckdb::SubDependency part{duckdb::AlterTableType::DROP_CONSTRAINT,
                                   duckdb::Identifier{fk.constraint_name}};
  for (const auto& dependency : table.dependencies.Set()) {
    if (dependency.entry.type == duckdb::CatalogType::TABLE_ENTRY &&
        dependency.subdependencies.count(part)) {
      return dependency.oid;
    }
  }
  return 0;
}

bool ReferencedByForeignKey(duckdb::CatalogTransaction transaction,
                            const duckdb::TableCatalogEntry& table) {
  bool referenced = false;
  table.catalog.Cast<BaseCatalog>().Oids().ForEachDependent(
    transaction, table.oid, [&](duckdb::CatalogEntry& member) {
      if (referenced || member.type != duckdb::CatalogType::TABLE_ENTRY ||
          member.oid == table.oid) {
        return;
      }
      const auto& referencing = member.Cast<duckdb::TableCatalogEntry>();
      referenced = absl::c_any_of(
        referencing.GetConstraints(), [&](const auto& constraint) {
          return constraint->type == duckdb::ConstraintType::FOREIGN_KEY &&
                 ReferencedTableOid(
                   referencing,
                   constraint->template Cast<duckdb::ForeignKeyConstraint>()) ==
                   table.oid;
        });
    });
  return referenced;
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
    if (dependent.type == duckdb::CatalogType::INDEX_ENTRY) {
      auto info = dependent.GetInfo();
      auto& index = info->Cast<duckdb::CreateIndexInfo>();
      const duckdb::IdentifierEquality equals{
        table.ParentCatalog().IsCaseSensitive()};
      bool renamed = false;
      const auto rename = [&](duckdb::ParsedExpression& expression) {
        duckdb::ParsedExpressionIterator::VisitExpressionMutable<
          duckdb::ColumnRefExpression>(
          expression, [&](duckdb::ColumnRefExpression& reference) {
            auto& names = reference.ColumnNamesMutable();
            if (equals(names.back(), old_column) &&
                (names.size() == 1 ||
                 equals(names[names.size() - 2], table.name))) {
              names.back() = new_column;
              renamed = true;
            }
          });
      };
      for (auto& expression : index.parsed_expressions) {
        rename(*expression);
      }
      if (index.where_clause) {
        rename(*index.where_clause);
      }
      if (renamed) {
        duckdb::ReplaceDefinitionInfo replace{std::move(info)};
        replace.allow_internal = true;
        schema.Alter(transaction, replace);
      }
      continue;
    }
    const auto refuse = [&] {
      auto& context = transaction.GetContext();
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
        ERR_MSG("cannot rename column ", old_column.GetIdentifierName(),
                " of table ", table.name.GetIdentifierName(),
                " because other objects depend on it"),
        ERR_DETAIL(pg::DescribeObject(context, dependent),
                   " refers to the column where its rename cannot be "
                   "followed"));
    };
    if (dependent.type == duckdb::CatalogType::VIEW_ENTRY) {
      auto info = dependent.GetInfo();
      auto& view = info->Cast<duckdb::CreateViewInfo>();
      rewriter.Visit(*view.query->node);
      if (rewriter.uncertain) {
        refuse();
      }
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
    if (rewriter.uncertain) {
      refuse();
    }
    if (rewriter.changed) {
      schema.Alter(transaction, *info->GetAlterInfo());
    }
  }
}

void DropColumnReaders(duckdb::CatalogTransaction transaction,
                       duckdb::TableCatalogEntry& table,
                       std::span<const duckdb::LogicalIndex> columns,
                       bool cascade) {
  std::vector<duckdb::CatalogEntry*> readers;
  std::vector<std::string> lines;
  auto& context = transaction.GetContext();
  for (const auto column : columns) {
    const auto& name = table.GetColumns().GetColumn(column).Name();
    absl::flat_hash_set<duckdb::idx_t> seen;
    for (const auto& edge : DirectDependents(transaction, table)) {
      auto& dependent = *edge.dependent;
      if (!seen.insert(dependent.oid).second) {
        continue;
      }
      ColumnRewriter reads{transaction, dependent, table, name, name};
      if (dependent.type == duckdb::CatalogType::VIEW_ENTRY) {
        auto info = dependent.GetInfo();
        reads.Visit(*info->Cast<duckdb::CreateViewInfo>().query->node);
      } else if (dependent.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
        auto info = dependent.GetInfo();
        for (auto& macro : info->Cast<duckdb::CreateMacroInfo>().macros) {
          reads.Visit(*macro->Cast<duckdb::TableMacroFunction>().query_node);
        }
      } else {
        continue;
      }
      if (!reads.changed && !reads.expands_all && !reads.uncertain) {
        continue;
      }
      lines.push_back(absl::StrCat(
        pg::DescribeObject(context, dependent), " depends on ",
        pg::DescribeObject(context, table,
                           static_cast<int32_t>(column.index + 1))));
      if (!absl::c_linear_search(readers, &dependent)) {
        readers.push_back(&dependent);
      }
    }
  }
  if (readers.empty()) {
    return;
  }
  if (!cascade) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop ",
              pg::DescribeObject(context, table,
                                 static_cast<int32_t>(columns[0].index + 1)),
              " because other objects depend on it"),
      ERR_DETAIL(absl::StrJoin(lines, "\n")),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }
  for (auto* reader : readers) {
    ConnectionContext::Get(context).AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
      ERR_MSG("drop cascades to ", pg::DescribeObject(context, *reader))));
    DropDependents(transaction, *reader, true);
    reader->ParentSchema(transaction).Cast<Schema>().Drop(transaction, *reader);
  }
}

void FollowColumnOids(duckdb::CatalogTransaction transaction,
                      const duckdb::TableCatalogEntry& previous,
                      const duckdb::TableCatalogEntry& current) {
  for (auto& owned : OwnedEntries(transaction, previous)) {
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
                        const duckdb::Identifier& new_name,
                        std::vector<PendingDefault>& defaults) {
  std::vector<duckdb::CatalogEntry*> members;
  schema.Cast<Schema>().ScanMembers(
    transaction, [&](duckdb::CatalogEntry& member) {
      if (!member.internal &&
          member.type != duckdb::CatalogType::SCHEMA_ENTRY) {
        members.push_back(&member);
      }
    });
  for (auto* member : members) {
    Follow(transaction, *member, new_name, member->name, defaults);
  }
}

}  // namespace sdb::catalog
