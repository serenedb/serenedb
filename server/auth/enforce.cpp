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

#include "auth/enforce.h"

#include <absl/strings/match.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_trigger_info.hpp>
#include <duckdb/parser/parsed_data/detach_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_copy_to_file.hpp>
#include <duckdb/planner/operator/logical_create.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_simple.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "catalog1/cluster.h"
#include "connector/duckdb_table_function.h"
#include "pg/commands/rbac.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"

namespace sdb::auth {
namespace {

using duckdb::AclMode;
using duckdb::CatalogType;
using duckdb::LogicalOperatorType;

std::string KindName(CatalogType type) {
  switch (type) {
    case CatalogType::TABLE_ENTRY:
      return "table";
    case CatalogType::VIEW_ENTRY:
      return "view";
    case CatalogType::SEQUENCE_ENTRY:
      return "sequence";
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return "function";
    case CatalogType::TYPE_ENTRY:
      return "type";
    case CatalogType::SCHEMA_ENTRY:
      return "schema";
    case CatalogType::DATABASE_ENTRY:
      return "database";
    case CatalogType::INDEX_ENTRY:
      return "index";
    case CatalogType::TOKENIZER_ENTRY:
      return "text search dictionary";
    case CatalogType::FOREIGN_SERVER_ENTRY:
      return "foreign server";
    default:
      return "object";
  }
}

[[noreturn]] void Denied(const duckdb::CatalogEntry& entry) {
  throw duckdb::PermissionException("permission denied for %s %s",
                                    KindName(entry.type),
                                    entry.name.GetIdentifierName());
}

[[noreturn]] void MustOwn(const duckdb::CatalogEntry& entry) {
  throw duckdb::PermissionException("must be owner of %s %s",
                                    KindName(entry.type),
                                    entry.name.GetIdentifierName());
}

bool Unowned(const duckdb::CatalogEntry& entry) {
  return entry.permissions.owner == pg::kInvalidOid;
}

CatalogType DefaultObjType(LogicalOperatorType type) {
  switch (type) {
    case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
      return CatalogType::SEQUENCE_ENTRY;
    case LogicalOperatorType::LOGICAL_CREATE_MACRO:
      return CatalogType::MACRO_ENTRY;
    case LogicalOperatorType::LOGICAL_CREATE_TYPE:
      return CatalogType::TYPE_ENTRY;
    case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
      return CatalogType::SCHEMA_ENTRY;
    case LogicalOperatorType::LOGICAL_CREATE_TRIGGER:
      return CatalogType::TRIGGER_ENTRY;
    default:
      return CatalogType::TABLE_ENTRY;
  }
}

void MergeGrant(duckdb::vector<duckdb::AclItem>& acl,
                const duckdb::AclItem& item) {
  auto it = std::ranges::find_if(acl, [&](const duckdb::AclItem& existing) {
    return existing.grantee == item.grantee && existing.grantor == item.grantor;
  });
  if (it == acl.end()) {
    acl.push_back(item);
    return;
  }
  it->privs |= item.privs;
  it->grant_option |= item.grant_option;
}

template<typename F>
void VisitExpressionTree(duckdb::Expression& expr, F& visit) {
  visit(expr);
  duckdb::ExpressionIterator::EnumerateChildren(
    expr,
    [&](duckdb::Expression& child) { VisitExpressionTree(child, visit); });
}

template<typename F>
void VisitOperatorExpressions(duckdb::LogicalOperator& op, F& visit) {
  duckdb::LogicalOperatorVisitor::EnumerateExpressions(
    op, [&](duckdb::unique_ptr<duckdb::Expression>* child) {
      VisitExpressionTree(**child, visit);
    });
}

std::vector<std::span<const duckdb::AclItem>> AllColumnAcls(
  const duckdb::TableCatalogEntry& table) {
  std::vector<std::span<const duckdb::AclItem>> acls;
  for (const auto& column : table.GetColumns().Logical()) {
    acls.push_back(column.Acl());
  }
  return acls;
}

bool IsRename(const duckdb::AlterInfo& info) {
  switch (info.type) {
    case duckdb::AlterType::ALTER_TABLE:
      return info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
             duckdb::AlterTableType::RENAME_TABLE;
    case duckdb::AlterType::ALTER_VIEW:
      return info.Cast<duckdb::AlterViewInfo>().alter_view_type ==
             duckdb::AlterViewType::RENAME_VIEW;
    case duckdb::AlterType::ALTER_SCALAR_FUNCTION:
      return info.Cast<duckdb::AlterScalarFunctionInfo>()
               .alter_scalar_function_type ==
             duckdb::AlterScalarFunctionType::RENAME_SCALAR_FUNCTION;
    default:
      return false;
  }
}

class Enforcer {
 public:
  Enforcer(duckdb::ClientContext& context, ConnectionContext& connection,
           duckdb::Binder& binder, duckdb::LogicalOperator& root)
    : _context{context},
      _connection{connection},
      _props{binder.GetStatementProperties()},
      _root{root},
      _caller{connection.GetRoleId()},
      _roles{RolesOf(&context)},
      _caller_closure{ComputeRoleClosure(*_roles, _caller)},
      _enforce{!_caller_closure.is_superuser} {}

  void Run() {
    if (_enforce) {
      CheckViews();
    }
    Collect(_root);
    Check(_root);
    if (!_enforce) {
      return;
    }
    _props.RegisterDBRead(catalog::ClusterOf(_context), _context);
    CheckResolved();
    CheckReturning();
    if (_file_copy) {
      throw duckdb::PermissionException("permission denied to COPY to a file");
    }
  }

 private:
  void Collect(duckdb::LogicalOperator& op) {
    switch (op.type) {
      case LogicalOperatorType::LOGICAL_UPDATE: {
        auto& update = op.Cast<duckdb::LogicalUpdate>();
        _dml_tables.emplace(update.table_index.index, &update.table);
        MarkTargetScans(op, update.table);
        break;
      }
      case LogicalOperatorType::LOGICAL_DELETE: {
        auto& del = op.Cast<duckdb::LogicalDelete>();
        _dml_tables.emplace(del.table_index.index, &del.table);
        MarkTargetScans(op, del.table);
        break;
      }
      case LogicalOperatorType::LOGICAL_MERGE_INTO: {
        auto& merge = op.Cast<duckdb::LogicalMergeInto>();
        _dml_tables.emplace(merge.table_index.index, &merge.table);
        MarkTargetScans(op, merge.table);
        break;
      }
      case LogicalOperatorType::LOGICAL_INSERT: {
        auto& insert = op.Cast<duckdb::LogicalInsert>();
        _dml_tables.emplace(insert.table_index.index, &insert.table);
        break;
      }
      default:
        break;
    }
    for (auto& child : op.children) {
      Collect(*child);
    }
  }

  void MarkTargetScans(duckdb::LogicalOperator& op,
                       const duckdb::TableCatalogEntry& table) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
      auto& get = op.Cast<duckdb::LogicalGet>();
      if (get.GetTable().get() == &table) {
        _target_scans.emplace(get.table_index.index, &get);
      }
    }
    for (auto& child : op.children) {
      MarkTargetScans(*child, table);
    }
  }

  void Check(duckdb::LogicalOperator& op) {
    if (op.type == LogicalOperatorType::LOGICAL_UPDATE &&
        !op.children.empty() &&
        op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
      auto& update = op.Cast<duckdb::LogicalUpdate>();
      const auto& projection =
        op.children[0]->Cast<duckdb::LogicalProjection>();
      auto& positions = _projection_reads[projection.table_index.index];
      for (duckdb::idx_t i = 0; i < update.expressions.size(); ++i) {
        auto collect = [&](duckdb::Expression& expr) {
          if (expr.GetExpressionType() !=
              duckdb::ExpressionType::BOUND_COLUMN_REF) {
            return;
          }
          const auto& binding =
            expr.Cast<duckdb::BoundColumnRefExpression>().Binding();
          if (binding.table_index.index != projection.table_index.index) {
            return;
          }
          const auto position = binding.column_index.GetIndex();
          if (i < update.columns.size() &&
              IsPassthrough(projection, position, update.table,
                            update.columns[i])) {
            return;
          }
          positions.insert(position);
        };
        VisitExpressionTree(*update.expressions[i], collect);
      }
    }
    auto visit = [&](duckdb::Expression& expr) {
      switch (expr.GetExpressionType()) {
        case duckdb::ExpressionType::BOUND_COLUMN_REF: {
          const auto& binding =
            expr.Cast<duckdb::BoundColumnRefExpression>().Binding();
          if (auto it = _dml_tables.find(binding.table_index.index);
              it != _dml_tables.end()) {
            _returning[it->second].insert(binding.column_index.GetIndex());
          }
          _scan_refs[binding.table_index.index].insert(
            binding.column_index.GetIndex());
          break;
        }
        case duckdb::ExpressionType::BOUND_FUNCTION: {
          const auto& name = expr.Cast<duckdb::BoundFunctionExpression>()
                               .Function()
                               .GetName()
                               .GetIdentifierName();
          if (name == "nextval") {
            _nextval = true;
          } else if (name == "currval") {
            _currval = true;
          }
          break;
        }
        default:
          break;
      }
    };
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION &&
        _projection_reads.contains(
          op.Cast<duckdb::LogicalProjection>().table_index.index)) {
      const auto& positions = _projection_reads.at(
        op.Cast<duckdb::LogicalProjection>().table_index.index);
      for (duckdb::idx_t i = 0; i < op.expressions.size(); ++i) {
        if (positions.contains(i)) {
          VisitExpressionTree(*op.expressions[i], visit);
        }
      }
    } else {
      VisitOperatorExpressions(op, visit);
    }

    switch (op.type) {
      case LogicalOperatorType::LOGICAL_GET:
        if (_enforce) {
          CheckGet(op.Cast<duckdb::LogicalGet>());
        }
        break;
      case LogicalOperatorType::LOGICAL_INSERT:
        if (_enforce) {
          CheckInsert(op.Cast<duckdb::LogicalInsert>());
        }
        break;
      case LogicalOperatorType::LOGICAL_UPDATE:
        if (_enforce) {
          CheckUpdate(op.Cast<duckdb::LogicalUpdate>());
        }
        break;
      case LogicalOperatorType::LOGICAL_DELETE:
        if (_enforce) {
          auto& del = op.Cast<duckdb::LogicalDelete>();
          RequireTablePrivilege(
            del.table, del.is_truncate ? AclMode::Truncate : AclMode::Delete);
        }
        break;
      case LogicalOperatorType::LOGICAL_MERGE_INTO:
        if (_enforce) {
          CheckMerge(op.Cast<duckdb::LogicalMergeInto>());
        }
        break;
      case LogicalOperatorType::LOGICAL_CREATE_TABLE: {
        auto& create = op.Cast<duckdb::LogicalCreateTable>();
        auto& info = create.info->base->Cast<duckdb::CreateTableInfo>();
        Stamp(info, CatalogType::TABLE_ENTRY, &create.schema);
        if (_enforce) {
          RequireSchemaCreate(create.schema);
          CheckForeignKeys(info, create.schema);
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_VIEW:
      case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
      case LogicalOperatorType::LOGICAL_CREATE_MACRO:
      case LogicalOperatorType::LOGICAL_CREATE_TYPE: {
        auto& create = op.Cast<duckdb::LogicalCreate>();
        Stamp(*create.info, DefaultObjType(op.type), create.schema);
        if (_enforce && create.schema) {
          RequireSchemaCreate(*create.schema);
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_TRIGGER: {
        auto& create = op.Cast<duckdb::LogicalCreate>();
        Stamp(*create.info, CatalogType::TRIGGER_ENTRY, create.schema);
        if (_enforce) {
          CheckCreateTrigger(create.info->Cast<duckdb::CreateTriggerInfo>());
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_SCHEMA: {
        auto& create = op.Cast<duckdb::LogicalCreate>();
        Stamp(*create.info, CatalogType::SCHEMA_ENTRY, nullptr);
        if (_enforce) {
          RequireDatabasePrivilege(AclMode::Create);
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_INDEX:
        if (_enforce) {
          RequireOwner(op.Cast<duckdb::LogicalCreateIndex>().table);
        }
        break;
      case LogicalOperatorType::LOGICAL_DROP:
        if (_enforce) {
          CheckDrop(
            op.Cast<duckdb::LogicalSimple>().info->Cast<duckdb::DropInfo>());
        }
        break;
      case LogicalOperatorType::LOGICAL_ALTER: {
        auto& info =
          op.Cast<duckdb::LogicalSimple>().info->Cast<duckdb::AlterInfo>();
        if (info.type == duckdb::AlterType::ALTER_PERMISSIONS) {
          ResolvePermissions(info.Cast<duckdb::AlterPermissionsInfo>());
        } else if (info.type == duckdb::AlterType::ALTER_ROLE) {
          pg::ResolveAlterRole(_context, info.Cast<duckdb::AlterRoleInfo>());
        } else if (_enforce) {
          CheckAlter(info);
        }
        const auto type = info.GetCatalogType();
        if (type == CatalogType::DATABASE_ENTRY ||
            type == CatalogType::ROLE_ENTRY) {
          info.GetQualifiedNameMutable() = duckdb::QualifiedName{
            duckdb::Identifier{catalog::ClusterCatalog::kDatabaseName},
            duckdb::Identifier{}, info.GetQualifiedName().Name()};
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_ATTACH:
        if (_enforce && !_caller_closure.Has(catalog::RoleOption::CreateDb)) {
          throw duckdb::PermissionException(
            "permission denied to create database");
        }
        break;
      case LogicalOperatorType::LOGICAL_DETACH:
        if (_enforce) {
          RequireDatabaseOwner(op.Cast<duckdb::LogicalSimple>()
                                 .info->Cast<duckdb::DetachInfo>()
                                 .name.GetIdentifierName());
        }
        break;
      case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
        if (_enforce &&
            op.Cast<duckdb::LogicalCopyToFile>().file_path != "/dev/stdout") {
          _file_copy = true;
        }
        break;
      case LogicalOperatorType::LOGICAL_EXPORT:
      case LogicalOperatorType::LOGICAL_LOAD:
      case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
      case LogicalOperatorType::LOGICAL_CREATE_SECRET:
        if (_enforce) {
          throw duckdb::PermissionException(
            "permission denied: superuser required");
        }
        break;
      default:
        break;
    }
    for (auto& child : op.children) {
      Check(*child);
    }
  }

  const RoleClosure& ClosureOf(duckdb::idx_t principal) {
    if (principal == _caller) {
      return _caller_closure;
    }
    auto [it, inserted] = _closures.try_emplace(principal);
    if (inserted) {
      it->second = ComputeRoleClosure(*_roles, principal);
    }
    return it->second;
  }

  duckdb::idx_t PrincipalFor(duckdb::idx_t table_index) const {
    const duckdb::StatementProperties::ViewScope* inner = nullptr;
    for (const auto& scope : _props.view_scopes) {
      if (scope.begin <= table_index && table_index < scope.end &&
          (!inner || scope.end - scope.begin < inner->end - inner->begin)) {
        inner = &scope;
      }
    }
    if (!inner || inner->view->security_invoker) {
      return _caller;
    }
    return inner->view->permissions.owner;
  }

  duckdb::idx_t EnclosingPrincipal(
    const duckdb::StatementProperties::ViewScope& scope) const {
    const duckdb::StatementProperties::ViewScope* inner = nullptr;
    for (const auto& other : _props.view_scopes) {
      if (other.view == scope.view && other.begin == scope.begin &&
          other.end == scope.end) {
        continue;
      }
      if (other.begin <= scope.begin && scope.end <= other.end &&
          (!inner || other.end - other.begin < inner->end - inner->begin)) {
        inner = &other;
      }
    }
    if (!inner || inner->view->security_invoker) {
      return _caller;
    }
    return inner->view->permissions.owner;
  }

  bool IsPassthrough(const duckdb::LogicalProjection& projection,
                     duckdb::idx_t position,
                     const duckdb::TableCatalogEntry& table,
                     duckdb::PhysicalIndex target) const {
    if (position >= projection.expressions.size()) {
      return false;
    }
    const auto& expr = *projection.expressions[position];
    if (expr.GetExpressionType() != duckdb::ExpressionType::BOUND_COLUMN_REF) {
      return false;
    }
    const auto& binding =
      expr.Cast<duckdb::BoundColumnRefExpression>().Binding();
    const auto get = _target_scans.find(binding.table_index.index);
    if (get == _target_scans.end()) {
      return false;
    }
    const auto& column_ids = get->second->GetColumnIds();
    const auto index = binding.column_index.GetIndex();
    if (index >= column_ids.size() || column_ids[index].IsRowIdColumn() ||
        column_ids[index].IsVirtualColumn()) {
      return false;
    }
    const auto& column = table.GetColumns().GetColumn(
      duckdb::LogicalIndex(column_ids[index].GetPrimaryIndex()));
    return !column.Generated() && column.Physical() == target;
  }

  void CheckIndexScan(duckdb::LogicalGet& get) {
    if (get.function.name != "iresearch_scan" || !get.bind_data) {
      return;
    }
    const auto& bind = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (!bind.IsViewBacked() || !bind.inverted_index) {
      return;
    }
    const auto& index = *bind.inverted_index;
    auto view =
      index.schema.GetEntry(index.catalog.GetCatalogTransaction(_context),
                            CatalogType::TABLE_ENTRY, index.GetTableName());
    if (!view || view->type != CatalogType::VIEW_ENTRY || Unowned(*view)) {
      return;
    }
    if (!ClosureOf(PrincipalFor(get.table_index.index))
           .Can(CatalogType::TABLE_ENTRY, view->permissions, AclMode::Select)) {
      Denied(*view);
    }
  }

  void CheckGet(duckdb::LogicalGet& get) {
    auto table = get.GetTable();
    if (!table) {
      CheckIndexScan(get);
      return;
    }
    if (Unowned(*table)) {
      return;
    }
    const auto& closure = ClosureOf(PrincipalFor(get.table_index.index));
    const bool target = _target_scans.contains(get.table_index.index);
    const auto& column_ids = get.GetColumnIds();
    std::vector<std::span<const duckdb::AclItem>> acls;
    const auto add = [&](const duckdb::ColumnIndex& column) {
      if (column.IsRowIdColumn() || column.IsVirtualColumn()) {
        return;
      }
      acls.push_back(
        table->GetColumns()
          .GetColumn(duckdb::LogicalIndex(column.GetPrimaryIndex()))
          .Acl());
    };
    if (target) {
      if (auto it = _scan_refs.find(get.table_index.index);
          it != _scan_refs.end()) {
        for (const auto position : it->second) {
          if (position < column_ids.size()) {
            add(column_ids[position]);
          }
        }
      }
      if (acls.empty()) {
        return;
      }
    } else {
      for (const auto& column : column_ids) {
        add(column);
      }
      if (acls.empty()) {
        if (!closure.CanAnyColumn(table->permissions, AclMode::Select,
                                  AllColumnAcls(*table))) {
          Denied(*table);
        }
        return;
      }
    }
    if (!closure.CanColumns(table->permissions, AclMode::Select, acls)) {
      Denied(*table);
    }
  }

  static bool ReferencesColumns(duckdb::Expression& expr) {
    bool found = false;
    auto visit = [&](duckdb::Expression& node) {
      found |=
        node.GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF;
    };
    VisitExpressionTree(expr, visit);
    return found;
  }

  void CheckInsert(duckdb::LogicalInsert& insert) {
    auto& table = insert.table;
    const auto& columns = table.GetColumns();
    std::vector<std::span<const duckdb::AclItem>> acls;
    auto* source = insert.children.empty() ? nullptr : insert.children[0].get();
    const bool per_column =
      source && source->type == LogicalOperatorType::LOGICAL_PROJECTION &&
      source->expressions.size() == columns.PhysicalColumnCount();
    duckdb::idx_t position = 0;
    for (const auto& column : columns.Physical()) {
      if (!per_column || ReferencesColumns(*source->expressions[position])) {
        acls.push_back(column.Acl());
      }
      ++position;
    }
    if (!_caller_closure.CanColumns(table.permissions, AclMode::Insert, acls)) {
      Denied(table);
    }
    if (insert.on_conflict_info.set_columns.empty()) {
      return;
    }
    std::vector<std::span<const duckdb::AclItem>> updated;
    for (const auto index : insert.on_conflict_info.set_columns) {
      updated.push_back(columns.GetColumn(index).Acl());
    }
    if (!_caller_closure.CanColumns(table.permissions, AclMode::Update,
                                    updated)) {
      Denied(table);
    }
  }

  void CheckUpdate(duckdb::LogicalUpdate& update) {
    auto& table = update.table;
    if (update.update_is_del_and_insert) {
      RequireTablePrivilege(table, AclMode::Update);
      return;
    }
    std::vector<std::span<const duckdb::AclItem>> acls;
    for (const auto index : update.columns) {
      acls.push_back(table.GetColumns().GetColumn(index).Acl());
    }
    if (!_caller_closure.CanColumns(table.permissions, AclMode::Update, acls)) {
      Denied(table);
    }
  }

  void CheckMerge(duckdb::LogicalMergeInto& merge) {
    for (const auto& [condition, actions] : merge.actions) {
      for (const auto& action : actions) {
        switch (action->action_type) {
          case duckdb::MergeActionType::MERGE_UPDATE:
            RequireTablePrivilege(merge.table, AclMode::Update);
            break;
          case duckdb::MergeActionType::MERGE_DELETE:
            RequireTablePrivilege(merge.table, AclMode::Delete);
            break;
          case duckdb::MergeActionType::MERGE_INSERT:
            RequireTablePrivilege(merge.table, AclMode::Insert);
            break;
          default:
            break;
        }
      }
    }
  }

  void CheckViews() {
    auto scopes = _props.view_scopes;
    std::ranges::sort(scopes, [](const auto& lhs, const auto& rhs) {
      return lhs.begin != rhs.begin ? lhs.begin < rhs.begin : lhs.end > rhs.end;
    });
    for (const auto& scope : scopes) {
      const auto& view = *scope.view;
      if (Unowned(view)) {
        continue;
      }
      if (!ClosureOf(EnclosingPrincipal(scope))
             .Can(CatalogType::TABLE_ENTRY, view.permissions,
                  AclMode::Select)) {
        Denied(view);
      }
    }
  }

  void CheckResolved() {
    const bool defines_relation =
      _root.type == LogicalOperatorType::LOGICAL_CREATE_TABLE ||
      _root.type == LogicalOperatorType::LOGICAL_ALTER;
    containers::FlatHashSet<const duckdb::CatalogEntry*> seen;
    if (_root.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
      const auto& dependencies =
        _root.Cast<duckdb::LogicalCreateTable>().info->dependencies.Set();
      for (const auto& dependency : dependencies) {
        if (dependency.entry.type != CatalogType::TYPE_ENTRY) {
          continue;
        }
        auto entry = duckdb::Catalog::GetEntry(
          _context,
          duckdb::EntryLookupInfo{
            CatalogType::TYPE_ENTRY,
            duckdb::QualifiedName{dependency.catalog, dependency.entry.schema,
                                  dependency.entry.name}},
          duckdb::OnEntryNotFound::RETURN_NULL);
        if (!entry || Unowned(*entry) || !seen.insert(entry.get()).second) {
          continue;
        }
        if (!_caller_closure.Can(entry->type, entry->permissions,
                                 AclMode::Usage)) {
          Denied(*entry);
        }
      }
    }
    for (const auto* entry : _props.resolved_entries) {
      if (!seen.insert(entry).second || Unowned(*entry)) {
        continue;
      }
      switch (entry->type) {
        case CatalogType::MACRO_ENTRY:
        case CatalogType::TABLE_MACRO_ENTRY:
          if (!_caller_closure.Can(entry->type, entry->permissions,
                                   AclMode::Execute)) {
            Denied(*entry);
          }
          break;
        case CatalogType::TYPE_ENTRY:
          if (defines_relation &&
              !_caller_closure.Can(entry->type, entry->permissions,
                                   AclMode::Usage)) {
            Denied(*entry);
          }
          break;
        case CatalogType::SEQUENCE_ENTRY:
          if (_nextval &&
              !_caller_closure.CanAny(entry->type, entry->permissions,
                                      AclMode::Usage | AclMode::Update)) {
            Denied(*entry);
          }
          if (_currval &&
              !_caller_closure.CanAny(entry->type, entry->permissions,
                                      AclMode::Usage | AclMode::Select)) {
            Denied(*entry);
          }
          break;
        default:
          break;
      }
    }
  }

  void CheckReturning() {
    for (const auto& [table, columns] : _returning) {
      const auto& list = table->GetColumns();
      std::vector<std::span<const duckdb::AclItem>> acls;
      for (const auto column : columns) {
        if (column < list.PhysicalColumnCount()) {
          acls.push_back(list.GetColumn(duckdb::PhysicalIndex(column)).Acl());
        }
      }
      if (!acls.empty() && !_caller_closure.CanColumns(table->permissions,
                                                       AclMode::Select, acls)) {
        Denied(*table);
      }
    }
  }

  void CheckForeignKeys(const duckdb::CreateTableInfo& info,
                        const duckdb::SchemaCatalogEntry& schema) {
    for (const auto& constraint : info.constraints) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
      if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
        continue;
      }
      const auto parent_schema = fk.info.schema.GetIdentifierName().empty()
                                   ? schema.name
                                   : fk.info.schema;
      auto parent = duckdb::Catalog::GetEntry(
        _context,
        duckdb::EntryLookupInfo(
          CatalogType::TABLE_ENTRY,
          duckdb::QualifiedName(schema.catalog.GetName(), parent_schema,
                                fk.info.table)),
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!parent || parent->type != CatalogType::TABLE_ENTRY) {
        continue;
      }
      auto& table = parent->Cast<duckdb::TableCatalogEntry>();
      std::vector<std::span<const duckdb::AclItem>> acls;
      for (const auto& column : fk.pk_columns) {
        duckdb::Identifier name = column;
        acls.push_back(table.GetColumn(table.GetColumnIndex(name)).Acl());
      }
      if (!_caller_closure.CanColumns(table.permissions, AclMode::References,
                                      acls)) {
        Denied(table);
      }
    }
  }

  void CheckCreateTrigger(const duckdb::CreateTriggerInfo& info) {
    auto entry = duckdb::Catalog::GetEntry(
      _context,
      duckdb::EntryLookupInfo(CatalogType::TABLE_ENTRY,
                              info.base_table->GetQualifiedName()),
      duckdb::OnEntryNotFound::RETURN_NULL);
    if (!entry) {
      return;
    }
    if (!_caller_closure.Owns(entry->permissions.owner) &&
        !_caller_closure.Can(CatalogType::TABLE_ENTRY, entry->permissions,
                             AclMode::Trigger)) {
      Denied(*entry);
    }
  }

  static bool IsSchemaScoped(CatalogType type) {
    switch (type) {
      case CatalogType::TABLE_ENTRY:
      case CatalogType::VIEW_ENTRY:
      case CatalogType::SEQUENCE_ENTRY:
      case CatalogType::TYPE_ENTRY:
      case CatalogType::MACRO_ENTRY:
      case CatalogType::TABLE_MACRO_ENTRY:
      case CatalogType::INDEX_ENTRY:
      case CatalogType::TRIGGER_ENTRY:
      case CatalogType::TOKENIZER_ENTRY:
        return true;
      default:
        return false;
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> FindEntry(
    CatalogType type, const duckdb::QualifiedName& name) {
    return duckdb::Catalog::GetEntry(_context,
                                     duckdb::EntryLookupInfo(type, name),
                                     duckdb::OnEntryNotFound::RETURN_NULL);
  }

  void CheckDrop(const duckdb::DropInfo& info) {
    const auto& name = info.GetQualifiedName();
    if (info.type == CatalogType::SCHEMA_ENTRY) {
      auto schema =
        duckdb::Catalog::GetSchema(_context, name.Catalog(), name.Name(),
                                   duckdb::OnEntryNotFound::RETURN_NULL);
      if (schema) {
        RequireOwner(*schema);
      }
      return;
    }
    if (!IsSchemaScoped(info.type)) {
      return;
    }
    if (auto entry = FindEntry(info.type, name)) {
      RequireOwner(*entry);
    }
  }

  void CheckAlter(const duckdb::AlterInfo& info) {
    const auto type = info.GetCatalogType();
    if (!IsSchemaScoped(type)) {
      return;
    }
    auto entry = FindEntry(type, info.GetQualifiedName());
    if (!entry) {
      return;
    }
    RequireOwner(*entry);
    if (IsRename(info)) {
      RequireSchemaCreate(entry->ParentSchema());
    }
  }

  void ResolvePermissions(duckdb::AlterPermissionsInfo& info) {
    if (!info.new_owner.empty()) {
      ResolveOwner(info);
    } else if (info.default_objtype != CatalogType::INVALID) {
      ResolveDefaultPrivileges(info);
    } else {
      ResolveGrant(info);
    }
  }

  [[noreturn]] static void NotSupported(std::string_view what) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG(what, " is not supported"));
  }

  duckdb::idx_t RoleId(std::string_view name) {
    auto& cluster = catalog::ClusterOf(_context);
    auto role = cluster.LookupRole(cluster.GetCatalogTransaction(_context),
                                   duckdb::Identifier{std::string{name}});
    if (!role) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", name, "\" does not exist"));
    }
    return role->oid;
  }

  duckdb::idx_t GranteeId(const std::string& name) {
    return name == "PUBLIC" ? duckdb::ACL_ID_PUBLIC : RoleId(name);
  }

  duckdb::idx_t RoleSpecId(const std::string& name) {
    if (absl::EqualsIgnoreCase(name, "CURRENT_USER") ||
        absl::EqualsIgnoreCase(name, "CURRENT_ROLE")) {
      return _caller;
    }
    if (absl::EqualsIgnoreCase(name, "SESSION_USER")) {
      return _connection.GetSessionRoleId();
    }
    return RoleId(name);
  }

  void RequireGrantable(const duckdb::CatalogEntry& entry) {
    const auto& perm = entry.permissions;
    if (!_enforce || _caller_closure.Owns(perm.owner)) {
      return;
    }
    const auto stored =
      perm.acl.empty()
        ? duckdb::Permissions::AclDefault(
            duckdb::Permissions::AclClass(entry.type), perm.owner)
        : perm.acl;
    auto held = _caller_closure.HeldModes(stored) |
                _caller_closure.GrantableModes(stored);
    if (entry.type == CatalogType::TABLE_ENTRY) {
      for (const auto& column :
           entry.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
        held |= _caller_closure.HeldModes(column.Acl()) |
                _caller_closure.GrantableModes(column.Acl());
      }
    }
    if (held == AclMode::NoRights) {
      Denied(entry);
    }
  }

  duckdb::CatalogEntry& ResolveTarget(duckdb::AlterPermissionsInfo& info) {
    const auto& name = info.GetQualifiedName();
    auto entry = FindEntry(info.entry_catalog_type, name);
    if (!entry && info.entry_catalog_type == CatalogType::MACRO_ENTRY) {
      entry = FindEntry(CatalogType::TABLE_MACRO_ENTRY, name);
    }
    if (!entry) {
      const bool relation = info.entry_catalog_type == CatalogType::TABLE_ENTRY;
      THROW_SQL_ERROR(
        ERR_CODE(relation ? ERRCODE_UNDEFINED_TABLE : ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG(relation ? "relation" : KindName(info.entry_catalog_type),
                " \"", name.ToString(), "\" does not exist"));
    }
    info.entry_catalog_type = entry->type == CatalogType::VIEW_ENTRY
                                ? CatalogType::TABLE_ENTRY
                                : entry->type;
    info.SetQualifiedName(entry->ParentCatalog().GetName(),
                          entry->ParentSchema().name, entry->name);
    return *entry;
  }

  void ResolveGrant(duckdb::AlterPermissionsInfo& info) {
    if (info.entry_catalog_type == CatalogType::SCHEMA_ENTRY) {
      NotSupported("GRANT ON SCHEMA");
    }
    if (info.entry_catalog_type == CatalogType::FOREIGN_SERVER_ENTRY) {
      NotSupported("GRANT ON FOREIGN SERVER");
    }
    info.grantee_id = GranteeId(info.grantee);
    if (!info.granted_by.empty()) {
      const auto granted_by = RoleId(info.granted_by);
      if (_enforce && !_caller_closure.MemberOf(granted_by)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
          ERR_MSG("must be member of role \"", info.granted_by, "\""));
      }
      info.grantors = {granted_by};
    } else if (_enforce) {
      info.grantors.push_back(_caller);
      for (const auto role : _caller_closure.closure) {
        if (role != _caller) {
          info.grantors.push_back(role);
        }
      }
    }
    if (info.entry_catalog_type == CatalogType::DATABASE_ENTRY) {
      const auto& name = info.GetQualifiedName().Name().GetIdentifierName();
      const auto database = DatabaseEntry(name);
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database \"", name, "\" does not exist"));
      }
      RequireGrantable(*database);
      return;
    }
    RequireGrantable(ResolveTarget(info));
  }

  void ResolveOwner(duckdb::AlterPermissionsInfo& info) {
    if (info.entry_catalog_type == CatalogType::SCHEMA_ENTRY) {
      NotSupported("ALTER SCHEMA OWNER");
    }
    auto& entry = ResolveTarget(info);
    info.new_owner_id = RoleSpecId(info.new_owner);
    if (!_enforce || info.new_owner_id == entry.permissions.owner) {
      return;
    }
    RequireOwner(entry);
    if (info.new_owner_id != _caller &&
        !_caller_closure.CanSet(info.new_owner_id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("must be able to SET ROLE \"",
                              _roles->NameOf(info.new_owner_id), "\""));
    }
    if (!ClosureOf(info.new_owner_id)
           .Can(CatalogType::SCHEMA_ENTRY, entry.ParentSchema().permissions,
                AclMode::Create)) {
      Denied(entry.ParentSchema());
    }
  }

  void ResolveDefaultPrivileges(duckdb::AlterPermissionsInfo& info) {
    if (!info.default_schema.empty()) {
      NotSupported("ALTER DEFAULT PRIVILEGES IN SCHEMA");
    }
    info.target_role = info.for_role.empty() ? _caller : RoleId(info.for_role);
    if (_enforce && !_caller_closure.MemberOf(info.target_role)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied to change default privileges"));
    }
    info.grantee_id = GranteeId(info.grantee);
    const auto& database = _connection.GetDatabase();
    if (!DatabaseEntry(database)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                      ERR_MSG("database \"", database, "\" does not exist"));
    }
    info.SetQualifiedName(duckdb::Identifier{}, duckdb::Identifier{},
                          duckdb::Identifier{database});
  }

  void RequireTablePrivilege(const duckdb::TableCatalogEntry& table,
                             AclMode need) {
    if (!_caller_closure.Can(CatalogType::TABLE_ENTRY, table.permissions,
                             need)) {
      Denied(table);
    }
  }

  void RequireSchemaCreate(const duckdb::SchemaCatalogEntry& schema) {
    if (!_caller_closure.Can(CatalogType::SCHEMA_ENTRY, schema.permissions,
                             AclMode::Create)) {
      Denied(schema);
    }
  }

  void RequireOwner(const duckdb::CatalogEntry& entry) {
    if (entry.type == CatalogType::INDEX_ENTRY) {
      auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      auto host =
        index.schema.GetEntry(index.catalog.GetCatalogTransaction(_context),
                              CatalogType::TABLE_ENTRY, index.GetTableName());
      if (host && !_caller_closure.Owns(host->permissions.owner)) {
        MustOwn(*host);
      }
      return;
    }
    if (!_caller_closure.Owns(entry.permissions.owner)) {
      MustOwn(entry);
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> DatabaseEntry(
    std::string_view name) {
    auto& cluster = catalog::ClusterOf(_context);
    return cluster.LookupDatabase(cluster.GetCatalogTransaction(_context),
                                  duckdb::Identifier{std::string{name}});
  }

  void RequireDatabasePrivilege(AclMode need) {
    auto database = DatabaseEntry(_connection.GetDatabase());
    if (database && !_caller_closure.Can(CatalogType::DATABASE_ENTRY,
                                         database->permissions, need)) {
      Denied(*database);
    }
  }

  void RequireDatabaseOwner(std::string_view name) {
    auto database = DatabaseEntry(name);
    if (database && !_caller_closure.Owns(database->permissions.owner)) {
      MustOwn(*database);
    }
  }

  void Stamp(duckdb::CreateInfo& info, CatalogType objtype,
             duckdb::optional_ptr<duckdb::SchemaCatalogEntry> schema) {
    info.permissions.owner = _caller;
    duckdb::vector<duckdb::AclItem> acl;
    const auto apply = [&](const duckdb::Permissions& holder) {
      for (const auto& defaults : holder.defaults) {
        if (defaults.role != _caller || defaults.objtype != objtype) {
          continue;
        }
        if (acl.empty()) {
          acl = duckdb::Permissions::AclDefault(objtype, _caller);
        }
        for (const auto& item : defaults.acl) {
          MergeGrant(acl, item);
        }
      }
    };
    if (schema) {
      apply(schema->permissions);
    }
    if (auto database = DatabaseEntry(_connection.GetDatabase())) {
      apply(database->permissions);
    }
    if (!acl.empty()) {
      info.permissions.acl = std::move(acl);
    }
  }

  duckdb::ClientContext& _context;
  ConnectionContext& _connection;
  duckdb::StatementProperties& _props;
  duckdb::LogicalOperator& _root;
  const duckdb::idx_t _caller;
  std::shared_ptr<const RoleGraph> _roles;
  RoleClosure _caller_closure;
  const bool _enforce;
  containers::NodeHashMap<duckdb::idx_t, RoleClosure> _closures;
  containers::FlatHashMap<duckdb::idx_t, const duckdb::LogicalGet*>
    _target_scans;
  containers::FlatHashMap<duckdb::idx_t, const duckdb::TableCatalogEntry*>
    _dml_tables;
  containers::FlatHashMap<const duckdb::TableCatalogEntry*,
                          containers::FlatHashSet<duckdb::idx_t>>
    _returning;
  containers::FlatHashMap<duckdb::idx_t, containers::FlatHashSet<duckdb::idx_t>>
    _scan_refs;
  containers::FlatHashMap<duckdb::idx_t, containers::FlatHashSet<duckdb::idx_t>>
    _projection_reads;
  bool _file_copy = false;
  bool _nextval = false;
  bool _currval = false;
};

}  // namespace

void EnforcePlan(duckdb::ClientContext& context, ConnectionContext& connection,
                 duckdb::Binder& binder, duckdb::LogicalOperator& plan) {
  if (connection.IsStorageConnection()) {
    return;
  }
  Enforcer{context, connection, binder, plan}.Run();
}

}  // namespace sdb::auth
