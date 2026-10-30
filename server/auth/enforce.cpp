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

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/function_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/create_role_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/detach_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_alter.hpp>
#include <duckdb/planner/operator/logical_attach.hpp>
#include <duckdb/planner/operator/logical_create.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_detach.hpp>
#include <duckdb/planner/operator/logical_drop.hpp>
#include <duckdb/planner/operator/logical_execute.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_vacuum.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "query/config.h"

namespace sdb::auth {
namespace {

using duckdb::AclMode;
using duckdb::CatalogType;
using duckdb::LogicalOperatorType;

[[noreturn]] void MustOwn(const duckdb::CatalogEntry& entry) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("must be owner of ", duckdb::PermissionKindName(entry.type), " ",
            entry.name.GetIdentifierName()));
}

void RequireUserSchema(duckdb::idx_t schema, const duckdb::CreateInfo& info) {
  if (const auto* system =
        pg::FindSystemObject(pg::kSystemNamespaces, schema)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to create \"", system->name, ".",
              info.GetQualifiedName().Name().GetIdentifierName(), "\""),
      ERR_DETAIL("System catalog modifications are currently disallowed."));
  }
}

[[noreturn]] void DenyRoleAction(std::string_view verb,
                                 std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", verb, " role"),
                  ERR_DETAIL(detail));
}

bool IsRelation(CatalogType type) {
  switch (type) {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY:
    case CatalogType::SEQUENCE_ENTRY:
    case CatalogType::INDEX_ENTRY:
      return true;
    default:
      return false;
  }
}

std::vector<std::span<const duckdb::AclItem>> AllColumnAcls(
  const duckdb::TableCatalogEntry& table) {
  std::vector<std::span<const duckdb::AclItem>> acls;
  for (const auto& column : table.GetColumns().Logical()) {
    acls.emplace_back(column.Acl());
  }
  return acls;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindServer(
  duckdb::ClientContext& context, const duckdb::Identifier& name) {
  for (const auto& db :
       duckdb::DatabaseManager::Get(*context.db).GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    auto& duck_catalog = catalog.Cast<duckdb::DuckCatalog>();
    auto server =
      duck_catalog.GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
        .GetEntry(duck_catalog.GetCatalogTransaction(context), name);
    if (server) {
      return server;
    }
  }
  return nullptr;
}

bool CanUseDatabase(duckdb::ClientContext& context,
                    const ConnectionContext& connection,
                    const RoleClosure& closure, std::string_view name) {
  if (name == connection.GetDatabase()) {
    return true;
  }
  auto& cluster = catalog::ClusterOf(context);
  auto database = cluster.GetCatalogSet(CatalogType::DATABASE_ENTRY)
                    .GetEntry(cluster.GetCatalogTransaction(context),
                              duckdb::Identifier{name});
  return !database || closure.Can(CatalogType::DATABASE_ENTRY,
                                  database->permissions, AclMode::Connect);
}

duckdb::LogicalOperator& StatementRoot(duckdb::LogicalOperator& root) {
  if ((root.type == LogicalOperatorType::LOGICAL_PREPARE ||
       root.type == LogicalOperatorType::LOGICAL_EXECUTE) &&
      !root.children.empty()) {
    return *root.children[0];
  }
  return root;
}

bool HasStatementRules(const duckdb::LogicalOperator& root) {
  switch (root.type) {
    case LogicalOperatorType::LOGICAL_EXPLAIN:
      return !root.children.empty() && HasStatementRules(*root.children[0]);
    case LogicalOperatorType::LOGICAL_CREATE_TABLE:
    case LogicalOperatorType::LOGICAL_CREATE_VIEW:
    case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
    case LogicalOperatorType::LOGICAL_CREATE_MACRO:
    case LogicalOperatorType::LOGICAL_CREATE_TYPE:
    case LogicalOperatorType::LOGICAL_CREATE_JOB:
    case LogicalOperatorType::LOGICAL_CREATE_TOKENIZER:
    case LogicalOperatorType::LOGICAL_CREATE_ROLE:
    case LogicalOperatorType::LOGICAL_CREATE_TRIGGER:
    case LogicalOperatorType::LOGICAL_DROP:
    case LogicalOperatorType::LOGICAL_ALTER:
    case LogicalOperatorType::LOGICAL_ATTACH:
    case LogicalOperatorType::LOGICAL_DETACH:
    case LogicalOperatorType::LOGICAL_VACUUM:
    case LogicalOperatorType::LOGICAL_EXECUTE:
      return true;
    default:
      return false;
  }
}

template<typename Info>
bool PointAtCluster(Info& info, CatalogType type) {
  if (type != CatalogType::DATABASE_ENTRY && type != CatalogType::ROLE_ENTRY) {
    return false;
  }
  info.GetQualifiedNameMutable() = duckdb::QualifiedName{
    duckdb::Identifier{catalog::ClusterCatalog::kDatabaseName},
    duckdb::Identifier{}, info.GetQualifiedName().Name()};
  return true;
}

class Enforcer {
 public:
  Enforcer(duckdb::ClientContext& context, ConnectionContext& connection,
           duckdb::LogicalOperator& root)
    : _context{context},
      _connection{connection},
      _root{StatementRoot(root)},
      _caller{connection.GetRoleId()},
      _roles{RolesOf(&context)},
      _caller_closure_owner{ClosureFor(&context, _caller)},
      _caller_closure{*_caller_closure_owner},
      _enforce{!_caller_closure.is_superuser} {}

  void Run() { Check(_root); }

 private:
  void Check(duckdb::LogicalOperator& op) {
    switch (op.type) {
      case LogicalOperatorType::LOGICAL_GET:
      case LogicalOperatorType::LOGICAL_INSERT:
      case LogicalOperatorType::LOGICAL_UPDATE:
      case LogicalOperatorType::LOGICAL_DELETE:
      case LogicalOperatorType::LOGICAL_MERGE_INTO:
        break;
      case LogicalOperatorType::LOGICAL_CREATE_TABLE: {
        auto& create = op.Cast<duckdb::LogicalCreateTable>();
        RequireUserSchema(create.schema.oid, create.info->Base());
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_VIEW:
      case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
      case LogicalOperatorType::LOGICAL_CREATE_MACRO:
      case LogicalOperatorType::LOGICAL_CREATE_TYPE:
      case LogicalOperatorType::LOGICAL_CREATE_JOB:
      case LogicalOperatorType::LOGICAL_CREATE_TOKENIZER:
      case LogicalOperatorType::LOGICAL_CREATE_TRIGGER: {
        auto& create = op.Cast<duckdb::LogicalCreate>();
        RequireUserSchema(create.schema->oid, *create.info);
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_ROLE: {
        auto& info =
          op.Cast<duckdb::LogicalCreate>().info->Cast<duckdb::CreateRoleInfo>();
        if (_enforce) {
          CheckCreateRole(info);
        }
        PointAtCluster(info, CatalogType::ROLE_ENTRY);
        _connection.MarkClusterWritten();
        break;
      }
      case LogicalOperatorType::LOGICAL_DROP: {
        auto& info = *op.Cast<duckdb::LogicalDrop>().info;
        RequireOutsideTransactionBlock(info);
        if (_enforce && info.type == CatalogType::ROLE_ENTRY) {
          CheckDropRole(info);
        }
        RequireDroppableNow(info);
        if (PointAtCluster(info, info.type)) {
          _connection.MarkClusterWritten();
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_ALTER: {
        auto& info = *op.Cast<duckdb::LogicalAlter>().info;
        if (info.type == duckdb::AlterType::ALTER_DATABASE) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("renaming a database is not supported"));
        }
        if (info.type == duckdb::AlterType::ALTER_PERMISSIONS) {
          ResolvePermissions(info.Cast<duckdb::AlterPermissionsInfo>());
        } else if (info.type == duckdb::AlterType::ALTER_ROLE) {
          if (_enforce) {
            CheckAlterRole(info.Cast<duckdb::AlterRoleInfo>());
          }
        }
        if (PointAtCluster(info, info.GetCatalogType())) {
          _connection.MarkClusterWritten();
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_ATTACH:
        RequireDatabaseNotBeingCreated(
          op.Cast<duckdb::LogicalAttach>().info->name);
        break;
      case LogicalOperatorType::LOGICAL_DETACH: {
        const auto name =
          op.Cast<duckdb::LogicalDetach>().info->name.GetIdentifierName();
        if (ClusterEntry(CatalogType::DATABASE_ENTRY, name)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("cannot detach database \"", name,
                                  "\", use DROP DATABASE"));
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_VACUUM: {
        auto& vacuum = op.Cast<duckdb::LogicalVacuum>();
        if (!_enforce || !vacuum.table || duckdb::IsUnowned(*vacuum.table)) {
          break;
        }
        auto& info = vacuum.GetInfo();
        if (!_caller_closure.Can(CatalogType::TABLE_ENTRY,
                                 vacuum.table->permissions,
                                 AclMode::Maintain)) {
          _connection.AddNotice(SQL_ERROR_DATA(
            ERR_CODE(ERRCODE_WARNING),
            ERR_MSG("permission denied to ",
                    info.options.vacuum ? "vacuum" : "analyze", " \"",
                    info.ref->Cast<duckdb::BaseTableRef>()
                      .Table()
                      .GetIdentifierName(),
                    "\", skipping it")));
          vacuum.table = nullptr;
          info.has_table = false;
          info.columns.clear();
          vacuum.children.clear();
        }
        return;
      }
      case LogicalOperatorType::LOGICAL_EXECUTE: {
        const auto& unbound =
          op.Cast<duckdb::LogicalExecute>().prepared->unbound_statement;
        if (unbound && unbound->type == duckdb::StatementType::DROP_STATEMENT) {
          const auto& info = *unbound->Cast<duckdb::DropStatement>().info;
          RequireOutsideTransactionBlock(info);
          RequireDroppableNow(info);
        }
        break;
      }
      case LogicalOperatorType::LOGICAL_CREATE_INDEX:
      case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
      case LogicalOperatorType::LOGICAL_CREATE_FOREIGN_SERVER:
      case LogicalOperatorType::LOGICAL_INVALID:
      case LogicalOperatorType::LOGICAL_PROJECTION:
      case LogicalOperatorType::LOGICAL_FILTER:
      case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      case LogicalOperatorType::LOGICAL_WINDOW:
      case LogicalOperatorType::LOGICAL_UNNEST:
      case LogicalOperatorType::LOGICAL_LIMIT:
      case LogicalOperatorType::LOGICAL_ORDER_BY:
      case LogicalOperatorType::LOGICAL_TOP_N:
      case LogicalOperatorType::LOGICAL_DISTINCT:
      case LogicalOperatorType::LOGICAL_SAMPLE:
      case LogicalOperatorType::LOGICAL_PIVOT:
      case LogicalOperatorType::LOGICAL_CHUNK_GET:
      case LogicalOperatorType::LOGICAL_DELIM_GET:
      case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
      case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
      case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
      case LogicalOperatorType::LOGICAL_CTE_REF:
      case LogicalOperatorType::LOGICAL_JOIN:
      case LogicalOperatorType::LOGICAL_DELIM_JOIN:
      case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
      case LogicalOperatorType::LOGICAL_ANY_JOIN:
      case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
      case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
      case LogicalOperatorType::LOGICAL_ASOF_JOIN:
      case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
      case LogicalOperatorType::LOGICAL_LOOKUP_JOIN:
      case LogicalOperatorType::LOGICAL_UNION:
      case LogicalOperatorType::LOGICAL_EXCEPT:
      case LogicalOperatorType::LOGICAL_INTERSECT:
      case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
      case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
      case LogicalOperatorType::LOGICAL_SECURE_VIEW:
      case LogicalOperatorType::LOGICAL_TRIGGER:
      case LogicalOperatorType::LOGICAL_EXPORT:
      case LogicalOperatorType::LOGICAL_LOAD:
      case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
      case LogicalOperatorType::LOGICAL_CREATE_SECRET:
      case LogicalOperatorType::LOGICAL_CONNECT:
      case LogicalOperatorType::LOGICAL_EXTERNAL_RESOURCE:
      case LogicalOperatorType::LOGICAL_COPY_DATABASE:
      case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
      case LogicalOperatorType::LOGICAL_PRAGMA:
      case LogicalOperatorType::LOGICAL_EXPLAIN:
      case LogicalOperatorType::LOGICAL_PREPARE:
      case LogicalOperatorType::LOGICAL_TRANSACTION:
      case LogicalOperatorType::LOGICAL_SET:
      case LogicalOperatorType::LOGICAL_RESET:
      case LogicalOperatorType::LOGICAL_DISCONNECT:
      case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
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
      if (const auto access = _roles->Access(principal)) {
        it->second = *access;
      } else {
        it->second.role = principal;
      }
    }
    return it->second;
  }

  const RoleMemberships& CallerMemberships() {
    if (!_caller_memberships) {
      _caller_memberships = MembershipsOf(*_roles, _caller);
    }
    return *_caller_memberships;
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> FindEntry(
    CatalogType type, const duckdb::QualifiedName& name) {
    return duckdb::Catalog::GetEntry(_context,
                                     duckdb::EntryLookupInfo(type, name),
                                     duckdb::OnEntryNotFound::RETURN_NULL);
  }

  void CheckDropRole(const duckdb::DropInfo& info) {
    if (!_caller_closure.Has(catalog::RoleOption::CreateRole)) {
      DenyRoleAction("drop",
                     "Only roles with the CREATEROLE attribute and the ADMIN "
                     "option on the target roles may drop roles.");
    }
    if (auto role =
          ClusterEntry(CatalogType::ROLE_ENTRY,
                       info.GetQualifiedName().Name().GetIdentifierName())) {
      RequireRoleAdmin(*role, "drop");
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
    auto role = cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
                  .GetEntry(cluster.GetCatalogTransaction(_context),
                            duckdb::Identifier{name});
    if (!role) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", name, "\" does not exist"));
    }
    return role->oid;
  }

  duckdb::idx_t GranteeId(std::string_view name) {
    return name == "public" ? duckdb::ACL_ID_PUBLIC : RoleId(name);
  }

  duckdb::idx_t RoleSpecId(std::string_view name) {
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
      duckdb::ThrowPermissionDenied(entry);
    }
  }

  duckdb::CatalogEntry& ResolveTarget(duckdb::AlterPermissionsInfo& info) {
    const auto& name = info.GetQualifiedName();
    if (info.entry_catalog_type == CatalogType::SCHEMA_ENTRY) {
      auto& schema =
        duckdb::Catalog::GetSchema(_context, name.Catalog(), name.Name());
      info.SetQualifiedName(schema.ParentCatalog().GetName(),
                            duckdb::Identifier(), schema.name);
      return schema;
    }
    auto entry = FindEntry(info.entry_catalog_type, name);
    if (!entry && info.entry_catalog_type == CatalogType::MACRO_ENTRY) {
      entry = FindEntry(CatalogType::TABLE_MACRO_ENTRY, name);
    }
    if (!entry) {
      const bool relation = info.entry_catalog_type == CatalogType::TABLE_ENTRY;
      THROW_SQL_ERROR(
        ERR_CODE(relation ? ERRCODE_UNDEFINED_TABLE : ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG(relation ? "relation"
                         : duckdb::PermissionKindName(info.entry_catalog_type),
                " \"", name.ToString(), "\" does not exist"));
    }
    info.entry_catalog_type = entry->type == CatalogType::VIEW_ENTRY
                                ? CatalogType::TABLE_ENTRY
                                : entry->type;
    info.SetQualifiedName(duckdb::QualifiedName::FromCatalogSchema(
      entry->ParentCatalog().GetName(),
      entry->ParentSchemaPath(
        entry->ParentCatalog().GetCatalogTransaction(_context)),
      entry->name));
    return *entry;
  }

  void ResolveGrant(duckdb::AlterPermissionsInfo& info) {
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
      info.grantors.emplace_back(_caller);
      for (const auto role : _caller_closure.closure) {
        if (role != _caller) {
          info.grantors.emplace_back(role);
        }
      }
    }
    if (info.entry_catalog_type == CatalogType::DATABASE_ENTRY) {
      const auto& name = info.GetQualifiedName().Name().GetIdentifierName();
      const auto database = ClusterEntry(CatalogType::DATABASE_ENTRY, name);
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database \"", name, "\" does not exist"));
      }
      RequireGrantable(*database);
      return;
    }
    if (info.entry_catalog_type == CatalogType::FOREIGN_SERVER_ENTRY) {
      const auto& name = info.GetQualifiedName().Name().GetIdentifierName();
      const auto server = ServerEntry(name);
      if (!server) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG("server \"", name, "\" does not exist"));
      }
      info.SetQualifiedName(server->ParentCatalog().GetName(),
                            duckdb::Identifier(), server->name);
      RequireGrantable(*server);
      return;
    }
    if (info.all_in_schema) {
      return;
    }
    RequireGrantable(ResolveTarget(info));
  }

  void ResolveOwner(duckdb::AlterPermissionsInfo& info) {
    auto& entry = ResolveTarget(info);
    info.new_owner_id = RoleSpecId(info.new_owner);
    const bool relation = IsRelation(entry.type);
    if (_enforce && relation) {
      RequireOwner(entry);
    }
    if (info.new_owner_id == entry.permissions.owner) {
      return;
    }
    if (entry.type == CatalogType::SEQUENCE_ENTRY) {
      RefuseOwnedSequence(entry);
    }
    if (!_enforce) {
      return;
    }
    if (!relation) {
      RequireOwner(entry);
    }
    if (info.new_owner_id != _caller &&
        !CallerMemberships().CanSet(info.new_owner_id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("must be able to SET ROLE \"",
                              _roles->NameOf(info.new_owner_id), "\""));
    }
    if (entry.type != CatalogType::SCHEMA_ENTRY &&
        !ClosureOf(info.new_owner_id)
           .Can(CatalogType::SCHEMA_ENTRY,
                entry.ParentSchema(_context).permissions, AclMode::Create)) {
      duckdb::ThrowPermissionDenied(entry.ParentSchema(_context));
    }
  }

  void RefuseOwnedSequence(duckdb::CatalogEntry& sequence) {
    auto& catalog = sequence.ParentCatalog();
    auto dependency_manager = catalog.GetDependencyManager();
    if (!dependency_manager) {
      return;
    }
    auto table = dependency_manager->OwnerOf(
      catalog.GetCatalogTransaction(_context), sequence);
    if (!table) {
      return;
    }
    const auto& name = sequence.name.GetIdentifierName();
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot change owner of sequence \"", name, "\""),
                    ERR_DETAIL("Sequence \"", name, "\" is linked to table \"",
                               table->name.GetIdentifierName(), "\"."));
  }

  void ResolveDefaultPrivileges(duckdb::AlterPermissionsInfo& info) {
    info.target_role = info.for_role.empty() ? _caller : RoleId(info.for_role);
    if (_enforce && !_caller_closure.MemberOf(info.target_role)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied to change default privileges"));
    }
    info.grantee_id = GranteeId(info.grantee);
    std::string database{duckdb::DatabaseManager::GetDefaultDatabase(_context)
                           .GetIdentifierName()};
    if (!info.default_schema.empty()) {
      auto& schema =
        duckdb::Catalog::GetSchema(_context, duckdb::Identifier{},
                                   duckdb::Identifier{info.default_schema});
      if (_enforce && !ClosureOf(info.target_role)
                         .Can(CatalogType::SCHEMA_ENTRY, schema.permissions,
                              AclMode::Create)) {
        duckdb::ThrowPermissionDenied(schema);
      }
      info.default_scope = schema.oid;
      database = schema.ParentCatalog().GetName().GetIdentifierName();
    }
    if (!ClusterEntry(CatalogType::DATABASE_ENTRY, database)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                      ERR_MSG("database \"", database, "\" does not exist"));
    }
    info.SetQualifiedName(duckdb::Identifier{}, duckdb::Identifier{},
                          duckdb::Identifier{database});
  }

  void RequireOwner(const duckdb::CatalogEntry& entry) {
    if (entry.type == CatalogType::INDEX_ENTRY) {
      auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      auto host =
        index.GetRelation(index.catalog.GetCatalogTransaction(_context));
      if (host && !_caller_closure.Owns(host->permissions.owner)) {
        MustOwn(*host);
      }
      return;
    }
    if (!_caller_closure.Owns(entry.permissions.owner)) {
      MustOwn(entry);
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> ClusterEntry(
    CatalogType type, std::string_view name) {
    auto& cluster = catalog::ClusterOf(_context);
    return cluster.GetCatalogSet(type).GetEntry(
      cluster.GetCatalogTransaction(_context), duckdb::Identifier{name});
  }

  void RequireDatabaseNotBeingCreated(const duckdb::Identifier& name) {
    auto database = duckdb::DatabaseManager::Get(_context).GetDatabase(name);
    if (!database || database->GetCatalog().GetCatalogType() !=
                       catalog::SereneDBCatalog::kStorageType) {
      return;
    }
    if (!ClusterEntry(CatalogType::DATABASE_ENTRY, name.GetIdentifierName())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
                      ERR_MSG("database \"", name.GetIdentifierName(),
                              "\" is being created by another transaction"));
    }
  }

  void RequireOutsideTransactionBlock(const duckdb::DropInfo& info) {
    if (!_connection.InTransactionBlock()) {
      return;
    }
    if (info.type == CatalogType::DATABASE_ENTRY) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
        ERR_MSG("DROP DATABASE cannot run inside a transaction block"));
    }
    if (info.type == CatalogType::PREPARED_STATEMENT && info.extra_drop_info &&
        info.extra_drop_info->Cast<duckdb::ExtraDropDiscardInfo>().target ==
          duckdb::DiscardTarget::ALL) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
        ERR_MSG("DISCARD ALL cannot run inside a transaction block"));
    }
  }

  void RequireDroppableNow(const duckdb::DropInfo& info) {
    if (info.type != CatalogType::DATABASE_ENTRY) {
      return;
    }
    auto database = duckdb::DatabaseManager::Get(_context).GetDatabase(
      info.GetQualifiedName().Name());
    if (database && database->GetCatalog().GetCatalogType() ==
                      catalog::SereneDBCatalog::kStorageType) {
      RequireDatabaseUnused(*database);
    }
  }

  void RequireDatabaseUnused(duckdb::AttachedDatabase& database) {
    if (duckdb::DatabaseManager::GetDefaultDatabase(_context) ==
        database.GetName()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("cannot drop the currently open database"));
    }
    auto& catalog = database.GetCatalog().Cast<duckdb::DuckCatalog>();
    size_t subscriptions = 0;
    catalog.GetCatalogSet(CatalogType::SUBSCRIPTION_ENTRY)
      .Scan([&](duckdb::CatalogEntry&) { ++subscriptions; });
    if (subscriptions != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_IN_USE),
        ERR_MSG("database \"", database.GetName().GetIdentifierName(),
                "\" is being used by logical replication subscription"),
        ERR_DETAIL(
          subscriptions == 1
            ? std::string{"There is 1 subscription."}
            : absl::StrCat("There are ", subscriptions, " subscriptions.")));
    }
    const auto others = pg::ProgressRegistry::Instance().OtherSessions(
      static_cast<int64_t>(database.oid), _connection.GetBackendPid(),
      absl::Seconds(5));
    if (others == 0) {
      return;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("database \"", database.GetName().GetIdentifierName(),
              "\" is being accessed by other users"),
      ERR_DETAIL(others == 1 ? std::string{"There is 1 other session using "
                                           "the database."}
                             : absl::StrCat("There are ", others,
                                            " other sessions using the "
                                            "database.")));
  }
  void RequireAttributeGrant(std::string_view verb,
                             catalog::RoleOption options) {
    const auto deny = [&](std::string_view attribute) {
      DenyRoleAction(
        verb, absl::StrCat("Only roles with the ", attribute, " attribute may ",
                           verb, " roles with the ", attribute, " attribute."));
    };
    if (HasOption(options, catalog::RoleOption::Superuser)) {
      deny("SUPERUSER");
    }
    if (HasOption(options, catalog::RoleOption::CreateDb) &&
        !_caller_closure.Has(catalog::RoleOption::CreateDb)) {
      deny("CREATEDB");
    }
    if (HasOption(options, catalog::RoleOption::Replication) &&
        !_caller_closure.Has(catalog::RoleOption::Replication)) {
      deny("REPLICATION");
    }
    if (HasOption(options, catalog::RoleOption::BypassRls) &&
        !_caller_closure.Has(catalog::RoleOption::BypassRls)) {
      deny("BYPASSRLS");
    }
  }

  void RequireRoleAdmin(const duckdb::CatalogEntry& role,
                        std::string_view verb) {
    if (role.Cast<catalog::RoleCatalogEntry>().IsSuperuser()) {
      DenyRoleAction(
        verb, absl::StrCat("Only roles with the SUPERUSER attribute may ", verb,
                           " roles with the SUPERUSER attribute."));
    }
    if (!_caller_closure.Has(catalog::RoleOption::CreateRole) ||
        !CallerMemberships().IsAdminOf(role.oid)) {
      DenyRoleAction(
        verb, absl::StrCat("Only roles with the CREATEROLE attribute and "
                           "the ADMIN option on role \"",
                           role.name.GetIdentifierName(), "\" may ", verb,
                           " this role."));
    }
  }

  void RequireGrantableRole(std::string_view name) {
    auto role = ClusterEntry(CatalogType::ROLE_ENTRY, name);
    if (role && !CallerMemberships().IsAdminOf(role->oid)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied to grant role \"",
                role->name.GetIdentifierName(), "\""),
        ERR_DETAIL("Only roles with the ADMIN option on role \"",
                   role->name.GetIdentifierName(), "\" may grant this role."));
    }
  }

  void CheckCreateRole(const duckdb::CreateRoleInfo& info) {
    if (!_caller_closure.Has(catalog::RoleOption::CreateRole)) {
      DenyRoleAction(
        "create", "Only roles with the CREATEROLE attribute may create roles.");
    }
    RequireAttributeGrant("create", info.options);
    for (const auto& name : info.in_roles) {
      RequireGrantableRole(name.GetIdentifierName());
    }
  }

  void CheckAlterRole(const duckdb::AlterRoleInfo& info) {
    auto role =
      ClusterEntry(CatalogType::ROLE_ENTRY,
                   info.GetQualifiedName().Name().GetIdentifierName());
    if (!role) {
      return;
    }
    if (!info.grant_role.empty()) {
      RequireGrantableRole(info.grant_role);
      return;
    }
    if (!info.new_name.empty()) {
      RequireRoleAdmin(*role, "rename");
      return;
    }
    const bool attributes = info.set_options != catalog::RoleOption::None ||
                            info.clear_options != catalog::RoleOption::None ||
                            info.set_conn_limit || info.set_valid_until;
    if (role->oid != _caller || attributes) {
      RequireRoleAdmin(*role, "alter");
      RequireAttributeGrant("alter", info.set_options);
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> ServerEntry(
    std::string_view name) {
    auto& catalog =
      duckdb::Catalog::GetCatalog(
        _context, duckdb::DatabaseManager::GetDefaultDatabase(_context))
        .Cast<duckdb::DuckCatalog>();
    return catalog.GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
      .GetEntry(catalog.GetCatalogTransaction(_context),
                duckdb::Identifier{name});
  }

  duckdb::ClientContext& _context;
  ConnectionContext& _connection;
  duckdb::LogicalOperator& _root;
  const duckdb::idx_t _caller;
  RoleGraphPtr _roles;
  RoleAccessRef _caller_closure_owner;
  const RoleClosure& _caller_closure;
  const bool _enforce;
  irs::containers::NodeHashMap<duckdb::idx_t, RoleClosure> _closures;
  std::optional<RoleMemberships> _caller_memberships;
};

}  // namespace

void EnforcePlan(duckdb::ClientContext& context, ConnectionContext& connection,
                 duckdb::LogicalOperator& plan) {
  if (HasStatementRules(StatementRoot(plan))) {
    Enforcer{context, connection, plan}.Run();
  }
}

namespace {

bool DatabaseVisible(duckdb::ClientContext& context,
                     const ConnectionContext& connection,
                     const RoleClosure& closure,
                     duckdb::AttachedDatabase& database) {
  if (database.GetCatalog().GetCatalogType() ==
      catalog::SereneDBCatalog::kStorageType) {
    return CanUseDatabase(context, connection, closure,
                          database.GetName().GetIdentifierName());
  }
  if (database.IsSystem() || database.IsTemporary()) {
    return true;
  }
  auto server = FindServer(context, database.GetName());
  return !server || closure.Can(CatalogType::FOREIGN_SERVER_ENTRY,
                                server->permissions, AclMode::Usage);
}

bool EntryVisible(duckdb::ClientContext& context,
                  const ConnectionContext& connection,
                  const RoleClosure& closure, duckdb::CatalogEntry& entry) {
  if (entry.type == CatalogType::DATABASE_ENTRY) {
    return CanUseDatabase(context, connection, closure,
                          entry.name.GetIdentifierName());
  }
  auto& parent = entry.ParentCatalog();
  if (parent.GetCatalogType() != catalog::ClusterCatalog::kStorageType &&
      !DatabaseVisible(context, connection, closure, parent.GetAttached())) {
    return false;
  }
  if ((entry.type == CatalogType::TABLE_FUNCTION_ENTRY ||
       entry.type == CatalogType::SCALAR_FUNCTION_ENTRY) &&
      entry.Cast<duckdb::FunctionEntry>().superuser_only) {
    return false;
  }
  if (entry.internal || duckdb::IsUnowned(entry)) {
    return true;
  }
  const auto& perm = entry.permissions;
  switch (entry.type) {
    case CatalogType::SCHEMA_ENTRY:
      return closure.CanAny(entry.type, perm, AclMode::Usage | AclMode::Create);
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY: {
      constexpr auto kAny = AclMode::Select | AclMode::Insert |
                            AclMode::Update | AclMode::Delete |
                            AclMode::Truncate | AclMode::References |
                            AclMode::Trigger | AclMode::Maintain;
      if (closure.CanAny(CatalogType::TABLE_ENTRY, perm, kAny)) {
        return true;
      }
      if (entry.type != CatalogType::TABLE_ENTRY) {
        return false;
      }
      const auto acls = AllColumnAcls(entry.Cast<duckdb::TableCatalogEntry>());
      return absl::c_any_of(
        std::array{AclMode::Select, AclMode::Insert, AclMode::Update,
                   AclMode::References},
        [&](AclMode mode) { return closure.CanAnyColumn(perm, mode, acls); });
    }
    case CatalogType::SEQUENCE_ENTRY:
      return closure.CanAny(entry.type, perm,
                            AclMode::Usage | AclMode::Select | AclMode::Update);
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return closure.Can(entry.type, perm, AclMode::Execute);
    case CatalogType::TYPE_ENTRY:
      return closure.Can(entry.type, perm, AclMode::Usage);
    case CatalogType::JOB_ENTRY:
      return closure.Owns(perm.owner);
    case CatalogType::INDEX_ENTRY: {
      auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      auto relation =
        index.GetRelation(index.catalog.GetCatalogTransaction(context));
      return !relation || EntryVisible(context, connection, closure, *relation);
    }
    default:
      return true;
  }
}

}  // namespace

bool CanSee(duckdb::ClientContext& context, duckdb::CatalogEntry& entry) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return true;
  }
  const auto closure = ClosureFor(&context, connection->GetRoleId());
  return closure->is_superuser ||
         EntryVisible(context, *connection, *closure, entry);
}

bool CanSeeDatabase(duckdb::ClientContext& context,
                    duckdb::AttachedDatabase& database) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return true;
  }
  const auto closure = ClosureFor(&context, connection->GetRoleId());
  return closure->is_superuser ||
         DatabaseVisible(context, *connection, *closure, database);
}

void CheckEntryReference(duckdb::ClientContext& context,
                         duckdb::CatalogEntry& entry) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return;
  }
  const auto closure = ClosureFor(&context, connection->GetRoleId());
  if (closure->is_superuser) {
    return;
  }
  auto& database = entry.type == CatalogType::DATABASE_ENTRY
                     ? entry.Cast<duckdb::AttachedDatabase>()
                     : entry.ParentCatalog().GetAttached();
  if (!DatabaseVisible(context, *connection, *closure, database)) {
    if (database.GetCatalog().GetCatalogType() ==
        catalog::SereneDBCatalog::kStorageType) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for database \"",
                              database.GetName().GetIdentifierName(), "\""),
                      ERR_DETAIL("User does not have CONNECT privilege."));
    }
    if (const auto server = FindServer(context, database.GetName())) {
      duckdb::ThrowPermissionDenied(*server);
    }
  }
  if (entry.type != CatalogType::DATABASE_ENTRY &&
      !EntryVisible(context, *connection, *closure, entry)) {
    duckdb::ThrowPermissionDenied(entry);
  }
}

}  // namespace sdb::auth
