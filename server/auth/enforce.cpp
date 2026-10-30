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
#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/function_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
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
        PointAtCluster(info, CatalogType::ROLE_ENTRY);
        _connection.MarkClusterWritten();
        break;
      }
      case LogicalOperatorType::LOGICAL_DROP: {
        auto& info = *op.Cast<duckdb::LogicalDrop>().info;
        RequireOutsideTransactionBlock(info);
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
  duckdb::ClientContext& _context;
  ConnectionContext& _connection;
  duckdb::LogicalOperator& _root;
  const duckdb::idx_t _caller;
  RoleAccessRef _caller_closure_owner;
  const RoleClosure& _caller_closure;
  const bool _enforce;
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
