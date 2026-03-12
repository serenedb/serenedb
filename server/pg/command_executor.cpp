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

#include "pg/command_executor.h"

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"

namespace sdb::pg {

CommandExecutor::CommandExecutor(std::shared_ptr<ExecContext> context)
  : _context{std::move(context)} {}

yaclib::Future<> CommandExecutor::RequestCancel() {
  _context->cancel();
  return {};
}

yaclib::Future<> CommandExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_query) {  // was fired
    return {};
  }

  auto f = ExecuteImpl();
  _query = nullptr;  // set fired
  return f;
}

DDLExecutor::DDLExecutor(std::shared_ptr<ExecContext> context, const Node& node)
  : CommandExecutor{std::move(context)}, _node{node} {}

yaclib::Future<> DDLExecutor::ExecuteImpl() {
  switch (_node.type) {
    case NodeTag::T_CreatedbStmt: {
      const auto& stmt = *castNode(CreatedbStmt, &_node);
      return CreateDatabase(*_context, stmt);
    }
    case NodeTag::T_DropdbStmt: {
      const auto& stmt = *castNode(DropdbStmt, &_node);
      return DropDatabase(*_context, stmt);
    }
    case NodeTag::T_CreateStmt: {
      const auto& stmt = *castNode(CreateStmt, &_node);
      return CreateTable(*_context, stmt);
    }
    case NodeTag::T_ViewStmt: {
      const auto& stmt = *castNode(ViewStmt, &_node);
      return CreateView(*_context, stmt);
    }
    case NodeTag::T_DropStmt: {
      const auto& stmt = *castNode(DropStmt, &_node);
      return DropObject(*_context, stmt);
    }
    case NodeTag::T_TransactionStmt: {
      const auto& stmt = *castNode(TransactionStmt, &_node);
      return Transaction(*_context, stmt);
    }
    case NodeTag::T_VariableSetStmt: {
      const auto& stmt = *castNode(VariableSetStmt, &_node);
      return VariableSet(*_context, stmt);
    }
    case NodeTag::T_CreateFunctionStmt: {
      const auto& stmt = *castNode(CreateFunctionStmt, &_node);
      return CreateFunction(*_context, stmt);
    }
    case NodeTag::T_CreateSchemaStmt: {
      const auto& stmt = *castNode(CreateSchemaStmt, &_node);
      return CreateSchema(*_context, stmt);
    }
    case NodeTag::T_VacuumStmt: {
      const auto& stmt = *castNode(VacuumStmt, &_node);
      return Vacuum(*_context, stmt);
    }
    default:
      SDB_UNREACHABLE();
  }
}

CTASCreateTableExecutor::CTASCreateTableExecutor(
  std::shared_ptr<ExecContext> context, const IntoClause& into,
  bool if_not_exists)
  : CommandExecutor{std::move(context)},
    _into{into},
    _if_not_exists{if_not_exists} {}

yaclib::Future<> CTASCreateTableExecutor::ExecuteImpl() {
  SDB_ASSERT(_query);
  return CreateTableCTAS(*_context, *_query, _into, _if_not_exists);
}

CreateIndexExecutor::CreateIndexExecutor(std::shared_ptr<ExecContext> context,
                                         const IndexStmt& stmt)
  : CommandExecutor{std::move(context)}, _stmt{stmt} {}

yaclib::Future<> CreateIndexExecutor::ExecuteImpl() {
  SDB_ASSERT(_query);
  return CreateIndex(*_context, *_query, _stmt);
}

FinishCreateIndexExecutor::FinishCreateIndexExecutor(
  std::shared_ptr<ExecContext> context, std::string_view schemaname,
  std::string_view index_name)
  : CommandExecutor{std::move(context)},
    _schemaname{schemaname},
    _index_name{index_name} {}

yaclib::Future<> FinishCreateIndexExecutor::ExecuteImpl() {
  const auto db = _context->GetDatabaseId();
  auto& conn_ctx = basics::downCast<ConnectionContext>(*_context);
  std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    _schemaname.empty() ? std::string_view{current_schema} : _schemaname;

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto snapshot = catalog.GetSnapshot();
  auto index = snapshot->GetRelation(db, schema, _index_name);
  SDB_ASSERT(index);
  auto shard = snapshot->GetIndexShard(index->GetId());
  SDB_ASSERT(shard);
  SDB_ASSERT(shard->GetType() == IndexType::Inverted);
  auto& inverted_index = basics::downCast<search::InvertedIndexShard>(*shard);

  return inverted_index.CommitWait().ThenInline([shard = std::move(shard)](
                                                  yaclib::Result<>&&) {
    auto& inverted_index = basics::downCast<search::InvertedIndexShard>(*shard);
    inverted_index.FinishCreation();
    inverted_index.StartTasks();
  });
}

RemoveTombstoneExecutor::RemoveTombstoneExecutor(
  std::shared_ptr<ExecContext> context, std::string_view schemaname,
  std::string_view name)
  : CommandExecutor{std::move(context)}, _schemaname{schemaname}, _name{name} {}

yaclib::Future<> RemoveTombstoneExecutor::ExecuteImpl() {
  return RemoveTombstone(*_context, _schemaname, _name);
}

}  // namespace sdb::pg
