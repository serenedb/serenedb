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

#include "basics/misc.hpp"
#include "pg/commands.h"

namespace sdb::pg {

CommandExecutor::CommandExecutor(
  std::shared_ptr<ExecContext> context,
  std::unique_ptr<CommandExecutorRequest> request)
  : _context{std::move(context)}, _request{std::move(request)} {}

yaclib::Future<> CommandExecutor::RequestCancel() {
  _context->cancel();
  return {};
}

yaclib::Future<> CommandExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_query) {  // was fired?
    return {};
  }

  auto f = ExecuteImpl();
  _query = nullptr;  // set fired
  return f;
}

yaclib::Future<> CommandExecutor::ExecuteImpl() {
  switch (_request->type) {
    case CommandRequestType::DDL: {
      const auto& node = static_cast<const DDLRequest&>(*_request).node;
      switch (node.type) {
        case NodeTag::T_CreatedbStmt: {
          const auto& stmt = *castNode(CreatedbStmt, &node);
          return CreateDatabase(*_context, stmt);
        }
        case NodeTag::T_DropdbStmt: {
          const auto& stmt = *castNode(DropdbStmt, &node);
          return DropDatabase(*_context, stmt);
        }
        case NodeTag::T_CreateStmt: {
          const auto& stmt = *castNode(CreateStmt, &node);
          return CreateTable(*_context, stmt);
        }
        case NodeTag::T_IndexStmt: {
          const auto& stmt = *castNode(IndexStmt, &node);
          return CreateIndex(*_context, stmt);
        }
        case NodeTag::T_ViewStmt: {
          const auto& stmt = *castNode(ViewStmt, &node);
          return CreateView(*_context, stmt);
        }
        case NodeTag::T_DropStmt: {
          const auto& stmt = *castNode(DropStmt, &node);
          return DropObject(*_context, stmt);
        }
        case NodeTag::T_TransactionStmt: {
          const auto& stmt = *castNode(TransactionStmt, &node);
          return Transaction(*_context, stmt);
        }
        case NodeTag::T_VariableSetStmt: {
          const auto& stmt = *castNode(VariableSetStmt, &node);
          return VariableSet(*_context, stmt);
        }
        case NodeTag::T_CreateFunctionStmt: {
          const auto& stmt = *castNode(CreateFunctionStmt, &node);
          return CreateFunction(*_context, stmt);
        }
        case NodeTag::T_CreateSchemaStmt: {
          const auto& stmt = *castNode(CreateSchemaStmt, &node);
          return CreateSchema(*_context, stmt);
        }
        case NodeTag::T_VacuumStmt: {
          const auto& stmt = *castNode(VacuumStmt, &node);
          return Vacuum(*_context, stmt);
        }
        default:
          SDB_UNREACHABLE();
      }
    }
    case CommandRequestType::CTASCreateTable: {
      const auto& req = static_cast<const CTASCreateTableRequest&>(*_request);
      SDB_ASSERT(_query);
      return CreateTableCTAS(*_context, *_query, req.into, req.if_not_exists);
    }
    case CommandRequestType::RemoveTombstone: {
      const auto& rel =
        static_cast<const RemoveTombstoneRequest&>(*_request).relation;
      return RemoveTombstone(*_context, rel);
    }
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace sdb::pg
