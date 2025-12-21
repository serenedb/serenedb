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

#include "pg/executor.h"

#include "pg/commands.h"
#include "yaclib/async/make.hpp"

namespace sdb::pg {

Executor::Executor(std::shared_ptr<ExecContext> context, const Node& node)
  : _context{std::move(context)}, _node{node} {}

yaclib::Future<> Executor::RequestCancel() {
  // TODO(mbkkt) connect ExecContext::cancel should return Future
  _context->cancel();
  return yaclib::MakeFuture();
}

yaclib::Future<Result> Executor::Execute() {
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
    case NodeTag::T_IndexStmt: {
      const auto& stmt = *castNode(IndexStmt, &_node);
      return CreateIndex(*_context, stmt);
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
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace sdb::pg
