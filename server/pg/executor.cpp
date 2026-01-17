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

Executor::Executor(std::shared_ptr<ExecContext> context,
                   std::shared_ptr<ExecutorRequest> req)
  : _context{std::move(context)}, _req{std::move(req)} {}

yaclib::Future<> Executor::RequestCancel() {
  // TODO(mbkkt) connect ExecContext::cancel should return Future
  _context->cancel();
  return yaclib::MakeFuture();
}

yaclib::Future<Result> Executor::ExecuteGenericDDLRequest() {
  SDB_ASSERT(_req->type == ExecutorRequest::Type::GenericDDL);
  const auto& req = static_cast<const GenericDDLRequest&>(*_req);
  const auto& node = req.node;
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
    default:
      SDB_UNREACHABLE();
  }
}

yaclib::Future<Result> Executor::Execute() {
  switch (_req->type) {
    using enum ExecutorRequest::Type;
    case GenericDDL:
      return ExecuteGenericDDLRequest();
    case CreateFunction:
      SDB_UNREACHABLE();  // Handled in ExecuteGenericDDLRequest
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace sdb::pg
