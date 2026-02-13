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

#include <yaclib/async/make.hpp>

#include "basics/down_cast.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {

yaclib::Future<Result> Transaction(ExecContext& context,
                                   const TransactionStmt& stmt) {
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);
  switch (stmt.kind) {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START: {
      if (!conn_ctx.HasTransactionBegin()) {
        auto isolation_level = IsolationLevel::RepeatableRead;
        VisitNodes(stmt.options, [&](const DefElem& option) {
          std::string_view opt_name = option.defname;
          if (opt_name == "transaction_isolation") {
            std::string_view level =
              strVal(&castNode(A_Const, option.arg)->val);
            if (level == "repeatable read") {
              isolation_level = IsolationLevel::RepeatableRead;
            } else {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                              ERR_MSG("transaction isolation level \"", level,
                                      "\" is not supported"));
            }
          } else if (opt_name == "transaction_read_only") {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
              ERR_MSG(
                "transaction READ WRITE | READ ONLY is not supported yet"));
          } else if (opt_name == "transaction_deferrable") {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
              ERR_MSG("transaction DEFERRABLE is not supported yet"));
          }
        });
        return yaclib::MakeFuture(conn_ctx.Begin(isolation_level));
      } else {
        conn_ctx.AddNotice(SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
          ERR_MSG("there is already a transaction in progress")));
      }
      break;
    }
    case TRANS_STMT_COMMIT:
      if (conn_ctx.HasTransactionBegin()) {
        return yaclib::MakeFuture(conn_ctx.Commit());
      } else {
        conn_ctx.AddNotice(
          SQL_ERROR_DATA(ERR_CODE(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                         ERR_MSG("there is no transaction in progress")));
      }
      break;
    case TRANS_STMT_ROLLBACK:
      if (conn_ctx.HasTransactionBegin()) {
        return yaclib::MakeFuture(conn_ctx.Rollback());
      } else {
        conn_ctx.AddNotice(
          SQL_ERROR_DATA(ERR_CODE(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                         ERR_MSG("there is no transaction in progress")));
      }
      break;
    default:
      SDB_UNREACHABLE();
  }
  return {};
}

}  // namespace sdb::pg
