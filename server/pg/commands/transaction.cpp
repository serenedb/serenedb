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

namespace sdb::pg {

yaclib::Future<Result> Transaction(ExecContext& context,
                                   const TransactionStmt& stmt) {
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);
  Result r;
  switch (stmt.kind) {
    case TRANS_STMT_BEGIN:
    case TRANS_STMT_START:
      if (!conn_ctx.InsideTransaction()) {
        conn_ctx.Begin();
      } else {
        r = {ERROR_QUERY_USER_WARN,
             "there is already a transaction in progress"};
      }
      break;
    case TRANS_STMT_COMMIT:
      if (conn_ctx.InsideTransaction()) {
        conn_ctx.Commit();
      } else {
        r = {ERROR_QUERY_USER_WARN, "there is no transaction in progress"};
      }
      break;
    case TRANS_STMT_ROLLBACK:
      if (conn_ctx.InsideTransaction()) {
        conn_ctx.Abort();
      } else {
        r = {ERROR_QUERY_USER_WARN, "there is no transaction in progress"};
      }
      break;
    default:
      SDB_UNREACHABLE();
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
