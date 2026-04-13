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

#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/system-compiler.h"
#include "catalog/database.h"
#include "catalog/databases.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"
#include "yaclib/async/make.hpp"

namespace sdb::pg {

yaclib::Future<> DropDatabase(ExecContext& ctx, const DropdbStmt& stmt) {
  auto r = catalog::DropDatabase(ctx, stmt.dbname);
  if (stmt.missing_ok && r.is(ERROR_SERVER_DATABASE_NOT_FOUND)) {
    basics::downCast<ConnectionContext>(ctx).AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
      ERR_MSG("database \"", stmt.dbname, "\" does not exist, skipping")));
    r = {};
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return {};
}

}  // namespace sdb::pg
