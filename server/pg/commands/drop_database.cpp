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

#include "catalog/database.h"
#include "catalog/databases.h"
#include "pg/commands.h"
#include "yaclib/async/make.hpp"

namespace sdb::pg {

yaclib::Future<Result> DropDatabase(ExecContext& ctx, const DropdbStmt& stmt) {
  auto r = catalog::DropDatabase(ctx, stmt.dbname);
  if (stmt.missing_ok && r.is(ERROR_SERVER_DATABASE_NOT_FOUND)) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
