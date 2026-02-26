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

#include <absl/strings/str_cat.h>

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/function.h"
#include "catalog/sql_function_impl.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<Result> CreateFunction(ExecContext& context,
                                      const CreateFunctionStmt& stmt) {
  // TODO: use correct schema -- what is it about?
  SDB_ASSERT(stmt.funcname);

  auto database_name = context.GetDatabase();
  const auto database_id = context.GetDatabaseId();

  auto& connection_context = basics::downCast<const ConnectionContext>(context);
  auto current_schema = connection_context.GetCurrentSchema();
  auto schema =
    ParseObjectName(stmt.funcname, database_name, current_schema).schema;

  auto function = CreateFunctionImpl(connection_context, database_id,
                                     database_name, current_schema, stmt);

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  auto r = catalog.CreateFunction(database_id, schema, function, stmt.replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    SDB_ASSERT(!stmt.replace);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_TABLE),
      ERR_MSG("relation \"", function->GetName(), "\" already exists"));
  }

  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
