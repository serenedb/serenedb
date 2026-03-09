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

#include "query/ctas_executor.h"

#include <absl/cleanup/cleanup.h>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "pg/connection_context.h"
#include "query/query.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::query {

void CTASVeloxExecutor::Rollback() {
  auto db = _context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(_context);
  const auto& rel = *_into.rel;
  std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
  SDB_ASSERT(!schema.empty());  // is supposed to be fallen in table creation
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::ignore = catalog.DropTable(db, schema, _into.rel->relname);
}

yaclib::Future<> CTASVeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query);
  if (!_runner) {
    _runner = _query->MakeRunner();
  }
  absl::Cleanup rollback = [&]() noexcept { Rollback(); };
  auto f = VeloxExecutor::Execute(batch);
  std::move(rollback).Cancel();
  return f;
}

}  // namespace sdb::query
