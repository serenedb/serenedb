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

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/sharding_strategy.h"
#include "catalog/table_options.h"
#include "connector/serenedb_connector.hpp"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"
#include "pg/sql_utils.h"
#include "query/query.h"
#include "query/transaction.h"
#include "storage_engine/engine_feature.h"

namespace sdb::query {

yaclib::Future<> CTASCommand::CreateTable() {
  _db = _context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(_context);
  std::string current_schema = conn_ctx.GetCurrentSchema();

  const auto& rel = *_into.rel;
  _schema = rel.schemaname ? rel.schemaname : std::move(current_schema);
  if (_schema.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("no schema has been selected to create in"));
  }
  _table_name = rel.relname;

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto database = catalog.GetSnapshot()->GetDatabase(_db);
  SDB_ENSURE(database, ERROR_SERVER_DATABASE_NOT_FOUND);

  catalog::CreateTableRequest request;
  request.name = _table_name;

  auto& columns = request.columns;
  columns.resize(_write.columnNames().size());
  for (size_t i = 0; i < columns.size(); ++i) {
    columns[i].id = i;
    columns[i].name = _write.columnNames()[i];
    columns[i].type = _write.columnExpressions()[i]->type();
  }

  catalog::CreateTableOptions options;
  auto r = MakeTableOptions(std::move(request), database->GetId(), options,
                            database->GetReplicationFactor(),
                            database->GetWriteConcern(), {});
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  catalog::CreateTableOperationOptions table_operation;
  table_operation.create_with_tombstone = true;

  r = catalog.CreateTable(_db, _schema, std::move(options), table_operation);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && _if_not_exists) {
    return {};
  } else if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", _table_name, "\" already exists"));
  }

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  auto snapshot = catalog.GetSnapshot();
  auto catalog_table = snapshot->GetTable(_db, _schema, _table_name);
  SDB_ASSERT(catalog_table);
  auto axiom_table =
    std::make_shared<connector::RocksDBTable>(*catalog_table, _transaction);
  axiom_table->BulkInsert() = true;
  _write.setTable(std::move(axiom_table));

  return {};
}

void CTASCommand::Rollback() {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::ignore = catalog.DropTable(_db, _schema, _table_name);
}

CreateTableExecutor::CreateTableExecutor(
  std::unique_ptr<CTASCommand> ctas_command)
  : _ctas_command{std::move(ctas_command)} {}

yaclib::Future<> CreateTableExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query);
  SDB_ASSERT(_ctas_command);

  if (std::exchange(_fired, true)) {
    return {};
  }

  auto f = _ctas_command->CreateTable();
  if (!f.Ready()) {
    return std::move(f).ThenInline([this] { _query->CompileQuery(); });
  }
  std::ignore = std::move(f).Touch().Ok();
  _query->CompileQuery();
  return {};
}

CTASVeloxExecutor::CTASVeloxExecutor(CTASCommand& ctas_command)
  : _ctas_command{ctas_command} {}

yaclib::Future<> CTASVeloxExecutor::Execute(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query);
  if (!_runner) {
    _runner = _query->MakeRunner();
  }
  try {
    return VeloxExecutor::Execute(batch);
  } catch (...) {
    _ctas_command.Rollback();
    throw;
  }
}

}  // namespace sdb::query
