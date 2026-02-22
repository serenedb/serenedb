#include "ctas.h"

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/sharding_strategy.h"
#include "catalog/table_options.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"
#include "pg/sql_utils.h"
#include "query/transaction.h"
#include "storage_engine/engine_feature.h"

namespace sdb::pg {

yaclib::Future<Result> CTASCommand::CreateTable() {
  _db = _context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(_context);
  std::string current_schema = conn_ctx.GetCurrentSchema();

  const auto& rel = *_stmt.into->rel;
  _schema = rel.schemaname ? rel.schemaname : std::move(current_schema);
  if (_schema.empty()) {
    return yaclib::MakeFuture<Result>(
      ERROR_BAD_PARAMETER, "no schema has been selected to create in");
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
    return yaclib::MakeFuture(std::move(r));
  }

  catalog::CreateTableOperationOptions table_operation;
  table_operation.in_memory_only = true;
  table_operation.create_with_tombstone = true;
  
  r = catalog.CreateTable(_db, _schema, std::move(options), table_operation);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && _stmt.if_not_exists) {
    r = {};
  }

  Objects objects;
  std::string_view schema{_schema};
  objects.ensureRelation(schema, _table_name);
  Resolve(_db, objects, conn_ctx);
  auto* object = objects.getRelation(schema, _table_name);
  SDB_ASSERT(object);
  object->EnsureTable(_transaction);
  _write.setTable(object->table);
  _created_table_id = object->object->GetId();

  _table_created = true;
  return yaclib::MakeFuture(std::move(r));
}

void CTASCommand::Rollback() {
  if (!_table_created || _is_persisted) {
    return;
  }
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::ignore = catalog.DropTable(_db, _schema, _table_name, nullptr);
}

yaclib::Future<Result> CTASCommand::PersistTableDefinition() {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  _is_persisted = true;
  return yaclib::MakeFuture(catalog.PersistTable(_created_table_id));
}

}  // namespace sdb::pg
