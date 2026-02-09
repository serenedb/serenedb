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
  const auto db = _context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(_context);
  std::string current_schema = conn_ctx.GetCurrentSchema();

  const auto& rel = *_stmt.into->rel;
  const std::string_view schema =
    rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
  if (schema.empty()) {
    return yaclib::MakeFuture<Result>(
      ERROR_BAD_PARAMETER, "no schema has been selected to create in");
  }
  const std::string_view table = rel.relname;

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto database = catalog.GetSnapshot()->GetDatabase(db);
  SDB_ENSURE(database, ERROR_SERVER_DATABASE_NOT_FOUND);

  catalog::CreateTableRequest request;
  request.name = table;

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
  // Set protective tombstone for CTAS
  options.createWithProtectiveTombstone = true;
  r = catalog.CreateTable(db, schema, std::move(options), {});
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && _stmt.if_not_exists) {
    r = {};
  }

  Objects objects;
  objects.ensureRelation(schema, table);
  Resolve(db, objects, conn_ctx);
  auto* object = objects.getRelation(schema, table);
  SDB_ASSERT(object);
  object->EnsureTable(_transaction);
  _write.setTable(object->table);

  _table_created = true;
  return yaclib::MakeFuture(std::move(r));
}

yaclib::Future<Result> CTASCommand::RemoveDropMarker() {
  Objects objects;
  const auto db = _context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(_context);
  std::string current_schema = conn_ctx.GetCurrentSchema();

  const auto& rel = *_stmt.into->rel;
  const std::string_view schema =
    rel.schemaname ? std::string_view{rel.schemaname} : current_schema;
  const std::string_view table = rel.relname;

  objects.ensureRelation(schema, table);
  Resolve(db, objects, conn_ctx);
  auto* object = objects.getRelation(schema, table);
  if (!object || !object->object) {
    return yaclib::MakeFuture<Result>(
      ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "Table not found after creation");
  }

  auto& engine = GetServerEngine();
  auto r = engine.RemoveTombstone(object->object->GetId());
  if (r.ok()) {
    _marker_removed = true;
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
