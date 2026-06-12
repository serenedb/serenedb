////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_physical_ctas.h"

#include <duckdb/main/appender.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_schema_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

struct CTASGlobalState final : public duckdb::GlobalSinkState {
  ObjectId database_id;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  // Appends ride a dedicated connection: the store table was created on
  // the catalog-store connection mid-statement, so it is invisible to (and
  // would write-write conflict with) the user transaction. The table only
  // becomes user-visible at the RemoveTombstone rename, so rows committed
  // through this connection appear atomically.
  std::unique_ptr<duckdb::Connection> store_conn;
  std::unique_ptr<duckdb::Appender> appender;
  duckdb::idx_t insert_count = 0;
  bool finalized = false;

  ~CTASGlobalState() final {
    try {
      appender.reset();
      store_conn.reset();
    } catch (...) {
    }
    if (!finalized && !table_name.empty()) {
      try {
        auto& catalog = catalog::CatalogFeature::instance().Global();
        std::ignore =
          catalog.DropTable(database_name, schema_name, table_name, true);
      } catch (...) {
      }
    }
  }
};

struct CTASSourceState final : public duckdb::GlobalSourceState {
  bool done = false;
};

}  // namespace

SereneDBPhysicalCTAS::SereneDBPhysicalCTAS(
  duckdb::PhysicalPlan& plan,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::SchemaCatalogEntry& schema, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan,
                             duckdb::PhysicalOperatorType::CREATE_TABLE_AS,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _info(std::move(info)),
    _schema(schema) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCTAS::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& schema_entry = _schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();

  auto& create_info = _info->Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  if (!table_info.options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            table_info.options.begin()->first, "\""));
  }

  catalog::CreateTableOptions options;
  options.name = table_info.table;

  for (auto& col : table_info.columns.Logical()) {
    catalog::Column sdb_col{{}, catalog::NextId(), col.Name(), col.Type()};
    if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
    options.columns.push_back(std::move(sdb_col));
  }

  // CTAS has no PK/UNIQUE constraints -- pkColumns stays empty, so the
  // Table constructor wires up a generated PK sequence.

  auto& catalog_impl = catalog::CatalogFeature::instance().Global();

  bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.create_with_tombstone = true;

  auto r = catalog_impl.CreateTable(database_id, _schema.name,
                                    std::move(options), op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   table_info.table);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_table = snapshot->GetTable(database_id, _schema.name,
                                          std::string{table_info.table});
  SDB_ASSERT(catalog_table);
  auto database = snapshot->GetDatabase(database_id);
  SDB_ASSERT(database);

  auto state = duckdb::make_uniq<CTASGlobalState>();
  state->database_id = database_id;
  state->database_name = database->GetName();
  state->schema_name = _schema.name;
  state->table_name = table_info.table;

  // The store table keeps its tombstone name until RemoveTombstone renames
  // it on success.
  state->store_conn =
    std::make_unique<duckdb::Connection>(*context.db);
  state->appender = std::make_unique<duckdb::Appender>(
    *state->store_conn, std::string{catalog::kStoreDatabaseName}, "main",
    catalog::DroppedStoreTableName(catalog_table->GetId()));

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.DropCatalogSnapshot();

  return state;
}

duckdb::SinkResultType SereneDBPhysicalCTAS::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalState>();
  if (!gstate.appender || chunk.size() == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  chunk.Flatten();
  gstate.appender->AppendDataChunk(chunk);
  gstate.insert_count += chunk.size();
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBPhysicalCTAS::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalState>();
  if (gstate.appender) {
    gstate.appender->Close();
    gstate.appender.reset();
    gstate.store_conn.reset();
  }
  // Rows are durable in the store but the table is still tombstone-named;
  // recovery must drop it. Name kept from the SST-ingest era.
  SDB_IF_FAILURE("crash_sst_sink_after_ingest") { SDB_IMMEDIATE_ABORT(); }
  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto r = catalog.RemoveTombstone(gstate.database_id, gstate.schema_name,
                                   gstate.table_name);
  if (!r.ok()) {
    throw duckdb::InternalException("Failed to remove tombstone: %s",
                                    std::string{r.errorMessage()});
  }
  gstate.finalized = true;
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalCTAS::GetGlobalSourceState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<CTASSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalCTAS::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& src = input.global_state.Cast<CTASSourceState>();
  if (src.done) {
    return duckdb::SourceResultType::FINISHED;
  }
  src.done = true;
  duckdb::idx_t count = 0;
  if (sink_state) {
    count = sink_state->Cast<CTASGlobalState>().insert_count;
  }
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(static_cast<int64_t>(count)));
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
