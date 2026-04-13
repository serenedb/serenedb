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

#include <duckdb/execution/execution_context.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_schema_entry.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// TODO(mbkkt) fix this, drop by object id! Otherwise rename can break this
struct CTASGlobalState : public SSTInsertGlobalState {
  std::string database_name;
  std::string schema_name;
  std::string table_name;

  ~CTASGlobalState() override {
    if (!finalized && !table_name.empty()) {
      try {
        auto& catalog = SerenedServer::Instance()
                          .getFeature<catalog::CatalogFeature>()
                          .Global();
        std::ignore = catalog.DropTable(database_name, schema_name, table_name);
      } catch (...) {
      }
    }
  }
};

}  // namespace

SereneDBPhysicalCTAS::SereneDBPhysicalCTAS(
  duckdb::PhysicalPlan& plan,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::SchemaCatalogEntry& schema, duckdb::idx_t estimated_cardinality)
  : SereneDBPhysicalSSTInsert(plan, nullptr, {duckdb::LogicalType::BIGINT},
                              estimated_cardinality),
    _info(std::move(info)),
    _schema(schema) {}

// --- GetGlobalSinkState: create table, then delegate to parent ---

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCTAS::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& schema_entry = _schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();

  auto& create_info = _info->Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  // Build CreateTableRequest from DuckDB types
  catalog::CreateTableRequest request;
  request.name = table_info.table;

  catalog::Column::Id next_col_id = 0;
  for (auto& col : table_info.columns.Logical()) {
    catalog::Column sdb_col;
    sdb_col.id = next_col_id++;
    sdb_col.name = col.Name();
    sdb_col.type = col.Type();
    if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
    request.columns.push_back(std::move(sdb_col));
  }

  // CTAS syntax doesn't support PK/UNIQUE constraints -- pkColumns stays empty,
  // so MakeTableOptions will assign a generated PK.

  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(database_id);
  SDB_ASSERT(database);

  catalog::CreateTableOptions options;
  auto r = catalog::MakeTableOptions(std::move(request), database_id, options,
                                     database->GetReplicationFactor(),
                                     database->GetWriteConcern(), false);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.create_with_tombstone = true;

  r = catalog_impl.CreateTable(database_id, _schema.name, std::move(options),
                               op_options);
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

  // Get the newly created table and set up SST writers.
  // Don't call parent's GetGlobalSinkState -- it creates index writers which
  // crash on a tombstoned table not yet visible in the connection snapshot.
  snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_table = snapshot->GetTable(database_id, _schema.name,
                                          std::string{table_info.table});
  SDB_ASSERT(catalog_table);

  auto state = duckdb::make_uniq<CTASGlobalState>();
  state->database_name = database->GetName();
  state->schema_name = _schema.name;
  state->table_name = table_info.table;
  SetupSSTState(*state, *catalog_table);

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();

  return state;
}

// --- Finalize: parent ingests SSTs, then we remove tombstone ---

duckdb::SinkFinalizeType SereneDBPhysicalCTAS::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto result =
    SereneDBPhysicalSSTInsert::Finalize(pipeline, event, context, input);

  // Remove tombstone if table was created (sink_state is non-null)
  if (sink_state) {
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    auto& table_info = _info->Base().Cast<duckdb::CreateTableInfo>();
    auto r = catalog.RemoveTombstone(
      _schema.Cast<SereneDBSchemaEntry>().GetDatabaseId(), _schema.name,
      table_info.table);
    if (!r.ok()) {
      throw duckdb::InternalException("Failed to remove tombstone: %s",
                                      std::string{r.errorMessage()});
    }
  }

  return result;
}

}  // namespace sdb::connector
