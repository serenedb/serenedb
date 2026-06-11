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
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/sequence.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// TODO(mbkkt) fix this, drop by object id! Otherwise rename can break this
struct CTASGlobalState final : public SSTInsertGlobalState {
  ObjectId database_id;
  std::string database_name;
  std::string schema_name;
  std::string table_name;

  ~CTASGlobalState() final {
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

  ApplyColumnModes(options.columns, table_info.options);
  ApplyStorageKind(options, table_info.options);
  SDB_ASSERT(options.storage == catalog::StorageKind::kRocksDB,
             "search-backed CTAS must route through SereneDBSearchInsert");

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

  // Get the newly created table and set up SST writers.
  // Don't call parent's GetGlobalSinkState -- it creates index writers which
  // crash on a tombstoned table not yet visible in the connection snapshot.
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_table = snapshot->GetTable(database_id, _schema.name,
                                          std::string{table_info.table});
  SDB_ASSERT(catalog_table);
  auto database = snapshot->GetDatabase(database_id);
  SDB_ASSERT(database);

  auto state = duckdb::make_uniq<CTASGlobalState>();
  state->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context));
  state->database_id = database_id;
  state->database_name = database->GetName();
  state->schema_name = _schema.name;
  state->table_name = table_info.table;
  SetupSSTState(*state, *catalog_table);

  state->generated_pk_seq = snapshot->GetObject<catalog::Sequence>(
    catalog_table->GetGeneratedPkSeqId());
  SDB_ASSERT(state->generated_pk_seq || !catalog_table->PKColumns().empty());

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.DropCatalogSnapshot();

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
    SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
    auto& gstate = sink_state->Cast<CTASGlobalState>();
    auto& catalog = catalog::CatalogFeature::instance().Global();
    auto r = catalog.RemoveTombstone(gstate.database_id, gstate.schema_name,
                                     gstate.table_name);
    if (!r.ok()) {
      throw duckdb::InternalException("Failed to remove tombstone: %s",
                                      std::string{r.errorMessage()});
    }
  }

  return result;
}

}  // namespace sdb::connector
