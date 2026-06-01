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

#include "connector/duckdb_physical_cached_ctas.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <utility>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/column_expr.h"
#include "connector/duckdb_schema_entry.h"
#include "pg/sql_exception.h"
#include "query/duckdb_engine.h"

namespace sdb::connector {

SereneDBCachedCtas::SereneDBCachedCtas(
  duckdb::PhysicalPlan& plan,
  duckdb::DuckTableEntry& cache_table,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> bound_constraints,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::SchemaCatalogEntry& user_schema, ObjectId database_id,
  ObjectId cache_table_id, duckdb::idx_t user_col_count,
  duckdb::idx_t estimated_cardinality)
  // PhysicalBatchInsert's `types` is the operator's *output* types
  // (the rowcount column); insert column types are derived from
  // `cache_table.GetTypes()`. Passing cache schema as `types` would
  // truncate the rowcount to TINYINT.
  : duckdb::PhysicalBatchInsert(
      plan, duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::BIGINT},
      cache_table, std::move(bound_constraints), estimated_cardinality),
    _info(std::move(info)),
    _user_schema(user_schema),
    _database_id(database_id),
    _cache_table_id(cache_table_id),
    _user_col_count(user_col_count) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBCachedCtas::GetGlobalSinkState(duckdb::ClientContext& context) const {
  // Build the serened CreateTableOptions from the bound CTAS info. CTAS
  // never has an explicit PK, so a generated-PK sequence is wired up by
  // LocalCatalog::CreateTable. Mirrors CreateCtasTable in
  // duckdb_physical_search_insert.cpp.
  auto& base_info = _info->Base();
  auto& table_info = base_info.Cast<duckdb::CreateTableInfo>();
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
  ApplyColumnModes(options.columns, table_info.options);
  ApplyStorageKind(options, table_info.options);
  options.cache_table_id = _cache_table_id;

  catalog::CreateTableOperationOptions op_options;
  op_options.create_with_tombstone = true;

  auto& catalog_impl =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  const bool if_not_exists =
    base_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  _user_table_name = options.name;
  auto r = catalog_impl.CreateTable(_database_id, _user_schema.name,
                                    std::move(options), op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      // No row groups will be produced. Returning nullptr signals
      // "skip this sink" to the executor (parent honors this in Sink
      // / Finalize -- both early-return on a null state).
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   table_info.table);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  // Resolve the gen-PK sequence from a snapshot that sees the new
  // (tombstoned) Table; Sink reserves PK ranges from it.
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_table =
    snapshot->GetTable(_database_id, _user_schema.name, _user_table_name);
  SDB_ASSERT(catalog_table);
  _gen_pk_seq =
    snapshot->GetObject<catalog::Sequence>(catalog_table->GetGeneratedPkSeqId());
  SDB_ASSERT(_gen_pk_seq);

  // Mark the cache DB's per-DB transaction read-write so the per-DB
  // commit accepts the writes. The bind-time RegisterDBModify on the
  // user catalog claimed MetaTransaction's single-DB slot; calling
  // ModifyDatabase on `sdb_cache$` would trip the rule. The per-DB
  // commit check only looks at its own transaction state, so this is
  // safe -- see SereneDBCachedInsert::GetGlobalSinkState for the same
  // handshake.
  auto& cache_catalog = duckdb::Catalog::GetCatalog(
    context, std::string{query::kSearchCacheDatabase});
  duckdb::Transaction::Get(context, cache_catalog.GetAttached()).SetReadWrite();

  return duckdb::PhysicalBatchInsert::GetGlobalSinkState(context);
}

duckdb::SinkResultType SereneDBCachedCtas::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  SDB_ASSERT(chunk.ColumnCount() == _user_col_count);

  duckdb::DataChunk cache_chunk;
  cache_chunk.Initialize(context.client, insert_types);

  duckdb::idx_t out_col = 0;
  cache_chunk.data[out_col++].Reference(duckdb::Value::TINYINT(0),
                                        duckdb::count_t(chunk.size()));

  // CTAS always generates the PK -- never an explicit-PK CTAS.
  SDB_ASSERT(_gen_pk_seq);
  const auto num_rows = chunk.size();
  const auto base = _gen_pk_seq->ReserveWriteUnsafe(num_rows);
  int64_t* out =
    duckdb::FlatVector::GetDataMutable<int64_t>(cache_chunk.data[out_col]);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    out[i] = static_cast<int64_t>(base + i);
  }
  ++out_col;

  for (duckdb::idx_t i = 0; i < _user_col_count; ++i) {
    cache_chunk.data[out_col + i].Reference(chunk.data[i]);
  }
  cache_chunk.SetCardinality(chunk.size());

  return duckdb::PhysicalBatchInsert::Sink(context, cache_chunk, input);
}

duckdb::SinkFinalizeType SereneDBCachedCtas::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto rt = duckdb::PhysicalBatchInsert::Finalize(pipeline, event, context,
                                                  input);
  if (rt != duckdb::SinkFinalizeType::READY) {
    return rt;
  }
  // Promote the catalog Table from tombstoned to live. On failure the
  // tombstone stays, and the drop-task cleans up both sides next
  // snapshot pass.
  auto& catalog_impl =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto r = catalog_impl.RemoveTombstone(_database_id, _user_schema.name,
                                        _user_table_name);
  if (!r.ok()) {
    throw duckdb::InternalException("Failed to remove CTAS tombstone: %s",
                                    std::string{r.errorMessage()});
  }
  return duckdb::SinkFinalizeType::READY;
}

}  // namespace sdb::connector
