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

#include "connector/duckdb_truncate_function.h"

#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/database.hpp>
#include <mutex>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

void TruncateResolvedTable(
  ConnectionContext& conn_ctx,
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const std::shared_ptr<catalog::Table>& table) {
  auto table_shard = snapshot->GetTableShard(table->GetId());
  SDB_ASSERT(table_shard);
  std::unique_lock lock{table_shard->GetTableLock()};

  auto& engine = GetServerEngine();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  auto* db = engine.db()->GetRootDB();

  rocksdb::WriteBatch batch;
  auto [start, end] = key_utils::CreateTableRange(table->GetId());
  auto s = batch.DeleteRange(cf, rocksdb::Slice{start}, rocksdb::Slice{end});
  if (!s.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("TRUNCATE: row range delete failed: ", s.ToString()));
  }

  auto index_shards = snapshot->GetIndexShardsByRelation(table->GetId());

  // Phase 1: queue per-shard range deletes AND, for every inverted-index
  // shard, lock its commit mutex via TruncateBegin -- mirrors the legacy
  // r_idx->truncateBegin(batch) handshake. The batch is built but not yet
  // written; the locks straddle db->Write so no concurrent search commit
  // can land between rocksdb's atomic write and the iresearch Clear.
  std::vector<std::pair<search::InvertedIndexShard*,
                        search::InvertedIndexShard::TruncateGuard>>
    inverted_guards;
  for (auto& shard : index_shards) {
    SDB_ASSERT(shard);
    auto [start, end] = key_utils::CreateTableRange(shard->GetId());
    auto s = batch.DeleteRange(cf, rocksdb::Slice{start}, rocksdb::Slice{end});
    if (!s.ok()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("TRUNCATE: index range delete failed: ", s.ToString()));
    }
    if (shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      auto& inverted = basics::downCast<search::InvertedIndexShard>(*shard);
      inverted_guards.emplace_back(&inverted, inverted.TruncateBegin());
    }
  }

  if (auto s = db->Write({}, &batch); !s.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("TRUNCATE: atomic write failed: ", s.ToString()));
  }

  // Phase 2: rocksdb is durable. Anchor the in-memory state to the
  // post-write seq while still holding every inverted shard's commit mutex
  // (released as the guards in `inverted_guards` are consumed below).
  // noexcept lambda: any failure here means rocksdb already committed but
  // iresearch / catalog state is mid-transition -- terminate so recovery
  // can replay cleanly from the WAL.
  [&]() noexcept {
    const auto seq = static_cast<Tick>(db->GetLatestSequenceNumber() - 1);
    if (int64_t current = table_shard->GetTableStats().num_rows; current > 0) {
      table_shard->UpdateNumRows(-current);
    }
    for (auto& [inverted, guard] : inverted_guards) {
      inverted->TruncateCommit(std::move(guard), seq, &conn_ctx);
    }
  }();
}

namespace {

struct TruncateBindData : public duckdb::FunctionData {
  // Parallel arrays: schemas[i] / tables[i]. Empty schemas[i] means "use
  // current connection schema".
  std::vector<std::string> schemas;
  std::vector<std::string> tables;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    auto copy = duckdb::make_uniq<TruncateBindData>();
    copy->schemas = schemas;
    copy->tables = tables;
    return copy;
  }
  bool Equals(const duckdb::FunctionData& other) const override {
    auto& o = other.Cast<TruncateBindData>();
    return schemas == o.schemas && tables == o.tables;
  }
};

void ParseInputs(const duckdb::vector<duckdb::Value>& inputs,
                 TruncateBindData& data) {
  if (inputs.size() != 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("TRUNCATE: expected (schemas, tables) lists"));
  }
  const auto& schemas_v = duckdb::ListValue::GetChildren(inputs[0]);
  const auto& tables_v = duckdb::ListValue::GetChildren(inputs[1]);
  if (schemas_v.size() != tables_v.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("TRUNCATE: schemas/tables length mismatch"));
  }
  if (tables_v.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("TRUNCATE: at least one table required"));
  }
  data.schemas.reserve(schemas_v.size());
  data.tables.reserve(tables_v.size());
  for (size_t i = 0; i < tables_v.size(); ++i) {
    data.schemas.emplace_back(schemas_v[i].IsNull()
                                ? std::string{}
                                : schemas_v[i].GetValue<std::string>());
    if (tables_v[i].IsNull()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("TRUNCATE: NULL table name"));
    }
    data.tables.emplace_back(tables_v[i].GetValue<std::string>());
  }
}

duckdb::unique_ptr<duckdb::FunctionData> TruncateBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<TruncateBindData>();
  ParseInputs(input.inputs, *data);

  return_types.emplace_back(duckdb::LogicalType::BOOLEAN);
  names.emplace_back("ok");
  return data;
}

void TruncateOne(ConnectionContext& conn_ctx,
                 const std::shared_ptr<const catalog::Snapshot>& snapshot,
                 const std::string& schema_name_in,
                 const std::string& table_name) {
  const auto database_id = conn_ctx.GetDatabaseId();
  const auto schema_name =
    schema_name_in.empty() ? conn_ctx.GetCurrentSchema() : schema_name_in;

  auto relation = snapshot->GetRelation(database_id, schema_name, table_name);
  if (!relation) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table_name, "\" does not exist"));
  }
  if (relation->GetType() != catalog::ObjectType::Table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", table_name, "\" is not a table"));
  }
  auto table = basics::downCast<catalog::Table>(std::move(relation));

  TruncateResolvedTable(conn_ctx, snapshot, table);
}

void TruncateExecute(duckdb::ClientContext& context,
                     duckdb::TableFunctionInput& input,
                     duckdb::DataChunk& output) {
  auto& bind_data = input.bind_data->Cast<TruncateBindData>();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  for (size_t i = 0; i < bind_data.tables.size(); ++i) {
    TruncateOne(conn_ctx, snapshot, bind_data.schemas[i], bind_data.tables[i]);
  }

  output.SetCardinality(0);
}

// PRAGMA serenedb_truncate(LIST<schemas>, LIST<tables>)
// Called when DuckDB transforms TRUNCATE into this PRAGMA.
void TruncatePragma(duckdb::ClientContext& context,
                    const duckdb::FunctionParameters& params) {
  TruncateBindData bind_data;
  ParseInputs(params.values, bind_data);

  duckdb::DataChunk dummy;
  duckdb::TableFunctionInput input{&bind_data, nullptr, nullptr};
  TruncateExecute(context, input, dummy);
}

}  // namespace

void RegisterTruncateFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  const auto str_list = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);

  duckdb::TableFunction func("serenedb_truncate", {str_list, str_list},
                             TruncateExecute, TruncateBind);
  loader.RegisterFunction(func);

  auto pragma = duckdb::PragmaFunction::PragmaCall(
    "serenedb_truncate", TruncatePragma, {str_list, str_list});
  loader.RegisterFunction(pragma);
}

}  // namespace sdb::connector
