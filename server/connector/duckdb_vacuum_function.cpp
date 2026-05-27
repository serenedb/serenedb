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

#include "connector/duckdb_vacuum_function.h"

#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "search/search_table_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

struct VacuumBindData : public duckdb::FunctionData {
  std::string option;
  std::string table_name;
  std::string schema_name;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<VacuumBindData>();
    copy->option = option;
    copy->table_name = table_name;
    copy->schema_name = schema_name;
    return copy;
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    auto& o = other.Cast<VacuumBindData>();
    return option == o.option && table_name == o.table_name &&
           schema_name == o.schema_name;
  }
};

duckdb::unique_ptr<duckdb::FunctionData> VacuumBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<VacuumBindData>();

  if (input.inputs.size() >= 1 && !input.inputs[0].IsNull()) {
    data->option = input.inputs[0].GetValue<std::string>();
  }
  if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
    data->table_name = input.inputs[1].GetValue<std::string>();
  }
  if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
    data->schema_name = input.inputs[2].GetValue<std::string>();
  }

  // CALL returns empty result
  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("ok");
  return data;
}

void VacuumExecute(duckdb::ClientContext& context,
                   duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  auto& bind_data = input.bind_data->Cast<VacuumBindData>();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto database_id = conn_ctx.GetDatabaseId();

  // Resolve tables
  std::vector<std::shared_ptr<catalog::Table>> tables;
  if (!bind_data.table_name.empty()) {
    auto schema = bind_data.schema_name.empty() ? conn_ctx.GetCurrentSchema()
                                                : bind_data.schema_name;
    auto table = snapshot->GetTable(database_id, schema, bind_data.table_name);
    if (!table) {
      throw duckdb::CatalogException("relation '%s' not found.",
                                     bind_data.table_name);
    }
    tables.push_back(std::move(table));
  } else {
    // All tables in current schema
    tables = snapshot->GetTables(database_id, conn_ctx.GetCurrentSchema());
  }

  auto option = bind_data.option;
  std::transform(option.begin(), option.end(), option.begin(), ::tolower);

  if (option == "update_indexes" || option.empty()) {
    // Commit inverted indexes
    for (const auto& table : tables) {
      for (auto shard : snapshot->GetIndexShardsByRelation(table->GetId())) {
        if (shard &&
            shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
          auto& inverted = basics::downCast<search::InvertedIndexShard>(*shard);
          std::ignore = std::move(inverted.CommitWait()).Get().Ok();
        }
      }
    }
    // Commit search-backed table shards (M4 PR 4.1): SearchTableShard
    // has no background commit thread yet, so VACUUM is currently the
    // only way to flush a kSearch shard's pending iresearch trxs into
    // a segment visible to subsequent scans.
    for (const auto& table : tables) {
      auto table_shard = snapshot->GetTableShard(table->GetId());
      if (table_shard &&
          table_shard->GetStorage() == catalog::StorageKind::kSearch) {
        auto& search_shard =
          basics::downCast<search::SearchTableShard>(*table_shard);
        search_shard.Commit();
      }
    }
  }

  if (option == "sync_stats") {
    auto& engine = GetServerEngine();
    for (const auto& table : tables) {
      auto shard = snapshot->GetTableShard(table->GetId());
      if (auto r = engine.SyncTableShard(*shard); !r.ok()) {
        throw duckdb::InternalException("SyncTableShard failed: %s",
                                        r.errorMessage());
      }
    }
  }

  if (option == "compact") {
    auto& engine = GetServerEngine();
    auto r = std::move(engine.compactAll(true, true)).Get().Ok();
    std::ignore = r;
  }

  if (option == "compact_indexes") {
    const auto policy = irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount{std::numeric_limits<size_t>::max()});
    irs::MergeWriter::FlushProgress progress = [] { return true; };
    for (const auto& table : tables) {
      for (auto shard : snapshot->GetIndexShardsByRelation(table->GetId())) {
        if (!shard ||
            shard->GetType() != catalog::ObjectType::InvertedIndexShard) {
          continue;
        }
        auto& inverted = basics::downCast<search::InvertedIndexShard>(*shard);
        std::ignore = std::move(inverted.CommitWait()).Get().Ok();
        for (size_t pass = 0; pass < 8; ++pass) {
          bool empty_consolidation = false;
          const auto [res, _] =
            inverted.ConsolidateUnsafe(policy, progress, empty_consolidation);
          if (!res.ok()) {
            throw duckdb::InternalException(
              "compact_indexes: consolidation failed: %s", res.errorMessage());
          }
          std::ignore = std::move(inverted.CommitWait()).Get().Ok();
          if (empty_consolidation) {
            break;
          }
        }
      }
    }
  }

  // Return empty result
  output.SetCardinality(0);
}

// PRAGMA serenedb_vacuum('option', 'table_name', 'schema_name')
// Called when DuckDB transforms VACUUM (UPDATE_INDEXES) t into this PRAGMA.
void VacuumPragma(duckdb::ClientContext& context,
                  const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  VacuumBindData bind_data;
  if (args.size() >= 1) {
    bind_data.option = args[0].GetValue<std::string>();
  }
  if (args.size() >= 2) {
    bind_data.table_name = args[1].GetValue<std::string>();
  }
  if (args.size() >= 3) {
    bind_data.schema_name = args[2].GetValue<std::string>();
  }

  duckdb::DataChunk dummy;
  duckdb::TableFunctionInput input{&bind_data, nullptr, nullptr};
  VacuumExecute(context, input, dummy);
}

}  // namespace

void RegisterVacuumFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  duckdb::TableFunction func("serenedb_vacuum", {}, VacuumExecute, VacuumBind);
  func.varargs = duckdb::LogicalType::VARCHAR;
  loader.RegisterFunction(func);

  auto pragma = duckdb::PragmaFunction::PragmaCall(
    "serenedb_vacuum", VacuumPragma, {duckdb::LogicalType::VARCHAR});
  pragma.varargs = duckdb::LogicalType::VARCHAR;
  loader.RegisterFunction(pragma);
}

}  // namespace sdb::connector
