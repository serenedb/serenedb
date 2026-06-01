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

#include <absl/strings/str_cat.h>

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/key_utils.hpp"
#include "connector/search_table_sink_writer.h"
#include "pg/connection_context.h"
#include "query/duckdb_engine.h"
#include "search/inverted_index_shard.h"
#include "search/search_table_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

enum class Scope : uint8_t {
  Database,
  Schema,
  Table,
  Index,
  All,
};

enum class Action : uint8_t {
  Refresh,
  Compact,
  SyncStats,
  CompactRocksdb,
};

struct Verb {
  Action action;
  Scope scope;
};

std::optional<Verb> ParseOption(std::string_view option) {
  static constexpr std::pair<std::string_view, Verb> kVerbs[] = {
    {"refresh_database", {Action::Refresh, Scope::Database}},
    {"refresh_schema", {Action::Refresh, Scope::Schema}},
    {"refresh_table", {Action::Refresh, Scope::Table}},
    {"refresh_index", {Action::Refresh, Scope::Index}},
    {"refresh_all", {Action::Refresh, Scope::All}},
    {"compact_database", {Action::Compact, Scope::Database}},
    {"compact_schema", {Action::Compact, Scope::Schema}},
    {"compact_table", {Action::Compact, Scope::Table}},
    {"compact_index", {Action::Compact, Scope::Index}},
    {"compact_all", {Action::Compact, Scope::All}},
    {"sync_stats_table", {Action::SyncStats, Scope::Table}},
    {"sync_stats_schema", {Action::SyncStats, Scope::Schema}},
    {"sync_stats_database", {Action::SyncStats, Scope::Database}},
    {"sync_stats_all", {Action::SyncStats, Scope::All}},
    {"compact_rocksdb", {Action::CompactRocksdb, Scope::All}},
  };
  for (const auto& [name, verb] : kVerbs) {
    if (option == name) {
      return verb;
    }
  }
  return std::nullopt;
}

struct VacuumBindData : public duckdb::FunctionData {
  std::string option;
  std::string name;
  std::string schema;
  std::string catalog;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<VacuumBindData>();
    copy->option = option;
    copy->name = name;
    copy->schema = schema;
    copy->catalog = catalog;
    return copy;
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    auto& o = other.Cast<VacuumBindData>();
    return option == o.option && name == o.name && schema == o.schema &&
           catalog == o.catalog;
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
    data->name = input.inputs[1].GetValue<std::string>();
  }
  if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
    data->schema = input.inputs[2].GetValue<std::string>();
  }
  if (input.inputs.size() >= 4 && !input.inputs[3].IsNull()) {
    data->catalog = input.inputs[3].GetValue<std::string>();
  }

  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("ok");
  return data;
}

// BaseTableRef parses up to 3 dot-separated identifiers and packs them into
// catalog/schema/table_name. The mapping depends on how many were given:
//   1 -> table_name=<name>, others empty
//   2 -> schema=<a>, table_name=<b>
//   3 -> catalog=<a>, schema=<b>, table_name=<c>
// For SCHEMA scope <schema> or <catalog>.<schema>, and DATABASE scope
// <db> only, re-pack the parts into their natural slots.
struct ResolvedName {
  std::string database;
  std::string schema;
  std::string object;
};

ResolvedName ResolveName(const VacuumBindData& bind, Scope scope,
                         const ConnectionContext& conn_ctx,
                         const catalog::Snapshot& snapshot) {
  ResolvedName out;
  switch (scope) {
    case Scope::Database: {
      if (!bind.schema.empty() || !bind.catalog.empty()) {
        throw duckdb::BinderException(
          "VACUUM (REFRESH_DATABASE|COMPACT_DATABASE|SYNC_STATS_DATABASE) "
          "expects a single database name");
      }
      out.database = bind.name;
      break;
    }
    case Scope::Schema: {
      if (!bind.catalog.empty()) {
        throw duckdb::BinderException(
          "VACUUM (REFRESH_SCHEMA|COMPACT_SCHEMA|SYNC_STATS_SCHEMA) expects "
          "[<database>.]<schema>");
      }
      out.database = bind.schema;
      out.schema = bind.name;
      break;
    }
    case Scope::Table:
    case Scope::Index: {
      out.database = bind.catalog;
      out.schema = bind.schema;
      out.object = bind.name;
      break;
    }
    case Scope::All:
      break;
  }

  if (out.database.empty()) {
    auto db = snapshot.GetDatabase(conn_ctx.GetDatabaseId());
    if (db) {
      out.database = std::string{db->GetName()};
    }
  }
  if (out.schema.empty() && (scope == Scope::Table || scope == Scope::Index)) {
    out.schema = conn_ctx.GetCurrentSchema();
  }
  return out;
}

ObjectId LookupDatabaseId(const catalog::Snapshot& snapshot,
                          std::string_view name) {
  auto db = snapshot.GetDatabase(name);
  if (!db) {
    throw duckdb::CatalogException("database '%s' does not exist.",
                                   std::string{name});
  }
  return db->GetId();
}

void RefreshInvertedShard(search::InvertedIndexShard& inverted) {
  std::ignore = std::move(inverted.CommitWait()).Get().Ok();
}

void CompactInvertedShard(search::InvertedIndexShard& inverted) {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  RefreshInvertedShard(inverted);
  for (size_t pass = 0; pass < 8; ++pass) {
    bool empty_compaction = false;
    const auto [res, _] =
      inverted.CompactUnsafe(kPolicy, kProgress, empty_compaction);
    if (!res.ok()) {
      throw duckdb::InternalException("compact_index: compaction failed: %s",
                                      res.errorMessage());
    }
    RefreshInvertedShard(inverted);
    if (empty_compaction) {
      break;
    }
  }
}

void ForEachInvertedShard(
  const catalog::Snapshot& snapshot, ObjectId relation_id,
  absl::FunctionRef<void(search::InvertedIndexShard&)> v) {
  for (auto& shard : snapshot.GetIndexShardsByRelation(relation_id)) {
    if (!shard || shard->GetType() != catalog::ObjectType::InvertedIndexShard) {
      continue;
    }
    v(basics::downCast<search::InvertedIndexShard>(*shard));
  }
}

// Drains the per-shard cache table (`sdb_cache$.cache_<id>`) into
// iresearch for a kSearch table. This is the M4 PR 4.x sync flow
// (see search_table_shard_native.md §5), called from
// VACUUM (REFRESH_TABLE) on a kSearch table.
//
// Step A scope: INSERT-only drain on generated-PK tables. Skips
// `sdb_op$=1` DELETE rows and explicit-PK encoding (added in Step B).
// Skips the gen-bracketed seqlock window / `sync_mu` / explicit tick
// reservation -- there's no concurrent sync or reader in the
// perf-test scope, and iresearch::Transaction::Commit() auto-allocates
// a tick.
//
// Flow:
//   1. Open a fresh DuckDB Connection on `sdb_cache$` (separate
//      MetaTransaction from the user statement -- cache writes don't
//      need to live in the user's txn).
//   2. SELECT rowid, sdb_op$, sdb_pk$, <user cols> FROM cache_<id>
//      ORDER BY rowid.
//   3. For each chunk, write user columns + per-row PK into the
//      iresearch transaction via SearchTableSinkWriter (lifted from
//      SereneDBSearchInsert::Finalize).
//   4. Commit the iresearch transaction; the caller follows up with
//      SearchTableShard::Commit() (RefreshCommit) to flush the segment.
//   5. DELETE FROM cache_<id> (everything visible to the cache_conn's
//      snapshot from step 1).
void DrainSearchCacheToIresearch(const catalog::Table& table,
                                 search::SearchTableShard& search_shard) {
  const auto cache_table_id = table.GetCacheTableId();
  if (!cache_table_id.isSet()) {
    return;  // Pre-cache kSearch table -- nothing to drain.
  }
  // STEP A: explicit-PK case is deferred to Step B (PK encoding needs
  // per-column UnifiedVectorFormat preparation from the cache chunk).
  SDB_ASSERT(table.PKColumns().empty(),
             "Cache sync Step A: explicit-PK kSearch not supported yet");

  // Build chunk-side column metadata in cache-row order. Cache schema
  // is [sdb_op$, sdb_pk$, <user_cols...>] for generated-PK tables, so
  // user_cols start at index 2 in each chunk we read. We also skip
  // kGeneratedPKId in the catalog Table (virtual column not stored).
  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> chunk_types;
  column_ids.reserve(table.Columns().size());
  chunk_types.reserve(table.Columns().size());
  for (const auto& col : table.Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    column_ids.push_back(col.GetId());
    chunk_types.push_back(col.type);
  }

  // Phase 1 + 2: open cache connection, SELECT rows.
  auto cache_conn = query::DuckDBEngine::Instance().CreateConnection();
  const auto cache_table_name = query::SearchCacheTableName(cache_table_id);
  const auto qualified = absl::StrCat("\"", query::kSearchCacheDatabase,
                                      "\".main.\"", cache_table_name, "\"");
  const auto select_sql =
    absl::StrCat("SELECT * FROM ", qualified, " ORDER BY rowid");
  auto result = cache_conn->Query(select_sql);
  if (result->HasError()) {
    SDB_THROW(ERROR_INTERNAL, "Cache sync SELECT failed: ", result->GetError());
  }

  // Phase 3: encode rows into the iresearch transaction.
  auto search_trx = search_shard.GetTransaction();
  auto table_key = key_utils::PrepareTableKey(table.GetId());
  SearchTableSinkWriter sink{search_trx};
  std::string pk_buffer;
  uint64_t drained_rows = 0;

  // Iterate result chunks. result->Fetch() returns one chunk at a time
  // until null.
  while (auto chunk = result->Fetch()) {
    const auto num_rows = chunk->size();
    if (num_rows == 0) {
      break;
    }
    sink.Init(num_rows);

    // Synthetic columns first: sdb_op$ at col 0, sdb_pk$ at col 1.
    // User columns from col 2 onwards.
    constexpr duckdb::idx_t kSyntheticCols = 2;
    SDB_ASSERT(chunk->ColumnCount() == kSyntheticCols + column_ids.size());

    // SwitchColumn per user column.
    for (duckdb::idx_t i = 0; i < column_ids.size(); ++i) {
      sink.SwitchColumn(column_ids[i], chunk_types[i],
                        chunk->data[kSyntheticCols + i], num_rows);
    }

    // Per-row PK. For STEP A we assume all rows are INSERTs (sdb_op$=0)
    // and skip the per-row op check; Step B will branch on sdb_op$.
    auto* pk_data = duckdb::FlatVector::GetData<int64_t>(chunk->data[1]);
    std::span<const duckdb_primary_key::PKColumn> empty_pk_columns;
    std::vector<duckdb::UnifiedVectorFormat> empty_pk_formats;
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      pk_buffer.clear();
      duckdb_primary_key::MakeColumnKey(
        empty_pk_formats, empty_pk_columns, row,
        static_cast<uint64_t>(pk_data[row]), table_key, [](std::string_view) {},
        pk_buffer);
      sink.Write(key_utils::ExtractRowKey(pk_buffer));
    }
    sink.Finish();
    drained_rows += num_rows;
  }

  if (drained_rows == 0) {
    return;  // Cache is empty -- nothing to commit.
  }

  // Phase 4a: commit iresearch transaction (auto-allocated tick).
  if (!search_trx.Commit()) {
    SDB_THROW(ERROR_INTERNAL,
              "iresearch transaction commit failed during cache sync");
  }

  // Phase 4c: drop drained rows from the cache. Connection::Query
  // auto-manages its own DuckDB transaction; sdb_cache$ is the only DB
  // this Connection touches so MetaTransaction's single-DB rule is
  // satisfied.
  const auto delete_sql = absl::StrCat("DELETE FROM ", qualified);
  auto del_result = cache_conn->Query(delete_sql);
  if (del_result->HasError()) {
    SDB_THROW(ERROR_INTERNAL,
              "Cache sync DELETE failed: ", del_result->GetError());
  }
}

void DispatchInverted(const catalog::Snapshot& snapshot, Action action,
                      Scope scope, const ResolvedName& target) {
  auto apply = [action](search::InvertedIndexShard& s) {
    if (action == Action::Refresh) {
      RefreshInvertedShard(s);
    } else {
      CompactInvertedShard(s);
    }
  };

  auto sync_search_shard = [&](auto& table) {
    auto table_shard = snapshot.GetTableShard(table->GetId());
    if (table_shard &&
        table_shard->GetStorage() == catalog::StorageKind::kSearch) {
      auto& search_shard =
        basics::downCast<search::SearchTableShard>(*table_shard);
      DrainSearchCacheToIresearch(*table, search_shard);
      search_shard.Commit();
    }
  };

  auto walk_schema = [&](ObjectId db_id, std::string_view schema) {
    for (auto& table : snapshot.GetTables(db_id, schema)) {
      ForEachInvertedShard(snapshot, table->GetId(), apply);
      // Commit search-backed table shards (M4 PR 4.1): SearchTableShard
      // has no background commit thread yet, so VACUUM is currently the
      // only way to flush a kSearch shard's pending iresearch trxs into
      // a segment visible to subsequent scans.
      sync_search_shard(table);
    }
  };

  auto walk_database = [&](ObjectId db_id) {
    for (auto& schema : snapshot.GetSchemas(db_id)) {
      walk_schema(db_id, schema->GetName());
    }
  };

  switch (scope) {
    case Scope::Index: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      for (auto& index : snapshot.GetIndexes(db_id, target.schema)) {
        if (index->GetType() != catalog::ObjectType::InvertedIndex ||
            index->GetName() != target.object) {
          continue;
        }
        auto shard = snapshot.GetIndexShard(index->GetId());
        if (!shard ||
            shard->GetType() != catalog::ObjectType::InvertedIndexShard) {
          continue;
        }
        apply(basics::downCast<search::InvertedIndexShard>(*shard));
        return;
      }
      throw duckdb::CatalogException("inverted index '%s' not found.",
                                     target.object);
    }
    case Scope::Table: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      auto table = snapshot.GetTable(db_id, target.schema, target.object);
      if (!table) {
        throw duckdb::CatalogException("relation '%s' not found.",
                                       target.object);
      }
      ForEachInvertedShard(snapshot, table->GetId(), apply);
      sync_search_shard(table);
      break;
    }
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      if (!snapshot.GetSchema(db_id, target.schema)) {
        throw duckdb::CatalogException("schema '%s' does not exist.",
                                       target.schema);
      }
      walk_schema(db_id, target.schema);
      break;
    }
    case Scope::Database: {
      walk_database(LookupDatabaseId(snapshot, target.database));
      break;
    }
    case Scope::All: {
      for (auto& db : snapshot.GetDatabases()) {
        walk_database(db->GetId());
      }
      break;
    }
  }
}

void DispatchSyncStats(const catalog::Snapshot& snapshot, Scope scope,
                       const ResolvedName& target) {
  auto& engine = GetServerEngine();
  auto sync_table = [&](ObjectId table_id) {
    auto shard = snapshot.GetTableShard(table_id);
    if (!shard) {
      return;
    }
    if (auto r = engine.SyncTableShard(*shard); !r.ok()) {
      throw duckdb::InternalException("SyncTableShard failed: %s",
                                      r.errorMessage());
    }
  };
  auto sync_schema = [&](ObjectId db_id, std::string_view schema) {
    for (auto& table : snapshot.GetTables(db_id, schema)) {
      sync_table(table->GetId());
    }
  };
  auto sync_database = [&](ObjectId db_id) {
    for (auto& schema : snapshot.GetSchemas(db_id)) {
      sync_schema(db_id, schema->GetName());
    }
  };

  switch (scope) {
    case Scope::Table: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      auto table = snapshot.GetTable(db_id, target.schema, target.object);
      if (!table) {
        throw duckdb::CatalogException("relation '%s' not found.",
                                       target.object);
      }
      sync_table(table->GetId());
      break;
    }
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      if (!snapshot.GetSchema(db_id, target.schema)) {
        throw duckdb::CatalogException("schema '%s' does not exist.",
                                       target.schema);
      }
      sync_schema(db_id, target.schema);
      break;
    }
    case Scope::Database:
      sync_database(LookupDatabaseId(snapshot, target.database));
      break;
    case Scope::All:
      for (auto& db : snapshot.GetDatabases()) {
        sync_database(db->GetId());
      }
      break;
    case Scope::Index:
      // Unreachable: no SYNC_STATS_INDEX verb in ParseOption's table.
      SDB_VERIFY(false);
  }
}

void VacuumExecute(duckdb::ClientContext& context,
                   duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  auto& bind_data = input.bind_data->Cast<VacuumBindData>();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  auto verb = ParseOption(bind_data.option);
  if (!verb) {
    throw duckdb::BinderException("unknown serenedb VACUUM option '%s'",
                                  bind_data.option);
  }

  const bool needs_name = verb->scope != Scope::All;
  if (needs_name && bind_data.name.empty()) {
    throw duckdb::BinderException("VACUUM (%s) requires an object name",
                                  bind_data.option);
  }
  if (!needs_name && !bind_data.name.empty()) {
    throw duckdb::BinderException("VACUUM (%s) does not take an argument",
                                  bind_data.option);
  }

  auto target = ResolveName(bind_data, verb->scope, conn_ctx, *snapshot);

  switch (verb->action) {
    case Action::Refresh:
    case Action::Compact:
      DispatchInverted(*snapshot, verb->action, verb->scope, target);
      break;
    case Action::SyncStats:
      DispatchSyncStats(*snapshot, verb->scope, target);
      break;
    case Action::CompactRocksdb: {
      auto& engine = GetServerEngine();
      std::ignore = std::move(engine.compactAll(true, true)).Get().Ok();
      break;
    }
  }

  output.SetCardinality(0);
}

// PRAGMA serenedb_vacuum('option', 'name', 'schema', 'catalog')
// Called when DuckDB transforms VACUUM (REFRESH_*|COMPACT_*|...) into this
// PRAGMA. The parameter positions mirror the BaseTableRef qualification
// produced by the parser.
void VacuumPragma(duckdb::ClientContext& context,
                  const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  VacuumBindData bind_data;
  if (args.size() >= 1) {
    bind_data.option = args[0].GetValue<std::string>();
  }
  if (args.size() >= 2) {
    bind_data.name = args[1].GetValue<std::string>();
  }
  if (args.size() >= 3) {
    bind_data.schema = args[2].GetValue<std::string>();
  }
  if (args.size() >= 4) {
    bind_data.catalog = args[3].GetValue<std::string>();
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
