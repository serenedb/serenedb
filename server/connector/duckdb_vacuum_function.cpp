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

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"

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
    } break;
    case Scope::Schema: {
      if (!bind.catalog.empty()) {
        throw duckdb::BinderException(
          "VACUUM (REFRESH_SCHEMA|COMPACT_SCHEMA|SYNC_STATS_SCHEMA) expects "
          "[<database>.]<schema>");
      }
      out.database = bind.schema;
      out.schema = bind.name;
    } break;
    case Scope::Table:
    case Scope::Index: {
      out.database = bind.catalog;
      out.schema = bind.schema;
      out.object = bind.name;
    } break;
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

void DispatchInverted(const catalog::Snapshot& snapshot, Action action,
                      Scope scope, const ResolvedName& target) {
  auto apply = [action](search::InvertedIndexShard& s) {
    if (action == Action::Refresh) {
      RefreshInvertedShard(s);
    } else {
      CompactInvertedShard(s);
    }
  };

  auto walk_schema = [&](ObjectId db_id, std::string_view schema) {
    for (auto& table : snapshot.GetTables(db_id, schema)) {
      ForEachInvertedShard(snapshot, table->GetId(), apply);
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
    } break;
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      if (!snapshot.GetSchema(db_id, target.schema)) {
        throw duckdb::CatalogException("schema '%s' does not exist.",
                                       target.schema);
      }
      walk_schema(db_id, target.schema);
    } break;
    case Scope::Database: {
      walk_database(LookupDatabaseId(snapshot, target.database));
    } break;
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
    } break;
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      if (!snapshot.GetSchema(db_id, target.schema)) {
        throw duckdb::CatalogException("schema '%s' does not exist.",
                                       target.schema);
      }
      sync_schema(db_id, target.schema);
    } break;
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
