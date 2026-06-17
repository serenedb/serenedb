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
#include <absl/strings/str_replace.h>

#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

enum class Scope : uint8_t {
  Database,
  Schema,
  Table,
  Index,
  Column,
  All,
};

enum class Action : uint8_t {
  Refresh,
  Compact,
  RecomputeStats,
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
    {"recompute_stats_table", {Action::RecomputeStats, Scope::Table}},
    {"recompute_stats_schema", {Action::RecomputeStats, Scope::Schema}},
    {"recompute_stats_database", {Action::RecomputeStats, Scope::Database}},
    {"recompute_stats_all", {Action::RecomputeStats, Scope::All}},
    {"recompute_stats_column", {Action::RecomputeStats, Scope::Column}},
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
  std::string column;
};

ResolvedName ResolveName(const VacuumBindData& bind, Scope scope,
                         const ConnectionContext& conn_ctx,
                         const catalog::Snapshot& snapshot) {
  ResolvedName out;
  switch (scope) {
    case Scope::Database: {
      if (!bind.schema.empty() || !bind.catalog.empty()) {
        throw duckdb::BinderException(
          "VACUUM (REFRESH_DATABASE|COMPACT_DATABASE) "
          "expects a single database name");
      }
      out.database = bind.name;
    } break;
    case Scope::Schema: {
      if (!bind.catalog.empty()) {
        throw duckdb::BinderException(
          "VACUUM (REFRESH_SCHEMA|COMPACT_SCHEMA) expects "
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
    case Scope::Column: {
      // [<schema>.]<table>.<column> -- the trailing identifier is the column.
      if (!bind.catalog.empty()) {
        out.schema = bind.catalog;
        out.object = bind.schema;
        out.column = bind.name;
      } else if (!bind.schema.empty()) {
        out.object = bind.schema;
        out.column = bind.name;
      } else {
        throw duckdb::BinderException(
          "VACUUM (RECOMPUTE_STATS_COLUMN) expects "
          "[<schema>.]<table>.<column>");
      }
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
  if (out.schema.empty() && (scope == Scope::Table || scope == Scope::Index ||
                             scope == Scope::Column)) {
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

void CompactInvertedStorage(search::InvertedIndexStorage& inverted) {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  inverted.Refresh();
  for (size_t pass = 0; pass < 8; ++pass) {
    bool empty_compaction = false;
    const auto [res, _] =
      inverted.CompactUnsafe(kPolicy, kProgress, empty_compaction);
    if (!res.ok()) {
      throw duckdb::InternalException("compact_index: compaction failed: %s",
                                      res.errorMessage());
    }
    inverted.Refresh();
    if (empty_compaction) {
      break;
    }
  }
}

void ForEachInvertedStorage(
  const catalog::Snapshot& snapshot, ObjectId relation_id,
  absl::FunctionRef<void(search::InvertedIndexStorage&)> v) {
  for (auto& index : snapshot.GetIndexesByRelation(relation_id)) {
    if (!index || index->GetType() != catalog::ObjectType::InvertedIndex) {
      continue;
    }
    if (auto storage =
          basics::downCast<const catalog::InvertedIndex>(*index).GetData()) {
      v(*storage);
    }
  }
}

void DispatchInverted(const catalog::Snapshot& snapshot, Action action,
                      Scope scope, const ResolvedName& target) {
  auto apply = [action](search::InvertedIndexStorage& s) {
    if (action == Action::Refresh) {
      s.Refresh();
    } else {
      CompactInvertedStorage(s);
    }
  };

  auto walk_schema = [&](ObjectId db_id, std::string_view schema) {
    for (auto& table : snapshot.GetTables(db_id, schema)) {
      ForEachInvertedStorage(snapshot, table->GetId(), apply);
    }
  };

  auto walk_database = [&](ObjectId db_id) {
    for (auto& schema : snapshot.GetSchemas(db_id)) {
      walk_schema(db_id, schema->GetName());
    }
  };

  switch (scope) {
    case Scope::Column:
      // No refresh/compact at column granularity.
      break;
    case Scope::Index: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      for (auto& index : snapshot.GetIndexes(db_id, target.schema)) {
        if (index->GetType() != catalog::ObjectType::InvertedIndex ||
            index->GetName() != target.object) {
          continue;
        }
        auto storage =
          basics::downCast<const catalog::InvertedIndex>(*index).GetData();
        if (!storage) {
          continue;
        }
        apply(*storage);
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
      ForEachInvertedStorage(snapshot, table->GetId(), apply);
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

// Recompute optimizer column statistics for the store tables backing the
// serenedb tables in scope, by running DuckDB's `VACUUM ANALYZE` on each store
// table. The user names serenedb tables; the hidden store is never exposed.
void DispatchRecomputeStats(duckdb::ClientContext& context,
                            const catalog::Snapshot& snapshot, Scope scope,
                            const ResolvedName& target) {
  duckdb::Connection conn(*context.db);
  auto analyze = [&](std::string_view db_name, std::string_view schema_name,
                     const catalog::Table& table,
                     std::string_view column = {}) {
    if (table.GetEngine() != catalog::TableEngine::Transactional ||
        table.Tombstoned()) {
      return;
    }
    auto store_name =
      catalog::StoreTableName(db_name, schema_name, table.GetName());
    auto quoted = absl::StrReplaceAll(store_name, {{"\"", "\"\""}});
    std::string column_clause;
    if (!column.empty()) {
      column_clause = absl::StrCat(
        " (\"", absl::StrReplaceAll(column, {{"\"", "\"\""}}), "\")");
    }
    auto result =
      conn.Query(absl::StrCat("VACUUM ANALYZE \"", catalog::kStoreDatabaseName,
                              "\".main.\"", quoted, "\"", column_clause));
    if (result->HasError()) {
      throw duckdb::InternalException("recompute_stats failed: %s",
                                      result->GetError());
    }
  };
  auto walk_schema = [&](ObjectId db_id, std::string_view db_name,
                         std::string_view schema) {
    for (auto& table : snapshot.GetTables(db_id, schema)) {
      analyze(db_name, schema, *table);
    }
  };
  auto walk_database = [&](ObjectId db_id, std::string_view db_name) {
    for (auto& schema : snapshot.GetSchemas(db_id)) {
      walk_schema(db_id, db_name, schema->GetName());
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
      analyze(target.database, target.schema, *table);
    } break;
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      if (!snapshot.GetSchema(db_id, target.schema)) {
        throw duckdb::CatalogException("schema '%s' does not exist.",
                                       target.schema);
      }
      walk_schema(db_id, target.database, target.schema);
    } break;
    case Scope::Database:
      walk_database(LookupDatabaseId(snapshot, target.database),
                    target.database);
      break;
    case Scope::All:
      for (auto& db : snapshot.GetDatabases()) {
        walk_database(db->GetId(), db->GetName());
      }
      break;
    case Scope::Column: {
      auto db_id = LookupDatabaseId(snapshot, target.database);
      auto table = snapshot.GetTable(db_id, target.schema, target.object);
      if (!table) {
        throw duckdb::CatalogException("relation '%s' not found.",
                                       target.object);
      }
      analyze(target.database, target.schema, *table, target.column);
    } break;
    case Scope::Index:
      // No recompute_stats_index verb in ParseOption's table.
      break;
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
    case Action::RecomputeStats:
      DispatchRecomputeStats(context, *snapshot, verb->scope, target);
      break;
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
