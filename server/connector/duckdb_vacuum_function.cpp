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

#include "auth/role_closure.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "catalog/table_options.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_object_index.h"
#include "connector/duckdb_table_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

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

ResolvedName ResolveName(duckdb::ClientContext& context,
                         const VacuumBindData& bind, Scope scope,
                         const ConnectionContext& conn_ctx) {
  ResolvedName out;
  switch (scope) {
    case Scope::Database: {
      if (!bind.schema.empty() || !bind.catalog.empty()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("VACUUM (REFRESH_DATABASE|COMPACT_DATABASE) "
                                "expects a single database name"));
      }
      out.database = bind.name;
    } break;
    case Scope::Schema: {
      if (!bind.catalog.empty()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("VACUUM (REFRESH_SCHEMA|COMPACT_SCHEMA) expects "
                  "[<database>.]<schema>"));
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
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("VACUUM (RECOMPUTE_STATS_COLUMN) expects "
                                "[<schema>.]<table>.<column>"));
      }
    } break;
    case Scope::All:
      break;
  }

  if (out.database.empty()) {
    out.database = DatabaseName(nullptr, conn_ctx.GetDatabaseId());
  }
  if (out.schema.empty() && (scope == Scope::Table || scope == Scope::Index ||
                             scope == Scope::Column)) {
    out.schema = conn_ctx.GetCurrentSchema();
  }
  return out;
}

ObjectId LookupDatabaseId(std::string_view name) {
  const auto id = FindDatabase(nullptr, name).Id();
  if (!id.isSet()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                    ERR_MSG("database \"", name, "\" does not exist"));
  }
  return id;
}

void CompactInvertedStorage(search::InvertedIndexStorage& inverted,
                            const catalog::CreateInvertedIndexInfo& index,
                            duckdb::ClientContext& context,
                            pg::ProgressMetrics* progress) {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  // Fired by the merge every ~16k docs: feeds the sub-index progress counter
  // and aborts the merge on user cancellation.
  const irs::MergeWriter::FlushProgress tick = [&context, progress] {
    if (progress) {
      pg::ProgressMetrics::Add(progress->step, 1);
    }
    return !context.IsInterrupted();
  };
  inverted.Refresh();
  for (size_t pass = 0; pass < 8; ++pass) {
    bool empty_compaction = false;
    // The merge encodes against the index definition the step captured, which
    // the step holds for the whole call.
    const auto [res, _] =
      inverted.CompactUnsafe(kPolicy, tick, empty_compaction, &index);
    if (!res.ok()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("compact_index: compaction failed: ", res.message()));
    }
    inverted.Refresh();
    if (empty_compaction) {
      break;
    }
  }
}

// One unit of inverted-index maintenance: either an index refresh/compaction
// or a Search-table commit, in the same per-table order the walk visits them.
// Owning pointers: the steps run after the collection walk finished.
struct InvertedStep {
  std::shared_ptr<search::InvertedIndexStorage> storage;
  catalog::IndexInfoRef index;
  std::shared_ptr<search::SearchTable> search_data;
};

// What VACUUM needs of one table, taken off its entry during the walk. Copied
// out rather than held as a pointer because resolving a table's indexes opens
// the schema's index set, which must not happen while the walk is holding the
// relation set.
struct MaintainTarget {
  ObjectId id;
  ObjectId schema_id;
  std::string schema;
  std::string name;
  catalog::TableEngine engine;
  std::shared_ptr<search::SearchTable> search_data;
  catalog::Permissions perm;
};

MaintainTarget MakeMaintainTarget(std::string_view schema,
                                  const connector::SereneDBTableEntry& table) {
  return {.id = catalog::IdOf(table),
          .schema_id = catalog::ParentIdOf(table),
          .schema = std::string{schema},
          .name = std::string{table.name.GetIdentifierName()},
          .engine = table.GetEngine(),
          .search_data = table.GetSearchData(),
          .perm = table.permissions};
}

// Every base table of `database`, or of one schema of it when `schema` is set.
std::vector<MaintainTarget> CollectMaintainTargets(
  duckdb::ClientContext& context, ObjectId database, std::string_view schema) {
  std::vector<MaintainTarget> out;
  connector::VisitTableEntries(
    context, database,
    [&](const catalog::CreateSchemaInfo& in_schema,
        const connector::SereneDBTableEntry& table) {
      if (schema.empty() || in_schema.GetName() == schema) {
        out.push_back(MakeMaintainTarget(in_schema.GetName(), table));
      }
    });
  return out;
}

void CollectInvertedSteps(duckdb::ClientContext* context,
                          const MaintainTarget& table,
                          std::vector<InvertedStep>& steps) {
  for (auto& index :
       connector::RelationIndexes(context, table.schema_id, table.id)) {
    if (!index->IsInverted()) {
      continue;
    }
    if (auto storage = index->GetData()) {
      steps.push_back({std::move(storage), index, nullptr});
    }
  }
  // Search tables also commit/consolidate/GC in the background; VACUUM is the
  // synchronous, on-demand path through the same maintenance ops.
  if (table.engine == catalog::TableEngine::Search) {
    steps.push_back({nullptr, nullptr, table.search_data});
  }
}

bool MayMaintain(ConnectionContext& conn_ctx, const catalog::Permissions& perm,
                 std::string_view name, std::string_view verb) {
  if (auth::ClosureFor(&conn_ctx.GetClientContext(), conn_ctx.GetRoleId())
        ->Can(duckdb::CatalogType::TABLE_ENTRY, perm,
              catalog::AclMode::Maintain)) {
    return true;
  }
  conn_ctx.AddNotice(SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_WARNING),
    ERR_MSG("permission denied to ", verb, " \"", name, "\", skipping it")));
  return false;
}

void DispatchInverted(duckdb::ClientContext& context,
                      ConnectionContext& conn_ctx, Action action, Scope scope,
                      const ResolvedName& target,
                      pg::ProgressMetrics* progress) {
  std::vector<InvertedStep> steps;

  const std::string_view verb =
    action == Action::Refresh ? "refresh" : "compact";
  auto walk = [&](ObjectId db_id, std::string_view schema) {
    for (const auto& table : CollectMaintainTargets(context, db_id, schema)) {
      if (!MayMaintain(conn_ctx, table.perm, table.name, verb)) {
        continue;
      }
      CollectInvertedSteps(&context, table, steps);
    }
  };

  switch (scope) {
    case Scope::Column:
      // No refresh/compact at column granularity.
      break;
    case Scope::Index: {
      auto db_id = LookupDatabaseId(target.database);
      bool found = false;
      std::vector<catalog::IndexInfoRef> indexes;
      connector::VisitIndexes(
        nullptr, db_id,
        [&](const catalog::IndexInfoRef& index) { indexes.push_back(index); });
      const auto schema_id =
        connector::FindSchemaId(nullptr, db_id, target.schema);
      for (auto& index : indexes) {
        if (!index->IsInverted() || index->GetParentId() != schema_id ||
            index->GetName() != target.object) {
          continue;
        }
        // An index has no owner of its own; maintenance rides on its table.
        auto relation =
          connector::LookupEntryById(context, db_id, index->GetRelationId());
        if (const auto* table =
              dynamic_cast<const connector::SereneDBTableEntry*>(
                relation.get());
            table != nullptr &&
            !MayMaintain(conn_ctx, table->permissions,
                         table->name.GetIdentifierName(), verb)) {
          return;
        }
        auto storage = index->GetData();
        if (!storage) {
          continue;
        }
        steps.push_back({std::move(storage), index, nullptr});
        found = true;
        break;
      }
      if (!found) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
    } break;
    case Scope::Table: {
      auto db_id = LookupDatabaseId(target.database);
      const auto* entry = connector::FindTableEntry(
        &context, db_id, target.schema, target.object);
      if (entry == nullptr) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
      const auto table = MakeMaintainTarget(target.schema, *entry);
      if (!MayMaintain(conn_ctx, table.perm, table.name, verb)) {
        return;
      }
      CollectInvertedSteps(&context, table, steps);
    } break;
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(target.database);
      if (!connector::FindSchema(nullptr, db_id, target.schema)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
          ERR_MSG("schema \"", target.schema, "\" does not exist"));
      }
      walk(db_id, target.schema);
    } break;
    case Scope::Database: {
      walk(LookupDatabaseId(target.database), {});
    } break;
    case Scope::All: {
      VisitDatabases(nullptr,
                     [&](const DatabaseRef& db) { walk(db.Id(), {}); });
      break;
    }
  }

  if (progress) {
    int64_t total = 0;
    for (const auto& step : steps) {
      total += step.index ? 1 : 0;
    }
    pg::ProgressMetrics::Set(progress->items_total, total);
    progress->SetPhase(pg::progress_phase::Vacuum::VacuumingIndexes);
  }
  for (auto& step : steps) {
    context.InterruptCheck();
    if (step.index) {
      const auto& inverted = catalog::InvertedInfo(*step.index);
      if (action == Action::Refresh) {
        irs::ProgressReportCallback report;
        if (progress) {
          // RefreshCommit reports 4 named stages, each iterating its own
          // work list; a stage transition is observed as a phase-name change.
          report = [progress, last_stage = std::string{}](
                     std::string_view stage_name, size_t current,
                     size_t total) mutable {
            if (stage_name != last_stage) {
              last_stage = stage_name;
              pg::ProgressMetrics::Add(progress->stage, 1);
            }
            pg::ProgressMetrics::Set(progress->steps_total,
                                     static_cast<int64_t>(total));
            pg::ProgressMetrics::Set(progress->step,
                                     static_cast<int64_t>(current));
          };
          pg::ProgressMetrics::Set(progress->stages_total, 4);
          pg::ProgressMetrics::Set(progress->stage, 0);
        }
        step.storage->Refresh(report);
      } else {
        CompactInvertedStorage(*step.storage, inverted, context, progress);
      }
      if (progress) {
        pg::ProgressMetrics::Add(progress->items_processed, 1);
        SDB_WAIT_ON_FAILURE("pause_vacuum_mid_walk");
      }
    } else if (const auto& search = step.search_data) {
      if (action == Action::Refresh) {
        search->VacuumRefresh();  // commit pending inserts + reclaim files
      } else {
        search->VacuumCompact();  // + merge segments
      }
    }
  }
}

// Recompute optimizer column statistics for the store tables backing the
// serenedb tables in scope, by running DuckDB's `VACUUM ANALYZE` on each store
// table. The user names serenedb tables; the hidden store is never exposed.
void DispatchRecomputeStats(duckdb::ClientContext& context,
                            ConnectionContext& conn_ctx, Scope scope,
                            const ResolvedName& target,
                            pg::ProgressMetrics* progress) {
  struct AnalyzeTarget {
    std::string database;
    std::string schema;
    std::string table;
    ObjectId relation;
    std::string column;
  };
  std::vector<AnalyzeTarget> targets;
  auto add = [&](std::string_view db_name, const MaintainTarget& table,
                 std::string_view column = {}) {
    if (table.engine != catalog::TableEngine::Transactional) {
      return;
    }
    if (!MayMaintain(conn_ctx, table.perm, table.name, "analyze")) {
      return;
    }
    targets.push_back({std::string{db_name}, table.schema,
                       std::string{table.name}, table.id, std::string{column}});
  };
  auto walk = [&](ObjectId db_id, std::string_view db_name,
                  std::string_view schema) {
    for (const auto& table : CollectMaintainTargets(context, db_id, schema)) {
      add(db_name, table);
    }
  };

  switch (scope) {
    case Scope::Table:
    case Scope::Column: {
      auto db_id = LookupDatabaseId(target.database);
      const auto* entry = connector::FindTableEntry(
        &context, db_id, target.schema, target.object);
      if (entry == nullptr) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
      add(target.database, MakeMaintainTarget(target.schema, *entry),
          target.column);
    } break;
    case Scope::Schema: {
      auto db_id = LookupDatabaseId(target.database);
      if (!connector::FindSchema(nullptr, db_id, target.schema)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
          ERR_MSG("schema \"", target.schema, "\" does not exist"));
      }
      walk(db_id, target.database, target.schema);
    } break;
    case Scope::Database:
      walk(LookupDatabaseId(target.database), target.database, {});
      break;
    case Scope::All:
      VisitDatabases(
        nullptr, [&](const DatabaseRef& db) { walk(db.Id(), db.Name(), {}); });
      break;
    case Scope::Index:
      // No recompute_stats_index verb in ParseOption's table.
      break;
  }

  if (progress) {
    pg::ProgressMetrics::Set(progress->items_total,
                             static_cast<int64_t>(targets.size()));
    progress->SetPhase(pg::progress_phase::Analyze::ComputingStatistics);
  }
  duckdb::Connection conn(*context.db);
  for (const auto& t : targets) {
    context.InterruptCheck();
    if (progress) {
      pg::ProgressMetrics::Set(progress->current_relid,
                               static_cast<int64_t>(t.relation.id()));
    }
    auto quoted = absl::StrReplaceAll(t.table, {{"\"", "\"\""}});
    std::string column_clause;
    if (!t.column.empty()) {
      column_clause = absl::StrCat(
        " (\"", absl::StrReplaceAll(t.column, {{"\"", "\"\""}}), "\")");
    }
    auto result = conn.Query(absl::StrCat(
      "VACUUM ANALYZE \"", absl::StrReplaceAll(t.database, {{"\"", "\"\""}}),
      "\".\"", absl::StrReplaceAll(t.schema, {{"\"", "\"\""}}), "\".\"", quoted,
      "\"", column_clause));
    if (result->HasError()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("recompute_stats failed: ", result->GetError()));
    }
    if (progress) {
      pg::ProgressMetrics::Add(progress->items_processed, 1);
      SDB_WAIT_ON_FAILURE("pause_recompute_stats_mid_walk");
    }
  }
}

void VacuumExecute(duckdb::ClientContext& context,
                   duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  auto& bind_data = input.bind_data->Cast<VacuumBindData>();
  auto& conn_ctx = GetSereneDBContext(context);

  auto verb = ParseOption(bind_data.option);
  if (!verb) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("unknown serenedb VACUUM option \"", bind_data.option, "\""));
  }

  const bool needs_name = verb->scope != Scope::All;
  if (needs_name && bind_data.name.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("VACUUM (", bind_data.option, ") requires an object name"));
  }
  if (!needs_name && !bind_data.name.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("VACUUM (", bind_data.option, ") does not take an argument"));
  }

  auto target = ResolveName(context, bind_data, verb->scope, conn_ctx);

  pg::ProgressMetrics* progress = nullptr;
  if (auto client_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    const auto datid = verb->scope == Scope::All
                         ? conn_ctx.GetDatabaseId()
                         : LookupDatabaseId(target.database);
    ObjectId relid;
    if (verb->scope == Scope::Table || verb->scope == Scope::Column) {
      if (const auto* table = connector::FindTableEntry(
            &context, datid, target.schema, target.object)) {
        relid = catalog::IdOf(*table);
      }
    }
    auto& metrics = client_state->Progress();
    if (verb->action == Action::RecomputeStats) {
      metrics.SetCommand(pg::ProgressCommand::Analyze);
      metrics.SetPhase(pg::progress_phase::Analyze::Initializing);
    } else {
      metrics.SetCommand(pg::ProgressCommand::Vacuum);
      metrics.SetPhase(pg::progress_phase::Vacuum::Initializing);
    }
    pg::ProgressMetrics::Set(metrics.relid, static_cast<int64_t>(relid.id()));
    progress = &metrics;
  }

  switch (verb->action) {
    case Action::Refresh:
    case Action::Compact:
      DispatchInverted(context, conn_ctx, verb->action, verb->scope, target,
                       progress);
      break;
    case Action::RecomputeStats:
      DispatchRecomputeStats(context, conn_ctx, verb->scope, target, progress);
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
