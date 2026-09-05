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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "auth/role_closure.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/search_table.h"
#include "catalog1/lookup.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
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
    out.database = conn_ctx.GetDatabase();
  }
  if (out.schema.empty() && (scope == Scope::Table || scope == Scope::Index ||
                             scope == Scope::Column)) {
    out.schema = conn_ctx.GetCurrentSchema();
  }
  return out;
}

duckdb::Catalog& LookupDatabase(duckdb::ClientContext& context,
                                std::string_view name) {
  auto found = duckdb::Catalog::GetCatalogEntry(
    context, duckdb::Identifier{std::string{name}});
  if (!found) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                    ERR_MSG("database \"", name, "\" does not exist"));
  }
  return *found;
}

// Every attached serenedb database. VACUUM maintains live storage, so the
// attachments are the population an "all databases" run walks.
std::vector<duckdb::reference<duckdb::Catalog>> AttachedDatabases(
  duckdb::ClientContext& context) {
  std::vector<duckdb::reference<duckdb::Catalog>> out;
  for (auto& attached :
       duckdb::DatabaseManager::Get(context).GetDatabases(context)) {
    auto& db_catalog = attached->GetCatalog();
    if (dynamic_cast<catalog::SereneDBCatalog*>(&db_catalog) != nullptr) {
      out.emplace_back(db_catalog);
    }
  }
  return out;
}

void CompactInvertedStorage(search::InvertedIndexStorage& inverted,
                            const irs::IndexFieldOptions& field_options,
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
      inverted.CompactUnsafe(kPolicy, tick, empty_compaction, &field_options);
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
  // Owned for the whole pass, not aliased into the index: a concurrent DROP
  // INDEX destroys the bound index, and a merge that started against this
  // encoding has to finish against it.
  std::shared_ptr<const irs::IndexFieldOptions> field_options;
  std::shared_ptr<search::SearchTable> search_data;
};

// What VACUUM needs of one table, taken off its entry during the walk. Copied
// out rather than held as a pointer because resolving a table's indexes opens
// the schema's index set, which must not happen while the walk is holding the
// relation set.
struct MaintainTarget {
  duckdb::idx_t id;
  // The table's own schema entry, which its indexes are read off later. The
  // table entry itself is not held: see the note above.
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> schema_entry;
  std::string schema;
  std::string name;
  catalog::TableEngine engine;
  std::shared_ptr<search::SearchTable> search_data;
  catalog::Permissions perm;
};

std::shared_ptr<search::SearchTable> MaintainStoreOf(
  const duckdb::TableCatalogEntry& table) {
  const auto* entry = dynamic_cast<const catalog::SearchTableEntry*>(&table);
  return entry ? entry->EnsureStorage() : nullptr;
}

MaintainTarget MakeMaintainTarget(std::string_view schema,
                                  duckdb::TableCatalogEntry& table) {
  return {.id = table.oid,
          .schema_entry = &table.ParentSchema(),
          .schema = std::string{schema},
          .name = std::string{table.name.GetIdentifierName()},
          .engine = dynamic_cast<const catalog::SearchTableEntry*>(&table)
                      ? catalog::TableEngine::Search
                      : catalog::TableEngine::Transactional,
          .search_data = MaintainStoreOf(table),
          .perm = table.permissions};
}

// Every base table of `database`, or of one schema of it when `schema` is set.
std::vector<MaintainTarget> CollectMaintainTargets(
  duckdb::ClientContext& context, duckdb::Catalog& database,
  std::string_view schema) {
  std::vector<MaintainTarget> out;
  database.ScanSchemas(context, [&](duckdb::SchemaCatalogEntry& schema_ref) {
    schema_ref.Scan(context, duckdb::CatalogType::TABLE_ENTRY,
                    [&](duckdb::CatalogEntry& entry) {
                      // Tables and views share one set, so the scan hands
                      // back both.
                      if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
                        return;
                      }
                      auto& table = entry.Cast<duckdb::TableCatalogEntry>();
                      const auto in_schema =
                        table.ParentSchema().name.GetIdentifierName();
                      if (schema.empty() || in_schema == schema) {
                        out.push_back(MakeMaintainTarget(in_schema, table));
                      }
                    });
  });
  return out;
}

void CollectInvertedSteps(duckdb::ClientContext& context,
                          const MaintainTarget& table,
                          std::vector<InvertedStep>& steps) {
  auto schema = table.schema_entry;
  SDB_ASSERT(schema);
  schema->Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      if (!IsInvertedIndex(index) ||
          index.GetTableName().GetIdentifierName() != table.name) {
        return;
      }
      const auto* inverted =
        dynamic_cast<const catalog::InvertedIndexEntry*>(&index);
      if (inverted && inverted->Storage()) {
        steps.push_back({inverted->Storage(), inverted->Config(), nullptr});
      }
    });
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
  auto walk = [&](duckdb::Catalog& database, std::string_view schema) {
    for (const auto& table :
         CollectMaintainTargets(context, database, schema)) {
      if (!MayMaintain(conn_ctx, table.perm, table.name, verb)) {
        continue;
      }
      CollectInvertedSteps(context, table, steps);
    }
  };

  switch (scope) {
    case Scope::Column:
      // No refresh/compact at column granularity.
      break;
    case Scope::Index: {
      auto entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{
          duckdb::CatalogType::INDEX_ENTRY,
          duckdb::QualifiedName{duckdb::Identifier{target.database},
                                duckdb::Identifier{target.schema},
                                duckdb::Identifier{target.object}}},
        duckdb::OnEntryNotFound::RETURN_NULL);
      duckdb::optional_ptr<duckdb::IndexCatalogEntry> index;
      const catalog::InvertedIndexEntry* inverted = nullptr;
      std::shared_ptr<search::InvertedIndexStorage> storage;
      if (entry) {
        index = &entry->Cast<duckdb::IndexCatalogEntry>();
        inverted =
          dynamic_cast<const catalog::InvertedIndexEntry*>(index.get());
        if (inverted) {
          storage = inverted->Storage();
        }
      }
      if (!storage) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
      // An index has no owner of its own; maintenance rides on its
      // relation (a table, or a view for view-backed indexes).
      auto relation = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{
          duckdb::CatalogType::TABLE_ENTRY,
          duckdb::QualifiedName{duckdb::Identifier{target.database},
                                duckdb::Identifier{target.schema},
                                index->GetTableName()}},
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (relation && !MayMaintain(conn_ctx, relation->permissions,
                                   relation->name.GetIdentifierName(), verb)) {
        return;
      }
      steps.push_back({std::move(storage), inverted->Config(), nullptr});
    } break;
    case Scope::Table: {
      auto entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{
          duckdb::CatalogType::TABLE_ENTRY,
          duckdb::QualifiedName{duckdb::Identifier{target.database},
                                duckdb::Identifier{target.schema},
                                duckdb::Identifier{target.object}}},
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
      const auto table = MakeMaintainTarget(
        target.schema, entry->Cast<duckdb::TableCatalogEntry>());
      if (!MayMaintain(conn_ctx, table.perm, table.name, verb)) {
        return;
      }
      CollectInvertedSteps(context, table, steps);
    } break;
    case Scope::Schema: {
      auto& database = LookupDatabase(context, target.database);
      if (!database.GetSchema(context, duckdb::Identifier{target.schema},
                              duckdb::OnEntryNotFound::RETURN_NULL)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
          ERR_MSG("schema \"", target.schema, "\" does not exist"));
      }
      walk(database, target.schema);
    } break;
    case Scope::Database: {
      walk(LookupDatabase(context, target.database), {});
    } break;
    case Scope::All: {
      // Listed first: a walk scans the database it is for, and doing that from
      // inside the listing re-enters the very set the listing holds.
      for (auto database : AttachedDatabases(context)) {
        walk(database, {});
      }
      break;
    }
  }

  if (progress) {
    int64_t total = 0;
    for (const auto& step : steps) {
      total += step.storage ? 1 : 0;
    }
    pg::ProgressMetrics::Set(progress->items_total, total);
    progress->SetPhase(pg::progress_phase::Vacuum::VacuumingIndexes);
  }
  for (auto& step : steps) {
    context.InterruptCheck();
    if (step.storage) {
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
        CompactInvertedStorage(*step.storage, *step.field_options, context,
                               progress);
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
    duckdb::idx_t relation;
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
  auto walk = [&](duckdb::Catalog& database, std::string_view schema) {
    const auto db_name = database.GetName().GetIdentifierName();
    for (const auto& table :
         CollectMaintainTargets(context, database, schema)) {
      add(db_name, table);
    }
  };

  switch (scope) {
    case Scope::Table:
    case Scope::Column: {
      auto entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{
          duckdb::CatalogType::TABLE_ENTRY,
          duckdb::QualifiedName{duckdb::Identifier{target.database},
                                duckdb::Identifier{target.schema},
                                duckdb::Identifier{target.object}}},
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", target.object, "\" does not exist"));
      }
      add(target.database,
          MakeMaintainTarget(target.schema,
                             entry->Cast<duckdb::TableCatalogEntry>()),
          target.column);
    } break;
    case Scope::Schema: {
      auto& database = LookupDatabase(context, target.database);
      if (!database.GetSchema(context, duckdb::Identifier{target.schema},
                              duckdb::OnEntryNotFound::RETURN_NULL)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
          ERR_MSG("schema \"", target.schema, "\" does not exist"));
      }
      walk(database, target.schema);
    } break;
    case Scope::Database:
      walk(LookupDatabase(context, target.database), {});
      break;
    case Scope::All: {
      for (auto database : AttachedDatabases(context)) {
        walk(database, {});
      }
      break;
    }
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
                               static_cast<int64_t>(t.relation));
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
    duckdb::idx_t relid = pg::kInvalidOid;
    if (verb->scope == Scope::Table || verb->scope == Scope::Column) {
      auto table = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{
          duckdb::CatalogType::TABLE_ENTRY,
          duckdb::QualifiedName{duckdb::Identifier{target.database},
                                duckdb::Identifier{target.schema},
                                duckdb::Identifier{target.object}}},
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (table) {
        relid = table->oid;
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
    pg::ProgressMetrics::Set(metrics.relid, static_cast<int64_t>(relid));
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
