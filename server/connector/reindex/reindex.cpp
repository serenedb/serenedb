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

#include "connector/reindex/reindex.h"

#include <absl/algorithm/container.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <functional>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>

#include "auth/enforce.h"
#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/primary_key.h"
#include "connector/reindex/observe.h"
#include "connector/search_remove_filter.hpp"
#include "connector/term_dict.h"
#include "connector/view_fast_path.h"
#include "connector/view_index_bind.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "search/inverted_index_storage.h"
#include "search/task.h"

namespace sdb::connector {
namespace {

constexpr absl::Duration kClaimPoll = absl::Milliseconds(100);

std::string ActionName(ReindexAction action) {
  switch (action) {
    case ReindexAction::UpToDate:
      return "up_to_date";
    case ReindexAction::Delta:
      return "delta";
    case ReindexAction::Rebuild:
      return "rebuild";
  }
  return {};
}

struct ReindexTarget {
  duckdb::QualifiedName name;
  duckdb::QualifiedName view;
  duckdb::idx_t database_id;
  // Every pass reads the index's configuration and its storage off the entry,
  // which owns both.
  duckdb::optional_ptr<const catalog::InvertedIndexEntry> index;
  duckdb::optional_ptr<duckdb::ViewCatalogEntry> view_entry;
  duckdb::unique_ptr<duckdb::CreateViewInfo> view_info;
  duckdb::idx_t relation_id = 0;
};

// Resolution plus every REINDEX precondition error.
ReindexTarget ResolveTarget(duckdb::ClientContext& context,
                            ConnectionContext& conn_ctx,
                            const duckdb::QualifiedName& requested) {
  const auto& name = requested.Name().GetIdentifierName();
  if (name.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("serenedb_reindex requires an index name"));
  }
  ReindexTarget target;
  const auto database_name =
    requested.Catalog().empty()
      ? duckdb::DatabaseManager::GetDefaultDatabase(context)
      : requested.Catalog();
  auto database = duckdb::Catalog::GetCatalogEntry(context, database_name);
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
                    ERR_MSG("database \"", database_name.GetIdentifierName(),
                            "\" does not exist"));
  }
  target.database_id = database->GetOid();
  auto index_entry = duckdb::Catalog::GetEntry(
    context,
    duckdb::EntryLookupInfo{
      duckdb::CatalogType::INDEX_ENTRY,
      duckdb::QualifiedName{database_name, requested.Schema(),
                            requested.Name()}},
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!index_entry ||
      index_entry->Cast<duckdb::IndexCatalogEntry>().index_type != "inverted") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  target.index = &index_entry->Cast<catalog::InvertedIndexEntry>();
  const auto schema_name = target.index->ParentSchema(context).name;
  target.name =
    duckdb::QualifiedName{database_name, schema_name, requested.Name()};
  // Views and tables share one catalog set, so the type has to be checked
  // rather than assumed from the lookup that found the entry.
  auto relation = target.index->GetRelation(
    target.index->catalog.GetCatalogTransaction(context));
  if (!relation || relation->type != duckdb::CatalogType::VIEW_ENTRY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("REINDEX is only supported for view-backed inverted indexes"));
  }
  auto& view = relation->Cast<duckdb::ViewCatalogEntry>();
  // PG semantics: REINDEX needs MAINTAIN (same as VACUUM). Enforced here --
  // the passes run on internal connections and reach no other gate.
  if (!auth::ClosureFor(&context, conn_ctx.GetRoleId())
         ->Can(duckdb::CatalogType::TABLE_ENTRY, view.permissions,
               duckdb::AclMode::Maintain)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for index \"", name, "\""));
  }
  target.view = duckdb::QualifiedName{database_name, schema_name, view.name};
  target.view_entry = &view;
  target.relation_id = view.oid;
  target.view_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      view.GetInfo());
  return target;
}

class ReindexSession {
 public:
  using NoticeSink = std::function<void(irs::pg::SqlErrorData&)>;

  ReindexSession(duckdb::DatabaseInstance& db, std::string_view user,
                 duckdb::idx_t role_id, const std::string& database,
                 duckdb::idx_t database_id, int32_t backend_pid,
                 NoticeSink sink)
    : _conn{db},
      _ctx{std::make_shared<ConnectionContext>(*_conn.context, user, role_id,
                                               database, database_id, nullptr,
                                               backend_pid, nullptr)},
      _sink{std::move(sink)} {
    SereneDBClientState::Register(*_conn.context, _ctx);
    _conn.context->session_user = user;
  }
  ~ReindexSession() {
    _ctx->ConsumeNotices([&](auto& notice) { _sink(notice); });
  }
  ReindexSession(const ReindexSession&) = delete;
  ReindexSession& operator=(const ReindexSession&) = delete;

  duckdb::ClientContext& Context() { return *_conn.context; }
  ConnectionContext& Conn() { return *_ctx; }
  void Begin() { _conn.BeginTransaction(); }
  void Commit() { _conn.Commit(); }

 private:
  duckdb::Connection _conn;
  std::shared_ptr<ConnectionContext> _ctx;
  NoticeSink _sink;
};

bool IcebergIdle(duckdb::ClientContext& context, const ViewFastPath& fp,
                 const search::SourcePosition& position, uint64_t definition) {
  if (!fp.catalog_ref) {
    return false;
  }
  const auto& ref = *fp.catalog_ref;
  auto* catalog = dynamic_cast<duckdb::IcebergCatalog*>(
    duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier{ref.catalog})
      .get());
  if (catalog && catalog->attach_options.max_table_staleness_micros.IsValid()) {
    if (auto schema =
          catalog->GetSchema(catalog->GetCatalogTransaction(context),
                             duckdb::Identifier{ref.schema},
                             duckdb::OnEntryNotFound::RETURN_NULL)) {
      catalog->table_request_cache.Expire(duckdb::IcebergTable::GetTableKey(
        *catalog, schema->Cast<duckdb::IcebergSchemaEntry>().namespace_items,
        ref.table));
    }
  }
  const auto entry = duckdb::Catalog::GetEntry(
    context,
    duckdb::EntryLookupInfo{
      duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName{duckdb::Identifier{ref.catalog},
                            duckdb::Identifier{ref.schema},
                            duckdb::Identifier{ref.table}}},
    duckdb::OnEntryNotFound::RETURN_NULL);
  const auto* table =
    dynamic_cast<const duckdb::IcebergTableSchemaVersion*>(entry.get());
  if (!table) {
    return false;
  }
  const auto latest = table->table_info.table_metadata.GetLatestSnapshot();
  return position ==
         search::SourcePosition{
           .definition = definition,
           .snapshot_id = latest ? latest->snapshot_id.value_or(0) : 0};
}

duckdb::unique_ptr<duckdb::LogicalOperator> BindView(
  duckdb::Binder& binder, const ReindexTarget& target) {
  duckdb::BaseTableRef ref;
  ref.SetQualifiedName(target.view);
  return std::move(binder.Bind(static_cast<duckdb::TableRef&>(ref)).plan);
}

irs::IndexWriter::Transaction BuildRemovals(
  search::InvertedIndexStorage& storage, RefreshPlan& plan) {
  auto trx = storage.GetTransaction();
  if (plan.drop.empty() && plan.masks.empty()) {
    return trx;
  }
  absl::c_sort(plan.drop);
  plan.drop.erase(std::unique(plan.drop.begin(), plan.drop.end()),
                  plan.drop.end());
  auto remove =
    std::make_shared<SearchRemovePrefixFilter>(term_dict::kPKFieldId);
  auto drop = plan.drop.begin();
  auto mask = plan.masks.begin();
  while (drop != plan.drop.end() || mask != plan.masks.end()) {
    if (mask == plan.masks.end() ||
        (drop != plan.drop.end() && *drop < mask->first)) {
      remove->AddFile(primary_key::PkFilePrefix(*drop++));
      continue;
    }
    SDB_ASSERT(drop == plan.drop.end() || *drop != mask->first);
    remove->AddFileRows(primary_key::PkFilePrefix(mask->first),
                        std::move(mask->second));
    ++mask;
  }
  trx.Remove(std::move(remove));
  return trx;
}

duckdb::unique_ptr<SereneDBCreateIndexInfo> PassInfo(
  const ReindexTarget& target, RefreshPlan& plan) {
  auto info = duckdb::make_uniq<SereneDBCreateIndexInfo>();
  info->index_type = "inverted";
  info->source_index = target.index->name;
  if (const auto key = target.index->options.find(catalog::kKeyColumnsOption);
      key != target.index->options.end()) {
    info->options.emplace(key->first, key->second);
  }
  for (const auto& expression : target.index->parsed_expressions) {
    info->expressions.emplace_back(expression->Copy());
    info->parsed_expressions.emplace_back(expression->Copy());
  }
  if (const auto predicate = target.index->where_clause.get()) {
    info->where_clause = predicate->Copy();
  }
  info->table = target.view.Name();
  info->SetSchema(target.view.Schema());
  info->SetCatalog(target.view.Catalog());
  info->pass_files = plan.scan_files;
  return info;
}

RefreshPlan Observe(RefreshSource source, const ObserveInput& in) {
  switch (source) {
    case RefreshSource::Rebuild:
      return ObserveRebuild(in);
    case RefreshSource::Files:
      return ObserveFiles(in);
    case RefreshSource::Iceberg:
      return ObserveIceberg(in);
  }
  SDB_UNREACHABLE();
}

void Fill(duckdb::ClientContext& context, ConnectionContext& conn,
          duckdb::Binder& binder, duckdb::ViewCatalogEntry& view,
          duckdb::unique_ptr<SereneDBCreateIndexInfo> info,
          duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  duckdb::CreateStatement statement;
  statement.info = std::move(info);
  auto create =
    view.catalog.BindCreateViewIndex(binder, statement, view, std::move(plan));
  auth::EnforcePlan(context, conn, binder, *create);
  create->ResolveOperatorTypes();
  auto result = context.Query(
    duckdb::make_uniq<duckdb::LogicalPlanStatement>(std::move(create)),
    duckdb::QueryParameters{});
  if (result->HasError()) {
    result->ThrowError();
  }
}

ReindexOutcome RefreshIndex(
  duckdb::ClientContext& context, ConnectionContext& conn,
  const duckdb::QualifiedName& index,
  const std::shared_ptr<search::InvertedIndexStorage>& storage,
  duckdb::optional_ptr<duckdb::ClientContext> caller) {
  auto target = ResolveTarget(context, conn, index);
  if (target.index->Storage() != storage) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", index.Name().GetIdentifierName(),
                            "\" does not exist"));
  }
  const auto fp = ResolveViewFastPath(
    context, duckdb::Catalog::GetCatalog(context, target.name.Catalog()),
    *target.view_info, catalog::ParseKeyColumns(target.index->options));
  const ViewFastPath* fast_path = fp ? &*fp : nullptr;
  const auto snapshot = storage->GetInvertedIndexSnapshot();
  const auto definition =
    ViewIndexDefinition(*target.view_info, target.index->column_ids,
                        target.index->where_clause.get());
  if (fast_path && fast_path->refresh_source == RefreshSource::Iceberg &&
      IcebergIdle(context, *fast_path, snapshot->position, definition)) {
    return {};
  }
  auto binder = duckdb::Binder::CreateBinder(context);
  auto plan = BindView(*binder, target);
  const auto config = target.index->Config();
  if (!fast_path && (config->pk.index_term ||
                     config->pk.column == catalog::PkColumnKind::Has)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("REINDEX of \"", target.name.Name().GetIdentifierName(),
              "\": the view no longer yields the row identity the index was "
              "built with; drop and recreate the index"));
  }
  duckdb::MultiFileBindData* bind = nullptr;
  if (fast_path) {
    auto& leaf = LeafScan(*plan);
    EnableIcebergSort(leaf.bind_data.get());
    bind = dynamic_cast<duckdb::MultiFileBindData*>(leaf.bind_data.get());
  }
  auto refresh = Observe(
    bind ? fast_path->refresh_source : RefreshSource::Rebuild,
    {
      .context = context,
      .index = target.name,
      .fast_path = fast_path,
      .view_info = *target.view_info,
      .bind = bind,
      .snapshot = *snapshot,
      .definition = definition,
      .next_file_id = storage->NextSourceFileId(),
      // Delta needs the view's support (recorded at fast-path
      // resolution) and pk terms (term-less indexes take the rebuild
      // road).
      .delta = fast_path && fast_path->supports_delta && config->pk.index_term,
    });
  if (refresh.outcome.action == ReindexAction::UpToDate) {
    if (refresh.position != snapshot->position) {
      storage->CommitPosition(refresh.position);
    }
    return refresh.outcome;
  }
  SDB_WAIT_ON_FAILURE("pause_reindex_before_pass");
  storage->BeginPass();
  if (refresh.outcome.action == ReindexAction::Rebuild ||
      !refresh.scan_files.empty()) {
    Fill(context, conn, *binder, *target.view_entry, PassInfo(target, refresh),
         std::move(plan));
  }
  if (caller) {
    caller->InterruptCheck();
  }
  SDB_IF_FAILURE("crash_reindex_before_publish") {
    storage->Refresh();
    SDB_IMMEDIATE_ABORT();
  }
  search::SourceFilesUpdate files{.added = std::move(refresh.scan_files),
                                  .live = std::move(refresh.live)};
  if (refresh.outcome.action == ReindexAction::Rebuild) {
    storage->PublishRebuild(refresh.position, std::move(files));
  } else {
    storage->PublishDelta(BuildRemovals(*storage, refresh), refresh.position,
                          std::move(files));
  }
  return refresh.outcome;
}

ReindexOutcome RunPass(
  ReindexSession& session, const duckdb::QualifiedName& index,
  const std::shared_ptr<search::InvertedIndexStorage>& storage,
  duckdb::optional_ptr<duckdb::ClientContext> caller) {
  SDB_WAIT_ON_FAILURE("pause_reindex_claimed");
  session.Begin();
  const auto outcome =
    RefreshIndex(session.Context(), session.Conn(), index, storage, caller);
  session.Commit();
  return outcome;
}

ReindexOutcome RunManualReindex(duckdb::ClientContext& context,
                                const duckdb::QualifiedName& index) {
  auto& conn_ctx = GetSereneDBContext(context);
  const auto target = ResolveTarget(context, conn_ctx, index);
  const auto storage = target.index->Storage();
  if (!storage) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", index.Name().GetIdentifierName(),
                            "\" does not exist"));
  }
  pg::ProgressMetrics* progress = nullptr;
  if (auto client_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey);
      client_state && client_state->progress_source) {
    progress = &client_state->Progress();
    progress->SetCommand(pg::ProgressCommand::Reindex);
    progress->SetPhase(pg::progress_phase::Reindex::WaitingForReindex);
    pg::ProgressMetrics::Set(progress->relid,
                             static_cast<int64_t>(target.relation_id));
    pg::ProgressMetrics::Set(progress->current_relid,
                             static_cast<int64_t>(target.index->oid));
  }
  const auto claim = search::InvertedIndexStorage::ReindexClaim::Acquire(
    *storage, [&] { return context.IsInterrupted(); }, kClaimPoll);
  if (!claim.Claimed()) {
    context.InterruptCheck();
  }
  SDB_ASSERT(claim.Claimed());
  if (progress) {
    progress->SetPhase(pg::progress_phase::Reindex::Refreshing);
  }
  ReindexSession session{
    *context.db,
    conn_ctx.user(),
    conn_ctx.GetRoleId(),
    target.name.Catalog().GetIdentifierName(),
    target.database_id,
    conn_ctx.GetBackendPid(),
    [&](auto& notice) { conn_ctx.AddNotice(std::move(notice)); }};
  session.Context().config.user_settings = context.config.user_settings;
  return RunPass(session, target.name, storage, &context);
}

struct ReindexBindData final : public duckdb::FunctionData {
  duckdb::QualifiedName index;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<ReindexBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return index == other.Cast<ReindexBindData>().index;
  }
};

duckdb::Identifier ArgIdentifier(const duckdb::vector<duckdb::Value>& args,
                                 duckdb::idx_t i) {
  if (i >= args.size() || args[i].IsNull()) {
    return {};
  }
  return duckdb::Identifier{args[i].GetValue<std::string>()};
}

duckdb::QualifiedName ReindexArgs(const duckdb::vector<duckdb::Value>& args) {
  return duckdb::QualifiedName{ArgIdentifier(args, 2), ArgIdentifier(args, 1),
                               ArgIdentifier(args, 0)};
}

duckdb::unique_ptr<duckdb::FunctionData> ReindexBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  auto data = duckdb::make_uniq<ReindexBindData>();
  data->index = ReindexArgs(input.inputs);
  return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
                  duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
                  duckdb::LogicalType::BIGINT};
  names = {"action", "files_added", "files_changed", "files_removed",
           "files_rescanned"};
  return data;
}

struct ReindexGlobalState final : public duckdb::GlobalTableFunctionState {
  bool done = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ReindexInitGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  return duckdb::make_uniq<ReindexGlobalState>();
}

void ReindexExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& gstate = input.global_state->Cast<ReindexGlobalState>();
  if (gstate.done) {
    return;
  }
  const auto outcome =
    RunManualReindex(context, input.bind_data->Cast<ReindexBindData>().index);
  output.SetValue(0, 0, duckdb::Value(ActionName(outcome.action)));
  output.SetValue(1, 0, duckdb::Value::BIGINT(outcome.files_added));
  output.SetValue(2, 0, duckdb::Value::BIGINT(outcome.files_changed));
  output.SetValue(3, 0, duckdb::Value::BIGINT(outcome.files_removed));
  output.SetValue(4, 0, duckdb::Value::BIGINT(outcome.files_rescanned));
  output.SetCardinality(1);
  gstate.done = true;
}

void ReindexPragma(duckdb::ClientContext& context,
                   const duckdb::FunctionParameters& parameters) {
  // PRAGMA / REINDEX statement form: silent, PG-style.
  RunManualReindex(context, ReindexArgs(parameters.values));
}

// The attachment an id names. The reindex loop is handed the id its index was
// registered under and runs with no session, so the name is not in hand.
duckdb::shared_ptr<duckdb::AttachedDatabase> FindAttachedById(
  duckdb::DatabaseInstance& db, duckdb::idx_t database_id) {
  for (auto& attached : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    if (attached->oid == database_id) {
      return attached;
    }
  }
  return nullptr;
}

// SDB_RBAC_DISABLED. The identity the periodic refresh runs under while
// permission checks are inert; the root role, as a manual owner-run REINDEX
// would be.
constexpr const char* kReindexStubUser = "postgres";

// ReindexLoop tick: one REINDEX on an internal session. Quiet outcomes return
// OK -- vanished index, claim lost to a manual run.
absl::StatusOr<bool> RunReindexTick(duckdb::DatabaseInstance& db,
                                    duckdb::idx_t database_id,
                                    duckdb::idx_t index_id) {
  try {
    const auto attached = FindAttachedById(db, database_id);
    if (!attached) {
      return false;
    }
    // SDB_RBAC_DISABLED. The tick resolved the relation's owner role and ran
    // under its name. Impersonation only ever mattered for permission checks,
    // and every check answers "allowed" until the RBAC phase -- so the name now
    // reaches nothing but notices and the log, while a role that had gone
    // missing failed the whole tick. Restore the lookup when enforcement lands.
    const std::string_view user = kReindexStubUser;

    duckdb::QualifiedName index_name;
    duckdb::idx_t owner_id = 0;
    std::shared_ptr<search::InvertedIndexStorage> storage;
    {
      duckdb::Connection conn{db};
      conn.BeginTransaction();
      auto& catalog = attached->GetCatalog().Cast<catalog::SereneDBCatalog>();
      const auto trx = catalog.GetCatalogTransaction(*conn.context);
      auto index =
        catalog.FindIn<duckdb::DuckIndexEntry>(conn.context.get(), index_id);
      if (!index || index->index_type != "inverted") {
        return false;
      }
      const auto relation = index->GetRelation(trx);
      if (!relation || relation->type != duckdb::CatalogType::VIEW_ENTRY) {
        return false;
      }
      index_name = duckdb::QualifiedName{attached->GetName(),
                                         index->GetSchemaName(), index->name};
      // Ownership itself is real (pg_class.relowner asserts it), so the id is
      // carried through.
      owner_id = relation->permissions.owner;
      storage = index->Cast<catalog::InvertedIndexEntry>().Storage();
      if (!storage || storage->GetTasksSettings().reindex_interval_msec == 0) {
        return false;
      }
    }
    const auto claim =
      search::InvertedIndexStorage::ReindexClaim::TryAcquire(*storage);
    if (!claim.Claimed()) {
      return false;
    }
    // The tick is this session's client: every notice terminates in the
    // server log.
    ReindexSession session{db,
                           user,
                           owner_id,
                           attached->GetName().GetIdentifierName(),
                           database_id,
                           /*backend_pid=*/0,
                           [&](auto& notice) {
                             SDB_INFO(SEARCH, "reindex \"",
                                      index_name.Name().GetIdentifierName(),
                                      "\": ", notice.errmsg);
                           }};
    return RunPass(session, index_name, storage, nullptr).action !=
           ReindexAction::UpToDate;
  } catch (const irs::SqlException& ex) {
    if (ex.error().errcode == ERRCODE_UNDEFINED_OBJECT) {
      return false;
    }
    return absl::InternalError(ex.message());
  } catch (const std::exception& ex) {
    return absl::InternalError(ex.what());
  }
}

}  // namespace

void RegisterReindexFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  search::SetReindexRunner(
    [&db](duckdb::idx_t database_id, duckdb::idx_t index_id) {
      return RunReindexTick(db, database_id, index_id);
    });

  duckdb::FunctionSignature signature;
  signature.AddArgs("args", duckdb::LogicalType::VARCHAR);
  duckdb::TableFunction func("serenedb_reindex", std::move(signature),
                             ReindexExecute, ReindexBind, ReindexInitGlobal);
  loader.RegisterFunction(func);

  auto pragma = duckdb::PragmaFunction::PragmaCall(
    "serenedb_reindex", ReindexPragma, {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR);
  loader.RegisterFunction(pragma);
}

}  // namespace sdb::connector
