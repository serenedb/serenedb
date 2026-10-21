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

#include "connector/functions/system.h"

#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/database_size.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <functional>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <ranges>
#include <span>

#include "auth/enforce.h"
#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "connector/pg_logical_types.h"
#include "network/cancel_registry.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "server/utils/build.h"

namespace sdb::catalog {
namespace {

struct RelationStorageSize {
  int64_t bytes = 0;
  int64_t persistent_blocks = 0;
};

RelationStorageSize StoreTableDataSize(duckdb::ClientContext& context,
                                       const duckdb::TableCatalogEntry& table) {
  RelationStorageSize result;
  if (!table.IsDuckTable()) {
    return result;
  }
  auto& storage = const_cast<duckdb::TableCatalogEntry&>(table).GetStorage();
  duckdb::QueryContext query_context(context);
  irs::containers::FlatHashSet<duckdb::block_id_t> blocks;
  int64_t transient_bytes = 0;
  for (const auto& info : storage.GetColumnSegmentInfo(query_context)) {
    if (info.persistent) {
      blocks.insert(info.block_id);
      blocks.insert(info.additional_blocks.begin(),
                    info.additional_blocks.end());
    } else {
      transient_bytes += static_cast<int64_t>(info.segment_size);
    }
  }
  const auto block_size =
    storage.GetTableIOManager().GetBlockManagerForRowData().GetBlockSize();
  result.persistent_blocks = static_cast<int64_t>(blocks.size());
  result.bytes = result.persistent_blocks * static_cast<int64_t>(block_size) +
                 transient_bytes;
  return result;
}

int64_t StoreTableIndexBytes(duckdb::ClientContext& context,
                             const duckdb::TableCatalogEntry& table) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  auto& info = *const_cast<duckdb::TableCatalogEntry&>(table)
                  .GetStorage()
                  .GetDataTableInfo();
  info.BindIndexes(context);
  int64_t total = 0;
  for (auto entry : info.GetIndexes().IndexEntries()) {
    if (entry->GetBindState() != duckdb::IndexBindState::BOUND) {
      continue;
    }
    const auto index = entry->GetReadHandle<duckdb::BoundIndex>();
    total += static_cast<int64_t>(index->GetAllocationSize());
  }
  return total;
}

int64_t SearchTableBytes(const catalog::SearchTableEntry& table) {
  // Introspection must not open a store as a side effect: report only what
  // is already open.
  const auto& data = table.Storage();
  if (!data) {
    return 0;
  }
  auto reader = data->GetDirectoryReader();
  if (!reader) {
    return 0;
  }
  int64_t total = 0;
  for (const auto& segment : reader.Meta().index_meta.segments) {
    total += static_cast<int64_t>(segment.meta.byte_size);
  }
  return total;
}

int64_t BoundIndexBytes(duckdb::ClientContext& context,
                        duckdb::DataTableInfo& info,
                        const duckdb::Identifier& name) {
  info.BindIndexes(context);
  auto entry = info.GetIndexes().FindEntry(name);
  if (!entry || entry->GetBindState() != duckdb::IndexBindState::BOUND) {
    return 0;
  }
  const auto bound = entry->GetReadHandle<duckdb::BoundIndex>();
  return static_cast<int64_t>(bound->GetAllocationSize());
}

// Every entry of one kind across the catalog's schemas, which is duckdb's own
// two-level scan rather than anything of ours.
void VisitEntries(duckdb::ClientContext& context, duckdb::Catalog& catalog,
                  duckdb::CatalogType type,
                  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  std::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  for (auto& schema : catalog.GetSchemas(context)) {
    schema.get().Scan(context, type, [&](duckdb::CatalogEntry& entry) {
      entries.emplace_back(entry);
    });
  }
  for (auto& entry : entries) {
    callback(entry.get());
  }
}

}  // namespace

int64_t RelationDataBytes(duckdb::ClientContext& context,
                          const duckdb::TableCatalogEntry& table) {
  const auto* search = dynamic_cast<const catalog::SearchTableEntry*>(&table);
  return search ? SearchTableBytes(*search)
                : StoreTableDataSize(context, table).bytes;
}

int64_t IndexEntryBytes(duckdb::ClientContext& context,
                        const duckdb::DuckIndexEntry& index) {
  if (const auto* inverted =
        dynamic_cast<const catalog::InvertedIndexEntry*>(&index)) {
    const auto& storage = inverted->Storage();
    if (!storage) {
      return 0;
    }
    return static_cast<int64_t>(storage->GetStats().indexSize);
  }
  // A secondary index is an ART on the store table, so its size is the
  // allocation the store table reports for it.
  return BoundIndexBytes(context, index.GetDataTableInfo(), index.name);
}

int64_t KeyIndexBytes(duckdb::ClientContext& context,
                      const duckdb::TableCatalogEntry& table,
                      const duckdb::UniqueConstraint& constraint) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  return BoundIndexBytes(context,
                         *const_cast<duckdb::TableCatalogEntry&>(table)
                            .GetStorage()
                            .GetDataTableInfo(),
                         constraint.GetName(table.name));
}

int64_t TableIndexesTotalBytes(duckdb::ClientContext& context,
                               duckdb::TableCatalogEntry& table) {
  int64_t total = dynamic_cast<const catalog::SearchTableEntry*>(&table)
                    ? 0
                    : StoreTableIndexBytes(context, table);
  table.ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      auto& index = entry.Cast<duckdb::DuckIndexEntry>();
      if (index.table_oid == table.oid && connector::IsInvertedIndex(index)) {
        total += IndexEntryBytes(context, index);
      }
    });
  return total;
}

duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         duckdb::Catalog& catalog,
                                         std::string_view only_schema) {
  duckdb::DatabaseSize result;
  auto& attached = catalog.GetAttached();
  if (attached.HasStorageManager()) {
    result.block_size =
      attached.GetStorageManager().GetDatabaseSize().block_size;
  }
  const auto in_scope = [&](const duckdb::CatalogEntry& entry) {
    return only_schema.empty() ||
           entry.ParentSchema(context).name == only_schema;
  };
  int64_t bytes = 0;
  int64_t blocks = 0;
  VisitEntries(context, catalog, duckdb::CatalogType::TABLE_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (!in_scope(entry)) {
                   return;
                 }
                 // Views and the index-name-as-table wrappers share this set
                 // and own no rows of their own; the cast is the filter.
                 const auto* table =
                   dynamic_cast<const duckdb::TableCatalogEntry*>(&entry);
                 if (!table) {
                   return;
                 }
                 if (const auto* search =
                       dynamic_cast<const catalog::SearchTableEntry*>(table)) {
                   bytes += SearchTableBytes(*search);
                   return;
                 }
                 const auto data = StoreTableDataSize(context, *table);
                 bytes += data.bytes + StoreTableIndexBytes(context, *table);
                 blocks += data.persistent_blocks;
               });
  VisitEntries(context, catalog, duckdb::CatalogType::INDEX_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (!in_scope(entry)) {
                   return;
                 }
                 auto* index = dynamic_cast<duckdb::DuckIndexEntry*>(&entry);
                 if (index && connector::IsInvertedIndex(*index)) {
                   bytes += IndexEntryBytes(context, *index);
                 }
               });
  result.bytes = static_cast<duckdb::idx_t>(bytes);
  result.total_blocks = static_cast<duckdb::idx_t>(blocks);
  result.used_blocks = result.total_blocks;
  return result;
}

}  // namespace sdb::catalog
namespace sdb::connector {
namespace {

[[noreturn]] void ThrowRoleNotFound(std::string_view role) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("role \"", role, "\" does not exist"));
}

[[noreturn]] void ThrowRelationNotFound(std::string_view rel) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                  ERR_MSG("relation \"", rel, "\" does not exist"));
}

[[noreturn]] void ThrowInvalidPrivilege(const irs::SqlException& e) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG(e.message()));
}

// Roles and databases are cluster-wide, so they are the two names no database
// catalog can resolve; everything else below goes through duckdb.
const catalog::RoleCatalogEntry* FindRole(duckdb::ClientContext& context,
                                          std::string_view name) {
  auto& cluster = catalog::ClusterOf(context);
  auto entry = cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
                 .GetEntry(cluster.GetCatalogTransaction(context),
                           duckdb::Identifier{name});
  return entry ? &entry->Cast<catalog::RoleCatalogEntry>() : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabase(
  duckdb::ClientContext& context, std::string_view name) {
  auto& cluster = catalog::ClusterOf(context);
  return cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .GetEntry(cluster.GetCatalogTransaction(context), duckdb::Identifier{name});
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabaseById(
  duckdb::ClientContext& context, duckdb::idx_t id) {
  auto& cluster = catalog::ClusterOf(context);
  duckdb::optional_ptr<duckdb::CatalogEntry> result;
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.GetCatalogTransaction(context),
          [&](duckdb::CatalogEntry& entry) {
            if (!result && entry.oid == id) {
              result = &entry;
            }
          });
  return result;
}

// The session's own database: the catalog every name and oid below resolves
// in.
duckdb::optional_ptr<duckdb::Catalog> SessionCatalog(
  duckdb::ClientContext& context) {
  return duckdb::Catalog::GetCatalogEntry(
    context, duckdb::DatabaseManager::GetDefaultDatabase(context));
}

// An oid is the one thing duckdb cannot resolve from a name, so these walk the
// session database's sets.
duckdb::optional_ptr<duckdb::CatalogEntry> FindByOid(
  duckdb::ClientContext& context, duckdb::CatalogType type, duckdb::idx_t id) {
  auto database = catalog::DefaultDuckCatalog(context);
  return database ? database->FindEntryById(&context, type, id) : nullptr;
}

// current_setting(name, missing_ok) -> text
// Ported from server/pg/functions/system.cpp CurrentSettingMissingOkFunction.
void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto count = args.size();
  auto names = args.data[0].Values<duckdb::string_t>();
  auto oks = args.data[1].Values<bool>();
  auto* result_ptr =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  std::optional<duckdb::idx_t> last_found;
  for (duckdb::idx_t row = 0; row < count; row++) {
    auto name_value = names[row];
    auto ok_value = oks[row];
    if (!name_value.IsValid() || !ok_value.IsValid()) {
      result_validity.SetInvalid(row);
      continue;
    }
    const bool missing_ok = ok_value.GetValue();
    const auto name = name_value.GetValue();
    if (last_found && names[*last_found].GetValue() == name) {
      result_ptr[row] = result_ptr[*last_found];
      continue;
    }
    const auto key = name.GetString();
    duckdb::Value value;
    if (context.TryGetCurrentSetting(duckdb::Identifier{key}, value)) {
      result_ptr[row] =
        duckdb::StringVector::AddString(result, value.ToString());
      last_found = row;
      continue;
    }
    if (missing_ok) {
      result_validity.SetInvalid(row);
      continue;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("unrecognized configuration parameter \"", key, "\""));
  }
}

// current_user / current_role -> the effective role (follows SET ROLE).
void CurrentUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.EffectiveUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

// session_user -> the session role (follows SET SESSION AUTHORIZATION, not
// SET ROLE).
void SessionUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.SessionUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

// pg_backend_pid() -> int4: this connection's backend PID, the same value sent
// in BackendKeyData (the high 32 bits of the random cancel key) so cancellation
// and pg_backend_pid() agree.
void PgBackendPidFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  const auto& conn_ctx = GetSereneDBContext(state.GetContext());
  result.Reference(duckdb::Value::INTEGER(conn_ctx.GetBackendPid()),
                   duckdb::count_t(args.size()));
}

struct SignalText {
  std::string_view denied;
  std::string_view superuser_detail;
  std::string_view member_detail;
};

constexpr SignalText kCancelText{
  .denied = "permission denied to cancel query",
  .superuser_detail =
    "Only roles with the SUPERUSER attribute may cancel "
    "queries of roles with the SUPERUSER attribute.",
  .member_detail =
    "Only roles with privileges of the role whose query is "
    "being canceled or with privileges of the "
    "\"pg_signal_backend\" role may cancel this query.",
};

constexpr SignalText kTerminateText{
  .denied = "permission denied to terminate process",
  .superuser_detail =
    "Only roles with the SUPERUSER attribute may terminate "
    "processes of roles with the SUPERUSER attribute.",
  .member_detail =
    "Only roles with privileges of the role whose process is "
    "being terminated or with privileges of the "
    "\"pg_signal_backend\" role may terminate this process.",
};

void CheckSignalAllowed(const auth::RoleGraph& roles,
                        const auth::RoleClosure& caller, duckdb::idx_t role,
                        bool terminate) {
  const auto* node = roles.Find(role);
  const bool superuser =
    role == pg::kInvalidOid || (node && node->is_superuser);
  if (superuser ? caller.is_superuser : caller.MemberOf(role)) {
    return;
  }
  const auto& text = terminate ? kTerminateText : kCancelText;
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE), ERR_MSG(text.denied),
    ERR_DETAIL(superuser ? text.superuser_detail : text.member_detail));
}

// Cancel (or, with `terminate`, terminate) each requested backend by pid ->
// BOOLEAN per row (true if the pid matched a live backend). Shared by
// pg_cancel_backend and pg_terminate_backend; it matches on the pid half of
// the cancel key alone, no secret.
void CancelBackendsByPid(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state, duckdb::Vector& result,
                         bool terminate) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto* registry = conn_ctx.GetCancelRegistry();
  const auto roles = auth::RolesOf(&context);
  const auto caller = auth::ClosureFor(&context, conn_ctx.GetRoleId());
  const auto pids = args.data[0].Values<int32_t>();
  auto out = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    const auto pid = pids[i];
    if (!pid.IsValid()) {
      out.WriteNull();
      continue;
    }
    const auto target = static_cast<uint32_t>(pid.GetValue());
    const auto match = registry ? registry->FindByPid(target)
                                : network::CancelRegistry::PidMatch{};
    if (!match.token) {
      // The pid is the high half of a random key, not a unique OS pid; two
      // backends collided on it, so cancelling either could hit the wrong one.
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", target,
                match.ambiguous
                  ? " matches more than one backend; not cancelling any"
                  : " is not a PostgreSQL backend process")));
      out.WriteValue(false);
      continue;
    }
    CheckSignalAllowed(*roles, *caller, match.token->Role(), terminate);
    if (terminate) {
      match.token->Terminate();
    } else {
      match.token->Cancel();
    }
    out.WriteValue(true);
  }
}

// pg_cancel_backend(pid) -> bool: interrupt the target backend's current query.
void PgCancelBackendFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  CancelBackendsByPid(args, state, result, /*terminate=*/false);
}

// pg_terminate_backend(pid) -> bool: interrupt the target backend's current
// query and stop its session (the connection closes).
void PgTerminateBackendFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  CancelBackendsByPid(args, state, result, /*terminate=*/true);
}

// set_config(name, value, is_local) -> text
// Ported from server/pg/functions/system.cpp SetConfigFunction.
void SetConfigFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, duckdb::string_t value,
        bool is_local) -> duckdb::string_t {
      duckdb::Value val{value};
      const duckdb::Identifier setting{name.GetString()};
      duckdb::PhysicalSet::SetVariable(
        context, setting,
        is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::AUTOMATIC, val);

      // Return actual stored value (callbacks may have modified it).
      duckdb::Value current;
      const bool ok = context.TryGetCurrentSetting(setting, current);
      SDB_ASSERT(ok);
      return duckdb::StringVector::AddString(result, current.ToString());
    });
}

// PG-style version string. Overrides DuckDB's built-in version()
void VersionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  auto value = duckdb::Value(
    absl::StrCat("PostgreSQL 18.3 (SereneDB ", SERENEDB_VERSION, ")"));
  result.Reference(value, duckdb::count_t(args.size()));
}

// search_path_canonical() -> text
// Returns the full catalog-qualified search path (catalog.schema,...).
// The PG-compliant SHOW search_path only lists schemas in the current database
// and keeps the literal "$user" placeholder; this function exposes the
// effective, resolved form (with "$user" expanded to the session user).
void SearchPathCanonicalFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto entries =
    duckdb::ClientData::Get(context).catalog_search_path->GetResolvedSetPaths();
  auto str = duckdb::CatalogSearchEntry::ListToString(entries);
  result.Reference(duckdb::Value{std::move(str)}, duckdb::count_t(args.size()));
}

template<bool kCountNulls>
void CountNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  const auto count = args.size();
  const auto ncols = args.ColumnCount();
  std::vector<duckdb::UnifiedVectorFormat> vdata(ncols);
  for (duckdb::idx_t col = 0; col < ncols; col++) {
    args.data[col].ToUnifiedFormat(count, vdata[col]);
  }
  auto out = duckdb::FlatVector::Writer<int32_t>(result, count);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t matched = 0;
    for (duckdb::idx_t col = 0; col < ncols; col++) {
      const auto idx = vdata[col].sel->get_index(row);
      if (vdata[col].validity.RowIsValid(idx) != kCountNulls) {
        matched++;
      }
    }
    out.WriteValue(matched);
  }
}

// num_nonnulls(...) -> int
// Ported from PG: counts non-null arguments.
void NumNonNullsFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  CountNullsFunction<false>(args, state, result);
}

// num_nulls(...) -> int
// Ported from PG: counts null arguments.
void NumNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  CountNullsFunction<true>(args, state, result);
}

// --- pg_typeof ---
// Returns regtype OID. The serializer formats regtype as PG type name.
void PgTypeofFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  const auto oid = pg::OidToSql(pg::Type2Oid(args.data[0].GetType()));
  result.Reference(duckdb::Value::BIGINT(oid), duckdb::count_t(args.size()));
}

duckdb::unique_ptr<duckdb::Expression> BindPgTypeof(
  duckdb::FunctionBindExpressionInput& input) {
  const auto oid =
    pg::OidToSql(pg::Type2Oid(input.children[0]->GetReturnType()));
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BIGINT(oid).WithType(pg::REGTYPE()));
}

void ToRegtypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t name) -> duckdb::optional<int64_t> {
      auto oid = pg::RegtypeIn(name.GetString());
      if (oid == pg::kInvalidOid) {
        return duckdb::nullopt;
      }
      return pg::OidToSql(oid);
    });
}

// format_type(oid, typmod) -> text
// TODO(Pasha) Account typmod?
// Keyed on the oid only (UnaryExecutor): psql calls format_type(oid, NULL),
// and a BinaryExecutor would NULL-propagate the NULL typmod and drop the name.
void FormatTypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](int64_t type_oid) -> duckdb::string_t {
      return duckdb::StringVector::AddString(
        result, pg::RegtypeOut(&context, pg::OidFromSql(type_oid)));
    });
}

// --- Size functions ---
// Ported from server/pg/functions/size.cpp

duckdb::TableCatalogEntry* RelationTable(const pg::Relation& relation) {
  if (!relation.entry ||
      relation.entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &relation.entry->Cast<duckdb::TableCatalogEntry>();
}

int64_t RelationMainForkBytes(duckdb::ClientContext& context,
                              const pg::Relation& relation) {
  if (relation.key.table) {
    return catalog::KeyIndexBytes(context, *relation.key.table,
                                  *relation.key.constraint);
  }
  if (const auto* table = RelationTable(relation)) {
    return catalog::RelationDataBytes(context, *table);
  }
  if (relation.entry->type == duckdb::CatalogType::INDEX_ENTRY) {
    return catalog::IndexEntryBytes(
      context, relation.entry->Cast<duckdb::DuckIndexEntry>());
  }
  return 0;
}

int64_t RelationForkBytes(duckdb::ClientContext& context,
                          const pg::Relation& relation, std::string_view fork) {
  if (fork == "main") {
    return RelationMainForkBytes(context, relation);
  }
  if (fork != "fsm" && fork != "vm" && fork != "init") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG("invalid fork name"),
      ERR_HINT(
        "Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."));
  }
  return 0;
}

int64_t RelationTotalBytes(duckdb::ClientContext& context,
                           const pg::Relation& relation) {
  auto* table = RelationTable(relation);
  if (!table) {
    return RelationMainForkBytes(context, relation);
  }
  return catalog::RelationDataBytes(context, *table) +
         catalog::TableIndexesTotalBytes(context, *table);
}

int64_t RelationIndexesBytes(duckdb::ClientContext& context,
                             const pg::Relation& relation) {
  auto* table = RelationTable(relation);
  return table ? catalog::TableIndexesTotalBytes(context, *table) : 0;
}

void RelationSizeFunction(
  duckdb::DataChunk& args, duckdb::ExpressionState& state,
  duckdb::Vector& result,
  absl::FunctionRef<int64_t(duckdb::ClientContext&, const pg::Relation&)>
    bytes) {
  auto& context = state.GetContext();
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<int64_t> {
      const auto relation = pg::RelationByOid(context, pg::OidFromSql(oid));
      if (!relation) {
        return duckdb::nullopt;
      }
      return bytes(context, relation);
    });
}

// pg_database_size(name) -> bigint
void PgDatabaseSizeNameFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view db_name{input.GetData(), input.GetSize()};
      auto database =
        duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier{db_name});
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database \"", db_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, *database, {}).bytes);
    });
}

// pg_database_size(oid) -> bigint
void PgDatabaseSizeOidFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto entry = FindDatabaseById(context, pg::OidFromSql(oid));
      auto database = entry
                        ? duckdb::Catalog::GetCatalogEntry(context, entry->name)
                        : nullptr;
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, *database, {}).bytes);
    });
}

// pg_schema_size(name) -> bigint -- non-standard, included for SereneDB tests.
void PgSchemaSizeNameFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto database = SessionCatalog(context);
  SDB_ASSERT(database);

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view schema_name{input.GetData(), input.GetSize()};
      auto schema =
        database->GetSchema(context, duckdb::Identifier{schema_name},
                            duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema \"", schema_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, *database, schema_name).bytes);
    });
}

// pg_schema_size(oid) -> bigint
void PgSchemaSizeOidFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  // A schema oid resolves in the session's own database, as it does in pg.
  auto database = catalog::DefaultDuckCatalog(context);

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto schema = database
                      ? database->FindSchemaById(context, pg::OidFromSql(oid))
                      : nullptr;
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, *database,
                                     schema->name.GetIdentifierName())
          .bytes);
    });
}

struct PrivCheckModes {
  duckdb::AclMode privs = duckdb::AclMode::NoRights;
  duckdb::AclMode grant_options = duckdb::AclMode::NoRights;
};

duckdb::AclMode PrivCheckKeyword(std::string_view keyword,
                                 duckdb::CatalogType type) {
  duckdb::AclMode mode;
  if (!duckdb::Permissions::TryParsePrivilege(keyword, type, mode)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized privilege type: \"", keyword, "\""));
  }
  return mode;
}

PrivCheckModes ParsePrivCheckText(std::string_view priv_text,
                                  duckdb::CatalogType type) {
  constexpr std::string_view kSuffix = " WITH GRANT OPTION";
  PrivCheckModes out;
  for (std::string_view tok :
       absl::StrSplit(priv_text, ',', absl::SkipEmpty())) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (stripped.size() > kSuffix.size() &&
        absl::EqualsIgnoreCase(
          stripped.substr(stripped.size() - kSuffix.size()), kSuffix)) {
      out.grant_options |= PrivCheckKeyword(
        stripped.substr(0, stripped.size() - kSuffix.size()), type);
    } else {
      out.privs |= PrivCheckKeyword(stripped, type);
    }
  }
  return out;
}

bool HoldsAnyMode(duckdb::ClientContext& context, duckdb::idx_t role_id,
                  const duckdb::Permissions& perm, duckdb::CatalogType type,
                  const PrivCheckModes& modes) {
  // The cached inherit-closure (superuser bit + sorted role ids): one hash
  // lookup unless a role DDL has happened since the last check.
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != duckdb::AclMode::NoRights &&
      closure->CanAny(type, perm, modes.privs)) {
    return true;
  }
  if (modes.grant_options == duckdb::AclMode::NoRights) {
    return false;
  }
  if (closure->Owns(perm.owner)) {
    return true;
  }
  const duckdb::AclMode held = closure->GrantableModes(perm.acl);
  return (held & modes.grant_options) != duckdb::AclMode::NoRights;
}

bool HasAnyPermissionsPrivilegeText(duckdb::ClientContext& context,
                                    duckdb::idx_t role_id,
                                    const duckdb::Permissions& perm,
                                    duckdb::CatalogType type,
                                    std::string_view priv_text) {
  return HoldsAnyMode(context, role_id, perm, type,
                      ParsePrivCheckText(priv_text, type));
}

bool HoldsUnchecked(const duckdb::CatalogEntry& entry,
                    std::string_view priv_text, duckdb::CatalogType type) {
  if (!auth::Unowned(entry)) {
    return false;
  }
  ParsePrivCheckText(priv_text, type);
  return true;
}

bool HasAnyPermissionsPrivilegeText(duckdb::ClientContext& context,
                                    duckdb::idx_t role_id,
                                    const duckdb::CatalogEntry& entry,
                                    duckdb::CatalogType type,
                                    std::string_view priv_text) {
  return HoldsUnchecked(entry, priv_text, type) ||
         HasAnyPermissionsPrivilegeText(context, role_id, entry.permissions,
                                        type, priv_text);
}

const duckdb::CatalogEntry& IndexedRelation(duckdb::ClientContext& context,
                                            const pg::Relation& relation) {
  if (relation.key.table) {
    return *relation.key.table;
  }
  const auto& index = relation.entry->Cast<duckdb::IndexCatalogEntry>();
  const auto host =
    index.GetRelation(index.catalog.GetCatalogTransaction(context));
  if (!host) {
    return index;
  }
  return *host;
}

// A view answers the relation privilege functions the way a table does -- pg
// spells both with the relation keywords -- so a relation name takes either.
bool HasRelationPrivilegeText(duckdb::ClientContext& context,
                              duckdb::idx_t role_id,
                              const pg::Relation& relation,
                              std::string_view priv_text) {
  if (relation.key.table ||
      relation.entry->type == duckdb::CatalogType::INDEX_ENTRY) {
    const auto& indexed = IndexedRelation(context, relation);
    return HoldsUnchecked(indexed, priv_text,
                          duckdb::CatalogType::TABLE_ENTRY) ||
           HasAnyPermissionsPrivilegeText(
             context, role_id,
             duckdb::Permissions{.owner = indexed.permissions.owner},
             duckdb::CatalogType::TABLE_ENTRY, priv_text);
  }
  const auto& entry = *relation.entry;
  if (HoldsUnchecked(entry, priv_text, duckdb::CatalogType::TABLE_ENTRY)) {
    return true;
  }
  if (entry.type != duckdb::CatalogType::SEQUENCE_ENTRY) {
    return HasAnyPermissionsPrivilegeText(context, role_id, entry.permissions,
                                          duckdb::CatalogType::TABLE_ENTRY,
                                          priv_text);
  }
  auto modes = ParsePrivCheckText(priv_text, duckdb::CatalogType::TABLE_ENTRY);
  const auto rights =
    duckdb::Permissions::AllPrivileges(duckdb::CatalogType::SEQUENCE_ENTRY);
  modes.privs = modes.privs & rights;
  modes.grant_options = modes.grant_options & rights;
  if (modes.privs == duckdb::AclMode::NoRights &&
      modes.grant_options == duckdb::AclMode::NoRights) {
    return auth::ClosureFor(&context, role_id)->is_superuser;
  }
  return HoldsAnyMode(context, role_id, entry.permissions,
                      duckdb::CatalogType::SEQUENCE_ENTRY, modes);
}

bool HasRelationPrivilege(duckdb::ClientContext& context, duckdb::idx_t role_id,
                          const pg::Relation& relation,
                          std::string_view priv_text) {
  try {
    return HasRelationPrivilegeText(context, role_id, relation, priv_text);
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

pg::Relation RelationNamed(duckdb::ClientContext& context,
                           std::string_view text) {
  const auto name = duckdb::QualifiedName::Parse(text);
  auto relation = pg::RelationByName(context, name);
  if (!relation) {
    ThrowRelationNotFound(name.Name().GetIdentifierName());
  }
  return relation;
}

std::optional<duckdb::idx_t> ResolveRoleOrPublic(duckdb::ClientContext& context,
                                                 std::string_view role_name) {
  if (absl::EqualsIgnoreCase(role_name, irs::StaticStrings::kPublic)) {
    return pg::kPublicGrantee;
  }
  if (const auto* role = FindRole(context, role_name)) {
    return role->oid;
  }
  return std::nullopt;
}

bool HasTablePrivilegeImpl(ConnectionContext& conn_ctx,
                           std::string_view role_name,
                           std::string_view table_name,
                           std::string_view priv_text) {
  auto& context = conn_ctx.GetClientContext();
  auto role_id = ResolveRoleOrPublic(context, role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  return HasRelationPrivilege(context, *role_id,
                              RelationNamed(context, table_name), priv_text);
}

void HasTablePrivilege3Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t table,
        duckdb::string_t priv) -> bool {
      return HasTablePrivilegeImpl(conn_ctx, {role.GetData(), role.GetSize()},
                                   {table.GetData(), table.GetSize()},
                                   {priv.GetData(), priv.GetSize()});
    });
}

void HasTablePrivilege2Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const std::string_view current = conn_ctx.user();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t table, duckdb::string_t priv) -> bool {
      return HasTablePrivilegeImpl(conn_ctx, current,
                                   {table.GetData(), table.GetSize()},
                                   {priv.GetData(), priv.GetSize()});
    });
}

bool HasTablePrivilegeByOidImpl(duckdb::ClientContext& context,
                                duckdb::idx_t role_id, duckdb::idx_t table_id,
                                std::string_view priv_text, bool& is_null) {
  const auto relation = pg::RelationByOid(context, table_id);
  is_null = !relation;
  return relation &&
         HasRelationPrivilege(context, role_id, relation, priv_text);
}

void HasTablePrivilegeOid2Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        state.GetContext(), current->oid, pg::OidFromSql(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      return is_null ? duckdb::nullopt : duckdb::optional<bool>{r};
    });
}

void HasTablePrivilegeOid3Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t toid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        state.GetContext(), pg::OidFromSql(roid), pg::OidFromSql(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

void HasTablePrivilegeOidName3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t tname,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      return HasRelationPrivilege(
        context, pg::OidFromSql(roid),
        RelationNamed(context, {tname.GetData(), tname.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void HasTablePrivilegeNameOid3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, int64_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t rname, int64_t toid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      auto role_id = ResolveRoleOrPublic(state.GetContext(),
                                         {rname.GetData(), rname.GetSize()});
      if (!role_id) {
        ThrowRoleNotFound({rname.GetData(), rname.GetSize()});
      }
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        state.GetContext(), *role_id, pg::OidFromSql(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

const char* ObjectClassWord(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return "schema";
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return "relation";
    case duckdb::CatalogType::MACRO_ENTRY:
      return "function";
    case duckdb::CatalogType::DATABASE_ENTRY:
      return "database";
    default:
      return "object";
  }
}

// No snapshot: every kind this answers for has its entry as the object.
bool HasObjectPrivilegeByName(duckdb::ClientContext& context,
                              duckdb::CatalogType type, duckdb::idx_t role_id,
                              std::string_view obj_name,
                              std::string_view priv_text) {
  // A database is not in the snapshot -- its entry is the object -- so its
  // owner and ACL come off the entry the cluster-global set holds.
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = FindDatabase(context, obj_name);
    if (!database) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("database \"", obj_name, "\" does not exist"));
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, *database, type,
                                            priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  // Nor is a schema, for the same reason. pg_catalog and information_schema
  // are the two names that resolve to nothing and are still answered for, just
  // below.
  auto database = SessionCatalog(context);
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    if (auto schema =
          database->GetSchema(context, duckdb::Identifier{obj_name},
                              duckdb::OnEntryNotFound::RETURN_NULL)) {
      try {
        return HasAnyPermissionsPrivilegeText(context, role_id, *schema, type,
                                              priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Nor is a sequence, whose name is in the relation namespace and whose
  // permissions come off the entry the schema's set holds.
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    if (auto sequence = duckdb::Catalog::GetEntry(
          context,
          duckdb::EntryLookupInfo{type, duckdb::QualifiedName::Parse(obj_name)},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      try {
        return HasAnyPermissionsPrivilegeText(context, role_id, *sequence, type,
                                              priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Nor is a type or a function. An unresolved name still answers below, the
  // way a built-in one does.
  if (type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY) {
    // A function name carries its argument list; the entry is keyed on the bare
    // name, as duckdb's macro sets are.
    const auto bare = type == duckdb::CatalogType::MACRO_ENTRY
                        ? obj_name.substr(0, obj_name.find('('))
                        : obj_name;
    const auto name =
      duckdb::QualifiedName::Parse(absl::StripAsciiWhitespace(bare));
    auto entry =
      duckdb::Catalog::GetEntry(context, duckdb::EntryLookupInfo{type, name},
                                duckdb::OnEntryNotFound::RETURN_NULL);
    if (!entry && type == duckdb::CatalogType::MACRO_ENTRY) {
      entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_MACRO_ENTRY, name},
        duckdb::OnEntryNotFound::RETURN_NULL);
    }
    if (entry) {
      try {
        return HasAnyPermissionsPrivilegeText(context, role_id, *entry, type,
                                              priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Every kind this function is registered for is handled above. What is left
  // is a name none of them holds.
  //
  // Functions, types and sequences include built-ins / objects not tracked as
  // catalog entries (version(), integer, ...). PG grants EXECUTE on functions,
  // USAGE on types and (for the owner/PUBLIC defaults) sequences, so serenedb
  // reports an unresolved object of these classes as held rather than erroring
  // on a name it cannot resolve.
  if (type == duckdb::CatalogType::MACRO_ENTRY ||
      type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    return true;
  }
  // pg_catalog / information_schema are virtual schemas absent from the
  // schema store. PG grants PUBLIC USAGE on both and restricts CREATE to
  // superusers.
  if (type == duckdb::CatalogType::SCHEMA_ENTRY &&
      (obj_name == irs::StaticStrings::kPgCatalogSchema ||
       obj_name == irs::StaticStrings::kInformationSchema)) {
    const auto modes =
      ParsePrivCheckText(priv_text, duckdb::CatalogType::SCHEMA_ENTRY);
    if ((modes.privs & duckdb::AclMode::Create) != duckdb::AclMode::NoRights ||
        (modes.grant_options & duckdb::AclMode::Create) !=
          duckdb::AclMode::NoRights) {
      return auth::ClosureFor(&context, role_id)->is_superuser;
    }
    return true;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG(ObjectClassWord(type), " \"", obj_name, "\" does not exist"));
}

bool HasObjectPrivilegeImpl(ConnectionContext& conn_ctx,
                            duckdb::CatalogType type,
                            std::string_view role_name,
                            std::string_view obj_name,
                            std::string_view priv_text) {
  auto role_id = ResolveRoleOrPublic(conn_ctx.GetClientContext(), role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  return HasObjectPrivilegeByName(conn_ctx.GetClientContext(), type, *role_id,
                                  obj_name, priv_text);
}

template<duckdb::CatalogType kType>
void HasObjectPrivilege3Function(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t obj,
        duckdb::string_t priv) -> bool {
      return HasObjectPrivilegeImpl(
        conn_ctx, kType, {role.GetData(), role.GetSize()},
        {obj.GetData(), obj.GetSize()}, {priv.GetData(), priv.GetSize()});
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilege2Function(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const std::string_view current = conn_ctx.user();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t obj, duckdb::string_t priv) -> bool {
      return HasObjectPrivilegeImpl(conn_ctx, kType, current,
                                    {obj.GetData(), obj.GetSize()},
                                    {priv.GetData(), priv.GetSize()});
    });
}

duckdb::optional_ptr<duckdb::CatalogEntry> ObjectEntryByOid(
  duckdb::ClientContext& context, duckdb::CatalogType type, duckdb::idx_t oid) {
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    const auto relation = pg::RelationByOid(context, oid);
    if (!relation.entry || relation.entry->type != type) {
      return nullptr;
    }
    return relation.entry;
  }
  auto entry = FindByOid(context, type, oid);
  if (!entry && type == duckdb::CatalogType::MACRO_ENTRY) {
    entry = FindByOid(context, duckdb::CatalogType::TABLE_MACRO_ENTRY, oid);
  }
  return entry;
}

bool HasObjectPrivilegeByOidImpl(duckdb::ClientContext& context,
                                 duckdb::CatalogType type,
                                 duckdb::idx_t role_id, duckdb::idx_t obj_id,
                                 std::string_view priv_text, bool& is_null) {
  is_null = false;
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = FindDatabaseById(context, obj_id);
    if (!database) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, *database, type,
                                            priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    auto database = catalog::DefaultDuckCatalog(context);
    auto schema =
      database ? database->FindSchemaById(context, obj_id) : nullptr;
    if (!schema) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, *schema, type,
                                            priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY ||
      type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY) {
    auto entry = ObjectEntryByOid(context, type, obj_id);
    if (!entry) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, *entry, type,
                                            priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  // As above: every kind this answers for has its entry as the object, so an
  // oid none of the sets holds names nothing.
  is_null = true;
  return false;
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOid2Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t ooid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r = HasObjectPrivilegeByOidImpl(
        state.GetContext(), kType, current->oid, pg::OidFromSql(ooid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOid3Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t ooid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      bool is_null = false;
      bool r = HasObjectPrivilegeByOidImpl(
        state.GetContext(), kType, pg::OidFromSql(roid), pg::OidFromSql(ooid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOidName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t obj,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      return HasObjectPrivilegeByName(
        state.GetContext(), kType, pg::OidFromSql(roid),
        {obj.GetData(), obj.GetSize()}, {priv.GetData(), priv.GetSize()});
    });
}

struct RolePrivMask {
  bool usage = false;
  bool member = false;
  bool set = false;
  bool admin = false;
};

RolePrivMask ParseRolePrivs(std::string_view priv_text) {
  RolePrivMask mask;
  for (std::string_view tok :
       absl::StrSplit(priv_text, ',', absl::SkipEmpty())) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (absl::EqualsIgnoreCase(stripped, "USAGE")) {
      mask.usage = true;
    } else if (absl::EqualsIgnoreCase(stripped, "MEMBER")) {
      mask.member = true;
    } else if (absl::EqualsIgnoreCase(stripped, "SET")) {
      mask.set = true;
    } else if (absl::EqualsIgnoreCase(stripped, "USAGE WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "USAGE WITH ADMIN OPTION") ||
               absl::EqualsIgnoreCase(stripped, "MEMBER WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "MEMBER WITH ADMIN OPTION") ||
               absl::EqualsIgnoreCase(stripped, "SET WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "SET WITH ADMIN OPTION")) {
      mask.admin = true;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized privilege type: \"", stripped, "\""));
    }
  }
  return mask;
}

bool PgHasRoleImpl(const auth::RoleGraph& roles, duckdb::idx_t member,
                   duckdb::idx_t target, std::string_view priv_text) {
  const auto mask = ParseRolePrivs(priv_text);
  const auto* role = roles.Find(member);
  if (role && role->is_superuser) {
    return mask.usage || mask.member || mask.set || mask.admin;
  }
  if (member == target) {
    return mask.usage || mask.member || mask.set;
  }
  const auto closure = auth::ComputeRoleClosure(roles, member);
  return (mask.usage && closure.MemberOf(target)) ||
         (mask.member && closure.IsMember(target)) ||
         (mask.set && closure.CanSet(target)) ||
         (mask.admin && closure.IsAdminOf(target));
}

duckdb::idx_t RoleIdByName(const auth::RoleGraph& roles,
                           std::string_view name) {
  const duckdb::Identifier wanted{name};
  for (const auto& [id, node] : roles.nodes) {
    if (node.name == wanted) {
      return id;
    }
  }
  ThrowRoleNotFound(name);
}

void PgHasRoleNameName3Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, duckdb::string_t role,
        duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, RoleIdByName(*roles, {user.GetData(), user.GetSize()}),
        RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleNameOid3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, RoleIdByName(*roles, {user.GetData(), user.GetSize()}),
        static_cast<uint64_t>(role), {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidName3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<int64_t, duckdb::string_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, static_cast<uint64_t>(user),
        RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidOid3Function(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<int64_t, int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*roles, static_cast<uint64_t>(user),
                           static_cast<uint64_t>(role),
                           {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleName2Function(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto roles = auth::RolesOf(&state.GetContext());
  const duckdb::idx_t member = RoleIdByName(*roles, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, member, RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOid2Function(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto roles = auth::RolesOf(&state.GetContext());
  const duckdb::idx_t member = RoleIdByName(*roles, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*roles, member, static_cast<uint64_t>(role),
                           {priv.GetData(), priv.GetSize()});
    });
}

// Whether `role_id` holds `priv` on any of `columns`, whose grants
// `column_acl` returns, of the relation with `perm`. PG resolves a column
// privilege via a table-level grant OR a per-column grant
// (pg_attribute.attacl); the optional "WITH GRANT OPTION" suffix additionally
// requires the grant-option bit on the column (or table). A list of privileges
// holds when any of them does.
bool ColumnsPrivHeld(duckdb::ClientContext& context, duckdb::idx_t role_id,
                     const duckdb::Permissions& perm, auto&& columns,
                     auto&& column_acl, std::string_view priv) {
  const auto modes = ParsePrivCheckText(priv, duckdb::CatalogType::TABLE_ENTRY);
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != duckdb::AclMode::NoRights &&
      closure->CanAny(duckdb::CatalogType::TABLE_ENTRY, perm, modes.privs)) {
    return true;
  }
  if (modes.grant_options != duckdb::AclMode::NoRights &&
      (closure->Owns(perm.owner) ||
       (closure->GrantableModes(perm.acl) & modes.grant_options) !=
         duckdb::AclMode::NoRights)) {
    return true;
  }
  for (const auto& column : columns) {
    const std::span<const duckdb::AclItem> acl = column_acl(column);
    if ((closure->HeldModes(acl) & modes.privs) != duckdb::AclMode::NoRights ||
        (closure->GrantableModes(acl) & modes.grant_options) !=
          duckdb::AclMode::NoRights) {
      return true;
    }
  }
  return false;
}

bool ColumnPrivHeld(duckdb::ClientContext& context, duckdb::idx_t role_id,
                    const duckdb::Permissions& perm,
                    std::span<const duckdb::AclItem> column_acl,
                    std::string_view priv) {
  return ColumnsPrivHeld(context, role_id, perm, std::span{&column_acl, 1},
                         std::identity{}, priv);
}

bool AnyColumnPrivHeld(duckdb::ClientContext& context, duckdb::idx_t role_id,
                       const duckdb::TableCatalogEntry& table,
                       std::string_view priv) {
  return HoldsUnchecked(table, priv, duckdb::CatalogType::TABLE_ENTRY) ||
         ColumnsPrivHeld(
           context, role_id, table.permissions, table.GetColumns().Logical(),
           [](const duckdb::ColumnDefinition& column)
             -> std::span<const duckdb::AclItem> { return column.Acl(); },
           priv);
}

bool HasColumnPrivByName(duckdb::ClientContext& context, duckdb::idx_t role_id,
                         const duckdb::TableCatalogEntry& table,
                         std::string_view col, std::string_view priv) {
  return HoldsUnchecked(table, priv, duckdb::CatalogType::TABLE_ENTRY) ||
         ColumnPrivHeld(
           context, role_id, table.permissions,
           table.GetColumns().GetColumn(duckdb::Identifier{col}).Acl(), priv);
}

// has_column_privilege(role, oid, attnum, priv): attnum is 1-based over the
// table's columns, the same numbering pg_attribute reports.
bool HasColumnPrivByAttnum(duckdb::ClientContext& context,
                           duckdb::idx_t role_id,
                           const duckdb::TableCatalogEntry& table,
                           int64_t attnum, std::string_view priv) {
  return HoldsUnchecked(table, priv, duckdb::CatalogType::TABLE_ENTRY) ||
         ColumnPrivHeld(
           context, role_id, table.permissions,
           table.GetColumns().GetColumn(duckdb::LogicalIndex(attnum - 1)).Acl(),
           priv);
}

[[noreturn]] void ThrowColumnNotFound(std::string_view column,
                                      std::string_view relation) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                  ERR_MSG("column \"", column, "\" of relation \"", relation,
                          "\" does not exist"));
}

bool ViewHasColumn(duckdb::ClientContext& context,
                   duckdb::ViewCatalogEntry& view, std::string_view column) {
  view.BindView(context);
  const auto info = view.GetColumnInfo();
  if (!info) {
    return false;
  }
  for (duckdb::idx_t i = 0; i < info->names.size(); ++i) {
    const auto& name = i < view.aliases.size() && !view.aliases[i].empty()
                         ? view.aliases[i]
                         : info->names[i];
    if (name.GetIdentifierName() == column) {
      return true;
    }
  }
  return false;
}

duckdb::ViewCatalogEntry* RelationView(const pg::Relation& relation) {
  if (!relation.entry ||
      relation.entry->type != duckdb::CatalogType::VIEW_ENTRY) {
    return nullptr;
  }
  return &relation.entry->Cast<duckdb::ViewCatalogEntry>();
}

pg::IndexColumns IndexColumnsOf(const pg::Relation& relation) {
  if (relation.key.table) {
    return pg::ColumnsOfIndex(relation.key);
  }
  if (relation.entry->type != duckdb::CatalogType::INDEX_ENTRY) {
    return {};
  }
  return pg::ColumnsOfIndex(relation.entry->Cast<duckdb::IndexCatalogEntry>(),
                            nullptr);
}

bool RelationHasColumn(duckdb::ClientContext& context,
                       const pg::Relation& relation, std::string_view column) {
  if (const auto* table = RelationTable(relation)) {
    return table->GetColumns().ColumnExists(duckdb::Identifier{column});
  }
  if (auto* view = RelationView(relation)) {
    return ViewHasColumn(context, *view, column);
  }
  return absl::c_any_of(
    IndexColumnsOf(relation).columns,
    [&](const pg::IndexColumn& indexed) { return indexed.name == column; });
}

int64_t RelationColumnCount(duckdb::ClientContext& context,
                            const pg::Relation& relation) {
  if (const auto* table = RelationTable(relation)) {
    return static_cast<int64_t>(table->GetColumns().LogicalColumnCount());
  }
  if (auto* view = RelationView(relation)) {
    view->BindView(context);
    const auto info = view->GetColumnInfo();
    return info ? static_cast<int64_t>(info->names.size()) : 0;
  }
  return static_cast<int64_t>(IndexColumnsOf(relation).columns.size());
}

std::string RelationLabel(const pg::Relation& relation) {
  if (relation.entry) {
    return std::string{relation.entry->name.GetIdentifierName()};
  }
  return pg::ConstraintName(*relation.key.table, *relation.key.constraint);
}

bool HasAnyColumnPrivilege(duckdb::ClientContext& context,
                           duckdb::idx_t role_id, const pg::Relation& relation,
                           std::string_view priv) {
  try {
    if (const auto* table = RelationTable(relation)) {
      return AnyColumnPrivHeld(context, role_id, *table, priv);
    }
    return HasRelationPrivilegeText(context, role_id, relation, priv);
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

bool HasColumnPrivilege(duckdb::ClientContext& context, duckdb::idx_t role_id,
                        const pg::Relation& relation, std::string_view column,
                        std::string_view priv) {
  if (!RelationHasColumn(context, relation, column)) {
    ThrowColumnNotFound(column, RelationLabel(relation));
  }
  try {
    if (const auto* table = RelationTable(relation)) {
      return HasColumnPrivByName(context, role_id, *table, column, priv);
    }
    return HasRelationPrivilegeText(context, role_id, relation, priv);
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

bool ColumnPrivilegeAt(duckdb::ClientContext& context, duckdb::idx_t role_id,
                       const pg::Relation& relation, int64_t attnum,
                       std::string_view priv, bool& held) {
  if (attnum < 1 || attnum > RelationColumnCount(context, relation)) {
    return false;
  }
  try {
    const auto* table = RelationTable(relation);
    held = table ? HasColumnPrivByAttnum(context, role_id, *table, attnum, priv)
                 : HasRelationPrivilegeText(context, role_id, relation, priv);
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
  return true;
}

void HasColumnPrivilegeNameName4Function(duckdb::DataChunk& args,
                                         duckdb::ExpressionState& state,
                                         duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      auto role = FindRole(context, {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      return HasColumnPrivilege(
        context, role->oid, RelationNamed(context, {t.GetData(), t.GetSize()}),
        {c.GetData(), c.GetSize()}, {p.GetData(), p.GetSize()});
    });
}

void HasColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                     duckdb::ExpressionState& state,
                                     duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      auto& context = state.GetContext();
      return HasColumnPrivilege(
        context, current->oid,
        RelationNamed(context, {t.GetData(), t.GetSize()}),
        {c.GetData(), c.GetSize()}, {p.GetData(), p.GetSize()});
    });
}

void HasColumnPrivilegeOidAttnum3Function(duckdb::DataChunk& args,
                                          duckdb::ExpressionState& state,
                                          duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int32_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, int32_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      auto& context = state.GetContext();
      const auto relation = pg::RelationByOid(context, pg::OidFromSql(toid));
      bool held = false;
      if (!relation ||
          !ColumnPrivilegeAt(context, current->oid, relation, attnum,
                             {p.GetData(), p.GetSize()}, held)) {
        return duckdb::nullopt;
      }
      return held;
    });
}

void HasColumnPrivilegeNameAttnum4Function(duckdb::DataChunk& args,
                                           duckdb::ExpressionState& state,
                                           duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    int16_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      auto role = FindRole(context, {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      bool held = false;
      if (!ColumnPrivilegeAt(context, role->oid,
                             RelationNamed(context, {t.GetData(), t.GetSize()}),
                             attnum, {p.GetData(), p.GetSize()}, held)) {
        return duckdb::nullopt;
      }
      return held;
    });
}

void HasColumnPrivilegeOidNameAttnum4Function(duckdb::DataChunk& args,
                                              duckdb::ExpressionState& state,
                                              duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t, int16_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      bool held = false;
      if (!ColumnPrivilegeAt(context, pg::OidFromSql(roid),
                             RelationNamed(context, {t.GetData(), t.GetSize()}),
                             attnum, {p.GetData(), p.GetSize()}, held)) {
        return duckdb::nullopt;
      }
      return held;
    });
}

void HasColumnPrivilegeOidOidAttnum4Function(duckdb::DataChunk& args,
                                             duckdb::ExpressionState& state,
                                             duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, int16_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t toid, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      const auto relation = pg::RelationByOid(context, pg::OidFromSql(toid));
      bool held = false;
      if (!relation ||
          !ColumnPrivilegeAt(context, pg::OidFromSql(roid), relation, attnum,
                             {p.GetData(), p.GetSize()}, held)) {
        return duckdb::nullopt;
      }
      return held;
    });
}

void HasAnyColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto& context = state.GetContext();
      auto role = FindRole(context, {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      return HasAnyColumnPrivilege(
        context, role->oid, RelationNamed(context, {t.GetData(), t.GetSize()}),
        {p.GetData(), p.GetSize()});
    });
}

void HasAnyColumnPrivilegeOid2Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      auto& context = state.GetContext();
      const auto relation = pg::RelationByOid(context, pg::OidFromSql(toid));
      if (!relation) {
        return duckdb::nullopt;
      }
      return HasAnyColumnPrivilege(context, current->oid, relation,
                                   {p.GetData(), p.GetSize()});
    });
}

void HasAnyColumnPrivilegeName2Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      auto& context = state.GetContext();
      return HasAnyColumnPrivilege(
        context, current->oid,
        RelationNamed(context, {t.GetData(), t.GetSize()}),
        {p.GetData(), p.GetSize()});
    });
}

bool SerialSequenceOf(duckdb::ClientContext& context,
                      std::string_view table_name, std::string_view column_name,
                      std::string& sequence) {
  const auto name = duckdb::QualifiedName::Parse(table_name);
  const auto relation = name.Name().GetIdentifierName();
  if (pg::ResolveRelation(context, name) == pg::kInvalidOid) {
    ThrowRelationNotFound(absl::StrJoin(
      name.Path(), ".", [](std::string* out, const duckdb::Identifier& part) {
        out->append(part.GetIdentifierName());
      }));
  }
  auto entry = duckdb::Catalog::GetEntry(
    context, duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY, name},
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!entry) {
    return false;
  }
  if (entry->type == duckdb::CatalogType::VIEW_ENTRY) {
    if (!ViewHasColumn(context, entry->Cast<duckdb::ViewCatalogEntry>(),
                       column_name)) {
      ThrowColumnNotFound(column_name, relation);
    }
    return false;
  }
  if (entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return false;
  }
  const duckdb::ColumnDefinition* column = nullptr;
  for (const auto& candidate :
       entry->Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
    if (candidate.Name().GetIdentifierName() == column_name) {
      column = &candidate;
      break;
    }
  }
  if (!column) {
    ThrowColumnNotFound(column_name, relation);
  }
  auto& catalog = entry->ParentCatalog();
  auto dependency_manager = catalog.GetDependencyManager();
  if (!dependency_manager) {
    return false;
  }
  const auto transaction = catalog.GetCatalogTransaction(context);
  const duckdb::IdentifierEquality equals(catalog.IsCaseSensitive());
  for (const auto& owned :
       dependency_manager->OwnedEntries(transaction, *entry)) {
    const auto& candidate = owned.get();
    if (candidate.type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      continue;
    }
    const auto& schema = candidate.ParentSchema(transaction);
    const auto& owner =
      candidate.Cast<duckdb::SequenceCatalogEntry>().owner_column;
    if (owner.IsValid()
          ? owner.GetIndex() != column->CatalogOid()
          : !duckdb::Binder::DefaultNamesSequence(
              *column, schema.GetQualifiedName(candidate.name), equals)) {
      continue;
    }
    sequence =
      absl::StrCat(pg::QuoteIdentifier(schema.name.GetIdentifierName()), ".",
                   pg::QuoteIdentifier(candidate.name.GetIdentifierName()));
    return true;
  }
  return false;
}

void PgGetSerialSequenceFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto tables = args.data[0].Values<duckdb::string_t>();
  const auto columns = args.data[1].Values<duckdb::string_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  std::string sequence;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto table = tables[row];
    const auto column = columns[row];
    if (!table.IsValid() || !column.IsValid() ||
        !SerialSequenceOf(
          context, {table.GetValue().GetData(), table.GetValue().GetSize()},
          {column.GetValue().GetData(), column.GetValue().GetSize()},
          sequence)) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue(duckdb::string_t{sequence});
  }
}

}  // namespace

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto register_stable = [&loader](duckdb::ScalarFunction func) {
    func.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    loader.RegisterFunction(func);
  };
  const auto register_volatile = [&loader](duckdb::ScalarFunction func) {
    func.SetStability(duckdb::FunctionStability::VOLATILE);
    loader.RegisterFunction(func);
  };

  // PG types are registered via duckdb_external_types in duckdb_engine.cpp
  // pg_typeof(any) -> regtype
  // current_setting(name, missing_ok) -> text
  // num_nonnulls(...) -> int
  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{
      "pg_typeof", {duckdb::LogicalType::ANY}, pg::REGTYPE(), PgTypeofFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    func.SetBindExpressionCallback(BindPgTypeof);
    loader.RegisterFunction(func);
  }

  // current_setting(name, missing_ok) -> text
  {
    duckdb::ScalarFunction func{
      "current_setting",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::VARCHAR,
      CurrentSetting2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    func.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    loader.RegisterFunction(func);
  }

  // set_config(name, value, is_local) -> text
  {
    duckdb::ScalarFunction func{
      "set_config",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::VARCHAR,
      SetConfigFunction};
    func.SetStability(duckdb::FunctionStability::VOLATILE);
    func.SetRequiresOrderedExecution(true);
    loader.RegisterFunction(func);
  }

  {
    duckdb::ScalarFunction func{
      "pg_get_serial_sequence",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::VARCHAR,
      PgGetSerialSequenceFunction};
    func.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    func.SetFallible();
    loader.RegisterFunction(func);
  }

  // search_path_canonical() -> text
  register_stable(duckdb::ScalarFunction{"search_path_canonical",
                                         {},
                                         duckdb::LogicalType::VARCHAR,
                                         SearchPathCanonicalFunction});

  // version() -> text (overrides DuckDB's built-in)
  register_stable(duckdb::ScalarFunction{
    "version", {}, duckdb::LogicalType::VARCHAR, VersionFunction});

  // pg_backend_pid() -> int4 (this connection's backend PID)
  register_stable(duckdb::ScalarFunction{
    "pg_backend_pid", {}, duckdb::LogicalType::INTEGER, PgBackendPidFunction});

  // pg_cancel_backend(pid) / pg_terminate_backend(pid) -> bool: cancel another
  // backend's query by pid (terminate degrades to cancel + a warning).
  register_volatile(duckdb::ScalarFunction{"pg_cancel_backend",
                                           {duckdb::LogicalType::INTEGER},
                                           duckdb::LogicalType::BOOLEAN,
                                           PgCancelBackendFunction});
  register_volatile(duckdb::ScalarFunction{"pg_terminate_backend",
                                           {duckdb::LogicalType::INTEGER},
                                           duckdb::LogicalType::BOOLEAN,
                                           PgTerminateBackendFunction});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.GetSignature()
      .AddArgs("args", duckdb::LogicalType::ANY)
      .AddKwargs("kwargs", duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.GetSignature()
      .AddArgs("args", duckdb::LogicalType::ANY)
      .AddKwargs("kwargs", duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // width_bucket(operand, low, high, count) -> int
  loader.RegisterFunction(duckdb::ScalarFunction{
    "width_bucket",
    {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE,
     duckdb::LogicalType::DOUBLE, duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::INTEGER,
    [](duckdb::DataChunk& args, duckdb::ExpressionState&,
       duckdb::Vector& result) {
      duckdb::GenericExecutor::ExecuteQuaternary<
        duckdb::PrimitiveType<double>, duckdb::PrimitiveType<double>,
        duckdb::PrimitiveType<double>, duckdb::PrimitiveType<int32_t>,
        duckdb::PrimitiveType<int32_t>>(
        args.data[0], args.data[1], args.data[2], args.data[3], result,
        args.size(),
        [](duckdb::PrimitiveType<double> operand,
           duckdb::PrimitiveType<double> low,
           duckdb::PrimitiveType<double> high,
           duckdb::PrimitiveType<int32_t> count)
          -> duckdb::PrimitiveType<int32_t> {
          if (count.val <= 0) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
              ERR_MSG("count must be greater than 0"));
          }
          if (low.val >= high.val) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
              ERR_MSG("lower bound must be less than upper bound"));
          }
          if (operand.val < low.val) {
            return {0};
          }
          if (operand.val >= high.val) {
            return {count.val + 1};
          }
          return {static_cast<int32_t>(
            (operand.val - low.val) / (high.val - low.val) * count.val + 1)};
        });
    }});

  // --- pg_*_size functions ---
  // --- pg_*_size functions: all take regclass (implicit cast from text) ---
  // pg_relation_size(regclass)
  register_volatile(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      RelationSizeFunction(args, state, result, RelationMainForkBytes);
    }});

  // pg_relation_size(regclass, text)
  register_volatile(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS(), duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t oid, duckdb::string_t fork) -> duckdb::optional<int64_t> {
          const auto relation = pg::RelationByOid(context, pg::OidFromSql(oid));
          if (!relation) {
            return duckdb::nullopt;
          }
          return RelationForkBytes(context, relation,
                                   {fork.GetData(), fork.GetSize()});
        });
    }});

  // pg_table_size(regclass)
  register_volatile(duckdb::ScalarFunction{
    "pg_table_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      RelationSizeFunction(args, state, result, RelationMainForkBytes);
    }});

  // pg_total_relation_size(regclass)
  register_volatile(duckdb::ScalarFunction{
    "pg_total_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      RelationSizeFunction(args, state, result, RelationTotalBytes);
    }});

  // pg_indexes_size(regclass)
  register_volatile(duckdb::ScalarFunction{
    "pg_indexes_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      RelationSizeFunction(args, state, result, RelationIndexesBytes);
    }});

  // Stub functions that throw "not supported"
  auto not_supported = [](duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Function is not supported in SereneDB"));
  };
  register_stable(duckdb::ScalarFunction{
    "pg_current_xact_id", {}, pg::XID8(), not_supported});
  register_volatile(duckdb::ScalarFunction{"pg_xact_status",
                                           {pg::XID8()},
                                           duckdb::LogicalType::VARCHAR,
                                           not_supported});

  {
    duckdb::ScalarFunction to_regtype_fn{
      "to_regtype",
      {duckdb::LogicalType::VARCHAR},
      pg::REGTYPE(),
      ToRegtypeFunction,
    };
    to_regtype_fn.SetNullHandling(
      duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    to_regtype_fn.SetStability(
      duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    duckdb::CreateScalarFunctionInfo info{std::move(to_regtype_fn)};
    info.SetSchema("pg_catalog");
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  {
    duckdb::ScalarFunction format_type_fn{
      "format_type",
      {pg::OID(), duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::VARCHAR,
      FormatTypeFunction,
    };
    // psql calls format_type(oid, NULL); with default null handling the NULL
    // typmod nulls the whole result before the function runs.
    format_type_fn.SetNullHandling(
      duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    format_type_fn.SetStability(
      duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    duckdb::CreateScalarFunctionInfo info{std::move(format_type_fn)};
    info.SetSchema("pg_catalog");
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }
  // pg_database_size(text) and pg_database_size(bigint/oid)
  register_volatile(duckdb::ScalarFunction{"pg_database_size",
                                           {duckdb::LogicalType::VARCHAR},
                                           duckdb::LogicalType::BIGINT,
                                           PgDatabaseSizeNameFunction});
  register_volatile(duckdb::ScalarFunction{"pg_database_size",
                                           {duckdb::LogicalType::BIGINT},
                                           duckdb::LogicalType::BIGINT,
                                           PgDatabaseSizeOidFunction});

  // pg_schema_size(text) and pg_schema_size(oid) -- non-standard helper.
  register_volatile(duckdb::ScalarFunction{"pg_schema_size",
                                           {duckdb::LogicalType::VARCHAR},
                                           duckdb::LogicalType::BIGINT,
                                           PgSchemaSizeNameFunction});
  register_volatile(duckdb::ScalarFunction{"pg_schema_size",
                                           {duckdb::LogicalType::BIGINT},
                                           duckdb::LogicalType::BIGINT,
                                           PgSchemaSizeOidFunction});

  register_stable(duckdb::ScalarFunction{
    "current_user", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  // current_role is same as current_user in postgres
  register_stable(duckdb::ScalarFunction{
    "current_role", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  register_stable(duckdb::ScalarFunction{
    "session_user", {}, duckdb::LogicalType::VARCHAR, SessionUserFunction});

  register_stable(duckdb::ScalarFunction{
    "has_table_privilege",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    HasTablePrivilege3Function});
  register_stable(duckdb::ScalarFunction{
    "has_table_privilege",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    HasTablePrivilege2Function});
  {
    duckdb::ScalarFunction func{"has_table_privilege",
                                {pg::OID(), duckdb::LogicalType::VARCHAR},
                                duckdb::LogicalType::BOOLEAN,
                                HasTablePrivilegeOid2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOidName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {duckdb::LogicalType::VARCHAR, pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeNameOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }

  const auto register_object_priv =
    [&register_stable]<duckdb::CatalogType kType>(std::string_view name) {
      register_stable(duckdb::ScalarFunction{
        duckdb::Identifier{name},
        {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
         duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilege3Function<kType>});
      register_stable(duckdb::ScalarFunction{
        duckdb::Identifier{name},
        {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilege2Function<kType>});
      {
        duckdb::ScalarFunction func{duckdb::Identifier{name},
                                    {pg::OID(), duckdb::LogicalType::VARCHAR},
                                    duckdb::LogicalType::BOOLEAN,
                                    HasObjectPrivilegeOid2Function<kType>};
        func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
        register_stable(func);
      }
      {
        duckdb::ScalarFunction func{duckdb::Identifier{name},
                                    {pg::OID(), duckdb::LogicalType::VARCHAR,
                                     duckdb::LogicalType::VARCHAR},
                                    duckdb::LogicalType::BOOLEAN,
                                    HasObjectPrivilegeOidName3Function<kType>};
        func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
        register_stable(func);
      }
      {
        duckdb::ScalarFunction func{
          duckdb::Identifier{name},
          {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
          duckdb::LogicalType::BOOLEAN,
          HasObjectPrivilegeOid3Function<kType>};
        func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
        register_stable(func);
      }
    };
  register_object_priv.operator()<duckdb::CatalogType::SCHEMA_ENTRY>(
    "has_schema_privilege");
  register_object_priv.operator()<duckdb::CatalogType::SEQUENCE_ENTRY>(
    "has_sequence_privilege");
  register_object_priv.operator()<duckdb::CatalogType::MACRO_ENTRY>(
    "has_function_privilege");
  register_object_priv.operator()<duckdb::CatalogType::DATABASE_ENTRY>(
    "has_database_privilege");
  register_object_priv.operator()<duckdb::CatalogType::TYPE_ENTRY>(
    "has_type_privilege");

  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameName3Function});
  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameOid3Function});
  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidName3Function});
  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidOid3Function});
  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleName2Function});
  register_stable(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOid2Function});

  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeNameName4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidAttnum3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::SMALLINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), pg::REGCLASS(), duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidOidAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }

  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeOid2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    register_stable(func);
  }
}

}  // namespace sdb::connector
