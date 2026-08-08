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

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/database_size.hpp>
#include <optional>
#include <ranges>

#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/build.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_index_entry.h"
#include "catalog/duckdb_index_scan_entry.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/duckdb_object_index.h"
#include "catalog/duckdb_system_table_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/secondary_index.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/virtual_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "network/cancel_registry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/system_catalog.h"
#include "search/inverted_index_storage.h"

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

[[noreturn]] void ThrowInvalidPrivilege(const SqlException& e) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG(e.message()));
}

// current_setting(name, missing_ok) -> text
// Ported from server/pg/functions/system.cpp CurrentSettingMissingOkFunction.
void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto count = args.size();
  duckdb::UnifiedVectorFormat name_data, ok_data;
  args.data[0].ToUnifiedFormat(name_data);
  args.data[1].ToUnifiedFormat(ok_data);
  const auto* name_ptr =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(name_data);
  const auto* ok_ptr = duckdb::UnifiedVectorFormat::GetData<bool>(ok_data);
  auto* result_ptr =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t row = 0; row < count; row++) {
    auto n_idx = name_data.sel->get_index(row);
    auto o_idx = ok_data.sel->get_index(row);
    if (!name_data.validity.RowIsValid(n_idx) ||
        !ok_data.validity.RowIsValid(o_idx)) {
      result_validity.SetInvalid(row);
      continue;
    }
    bool missing_ok = ok_ptr[o_idx];
    auto key = name_ptr[n_idx].GetString();
    duckdb::Value value;
    if (context.TryGetCurrentSetting(key, value)) {
      result_ptr[row] =
        duckdb::StringVector::AddString(result, value.ToString());
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

// Cancel (or, with `terminate`, terminate) each requested backend by pid ->
// BOOLEAN per row (true if the pid matched a live backend). Shared by
// pg_cancel_backend and pg_terminate_backend. Authorised by being a session
// (single superuser), so it matches on the pid half of the cancel key alone,
// no secret.
void CancelBackendsByPid(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state, duckdb::Vector& result,
                         bool terminate) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto* registry = conn_ctx.GetCancelRegistry();
  duckdb::UnifiedVectorFormat pids;
  args.data[0].ToUnifiedFormat(pids);
  const auto* pid = duckdb::UnifiedVectorFormat::GetData<int32_t>(pids);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  using CancelResult = network::CancelRegistry::CancelResult;
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    const auto idx = pids.sel->get_index(i);
    if (!pids.validity.RowIsValid(idx)) {
      validity.SetInvalid(i);
      continue;
    }
    const auto target = static_cast<uint32_t>(pid[idx]);
    const auto outcome = registry ? registry->CancelByPid(target, terminate)
                                  : CancelResult::NotFound;
    out[i] = outcome == CancelResult::Cancelled;
    if (outcome == CancelResult::Ambiguous) {
      // The pid is the high half of a random key, not a unique OS pid; two
      // backends collided on it, so cancelling either could hit the wrong one.
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", target,
                " matches more than one backend; not cancelling any")));
    } else if (outcome == CancelResult::NotFound) {
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", target, " is not a PostgreSQL backend process")));
    }
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
      duckdb::Value val{std::string{value.GetData(), value.GetSize()}};
      duckdb::PhysicalSet::SetVariable(
        context, duckdb::String::Reference(name.GetData(), name.GetSize()),
        is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::AUTOMATIC, val);

      // Return actual stored value (callbacks may have modified it).
      duckdb::Value current;
      const bool ok = context.TryGetCurrentSetting(name.GetString(), current);
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

// num_nonnulls(...) -> int
// Ported from PG: counts non-null arguments.
void NumNonNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  auto count = args.size();
  auto* result_data = duckdb::FlatVector::GetDataMutable<int32_t>(result);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t non_nulls = 0;
    for (duckdb::idx_t col = 0; col < args.ColumnCount(); col++) {
      duckdb::UnifiedVectorFormat vdata;
      args.data[col].ToUnifiedFormat(count, vdata);
      auto idx = vdata.sel->get_index(row);
      if (vdata.validity.RowIsValid(idx)) {
        non_nulls++;
      }
    }
    result_data[row] = non_nulls;
  }
}

// num_nulls(...) -> int
// Ported from PG: counts null arguments.
void NumNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  auto count = args.size();
  auto* result_data = duckdb::FlatVector::GetDataMutable<int32_t>(result);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t nulls = 0;
    for (duckdb::idx_t col = 0; col < args.ColumnCount(); col++) {
      duckdb::UnifiedVectorFormat vdata;
      args.data[col].ToUnifiedFormat(count, vdata);
      auto idx = vdata.sel->get_index(row);
      if (!vdata.validity.RowIsValid(idx)) {
        nulls++;
      }
    }
    result_data[row] = nulls;
  }
}

// --- pg_typeof ---
// Returns regtype OID. The serializer formats regtype as PG type name.
void PgTypeofFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  auto oid = static_cast<int64_t>(pg::Type2Oid(args.data[0].GetType()));
  result.Reference(duckdb::Value::BIGINT(oid), duckdb::count_t(args.size()));
}

duckdb::unique_ptr<duckdb::Expression> BindPgTypeof(
  duckdb::FunctionBindExpressionInput& input) {
  auto oid =
    static_cast<int64_t>(pg::Type2Oid(input.children[0]->GetReturnType()));
  auto val = duckdb::Value::BIGINT(oid);
  val.Reinterpret(pg::REGTYPE());
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(val));
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
      return static_cast<int64_t>(oid);
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
      // User-defined types (enum, composite, ...) are catalog objects; resolve
      // their real name there. Built-ins aren't catalog objects, so fall back
      // to the static oid->name map (RegtypeOut, which otherwise renders an
      // unknown oid as its bare number).
      if (auto type = catalog::FindSession<catalog::SereneDBTypeEntry>(
            context, ObjectId{static_cast<uint64_t>(type_oid)})) {
        return duckdb::StringVector::AddString(result,
                                               type->name.GetIdentifierName());
      }
      return duckdb::StringVector::AddString(result, pg::RegtypeOut(type_oid));
    });
}

// --- Size functions ---
// Ported from server/pg/functions/size.cpp

// The relation `oid` names, resolved through the database's id index rather
// than by walking its schemas. Null when this database holds no such object.
duckdb::optional_ptr<duckdb::CatalogEntry> RelationEntryByOid(
  duckdb::ClientContext& context, uint64_t oid) {
  return catalog::LookupEntryById(context, ObjectId{oid});
}

[[noreturn]] void ThrowNoRelationWithOid(uint64_t oid) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                  ERR_MSG("relation with OID ", oid, " does not exist"));
}

int64_t GetRelationForkSize(duckdb::ClientContext& context, uint64_t oid,
                            std::string_view fork, bool table_only = false) {
  auto entry = RelationEntryByOid(context, oid);
  if (!entry) {
    ThrowNoRelationWithOid(oid);
  }
  auto* table = dynamic_cast<catalog::SereneDBTableEntry*>(entry.get());
  if (table_only && table == nullptr) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", entry->name.GetIdentifierName(), "\" is not a table"));
  }
  if (fork != "main") {
    return 0;
  }
  if (table != nullptr) {
    return RelationDataBytes(context, *table);
  }
  if (auto* index = dynamic_cast<catalog::SereneDBIndexEntry*>(entry.get())) {
    return IndexEntryBytes(context, *index);
  }
  return 0;
}

int64_t GetRelationTotalSize(duckdb::ClientContext& context, uint64_t oid) {
  auto entry = RelationEntryByOid(context, oid);
  if (!entry) {
    ThrowNoRelationWithOid(oid);
  }
  auto* table = dynamic_cast<catalog::SereneDBTableEntry*>(entry.get());
  if (table == nullptr) {
    return GetRelationForkSize(context, oid, "main");
  }
  return RelationDataBytes(context, *table) +
         TableIndexesTotalBytes(context, *table);
}

int64_t GetTableIndexesSize(duckdb::ClientContext& context, uint64_t oid) {
  auto entry = RelationEntryByOid(context, oid);
  if (!entry) {
    ThrowNoRelationWithOid(oid);
  }
  auto* table = dynamic_cast<catalog::SereneDBTableEntry*>(entry.get());
  return table == nullptr ? 0 : TableIndexesTotalBytes(context, *table);
}

// pg_database_size(name) -> bigint
void PgDatabaseSizeNameFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view db_name{input.GetData(), input.GetSize()};
      auto database = catalog::FindDatabase(&context, db_name);
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database \"", db_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, database.Id()).bytes);
    });
}

// pg_database_size(oid) -> bigint
void PgDatabaseSizeOidFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      // Try our catalog by OID first
      auto database =
        catalog::FindDatabase(&context, ObjectId{static_cast<uint64_t>(oid)});
      if (!database) {
        // DuckDB's pg_database OIDs don't match ours -- fall back to
        // current database (covers the common pg_database_size(d.oid)
        // WHERE d.datname = current_database() pattern)
        database = catalog::FindDatabase(&context, conn_ctx.GetDatabaseId());
      }
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, database.Id()).bytes);
    });
}

// pg_schema_size(name) -> bigint -- non-standard, included for SereneDB tests.
void PgSchemaSizeNameFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto database_id = conn_ctx.GetDatabaseId();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view schema_name{input.GetData(), input.GetSize()};
      auto schema = catalog::FindSchema(&context, database_id, schema_name);
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema \"", schema_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, database_id, schema_name).bytes);
    });
}

// pg_schema_size(oid) -> bigint
void PgSchemaSizeOidFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto schema =
        catalog::FindSchema(&context, ObjectId{static_cast<uint64_t>(oid)});
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        catalog::DatabaseStorageSize(context, catalog::ParentIdOf(*schema),
                                     catalog::SchemaNameOf(*schema))
          .bytes);
    });
}

struct PrivCheckModes {
  catalog::AclMode privs = catalog::AclMode::NoRights;
  catalog::AclMode grant_options = catalog::AclMode::NoRights;
};

catalog::AclMode PrivCheckKeyword(std::string_view keyword,
                                  duckdb::CatalogType type) {
  auto parsed = auth::TryParseAclKeyword(keyword, type);
  if (!parsed) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized privilege type: \"", keyword, "\""));
  }
  return *parsed;
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

bool HasAnyPermissionsPrivilegeText(duckdb::ClientContext& context,
                                    ObjectId role_id,
                                    const catalog::Permissions& perm,
                                    duckdb::CatalogType type,
                                    std::string_view priv_text) {
  const auto modes = ParsePrivCheckText(priv_text, type);
  // The cached inherit-closure (superuser bit + sorted role ids): one hash
  // lookup unless a role DDL has happened since the last check.
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != catalog::AclMode::NoRights &&
      closure->CanAny(type, perm, modes.privs)) {
    return true;
  }
  if (modes.grant_options == catalog::AclMode::NoRights) {
    return false;
  }
  if (closure->Owns(catalog::OwnerOf(perm))) {
    return true;
  }
  const catalog::AclMode held = closure->GrantableModes(perm.acl);
  return (held & modes.grant_options) != catalog::AclMode::NoRights;
}

bool HasAnyTablePrivilegeText(duckdb::ClientContext& context, ObjectId role_id,
                              const catalog::Permissions& perm,
                              std::string_view priv_text) {
  return HasAnyPermissionsPrivilegeText(
    context, role_id, perm, duckdb::CatalogType::TABLE_ENTRY, priv_text);
}

// The grants `entry` answers a relation privilege check with. Postgres gives a
// table and a view the same vocabulary, so both come back here; anything else
// under the relation namespace -- the index-as-table wrapper -- is not one, and
// a null makes the caller answer NULL rather than false.
const catalog::Permissions* RelationPermissions(
  const duckdb::CatalogEntry* entry) {
  if (entry == nullptr || !catalog::IsHostedEntry(*entry)) {
    return nullptr;
  }
  if (entry->type != duckdb::CatalogType::TABLE_ENTRY &&
      entry->type != duckdb::CatalogType::VIEW_ENTRY) {
    return nullptr;
  }
  // The index-as-table wrapper shares the relation namespace and duckdb's
  // TABLE_ENTRY with it, but postgres gives an index no ACL of its own.
  return dynamic_cast<const catalog::SereneDBIndexScanEntry*>(entry)
           ? nullptr
           : &entry->permissions;
}

std::optional<ObjectId> ResolveRoleOrPublic(duckdb::ClientContext* context,
                                            std::string_view role_name) {
  if (absl::EqualsIgnoreCase(role_name, StaticStrings::kPublic)) {
    return catalog::kPublicGrantee;
  }
  if (auto role = catalog::FindRole(context, role_name)) {
    return role->GetId();
  }
  return std::nullopt;
}

// GetSystemTable asserts on non-system schemas, so an unqualified name (which
// ParseObjectName defaulted to the current schema) falls back to pg_catalog.
const catalog::VirtualTable* ResolveSystemRelation(ConnectionContext& conn_ctx,
                                                   const pg::ObjectName& name) {
  if (name.schema == StaticStrings::kPgCatalogSchema ||
      name.schema == StaticStrings::kInformationSchema) {
    return pg::GetSystemTable(name.schema, name.relation);
  }
  if (name.schema == conn_ctx.GetCurrentSchema()) {
    return pg::GetSystemTable(StaticStrings::kPgCatalogSchema, name.relation);
  }
  return nullptr;
}

// A system relation has no catalog definition: it exists for the life of the
// statement that reads it. Its owner is root and its ACL the one grant the
// table declares.
catalog::Permissions SystemRelationPermissions(
  const catalog::VirtualTable& sys) {
  return catalog::Permissions{
    id::kRootUser, catalog::Acl{sys.GetAcl().begin(), sys.GetAcl().end()}};
}

bool SystemRelationHasColumn(const catalog::VirtualTable& sys,
                             std::string_view column) {
  for (const auto& [name, type] :
       duckdb::StructType::GetChildTypes(sys.RowType())) {
    if (name.GetIdentifierName() == column) {
      return true;
    }
  }
  return false;
}

bool HasTablePrivilegeImpl(ConnectionContext& conn_ctx,
                           std::string_view role_name,
                           std::string_view table_name,
                           std::string_view priv_text) {
  auto role_id = ResolveRoleOrPublic(&conn_ctx.GetClientContext(), role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  const auto current_schema = conn_ctx.GetCurrentSchema();
  const auto name = pg::ParseObjectName(table_name, current_schema);
  // A view shares the relation namespace and answers has_table_privilege the
  // way PG does -- spelled with the relation keywords -- so the lookup takes
  // either and the grants come off whichever entry it found.
  auto entry = catalog::FindRelationEntry(&conn_ctx.GetClientContext(),
                                          conn_ctx.GetDatabaseId(), name.schema,
                                          name.relation);
  try {
    if (const auto* perm = RelationPermissions(entry.get())) {
      return HasAnyPermissionsPrivilegeText(
        conn_ctx.GetClientContext(), *role_id, *perm,
        duckdb::CatalogType::TABLE_ENTRY, priv_text);
    }
    if (const auto* sys = ResolveSystemRelation(conn_ctx, name)) {
      return HasAnyPermissionsPrivilegeText(
        conn_ctx.GetClientContext(), *role_id, SystemRelationPermissions(*sys),
        duckdb::CatalogType::TABLE_ENTRY, priv_text);
    }
    ThrowRelationNotFound(name.relation);
  } catch (const SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
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
  const std::string current{conn_ctx.user()};
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t table, duckdb::string_t priv) -> bool {
      return HasTablePrivilegeImpl(conn_ctx, current,
                                   {table.GetData(), table.GetSize()},
                                   {priv.GetData(), priv.GetSize()});
    });
}

bool HasTablePrivilegeByOidImpl(duckdb::ClientContext& context,
                                ObjectId role_id, ObjectId table_id,
                                std::string_view priv_text, bool& is_null) {
  is_null = false;
  const auto* perm =
    RelationPermissions(catalog::LookupEntryById(context, table_id).get());
  if (perm == nullptr) {
    is_null = true;
    return false;
  }
  try {
    return HasAnyPermissionsPrivilegeText(
      context, role_id, *perm, duckdb::CatalogType::TABLE_ENTRY, priv_text);
  } catch (const SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

void HasTablePrivilegeOid2Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r =
        HasTablePrivilegeByOidImpl(state.GetContext(), current->GetId(),
                                   ObjectId{static_cast<uint64_t>(toid)},
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
        state.GetContext(), ObjectId{static_cast<uint64_t>(roid)},
        ObjectId{static_cast<uint64_t>(toid)}, {priv.GetData(), priv.GetSize()},
        is_null);
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
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t tname,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      const auto name =
        pg::ParseObjectName({tname.GetData(), tname.GetSize()}, current_schema);
      const auto* table =
        catalog::FindTableEntry(&state.GetContext(), conn_ctx.GetDatabaseId(),
                                name.schema, name.relation);
      const ObjectId role{static_cast<uint64_t>(roid)};
      const std::string_view priv_text{priv.GetData(), priv.GetSize()};
      try {
        if (table) {
          return HasAnyPermissionsPrivilegeText(
            state.GetContext(), role, table->permissions,
            duckdb::CatalogType::TABLE_ENTRY, priv_text);
        } else if (const auto* sys = ResolveSystemRelation(conn_ctx, name)) {
          return HasAnyPermissionsPrivilegeText(
            state.GetContext(), role, SystemRelationPermissions(*sys),
            duckdb::CatalogType::TABLE_ENTRY, priv_text);
        } else {
          ThrowRelationNotFound(name.relation);
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
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
      auto role_id = ResolveRoleOrPublic(&state.GetContext(),
                                         {rname.GetData(), rname.GetSize()});
      if (!role_id) {
        ThrowRoleNotFound({rname.GetData(), rname.GetSize()});
      }
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        state.GetContext(), *role_id, ObjectId{static_cast<uint64_t>(toid)},
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
                              ConnectionContext& conn_ctx,
                              duckdb::CatalogType type, ObjectId role_id,
                              std::string_view obj_name,
                              std::string_view priv_text) {
  // A database is not in the snapshot -- its entry is the object -- so its
  // owner and ACL come off the entry the cluster-global set holds.
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = catalog::FindDatabase(&context, obj_name);
    if (!database) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("database \"", obj_name, "\" does not exist"));
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, database.perm,
                                            type, priv_text);
    } catch (const SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  // Nor is a schema, for the same reason. pg_catalog and information_schema
  // are the two names that resolve to nothing and are still answered for, just
  // below.
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    catalog::Permissions schema_perm;
    if (auto schema = catalog::FindSchema(&context, conn_ctx.GetDatabaseId(),
                                          obj_name, &schema_perm)) {
      try {
        return HasAnyPermissionsPrivilegeText(context, role_id, schema_perm,
                                              type, priv_text);
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Nor is a sequence, whose name is in the relation namespace and whose
  // permissions come off the entry the schema's set holds.
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    const std::string current_schema = conn_ctx.GetCurrentSchema();
    const auto name = pg::ParseObjectName(obj_name, current_schema);
    const auto schema_id =
      catalog::FindSchemaId(&context, conn_ctx.GetDatabaseId(), name.schema);
    if (schema_id.isSet()) {
      if (const auto* sequence = catalog::Find<catalog::SereneDBSequenceEntry>(
            &context, schema_id, name.relation)) {
        try {
          return HasAnyPermissionsPrivilegeText(
            context, role_id, sequence->permissions, type, priv_text);
        } catch (const SqlException& e) {
          ThrowInvalidPrivilege(e);
        }
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
    const std::string current_schema = conn_ctx.GetCurrentSchema();
    const auto name =
      pg::ParseObjectName(absl::StripAsciiWhitespace(bare), current_schema);
    const auto schema_id =
      catalog::FindSchemaId(&context, conn_ctx.GetDatabaseId(), name.schema);
    if (schema_id.isSet()) {
      const auto* user_type = type == duckdb::CatalogType::TYPE_ENTRY
                                ? catalog::Find<catalog::SereneDBTypeEntry>(
                                    &context, schema_id, name.relation)
                                : nullptr;
      const auto* function =
        type == duckdb::CatalogType::MACRO_ENTRY
          ? catalog::FindFunction(&context, schema_id, name.relation)
          : nullptr;
      const catalog::Permissions* perm =
        user_type != nullptr  ? &user_type->permissions
        : function != nullptr ? &function->permissions
                              : nullptr;
      if (perm != nullptr) {
        try {
          return HasAnyPermissionsPrivilegeText(context, role_id, *perm, type,
                                                priv_text);
        } catch (const SqlException& e) {
          ThrowInvalidPrivilege(e);
        }
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
      (obj_name == StaticStrings::kPgCatalogSchema ||
       obj_name == StaticStrings::kInformationSchema)) {
    const auto modes =
      ParsePrivCheckText(priv_text, duckdb::CatalogType::SCHEMA_ENTRY);
    if ((modes.privs & catalog::AclMode::Create) !=
          catalog::AclMode::NoRights ||
        (modes.grant_options & catalog::AclMode::Create) !=
          catalog::AclMode::NoRights) {
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
  auto role_id = ResolveRoleOrPublic(&conn_ctx.GetClientContext(), role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  return HasObjectPrivilegeByName(conn_ctx.GetClientContext(), conn_ctx, type,
                                  *role_id, obj_name, priv_text);
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
  const std::string current{conn_ctx.user()};
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t obj, duckdb::string_t priv) -> bool {
      return HasObjectPrivilegeImpl(conn_ctx, kType, current,
                                    {obj.GetData(), obj.GetSize()},
                                    {priv.GetData(), priv.GetSize()});
    });
}

bool HasObjectPrivilegeByOidImpl(duckdb::ClientContext& context,
                                 duckdb::CatalogType type, ObjectId role_id,
                                 ObjectId obj_id, std::string_view priv_text,
                                 bool& is_null) {
  is_null = false;
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = catalog::FindDatabase(&context, obj_id);
    if (!database) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, database.perm,
                                            type, priv_text);
    } catch (const SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    catalog::Permissions schema_perm;
    auto schema = catalog::FindSchema(&context, obj_id, &schema_perm);
    if (!schema) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, schema_perm, type,
                                            priv_text);
    } catch (const SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    const auto* sequence =
      catalog::FindSession<catalog::SereneDBSequenceEntry>(context, obj_id);
    if (sequence == nullptr) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(
        context, role_id, sequence->permissions, type, priv_text);
    } catch (const SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY) {
    const auto* user_type =
      type == duckdb::CatalogType::TYPE_ENTRY
        ? catalog::FindSession<catalog::SereneDBTypeEntry>(context, obj_id)
        : nullptr;
    const auto* function = type == duckdb::CatalogType::MACRO_ENTRY
                             ? catalog::FindSessionFunction(context, obj_id)
                             : nullptr;
    const catalog::Permissions* perm =
      user_type != nullptr  ? &user_type->permissions
      : function != nullptr ? &function->permissions
                            : nullptr;
    if (perm == nullptr) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(context, role_id, *perm, type,
                                            priv_text);
    } catch (const SqlException& e) {
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
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t ooid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r =
        HasObjectPrivilegeByOidImpl(state.GetContext(), kType, current->GetId(),
                                    ObjectId{static_cast<uint64_t>(ooid)},
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
        state.GetContext(), kType, ObjectId{static_cast<uint64_t>(roid)},
        ObjectId{static_cast<uint64_t>(ooid)}, {priv.GetData(), priv.GetSize()},
        is_null);
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
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t obj,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      return HasObjectPrivilegeByName(state.GetContext(), conn_ctx, kType,
                                      ObjectId{static_cast<uint64_t>(roid)},
                                      {obj.GetData(), obj.GetSize()},
                                      {priv.GetData(), priv.GetSize()});
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

bool PgHasRoleImpl(const auth::RoleGraph& roles, ObjectId member,
                   ObjectId target, std::string_view priv_text) {
  const auto mask = ParseRolePrivs(priv_text);
  const auto* role = roles.Find(member);
  if (role != nullptr && role->is_superuser) {
    return mask.usage || mask.member || mask.set || mask.admin;
  }
  if (member == target) {
    return mask.usage || mask.member || mask.set;
  }
  bool ok = false;
  if (mask.usage) {
    ok = ok || auth::ComputeRoleClosure(roles, member).MemberOf(target);
  }
  if (mask.member) {
    ok = ok || auth::ComputeMembershipClosure(roles, member).contains(target);
  }
  if (mask.set) {
    ok = ok || auth::ComputeSetRoleClosure(roles, member).contains(target);
  }
  if (mask.admin) {
    ok = ok || auth::HasAdminOption(roles, member, target);
  }
  return ok;
}

ObjectId RoleIdByName(const auth::RoleGraph& roles, std::string_view name) {
  for (const auto& [id, node] : roles.nodes) {
    if (node.name == name) {
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
        ObjectId{static_cast<uint64_t>(role)},
        {priv.GetData(), priv.GetSize()});
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
        *roles, ObjectId{static_cast<uint64_t>(user)},
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
      return PgHasRoleImpl(*roles, ObjectId{static_cast<uint64_t>(user)},
                           ObjectId{static_cast<uint64_t>(role)},
                           {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleName2Function(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto roles = auth::RolesOf(&state.GetContext());
  const ObjectId member = RoleIdByName(*roles, conn_ctx.user());
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
  const ObjectId member = RoleIdByName(*roles, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*roles, member,
                           ObjectId{static_cast<uint64_t>(role)},
                           {priv.GetData(), priv.GetSize()});
    });
}

bool AttnumExists(const catalog::SereneDBTableEntry& table, int64_t attnum) {
  return attnum >= 1 && attnum <= static_cast<int64_t>(
                                    table.GetColumns().LogicalColumnCount());
}

// Whether `role_id` holds `priv` on `column` of `table`. PG resolves a column
// privilege via a table-level grant OR a per-column grant
// (pg_attribute.attacl); the optional "WITH GRANT OPTION" suffix additionally
// requires the grant-option bit on the column (or table).
bool ColumnPrivHeld(duckdb::ClientContext& context, ObjectId role_id,
                    const catalog::SereneDBTableEntry& table,
                    const duckdb::ColumnDefinition& column,
                    std::string_view priv) {
  const auto modes = ParsePrivCheckText(priv, duckdb::CatalogType::TABLE_ENTRY);
  const catalog::AclView column_acl =
    table.GetColumnAcl(ObjectId{column.CatalogOid()});
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != catalog::AclMode::NoRights &&
      closure->CanColumns(table.permissions, modes.privs, {&column_acl, 1})) {
    return true;
  }
  if (modes.grant_options == catalog::AclMode::NoRights) {
    return false;
  }
  const auto& rc = *closure;
  if (rc.Owns(catalog::OwnerOf(table.permissions))) {
    return true;
  }
  const auto held =
    rc.GrantableModes(table.permissions.acl) | rc.GrantableModes(column_acl);
  return (held & modes.grant_options) != catalog::AclMode::NoRights;
}

bool HasColumnPrivByName(duckdb::ClientContext& context, ObjectId role_id,
                         const catalog::SereneDBTableEntry& table,
                         std::string_view col, std::string_view priv) {
  const auto& columns = table.GetColumns();
  const duckdb::Identifier key{col};
  if (!columns.ColumnExists(key)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", col, "\" of relation \"",
              table.name.GetIdentifierName(), "\" does not exist"));
  }
  return ColumnPrivHeld(context, role_id, table, columns.GetColumn(key), priv);
}

// has_column_privilege(role, oid, attnum, priv): attnum is 1-based over the
// table's columns, the same numbering pg_attribute reports.
bool HasColumnPrivByAttnum(duckdb::ClientContext& context, ObjectId role_id,
                           const catalog::SereneDBTableEntry& table,
                           int64_t attnum, std::string_view priv) {
  if (!AttnumExists(table, attnum)) {
    return false;
  }
  return ColumnPrivHeld(
    context, role_id, table,
    table.GetColumns().GetColumn(duckdb::LogicalIndex(attnum - 1)), priv);
}

// System relations have no per-column ACLs: the column privilege reduces to the
// relation-level privilege once the column is known to exist.
bool SystemRelationColumnPriv(ConnectionContext& conn_ctx, ObjectId role_id,
                              const pg::ObjectName& name, std::string_view col,
                              std::string_view priv) {
  const auto* sys = ResolveSystemRelation(conn_ctx, name);
  if (sys == nullptr) {
    ThrowRelationNotFound(name.relation);
  }
  if (!SystemRelationHasColumn(*sys, col)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", col, "\" of relation \"",
                            sys->GetName(), "\" does not exist"));
  }
  return HasAnyPermissionsPrivilegeText(conn_ctx.GetClientContext(), role_id,
                                        SystemRelationPermissions(*sys),
                                        duckdb::CatalogType::TABLE_ENTRY, priv);
}

void HasColumnPrivilegeNameName4Function(duckdb::DataChunk& args,
                                         duckdb::ExpressionState& state,
                                         duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role =
        catalog::FindRole(&state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      const auto name =
        pg::ParseObjectName({t.GetData(), t.GetSize()}, current_schema);
      const auto* table =
        catalog::FindTableEntry(&state.GetContext(), conn_ctx.GetDatabaseId(),
                                name.schema, name.relation);
      const std::string_view col{c.GetData(), c.GetSize()};
      const std::string_view priv{p.GetData(), p.GetSize()};
      try {
        if (table) {
          return HasColumnPrivByName(state.GetContext(), role->GetId(), *table,
                                     col, priv);
        } else {
          return SystemRelationColumnPriv(conn_ctx, role->GetId(), name, col,
                                          priv);
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                     duckdb::ExpressionState& state,
                                     duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto name =
        pg::ParseObjectName({t.GetData(), t.GetSize()}, current_schema);
      const auto* table =
        catalog::FindTableEntry(&state.GetContext(), conn_ctx.GetDatabaseId(),
                                name.schema, name.relation);
      const std::string_view col{c.GetData(), c.GetSize()};
      const std::string_view priv{p.GetData(), p.GetSize()};
      try {
        if (table) {
          return HasColumnPrivByName(state.GetContext(), current->GetId(),
                                     *table, col, priv);
        } else {
          return SystemRelationColumnPriv(conn_ctx, current->GetId(), name, col,
                                          priv);
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeOidAttnum3Function(duckdb::DataChunk& args,
                                          duckdb::ExpressionState& state,
                                          duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int32_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, int32_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto* table = catalog::FindSessionTableEntry(
        state.GetContext(), ObjectId{static_cast<uint64_t>(toid)});
      if (!table || !AttnumExists(*table, attnum)) {
        return duckdb::nullopt;
      }
      try {
        return HasColumnPrivByAttnum(state.GetContext(), current->GetId(),
                                     *table, attnum,
                                     {p.GetData(), p.GetSize()});
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

// Column privilege by (role, table-by-name, attnum, priv), with the same
// system-relation fallback as the name-column path. Returns a tri-state: an
// unset optional means the attnum is out of range (SQL NULL, matching PG).
std::optional<bool> ColumnPrivByNameTableAttnum(ConnectionContext& conn_ctx,
                                                ObjectId role_id,
                                                std::string_view current_schema,
                                                std::string_view table_name,
                                                int64_t attnum,
                                                std::string_view priv) {
  const auto name = pg::ParseObjectName(table_name, current_schema);
  const auto* table = catalog::FindTableEntry(&conn_ctx.GetClientContext(),
                                              conn_ctx.GetDatabaseId(),
                                              name.schema, name.relation);
  if (table) {
    if (!AttnumExists(*table, attnum)) {
      return std::nullopt;
    }
    return HasColumnPrivByAttnum(conn_ctx.GetClientContext(), role_id, *table,
                                 attnum, priv);
  }
  const auto* sys = ResolveSystemRelation(conn_ctx, name);
  if (sys == nullptr) {
    ThrowRelationNotFound(name.relation);
  }
  // System relations carry no per-column ACL; a valid attnum reduces to the
  // relation-level privilege.
  const auto cols = duckdb::StructType::GetChildTypes(sys->RowType());
  if (attnum < 1 || attnum > static_cast<int64_t>(cols.size())) {
    return std::nullopt;
  }
  return HasAnyPermissionsPrivilegeText(conn_ctx.GetClientContext(), role_id,
                                        SystemRelationPermissions(*sys),
                                        duckdb::CatalogType::TABLE_ENTRY, priv);
}

void HasColumnPrivilegeNameAttnum4Function(duckdb::DataChunk& args,
                                           duckdb::ExpressionState& state,
                                           duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    int16_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role =
        catalog::FindRole(&state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      try {
        auto r = ColumnPrivByNameTableAttnum(
          conn_ctx, role->GetId(), current_schema, {t.GetData(), t.GetSize()},
          attnum, {p.GetData(), p.GetSize()});
        if (r) {
          return *r;
        } else {
          return duckdb::nullopt;
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeOidNameAttnum4Function(duckdb::DataChunk& args,
                                              duckdb::ExpressionState& state,
                                              duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t, int16_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      try {
        auto r = ColumnPrivByNameTableAttnum(
          conn_ctx, ObjectId{static_cast<uint64_t>(roid)}, current_schema,
          {t.GetData(), t.GetSize()}, attnum, {p.GetData(), p.GetSize()});
        if (r) {
          return *r;
        } else {
          return duckdb::nullopt;
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
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
      const auto* table = catalog::FindSessionTableEntry(
        state.GetContext(), ObjectId{static_cast<uint64_t>(toid)});
      if (!table || !AttnumExists(*table, attnum)) {
        return duckdb::nullopt;
      }
      try {
        return HasColumnPrivByAttnum(
          state.GetContext(), ObjectId{static_cast<uint64_t>(roid)}, *table,
          attnum, {p.GetData(), p.GetSize()});
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role =
        catalog::FindRole(&state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      const auto name =
        pg::ParseObjectName({t.GetData(), t.GetSize()}, current_schema);
      const auto* table =
        catalog::FindTableEntry(&state.GetContext(), conn_ctx.GetDatabaseId(),
                                name.schema, name.relation);
      try {
        if (table) {
          return HasAnyTablePrivilegeText(state.GetContext(), role->GetId(),
                                          table->permissions,
                                          {p.GetData(), p.GetSize()});
        } else if (const auto* sys = ResolveSystemRelation(conn_ctx, name)) {
          return HasAnyTablePrivilegeText(state.GetContext(), role->GetId(),
                                          SystemRelationPermissions(*sys),
                                          {p.GetData(), p.GetSize()});
        } else {
          ThrowRelationNotFound(name.relation);
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeOid2Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto* perm = RelationPermissions(
        catalog::LookupEntryById(state.GetContext(),
                                 ObjectId{static_cast<uint64_t>(toid)})
          .get());
      if (perm == nullptr) {
        return duckdb::nullopt;
      }
      try {
        return HasAnyTablePrivilegeText(state.GetContext(), current->GetId(),
                                        *perm, {p.GetData(), p.GetSize()});
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeName2Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = catalog::FindRole(&state.GetContext(), conn_ctx.user());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto name =
        pg::ParseObjectName({t.GetData(), t.GetSize()}, current_schema);
      const auto* table =
        catalog::FindTableEntry(&state.GetContext(), conn_ctx.GetDatabaseId(),
                                name.schema, name.relation);
      try {
        if (table) {
          return HasAnyTablePrivilegeText(state.GetContext(), current->GetId(),
                                          table->permissions,
                                          {p.GetData(), p.GetSize()});
        } else if (const auto* sys = ResolveSystemRelation(conn_ctx, name)) {
          return HasAnyTablePrivilegeText(state.GetContext(), current->GetId(),
                                          SystemRelationPermissions(*sys),
                                          {p.GetData(), p.GetSize()});
        } else {
          ThrowRelationNotFound(name.relation);
        }
      } catch (const SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

}  // namespace

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

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
    loader.RegisterFunction(func);
  }

  // set_config(name, value, is_local) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_config",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN},
    duckdb::LogicalType::VARCHAR,
    SetConfigFunction});

  // search_path_canonical() -> text
  loader.RegisterFunction(duckdb::ScalarFunction{"search_path_canonical",
                                                 {},
                                                 duckdb::LogicalType::VARCHAR,
                                                 SearchPathCanonicalFunction});

  // version() -> text (overrides DuckDB's built-in)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "version", {}, duckdb::LogicalType::VARCHAR, VersionFunction});

  // pg_backend_pid() -> int4 (this connection's backend PID)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_backend_pid", {}, duckdb::LogicalType::INTEGER, PgBackendPidFunction});

  // pg_cancel_backend(pid) / pg_terminate_backend(pid) -> bool: cancel another
  // backend's query by pid (terminate degrades to cancel + a warning).
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_cancel_backend",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::BOOLEAN,
                                                 PgCancelBackendFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_terminate_backend",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::BOOLEAN,
                                                 PgTerminateBackendFunction});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.SetVarArgs(duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.SetVarArgs(duckdb::LogicalType::ANY);
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
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(state.GetContext(),
                                     static_cast<uint64_t>(oid), "main");
        });
    }});

  // pg_relation_size(regclass, text)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS(), duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t oid, duckdb::string_t fork) -> int64_t {
          std::string_view f{fork.GetData(), fork.GetSize()};
          return GetRelationForkSize(state.GetContext(),
                                     static_cast<uint64_t>(oid), f);
        });
    }});

  // pg_table_size(regclass)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_table_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(state.GetContext(),
                                     static_cast<uint64_t>(oid), "main", true);
        });
    }});

  // pg_total_relation_size(regclass)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_total_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationTotalSize(state.GetContext(),
                                      static_cast<uint64_t>(oid));
        });
    }});

  // pg_indexes_size(regclass)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_indexes_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetTableIndexesSize(state.GetContext(),
                                     static_cast<uint64_t>(oid));
        });
    }});

  // Stub functions that throw "not supported"
  auto not_supported = [](duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Function is not supported in SereneDB"));
  };
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_current_xact_id", {}, pg::XID8(), not_supported});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_xact_status",
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
    duckdb::CreateScalarFunctionInfo info{std::move(format_type_fn)};
    info.SetSchema("pg_catalog");
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  // pg_database_size(text) and pg_database_size(bigint/oid)
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_database_size",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgDatabaseSizeNameFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_database_size",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgDatabaseSizeOidFunction});

  // pg_schema_size(text) and pg_schema_size(oid) -- non-standard helper.
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_schema_size",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgSchemaSizeNameFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_schema_size",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgSchemaSizeOidFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_user", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  // current_role is same as current_user in postgres
  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_role", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "session_user", {}, duckdb::LogicalType::VARCHAR, SessionUserFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "has_table_privilege",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    HasTablePrivilege3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
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
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOidName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {duckdb::LogicalType::VARCHAR, pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeNameOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  const auto register_object_priv = [&loader]<duckdb::CatalogType kType>(
                                      std::string_view name) {
    loader.RegisterFunction(duckdb::ScalarFunction{
      duckdb::Identifier{name},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasObjectPrivilege3Function<kType>});
    loader.RegisterFunction(duckdb::ScalarFunction{
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
      loader.RegisterFunction(func);
    }
    {
      duckdb::ScalarFunction func{
        duckdb::Identifier{name},
        {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilegeOidName3Function<kType>};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      loader.RegisterFunction(func);
    }
    {
      duckdb::ScalarFunction func{
        duckdb::Identifier{name},
        {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilegeOid3Function<kType>};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      loader.RegisterFunction(func);
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

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameName3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameOid3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidName3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidOid3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleName2Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
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
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidAttnum3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::SMALLINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), pg::REGCLASS(), duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidOidAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeOid2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
