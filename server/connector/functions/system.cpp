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
#include <optional>

#include "auth/acl.h"
#include "auth/privilege.h"
#include "basics/build.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/secondary_index.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
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

[[noreturn]] void ThrowInvalidPrivilege(const basics::Exception& e) {
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
    throw duckdb::InvalidInputException(
      "unrecognized configuration parameter \"%s\"", key);
  }
}

void CurrentUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  result.Reference(conn_ctx.user(), duckdb::count_t(args.size()));
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

// format_type(oid, typmod) -> text
// TODO(Pasha) Account typmod?
// Keyed on the oid only (UnaryExecutor): psql calls format_type(oid, NULL),
// and a BinaryExecutor would NULL-propagate the NULL typmod and drop the name.
void FormatTypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  auto snapshot =
    GetSereneDBContext(state.GetContext()).EnsureCatalogSnapshot();
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](int64_t type_oid) -> duckdb::string_t {
      // User-defined types (enum, composite, ...) are catalog objects; resolve
      // their real name there. Built-ins aren't catalog objects, so fall back
      // to the static oid->name map (RegtypeOut, which otherwise renders an
      // unknown oid as its bare number).
      if (auto object =
            snapshot->GetObject(ObjectId{static_cast<uint64_t>(type_oid)})) {
        return duckdb::StringVector::AddString(result, object->GetName());
      }
      return duckdb::StringVector::AddString(result, pg::RegtypeOut(type_oid));
    });
}

// --- Size functions ---
// Ported from server/pg/functions/size.cpp

// Store-table row count as the size proxy: the native engine keeps no
// cheap per-table byte size (one shared file), and PG callers mostly test
// emptiness. TODO(M2): commit-time byte accounting.
int64_t StoreTableSizeProxy(duckdb::ClientContext& context,
                            const catalog::Snapshot& snapshot,
                            const catalog::Object& rel) {
  auto table = snapshot.GetObject<catalog::Table>(rel.GetId());
  if (!table || table->GetEngine() != catalog::TableEngine::Transactional ||
      table->Tombstoned()) {
    return 0;
  }
  auto schema = snapshot.GetObject<catalog::Schema>(table->GetParentId());
  if (!schema) {
    return 0;
  }
  auto database = snapshot.GetDatabase(schema->GetParentId());
  if (!database) {
    return 0;
  }
  auto store_name = catalog::StoreTableName(
    database->GetName(), schema->GetName(), table->GetName());
  duckdb::EntryLookupInfo lookup(duckdb::CatalogType::TABLE_ENTRY, store_name);
  auto entry = duckdb::Catalog::GetEntry(
    context, std::string{catalog::kStoreDatabaseName}, "main", lookup,
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!entry) {
    return 0;
  }
  return static_cast<int64_t>(
    entry->Cast<duckdb::TableCatalogEntry>().GetStorage().GetTotalRows());
}

// Helper: get fork size for a relation OID.
int64_t StoreSchemaSize(duckdb::ClientContext& context,
                        const catalog::Snapshot& snapshot, ObjectId database_id,
                        std::string_view schema_name) {
  int64_t total = 0;
  for (auto& rel : snapshot.GetRelations(database_id, schema_name)) {
    if (rel->GetType() != catalog::ObjectType::Table) {
      continue;
    }
    total += StoreTableSizeProxy(context, snapshot, *rel);
  }
  return total;
}

int64_t StoreDatabaseSize(duckdb::ClientContext& context,
                          const catalog::Snapshot& snapshot,
                          ObjectId database_id) {
  int64_t total = 0;
  for (auto& schema : snapshot.GetSchemas(database_id)) {
    total += StoreSchemaSize(context, snapshot, database_id, schema->GetName());
  }
  return total;
}

// Ported from server/pg/functions/size.cpp GetRelationForkSize.
int64_t GetRelationForkSize(duckdb::ClientContext& context,
                            const catalog::Snapshot& snapshot, uint64_t oid,
                            std::string_view fork, bool table_only = false) {
  auto rel = snapshot.GetObject(ObjectId{oid});
  if (!rel) {
    throw duckdb::CatalogException("relation with OID %llu does not exist",
                                   oid);
  }
  if (table_only && rel->GetType() != catalog::ObjectType::Table) {
    throw duckdb::CatalogException("\"%s\" is not a table",
                                   std::string{rel->GetName()});
  }
  if (fork != "main") {
    return 0;
  }
  switch (rel->GetType()) {
    case catalog::ObjectType::Table:
      return StoreTableSizeProxy(context, snapshot, *rel);
    case catalog::ObjectType::SecondaryIndex: {
      // Native ART indexes live inside the store file; report the table
      // row count as the proxy.
      auto index = snapshot.GetObject<catalog::SecondaryIndex>(rel->GetId());
      if (!index) {
        return 0;
      }
      auto table = snapshot.GetObject(index->GetRelationId());
      return table ? StoreTableSizeProxy(context, snapshot, *table) : 0;
    }
    case catalog::ObjectType::InvertedIndex: {
      auto storage =
        basics::downCast<const catalog::InvertedIndex>(*rel).GetData();
      if (!storage) {
        return 0;
      }
      return static_cast<int64_t>(storage->GetStats().indexSize);
    }
    default:
      return 0;
  }
}

// pg_database_size(name) -> bigint
void PgDatabaseSizeNameFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view db_name{input.GetData(), input.GetSize()};
      auto database = snapshot->GetDatabase(db_name);
      if (!database) {
        throw duckdb::CatalogException("database \"%s\" does not exist",
                                       std::string{db_name});
      }
      return StoreDatabaseSize(context, *snapshot, database->GetId());
    });
}

// pg_database_size(oid) -> bigint
void PgDatabaseSizeOidFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      // Try our catalog by OID first
      auto database =
        snapshot->GetDatabase(ObjectId{static_cast<uint64_t>(oid)});
      if (!database) {
        // DuckDB's pg_database OIDs don't match ours -- fall back to
        // current database (covers the common pg_database_size(d.oid)
        // WHERE d.datname = current_database() pattern)
        database = snapshot->GetDatabase(conn_ctx.GetDatabaseId());
      }
      if (!database) {
        throw duckdb::CatalogException("database with OID %lld does not exist",
                                       oid);
      }
      return StoreDatabaseSize(context, *snapshot, database->GetId());
    });
}

// pg_schema_size(name) -> bigint -- non-standard, included for SereneDB tests.
void PgSchemaSizeNameFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto database_id = conn_ctx.GetDatabaseId();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view schema_name{input.GetData(), input.GetSize()};
      auto schema = snapshot->GetSchema(database_id, schema_name);
      if (!schema) {
        throw duckdb::CatalogException("schema \"%s\" does not exist",
                                       std::string{schema_name});
      }
      return StoreSchemaSize(context, *snapshot, database_id, schema_name);
    });
}

// pg_schema_size(oid) -> bigint
void PgSchemaSizeOidFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto schema = snapshot->GetObject<catalog::Schema>(
        ObjectId{static_cast<uint64_t>(oid)});
      if (!schema) {
        throw duckdb::CatalogException("schema with OID %lld does not exist",
                                       oid);
      }
      return StoreSchemaSize(context, *snapshot, schema->GetParentId(),
                             schema->GetName());
    });
}

struct PrivCheckModes {
  catalog::AclMode privs = catalog::AclMode::NoRights;
  catalog::AclMode grant_options = catalog::AclMode::NoRights;
};

PrivCheckModes ParsePrivCheckText(std::string_view priv_text,
                                  catalog::ObjectType type) {
  constexpr std::string_view kSuffix = " WITH GRANT OPTION";
  PrivCheckModes out;
  for (std::string_view tok :
       absl::StrSplit(priv_text, ',', absl::SkipEmpty())) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (stripped.size() > kSuffix.size() &&
        absl::EqualsIgnoreCase(
          stripped.substr(stripped.size() - kSuffix.size()), kSuffix)) {
      out.grant_options |= auth::ParseAclMode(
        stripped.substr(0, stripped.size() - kSuffix.size()), type);
    } else {
      out.privs |= auth::ParseAclMode(stripped, type);
    }
  }
  return out;
}

bool HasAnyObjectPrivilegeText(const catalog::Snapshot& snapshot,
                               ObjectId role_id, const catalog::Object& object,
                               catalog::ObjectType type,
                               std::string_view priv_text) {
  const auto modes = ParsePrivCheckText(priv_text, type);
  if (modes.privs != catalog::AclMode::NoRights &&
      auth::HasAnyPrivilege(snapshot, role_id, object, modes.privs)) {
    return true;
  }
  if (modes.grant_options == catalog::AclMode::NoRights) {
    return false;
  }
  if (auto role = snapshot.GetObject<catalog::Role>(role_id);
      role && role->IsSuperuser()) {
    return true;
  }
  const auto roles = auth::ComputeEffectiveRoles(snapshot, role_id);
  const auto owner =
    object.GetOwner().isSet() ? object.GetOwner() : id::kRootUser;
  if (roles.contains(owner)) {
    return true;
  }
  catalog::AclMode held = catalog::AclMode::NoRights;
  for (const auto& item : object.GetAcl()) {
    if (item.grantee == catalog::kPublicGrantee ||
        roles.contains(item.grantee)) {
      held |= item.grant_option;
    }
  }
  return (held & modes.grant_options) != catalog::AclMode::NoRights;
}

bool HasAnyTablePrivilegeText(const catalog::Snapshot& snapshot,
                              ObjectId role_id, const catalog::Object& table,
                              std::string_view priv_text) {
  return HasAnyObjectPrivilegeText(snapshot, role_id, table,
                                   catalog::ObjectType::Table, priv_text);
}

std::optional<ObjectId> ResolveRoleOrPublic(const catalog::Snapshot& snap,
                                            std::string_view role_name) {
  if (absl::EqualsIgnoreCase(role_name, StaticStrings::kPublic)) {
    return catalog::kPublicGrantee;
  }
  if (auto role = snap.GetRole(role_name)) {
    return role->GetId();
  }
  return std::nullopt;
}

std::shared_ptr<const catalog::Snapshot> GlobalSnapshot() {
  return catalog::GetCatalog().GetCatalogSnapshot();
}

bool HasTablePrivilegeImpl(ConnectionContext& conn_ctx,
                           std::string_view role_name,
                           std::string_view table_name,
                           std::string_view priv_text) {
  auto snapshot = GlobalSnapshot();
  auto role_id = ResolveRoleOrPublic(*snapshot, role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  const auto current_schema = conn_ctx.GetCurrentSchema();
  const auto name = pg::ParseObjectName(table_name, current_schema);
  auto table =
    snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                       name.schema, name.relation);
  if (!table) {
    ThrowRelationNotFound(name.relation);
  }
  try {
    return HasAnyObjectPrivilegeText(*snapshot, *role_id, *table,
                                     catalog::ObjectType::Table, priv_text);
  } catch (const basics::Exception& e) {
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

bool HasTablePrivilegeByOidImpl(const catalog::Snapshot& snapshot,
                                ObjectId role_id, ObjectId table_id,
                                std::string_view priv_text, bool& is_null) {
  is_null = false;
  auto table = snapshot.GetObject<catalog::Table>(table_id);
  if (!table) {
    is_null = true;
    return false;
  }
  try {
    return HasAnyObjectPrivilegeText(snapshot, role_id, *table,
                                     catalog::ObjectType::Table, priv_text);
  } catch (const basics::Exception& e) {
    ThrowInvalidPrivilege(e);
  }
}

void HasTablePrivilegeOid2Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  duckdb::UnifiedVectorFormat tdata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), tdata);
  args.data[1].ToUnifiedFormat(args.size(), pdata);
  const auto* toid = duckdb::UnifiedVectorFormat::GetData<int64_t>(tdata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ti = tdata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!tdata.validity.RowIsValid(ti) || !pdata.validity.RowIsValid(pi) ||
        !current) {
      validity.SetInvalid(i);
      continue;
    }
    bool is_null = false;
    bool r = HasTablePrivilegeByOidImpl(
      *snapshot, current->GetId(), ObjectId{static_cast<uint64_t>(toid[ti])},
      {priv[pi].GetData(), priv[pi].GetSize()}, is_null);
    if (is_null) {
      validity.SetInvalid(i);
    } else {
      out[i] = r;
    }
  }
}

void HasTablePrivilegeOid3Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::UnifiedVectorFormat rdata, tdata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), rdata);
  args.data[1].ToUnifiedFormat(args.size(), tdata);
  args.data[2].ToUnifiedFormat(args.size(), pdata);
  const auto* roid = duckdb::UnifiedVectorFormat::GetData<int64_t>(rdata);
  const auto* toid = duckdb::UnifiedVectorFormat::GetData<int64_t>(tdata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ri = rdata.sel->get_index(i);
    auto ti = tdata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!rdata.validity.RowIsValid(ri) || !tdata.validity.RowIsValid(ti) ||
        !pdata.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    bool is_null = false;
    bool r = HasTablePrivilegeByOidImpl(
      *snapshot, ObjectId{static_cast<uint64_t>(roid[ri])},
      ObjectId{static_cast<uint64_t>(toid[ti])},
      {priv[pi].GetData(), priv[pi].GetSize()}, is_null);
    if (is_null) {
      validity.SetInvalid(i);
    } else {
      out[i] = r;
    }
  }
}

void HasTablePrivilegeOidName3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::UnifiedVectorFormat rdata, tdata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), rdata);
  args.data[1].ToUnifiedFormat(args.size(), tdata);
  args.data[2].ToUnifiedFormat(args.size(), pdata);
  const auto* roid = duckdb::UnifiedVectorFormat::GetData<int64_t>(rdata);
  const auto* tname =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(tdata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ri = rdata.sel->get_index(i);
    auto ti = tdata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!rdata.validity.RowIsValid(ri) || !tdata.validity.RowIsValid(ti) ||
        !pdata.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    const auto name = pg::ParseObjectName(
      {tname[ti].GetData(), tname[ti].GetSize()}, current_schema);
    auto table =
      snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                         name.schema, name.relation);
    if (!table) {
      ThrowRelationNotFound(name.relation);
    }
    try {
      out[i] = HasAnyObjectPrivilegeText(
        *snapshot, ObjectId{static_cast<uint64_t>(roid[ri])}, *table,
        catalog::ObjectType::Table, {priv[pi].GetData(), priv[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasTablePrivilegeNameOid3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::UnifiedVectorFormat rdata, tdata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), rdata);
  args.data[1].ToUnifiedFormat(args.size(), tdata);
  args.data[2].ToUnifiedFormat(args.size(), pdata);
  const auto* rname =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(rdata);
  const auto* toid = duckdb::UnifiedVectorFormat::GetData<int64_t>(tdata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ri = rdata.sel->get_index(i);
    auto ti = tdata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!rdata.validity.RowIsValid(ri) || !tdata.validity.RowIsValid(ti) ||
        !pdata.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    auto role_id = ResolveRoleOrPublic(
      *snapshot, {rname[ri].GetData(), rname[ri].GetSize()});
    if (!role_id) {
      ThrowRoleNotFound({rname[ri].GetData(), rname[ri].GetSize()});
    }
    bool is_null = false;
    bool r = HasTablePrivilegeByOidImpl(
      *snapshot, *role_id, ObjectId{static_cast<uint64_t>(toid[ti])},
      {priv[pi].GetData(), priv[pi].GetSize()}, is_null);
    if (is_null) {
      validity.SetInvalid(i);
    } else {
      out[i] = r;
    }
  }
}

const char* ObjectClassWord(catalog::ObjectType type) {
  switch (type) {
    case catalog::ObjectType::Schema:
      return "schema";
    case catalog::ObjectType::Sequence:
      return "relation";
    case catalog::ObjectType::PgSqlFunction:
      return "function";
    case catalog::ObjectType::Database:
      return "database";
    default:
      return "object";
  }
}

std::shared_ptr<catalog::Object> ResolveObjectByName(
  const catalog::Snapshot& snapshot, ConnectionContext& conn_ctx,
  catalog::ObjectType type, std::string_view obj_name) {
  const auto db_id = conn_ctx.GetDatabaseId();
  if (type == catalog::ObjectType::Database) {
    return snapshot.GetDatabase(obj_name);
  }
  if (type == catalog::ObjectType::Schema) {
    return snapshot.GetSchema(db_id, obj_name);
  }
  const auto current_schema = conn_ctx.GetCurrentSchema();
  if (type == catalog::ObjectType::PgSqlFunction) {
    const auto bare = obj_name.substr(0, obj_name.find('('));
    const auto name =
      pg::ParseObjectName(absl::StripAsciiWhitespace(bare), current_schema);
    return snapshot.GetFunction(catalog::NoAccessCheck(), db_id, name.schema,
                                name.relation);
  }
  if (type == catalog::ObjectType::PgSqlType) {
    const auto name = pg::ParseObjectName(obj_name, current_schema);
    return snapshot.GetType(catalog::NoAccessCheck(), db_id, name.schema,
                            name.relation);
  }
  const auto name = pg::ParseObjectName(obj_name, current_schema);
  auto schema = snapshot.GetSchema(db_id, name.schema);
  if (!schema) {
    return nullptr;
  }
  return snapshot.GetSequence(catalog::NoAccessCheck(), db_id, schema->GetId(),
                              name.relation);
}

std::shared_ptr<catalog::Object> ResolveObjectByOid(
  const catalog::Snapshot& snapshot, catalog::ObjectType type, ObjectId oid) {
  auto obj = snapshot.GetObject(oid);
  if (!obj || obj->GetType() != type) {
    return nullptr;
  }
  return obj;
}

bool HasObjectPrivilegeImpl(ConnectionContext& conn_ctx,
                            catalog::ObjectType type,
                            std::string_view role_name,
                            std::string_view obj_name,
                            std::string_view priv_text) {
  auto snapshot = GlobalSnapshot();
  auto role_id = ResolveRoleOrPublic(*snapshot, role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  auto object = ResolveObjectByName(*snapshot, conn_ctx, type, obj_name);
  if (!object) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG(ObjectClassWord(type), " \"", obj_name, "\" does not exist"));
  }
  try {
    return HasAnyObjectPrivilegeText(*snapshot, *role_id, *object, type,
                                     priv_text);
  } catch (const basics::Exception& e) {
    ThrowInvalidPrivilege(e);
  }
}

template<catalog::ObjectType kType>
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

template<catalog::ObjectType kType>
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

bool HasObjectPrivilegeByOidImpl(const catalog::Snapshot& snapshot,
                                 catalog::ObjectType type, ObjectId role_id,
                                 ObjectId obj_id, std::string_view priv_text,
                                 bool& is_null) {
  is_null = false;
  auto object = ResolveObjectByOid(snapshot, type, obj_id);
  if (!object) {
    is_null = true;
    return false;
  }
  try {
    return HasAnyObjectPrivilegeText(snapshot, role_id, *object, type,
                                     priv_text);
  } catch (const basics::Exception& e) {
    ThrowInvalidPrivilege(e);
  }
}

template<catalog::ObjectType kType>
void HasObjectPrivilegeOid2Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  duckdb::UnifiedVectorFormat odata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), odata);
  args.data[1].ToUnifiedFormat(args.size(), pdata);
  const auto* ooid = duckdb::UnifiedVectorFormat::GetData<int64_t>(odata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto oi = odata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!odata.validity.RowIsValid(oi) || !pdata.validity.RowIsValid(pi) ||
        !current) {
      validity.SetInvalid(i);
      continue;
    }
    bool is_null = false;
    bool r = HasObjectPrivilegeByOidImpl(
      *snapshot, kType, current->GetId(),
      ObjectId{static_cast<uint64_t>(ooid[oi])},
      {priv[pi].GetData(), priv[pi].GetSize()}, is_null);
    if (is_null) {
      validity.SetInvalid(i);
    } else {
      out[i] = r;
    }
  }
}

template<catalog::ObjectType kType>
void HasObjectPrivilegeOid3Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::UnifiedVectorFormat rdata, odata, pdata;
  args.data[0].ToUnifiedFormat(args.size(), rdata);
  args.data[1].ToUnifiedFormat(args.size(), odata);
  args.data[2].ToUnifiedFormat(args.size(), pdata);
  const auto* roid = duckdb::UnifiedVectorFormat::GetData<int64_t>(rdata);
  const auto* ooid = duckdb::UnifiedVectorFormat::GetData<int64_t>(odata);
  const auto* priv =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ri = rdata.sel->get_index(i);
    auto oi = odata.sel->get_index(i);
    auto pi = pdata.sel->get_index(i);
    if (!rdata.validity.RowIsValid(ri) || !odata.validity.RowIsValid(oi) ||
        !pdata.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    bool is_null = false;
    bool r = HasObjectPrivilegeByOidImpl(
      *snapshot, kType, ObjectId{static_cast<uint64_t>(roid[ri])},
      ObjectId{static_cast<uint64_t>(ooid[oi])},
      {priv[pi].GetData(), priv[pi].GetSize()}, is_null);
    if (is_null) {
      validity.SetInvalid(i);
    } else {
      out[i] = r;
    }
  }
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

bool PgHasRoleImpl(const catalog::Snapshot& snapshot, ObjectId member,
                   ObjectId target, std::string_view priv_text) {
  const auto mask = ParseRolePrivs(priv_text);
  auto role = snapshot.GetObject<catalog::Role>(member);
  if (role && role->IsSuperuser()) {
    return mask.usage || mask.member || mask.set || mask.admin;
  }
  if (member == target) {
    return mask.usage || mask.member || mask.set;
  }
  bool ok = false;
  if (mask.usage) {
    ok = ok || auth::ComputeEffectiveRoles(snapshot, member).contains(target);
  }
  if (mask.member) {
    ok =
      ok || auth::ComputeMembershipClosure(snapshot, member).contains(target);
  }
  if (mask.set) {
    ok = ok || auth::ComputeSetRoleClosure(snapshot, member).contains(target);
  }
  if (mask.admin) {
    ok = ok || auth::HasAdminOption(snapshot, member, target);
  }
  return ok;
}

ObjectId RoleIdByName(const catalog::Snapshot& snapshot,
                      std::string_view name) {
  auto role = snapshot.GetRole(name);
  if (!role) {
    ThrowRoleNotFound(name);
  }
  return role->GetId();
}

void PgHasRoleNameName3Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, duckdb::string_t role,
        duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *snapshot, RoleIdByName(*snapshot, {user.GetData(), user.GetSize()}),
        RoleIdByName(*snapshot, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleNameOid3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *snapshot, RoleIdByName(*snapshot, {user.GetData(), user.GetSize()}),
        ObjectId{static_cast<uint64_t>(role)},
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidName3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::TernaryExecutor::Execute<int64_t, duckdb::string_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *snapshot, ObjectId{static_cast<uint64_t>(user)},
        RoleIdByName(*snapshot, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidOid3Function(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto snapshot = GlobalSnapshot();
  duckdb::TernaryExecutor::Execute<int64_t, int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*snapshot, ObjectId{static_cast<uint64_t>(user)},
                           ObjectId{static_cast<uint64_t>(role)},
                           {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleName2Function(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  const ObjectId member = RoleIdByName(*snapshot, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *snapshot, member,
        RoleIdByName(*snapshot, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOid2Function(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  const ObjectId member = RoleIdByName(*snapshot, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*snapshot, member,
                           ObjectId{static_cast<uint64_t>(role)},
                           {priv.GetData(), priv.GetSize()});
    });
}

bool ColumnExists(const catalog::Table& table, std::string_view col_name) {
  for (const auto& c : table.Columns()) {
    if (c.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    if (c.GetName() == col_name) {
      return true;
    }
  }
  return false;
}

bool AttnumExists(const catalog::Table& table, int64_t attnum) {
  if (attnum < 1) {
    return false;
  }
  int64_t n = 0;
  for (const auto& c : table.Columns()) {
    if (c.GetId() != catalog::Column::kGeneratedPKId) {
      ++n;
    }
  }
  return attnum <= n;
}

bool HasColumnPrivByName(const catalog::Snapshot& snapshot, ObjectId role_id,
                         const catalog::Table& table, std::string_view col,
                         std::string_view priv) {
  if (!ColumnExists(table, col)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", col, "\" of relation \"",
                            table.GetName(), "\" does not exist"));
  }
  return HasAnyTablePrivilegeText(snapshot, role_id, table, priv);
}

void HasColumnPrivilegeNameName4Function(duckdb::DataChunk& args,
                                         duckdb::ExpressionState& state,
                                         duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::UnifiedVectorFormat ud, td, cd, pd;
  args.data[0].ToUnifiedFormat(args.size(), ud);
  args.data[1].ToUnifiedFormat(args.size(), td);
  args.data[2].ToUnifiedFormat(args.size(), cd);
  args.data[3].ToUnifiedFormat(args.size(), pd);
  const auto* u = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(ud);
  const auto* t = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(td);
  const auto* c = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(cd);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ui = ud.sel->get_index(i), ti = td.sel->get_index(i);
    auto ci = cd.sel->get_index(i), pi = pd.sel->get_index(i);
    if (!ud.validity.RowIsValid(ui) || !td.validity.RowIsValid(ti) ||
        !cd.validity.RowIsValid(ci) || !pd.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    auto role = snapshot->GetRole({u[ui].GetData(), u[ui].GetSize()});
    if (!role) {
      ThrowRoleNotFound({u[ui].GetData(), u[ui].GetSize()});
    }
    const auto name =
      pg::ParseObjectName({t[ti].GetData(), t[ti].GetSize()}, current_schema);
    auto table =
      snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                         name.schema, name.relation);
    if (!table) {
      ThrowRelationNotFound(name.relation);
    }
    try {
      out[i] = HasColumnPrivByName(*snapshot, role->GetId(), *table,
                                   {c[ci].GetData(), c[ci].GetSize()},
                                   {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                     duckdb::ExpressionState& state,
                                     duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::UnifiedVectorFormat td, cd, pd;
  args.data[0].ToUnifiedFormat(args.size(), td);
  args.data[1].ToUnifiedFormat(args.size(), cd);
  args.data[2].ToUnifiedFormat(args.size(), pd);
  const auto* t = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(td);
  const auto* c = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(cd);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ti = td.sel->get_index(i), ci = cd.sel->get_index(i);
    auto pi = pd.sel->get_index(i);
    if (!td.validity.RowIsValid(ti) || !cd.validity.RowIsValid(ci) ||
        !pd.validity.RowIsValid(pi) || !current) {
      validity.SetInvalid(i);
      continue;
    }
    const auto name =
      pg::ParseObjectName({t[ti].GetData(), t[ti].GetSize()}, current_schema);
    auto table =
      snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                         name.schema, name.relation);
    if (!table) {
      ThrowRelationNotFound(name.relation);
    }
    try {
      out[i] = HasColumnPrivByName(*snapshot, current->GetId(), *table,
                                   {c[ci].GetData(), c[ci].GetSize()},
                                   {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasColumnPrivilegeOidAttnum3Function(duckdb::DataChunk& args,
                                          duckdb::ExpressionState& state,
                                          duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  duckdb::UnifiedVectorFormat td, cd, pd;
  args.data[0].ToUnifiedFormat(args.size(), td);
  args.data[1].ToUnifiedFormat(args.size(), cd);
  args.data[2].ToUnifiedFormat(args.size(), pd);
  const auto* toid = duckdb::UnifiedVectorFormat::GetData<int64_t>(td);
  const auto* attnum = duckdb::UnifiedVectorFormat::GetData<int32_t>(cd);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ti = td.sel->get_index(i), ci = cd.sel->get_index(i);
    auto pi = pd.sel->get_index(i);
    if (!td.validity.RowIsValid(ti) || !cd.validity.RowIsValid(ci) ||
        !pd.validity.RowIsValid(pi) || !current) {
      validity.SetInvalid(i);
      continue;
    }
    auto table = snapshot->GetObject<catalog::Table>(
      ObjectId{static_cast<uint64_t>(toid[ti])});
    if (!table || !AttnumExists(*table, attnum[ci])) {
      validity.SetInvalid(i);
      continue;
    }
    try {
      out[i] = HasAnyTablePrivilegeText(*snapshot, current->GetId(), *table,
                                        {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasAnyColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::UnifiedVectorFormat ud, td, pd;
  args.data[0].ToUnifiedFormat(args.size(), ud);
  args.data[1].ToUnifiedFormat(args.size(), td);
  args.data[2].ToUnifiedFormat(args.size(), pd);
  const auto* u = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(ud);
  const auto* t = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(td);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ui = ud.sel->get_index(i), ti = td.sel->get_index(i);
    auto pi = pd.sel->get_index(i);
    if (!ud.validity.RowIsValid(ui) || !td.validity.RowIsValid(ti) ||
        !pd.validity.RowIsValid(pi)) {
      validity.SetInvalid(i);
      continue;
    }
    auto role = snapshot->GetRole({u[ui].GetData(), u[ui].GetSize()});
    if (!role) {
      ThrowRoleNotFound({u[ui].GetData(), u[ui].GetSize()});
    }
    const auto name =
      pg::ParseObjectName({t[ti].GetData(), t[ti].GetSize()}, current_schema);
    auto table =
      snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                         name.schema, name.relation);
    if (!table) {
      ThrowRelationNotFound(name.relation);
    }
    try {
      out[i] = HasAnyTablePrivilegeText(*snapshot, role->GetId(), *table,
                                        {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasAnyColumnPrivilegeOid2Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  duckdb::UnifiedVectorFormat td, pd;
  args.data[0].ToUnifiedFormat(args.size(), td);
  args.data[1].ToUnifiedFormat(args.size(), pd);
  const auto* toid = duckdb::UnifiedVectorFormat::GetData<int64_t>(td);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ti = td.sel->get_index(i), pi = pd.sel->get_index(i);
    if (!td.validity.RowIsValid(ti) || !pd.validity.RowIsValid(pi) ||
        !current) {
      validity.SetInvalid(i);
      continue;
    }
    auto table = snapshot->GetObject<catalog::Table>(
      ObjectId{static_cast<uint64_t>(toid[ti])});
    if (!table) {
      validity.SetInvalid(i);
      continue;
    }
    try {
      out[i] = HasAnyTablePrivilegeText(*snapshot, current->GetId(), *table,
                                        {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
}

void HasAnyColumnPrivilegeName2Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto snapshot = GlobalSnapshot();
  auto current = snapshot->GetRole(conn_ctx.user());
  const auto current_schema = conn_ctx.GetCurrentSchema();
  duckdb::UnifiedVectorFormat td, pd;
  args.data[0].ToUnifiedFormat(args.size(), td);
  args.data[1].ToUnifiedFormat(args.size(), pd);
  const auto* t = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(td);
  const auto* p = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pd);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < args.size(); i++) {
    auto ti = td.sel->get_index(i), pi = pd.sel->get_index(i);
    if (!td.validity.RowIsValid(ti) || !pd.validity.RowIsValid(pi) ||
        !current) {
      validity.SetInvalid(i);
      continue;
    }
    const auto name =
      pg::ParseObjectName({t[ti].GetData(), t[ti].GetSize()}, current_schema);
    auto table =
      snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(),
                         name.schema, name.relation);
    if (!table) {
      ThrowRelationNotFound(name.relation);
    }
    try {
      out[i] = HasAnyTablePrivilegeText(*snapshot, current->GetId(), *table,
                                        {p[pi].GetData(), p[pi].GetSize()});
    } catch (const basics::Exception& e) {
      ThrowInvalidPrivilege(e);
    }
  }
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
  // version() -> text (overrides DuckDB's built-in)
  // width_bucket(operand, low, high, count) -> int
  // --- pg_*_size functions ---
  // --- pg_*_size functions: all take regclass (implicit cast from text) ---
  // pg_relation_size(regclass)
  // pg_relation_size(regclass, text)
  // pg_table_size(regclass)
  // pg_total_relation_size(regclass)
  // current_role is same as current_user in postgres
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

  loader.RegisterFunction(duckdb::ScalarFunction{
    "version", {}, duckdb::LogicalType::VARCHAR, VersionFunction});

  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.SetVarArgs(duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.SetVarArgs(duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

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
            throw duckdb::InvalidInputException("count must be greater than 0");
          }
          if (low.val >= high.val) {
            throw duckdb::InvalidInputException(
              "lower bound must be less than upper bound");
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

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(state.GetContext(), *snap,
                                     static_cast<uint64_t>(oid), "main");
        });
    }});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {pg::REGCLASS(), duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t oid, duckdb::string_t fork) -> int64_t {
          std::string_view f{fork.GetData(), fork.GetSize()};
          return GetRelationForkSize(state.GetContext(), *snap,
                                     static_cast<uint64_t>(oid), f);
        });
    }});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_table_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(state.GetContext(), *snap,
                                     static_cast<uint64_t>(oid), "main", true);
        });
    }});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_total_relation_size",
    {pg::REGCLASS()},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(state.GetContext(), *snap,
                                     static_cast<uint64_t>(oid), "main");
        });
    }});

  // pg_indexes_size(regclass)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_indexes_size",
                           {pg::REGCLASS()},
                           duckdb::LogicalType::BIGINT,
                           [](duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
                             duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
                               args.data[0], result, args.size(),
                               [](int64_t) -> int64_t { return 0; });
                           }});

  // Stub functions that throw "not supported"
  auto not_supported = [](duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
    throw duckdb::NotImplementedException(
      "Function is not supported in SereneDB");
  };
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_current_xact_id", {}, pg::XID8(), not_supported});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_xact_status",
                                                 {pg::XID8()},
                                                 duckdb::LogicalType::VARCHAR,
                                                 not_supported});

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
    info.schema = "pg_catalog";
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

  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_role", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "session_user", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

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

  const auto register_object_priv =
    [&loader]<catalog::ObjectType kType>(std::string_view name) {
      loader.RegisterFunction(duckdb::ScalarFunction{
        std::string{name},
        {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
         duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilege3Function<kType>});
      loader.RegisterFunction(duckdb::ScalarFunction{
        std::string{name},
        {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilege2Function<kType>});
      {
        duckdb::ScalarFunction func{std::string{name},
                                    {pg::OID(), duckdb::LogicalType::VARCHAR},
                                    duckdb::LogicalType::BOOLEAN,
                                    HasObjectPrivilegeOid2Function<kType>};
        func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
        loader.RegisterFunction(func);
      }
      {
        duckdb::ScalarFunction func{
          std::string{name},
          {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
          duckdb::LogicalType::BOOLEAN,
          HasObjectPrivilegeOid3Function<kType>};
        func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
        loader.RegisterFunction(func);
      }
    };
  register_object_priv.operator()<catalog::ObjectType::Schema>(
    "has_schema_privilege");
  register_object_priv.operator()<catalog::ObjectType::Sequence>(
    "has_sequence_privilege");
  register_object_priv.operator()<catalog::ObjectType::PgSqlFunction>(
    "has_function_privilege");
  register_object_priv.operator()<catalog::ObjectType::Database>(
    "has_database_privilege");
  register_object_priv.operator()<catalog::ObjectType::PgSqlType>(
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
