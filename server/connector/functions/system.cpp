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
#include "catalog/table.h"
#include "catalog/store/store.h"
#include "catalog/secondary_index.h"
#include <duckdb/main/database.hpp>
#include <duckdb/main/connection.hpp>

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>

#include "basics/build.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {
namespace {

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
      return duckdb::StringVector::AddString(
        result, pg::RegtypeOut(static_cast<uint64_t>(type_oid)));
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
  duckdb::Connection conn(*context.db);
  auto store_name = catalog::StoreTableName(
    database->GetName(), schema->GetName(), table->GetName());
  auto result = conn.Query(
    absl::StrCat("SELECT count(*) FROM \"", catalog::kStoreDatabaseName,
                 "\".main.\"", store_name, "\""));
  if (result->HasError()) {
    return 0;
  }
  auto chunk = result->Fetch();
  if (!chunk || chunk->size() == 0) {
    return 0;
  }
  return chunk->GetValue(0, 0).GetValue<int64_t>();
}

// Helper: get fork size for a relation OID.
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
      auto shard = snapshot.GetIndexShard(rel->GetId());
      SDB_ASSERT(shard);
      SDB_ASSERT(shard->GetType() == catalog::ObjectType::InvertedIndexShard);
      return static_cast<int64_t>(
        basics::downCast<search::InvertedIndexShard>(shard.get())
          ->GetStats()
          .indexSize);
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
      return static_cast<int64_t>(
        GetServerEngine().GetDatabaseSize(*snapshot, database->GetId()));
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
      return static_cast<int64_t>(
        GetServerEngine().GetDatabaseSize(*snapshot, database->GetId()));
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
      return static_cast<int64_t>(GetServerEngine().GetSchemaSize(
        *snapshot, database_id, std::string{schema_name}));
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
      return static_cast<int64_t>(GetServerEngine().GetSchemaSize(
        *snapshot, schema->GetParentId(), schema->GetName()));
    });
}

}  // namespace

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  // PG types are registered via duckdb_external_types in duckdb_engine.cpp

  // pg_typeof(any) -> regtype
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

  // --- pg_*_size functions ---
  // --- pg_*_size functions: all take regclass (implicit cast from text) ---

  // pg_relation_size(regclass)
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
          return GetRelationForkSize(state.GetContext(), *snap, static_cast<uint64_t>(oid), "main");
        });
    }});

  // pg_relation_size(regclass, text)
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
          return GetRelationForkSize(state.GetContext(), *snap, static_cast<uint64_t>(oid), f);
        });
    }});

  // pg_table_size(regclass)
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
          return GetRelationForkSize(state.GetContext(), *snap, static_cast<uint64_t>(oid), "main",
                                     true);
        });
    }});

  // pg_total_relation_size(regclass)
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
          return GetRelationForkSize(state.GetContext(), *snap, static_cast<uint64_t>(oid), "main");
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

  // current_role is same as current_user in postgres
  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_role", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});
}

}  // namespace sdb::connector
