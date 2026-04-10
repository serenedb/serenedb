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

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>

#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/sql_collector.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

// current_setting(name, missing_ok) -> text
// Ported from server/pg/functions/system.cpp CurrentSettingMissingOkFunction.
void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto count = args.size();

  duckdb::BinaryExecutor::ExecuteWithNulls<duckdb::string_t, bool,
                                           duckdb::string_t>(
    args.data[0], args.data[1], result, count,
    [&](duckdb::string_t name, bool missing_ok, duckdb::ValidityMask& mask,
        duckdb::idx_t idx) -> duckdb::string_t {
      std::string key{name.GetData(), name.GetSize()};
      duckdb::Value value;
      if (context.TryGetCurrentSetting(key, value)) {
        auto str = value.ToString();
        return duckdb::StringVector::AddString(result, str);
      }
      if (missing_ok) {
        mask.SetInvalid(idx);
        return duckdb::string_t();
      }
      throw duckdb::InvalidInputException(
        "unrecognized configuration parameter \"%s\"", key);
    });
}

// set_config(name, value, is_local) -> text
// Ported from server/pg/functions/system.cpp SetConfigFunction.
void SetConfigFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& db_config = duckdb::DBConfig::GetConfig(*context.db);

  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, duckdb::string_t value,
        bool /*is_local*/) -> duckdb::string_t {
      std::string key{name.GetData(), name.GetSize()};
      std::string val{value.GetData(), value.GetSize()};
      // Set via TryGetSettingIndex + SetUserSetting on client config
      duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
      auto setting_index = db_config.TryGetSettingIndex(key, option);
      if (setting_index.IsValid()) {
        context.config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                                    duckdb::Value(val));
      } else {
        throw duckdb::InvalidInputException(
          "unrecognized configuration parameter \"%s\"", key);
      }
      return duckdb::StringVector::AddString(result, val);
    });
}

// num_nonnulls(...) -> int
// Ported from PG: counts non-null arguments.
void NumNonNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  auto count = args.size();
  auto result_data = duckdb::FlatVector::GetData<int32_t>(result);

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
  auto result_data = duckdb::FlatVector::GetData<int32_t>(result);

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
static void PgTypeofFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                             duckdb::Vector& result) {
  auto oid = static_cast<int64_t>(pg::Type2Oid(args.data[0].GetType()));
  result.Reference(duckdb::Value::BIGINT(oid));
}

static duckdb::unique_ptr<duckdb::Expression> BindPgTypeof(
  duckdb::FunctionBindExpressionInput& input) {
  auto oid = static_cast<int64_t>(
    pg::Type2Oid(input.children[0]->return_type));
  auto val = duckdb::Value::BIGINT(oid);
  val.Reinterpret(pg::REGTYPE());
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(val));
}

// --- Size functions ---
// Ported from server/pg/functions/size.cpp

// Helper: resolve relation name to OID via catalog snapshot.
// Ported from pg/pg_types.cpp RegclassIn.
static uint64_t ResolveRegclass(duckdb::ClientContext& context,
                                std::string_view name) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto current_schema = conn_ctx.GetCurrentSchemaFromSnapshot(snapshot);
  auto object_name = pg::ParseObjectName(name, current_schema);
  auto relation = snapshot->GetRelation(conn_ctx.GetDatabaseId(),
                                        object_name.schema,
                                        object_name.relation);
  if (relation) {
    return relation->GetId().id();
  }
  throw duckdb::CatalogException("relation \"%s\" does not exist",
                                 std::string{name});
}

// Helper: get fork size for a relation OID.
// Ported from server/pg/functions/size.cpp GetRelationForkSize.
static int64_t GetRelationForkSize(const catalog::Snapshot& snapshot,
                                   uint64_t oid, std::string_view fork,
                                   bool table_only = false) {
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
      return static_cast<int64_t>(GetServerEngine().GetTableSize(rel->GetId()));
    case catalog::ObjectType::SecondaryIndex: {
      auto shard = snapshot.GetIndexShard(rel->GetId());
      SDB_ASSERT(shard);
      return static_cast<int64_t>(
        GetServerEngine().GetTableSize(shard->GetId()));
    }
    default:
      return 0;
  }
}

// pg_relation_size(regclass) -> bigint
// pg_relation_size(text) -> bigint (implicit regclass cast)
static void PgRelationSizeFunction(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> int64_t {
      auto oid = ResolveRegclass(context, {input.GetData(), input.GetSize()});
      return GetRelationForkSize(*snapshot, oid, "main");
    });
}

// pg_relation_size(regclass, fork) -> bigint
static void PgRelationSizeForkFunction(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t input, duckdb::string_t fork) -> int64_t {
      auto oid = ResolveRegclass(context, {input.GetData(), input.GetSize()});
      std::string_view fork_name{fork.GetData(), fork.GetSize()};
      if (fork_name != "main" && fork_name != "fsm" && fork_name != "vm" &&
          fork_name != "init") {
        throw duckdb::InvalidInputException("invalid fork name");
      }
      return GetRelationForkSize(*snapshot, oid, fork_name);
    });
}

// pg_table_size(regclass) -> bigint
static void PgTableSizeFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> int64_t {
      auto oid = ResolveRegclass(context, {input.GetData(), input.GetSize()});
      return GetRelationForkSize(*snapshot, oid, "main", true);
    });
}

// pg_total_relation_size(regclass) -> bigint
// In RocksDB, identical to pg_relation_size (indexes embedded in LSM tree)
static void PgTotalRelationSizeFunction(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> int64_t {
      auto oid = ResolveRegclass(context, {input.GetData(), input.GetSize()});
      return GetRelationForkSize(*snapshot, oid, "main");
    });
}

// pg_indexes_size(regclass) -> bigint
// Always returns 0 — in RocksDB, indexes are embedded in LSM tree
static void PgIndexesSizeFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState&,
                                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [](duckdb::string_t) -> int64_t { return 0; });
}

// pg_database_size(name) -> bigint
static void PgDatabaseSizeNameFunction(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> int64_t {
      std::string_view db_name{input.GetData(), input.GetSize()};
      auto database = snapshot->GetDatabase(db_name);
      if (!database) {
        throw duckdb::CatalogException(
          "database \"%s\" does not exist", std::string{db_name});
      }
      return static_cast<int64_t>(
        GetServerEngine().GetDatabaseSize(*snapshot, database->GetId()));
    });
}

// pg_database_size(oid) -> bigint
static void PgDatabaseSizeOidFunction(duckdb::DataChunk& args,
                                      duckdb::ExpressionState& state,
                                      duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto database = snapshot->GetDatabase(ObjectId{static_cast<uint64_t>(oid)});
      if (!database) {
        throw duckdb::CatalogException(
          "database with OID %lld does not exist", oid);
      }
      return static_cast<int64_t>(
        GetServerEngine().GetDatabaseSize(*snapshot, database->GetId()));
    });
}

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  // PG types are registered via duckdb_external_types in duckdb_engine.cpp

  // pg_typeof(any) -> regtype
  {
    duckdb::ScalarFunction func{
      "pg_typeof", {duckdb::LogicalType::ANY},
      pg::REGTYPE(), PgTypeofFunction};
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    func.bind_expression = BindPgTypeof;
    loader.RegisterFunction(func);
  }

  // current_setting(name, missing_ok) -> text
  {
    duckdb::ScalarFunction func{
      "current_setting",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::VARCHAR,
      CurrentSetting2Function};
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }

  // set_config(name, value, is_local) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_config",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN},
    duckdb::LogicalType::VARCHAR,
    SetConfigFunction});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.varargs = duckdb::LogicalType::ANY;
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.varargs = duckdb::LogicalType::ANY;
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }

  // --- pg_*_size functions ---
  // pg_relation_size(text), pg_relation_size(text, fork)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgRelationSizeFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgRelationSizeForkFunction});
  // pg_relation_size(bigint/regclass), pg_relation_size(bigint, fork)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size", {duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(*snap, static_cast<uint64_t>(oid), "main");
        });
    }});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_relation_size",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t oid, duckdb::string_t fork) -> int64_t {
          std::string_view f{fork.GetData(), fork.GetSize()};
          return GetRelationForkSize(*snap, static_cast<uint64_t>(oid), f);
        });
    }});

  // pg_table_size(text), pg_table_size(bigint)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_table_size", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgTableSizeFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_table_size", {duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(*snap, static_cast<uint64_t>(oid), "main",
                                     true);
        });
    }});

  // pg_total_relation_size(text), pg_total_relation_size(bigint)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_total_relation_size", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgTotalRelationSizeFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_total_relation_size", {duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& ctx = GetSereneDBContext(state.GetContext());
      auto snap = ctx.EnsureCatalogSnapshot();
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
          return GetRelationForkSize(*snap, static_cast<uint64_t>(oid), "main");
        });
    }});

  // pg_indexes_size(text), pg_indexes_size(bigint)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_indexes_size", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgIndexesSizeFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_indexes_size", {duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::BIGINT,
    [](duckdb::DataChunk& args, duckdb::ExpressionState&,
       duckdb::Vector& result) {
      duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
        args.data[0], result, args.size(),
        [](int64_t) -> int64_t { return 0; });
    }});

  // pg_database_size(text) and pg_database_size(bigint/oid)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_database_size", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BIGINT, PgDatabaseSizeNameFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_database_size", {duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::BIGINT, PgDatabaseSizeOidFunction});
}

}  // namespace sdb::connector
