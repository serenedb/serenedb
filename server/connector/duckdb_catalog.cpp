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

#include "connector/duckdb_catalog.h"

#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/operator/order/physical_order.hpp>
#include <duckdb/execution/operator/persistent/physical_batch_insert.hpp>
#include <duckdb/execution/operator/persistent/physical_delete.hpp>
#include <duckdb/execution/operator/persistent/physical_insert.hpp>
#include <duckdb/execution/operator/persistent/physical_merge_into.hpp>
#include <duckdb/execution/operator/persistent/physical_update.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_simple.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/database_size.hpp>
#include <ranges>

#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/pk_spec.h"
#include "catalog/schema.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_truncate.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/search_table_dispatch.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

// Align catalog_type with the surviving macros: MACRO_ENTRY iff all remaining
// macros are scalar, else TABLE_MACRO_ENTRY. Prevents a mismatched catalog
// bucket (e.g. a TableMacroFunction left in a MACRO_ENTRY bucket) which breaks
// Cast<>() at lookup time.
void AlignMacroCatalogType(duckdb::CreateMacroInfo& new_info) {
  const bool all_scalar =
    !new_info.macros.empty() &&
    std::ranges::all_of(new_info.macros, [](const auto& m) {
      return m->type == duckdb::MacroType::SCALAR_MACRO;
    });
  new_info.type = all_scalar ? duckdb::CatalogType::MACRO_ENTRY
                             : duckdb::CatalogType::TABLE_MACRO_ENTRY;
}

// DROP FUNCTION name(type, ...) -- selective overload removal.
// Fetches the existing PgSqlFunction, finds the matching overload by
// parameter signature, and either removes just that overload (updating the
// stored function) or drops the whole function if it was the last one.
bool DropFunctionOverload(catalog::Catalog& catalog,
                          duckdb::ClientContext& context,
                          duckdb::DropInfo& info) {
  const auto& info_catalog =
    info.GetQualifiedName().Catalog().GetIdentifierName();
  const auto& info_schema =
    info.GetQualifiedName().Schema().GetIdentifierName();
  const auto& info_name = info.GetQualifiedName().Name().GetIdentifierName();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info_catalog);
  if (!db) {
    return false;
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(catalog::NoAccessCheck(), database_id,
                                        info_schema, info_name);
  if (!existing) {
    return false;
  }

  // Resolve UNBOUND types from the DROP statement to concrete types.
  auto binder = duckdb::Binder::CreateBinder(context);
  for (auto& t : info.func_parameters) {
    binder->BindLogicalType(t);
  }

  auto& macro_info = existing->GetInfo();
  // Find the matching overload by parameter signature.
  ssize_t match_idx = -1;
  for (size_t i = 0; i < macro_info.macros.size(); ++i) {
    auto& macro = *macro_info.macros[i];
    if (macro.types.size() != info.func_parameters.size()) {
      continue;
    }
    bool match = true;
    for (size_t j = 0; j < macro.types.size(); ++j) {
      if (macro.types[j] != info.func_parameters[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      match_idx = static_cast<ssize_t>(i);
      break;
    }
  }
  if (match_idx < 0) {
    return false;
  }

  // PG: DROP FUNCTION on a procedure (or vice versa) is an error.
  auto& matched = *macro_info.macros[match_idx];
  if (matched.is_procedure != info.is_procedure) {
    auto expect = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(info_name, "() is not a ", expect));
  }

  if (macro_info.macros.size() == 1) {
    // Last overload -- drop the whole function.
    catalog.DropFunction(catalog::ActingAs(context), info_catalog, info_schema,
                         info_name, info.cascade, /*missing_ok=*/true);
    return true;
  }

  // Remove just the matched overload and update the stored function.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      macro_info.Copy());
  new_info->macros.erase(new_info->macros.begin() + match_idx);
  AlignMacroCatalogType(*new_info);

  auto function = std::make_shared<catalog::PgSqlFunction>(
    catalog::Permissions{}, ObjectId{}, ObjectId{}, info_name,
    std::move(new_info));
  catalog.CreateFunction(catalog::ActingAs(context), database_id, info_schema,
                         std::move(function), /*replace=*/true,
                         /*if_not_exists=*/false);
  return true;
}

// DROP FUNCTION/PROCEDURE name -- drop overloads matching the drop kind.
// PG: DROP FUNCTION drops only function overloads, DROP PROCEDURE drops only
// procedure overloads. If mixed (func + proc under same name), keep the other.
bool DropFunctionByKind(duckdb::ClientContext& context,
                        catalog::Catalog& catalog,
                        const duckdb::DropInfo& info) {
  const auto& info_catalog =
    info.GetQualifiedName().Catalog().GetIdentifierName();
  const auto& info_schema =
    info.GetQualifiedName().Schema().GetIdentifierName();
  const auto& info_name = info.GetQualifiedName().Name().GetIdentifierName();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info_catalog);
  if (!db) {
    return false;
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(catalog::NoAccessCheck(), database_id,
                                        info_schema, info_name);
  if (!existing) {
    return false;
  }

  auto& macros = existing->GetInfo().macros;
  bool all_match = true;
  bool any_match = false;
  for (auto& m : macros) {
    if (m->is_procedure == info.is_procedure) {
      any_match = true;
    } else {
      all_match = false;
    }
  }
  if (!any_match) {
    auto kind = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("could not find a ", kind, " named \"", info_name, "\""));
  }
  if (all_match) {
    catalog.DropFunction(catalog::ActingAs(context), info_catalog, info_schema,
                         info_name, info.cascade, /*missing_ok=*/true);
    return true;
  }
  // Mixed: remove only matching overloads, keep the rest.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      existing->GetInfo().Copy());
  std::erase_if(new_info->macros, [&](const auto& m) {
    return m->is_procedure == info.is_procedure;
  });
  AlignMacroCatalogType(*new_info);

  auto function = std::make_shared<catalog::PgSqlFunction>(
    catalog::Permissions{}, ObjectId{}, ObjectId{}, info_name,
    std::move(new_info));
  catalog.CreateFunction(catalog::ActingAs(context), database_id, info_schema,
                         std::move(function), /*replace=*/true,
                         /*if_not_exists=*/false);
  return true;
}

}  // namespace

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info) {
  auto& catalog = catalog::GetCatalog();
  const auto& info_catalog =
    info.GetQualifiedName().Catalog().GetIdentifierName();
  const auto& info_schema =
    info.GetQualifiedName().Schema().GetIdentifierName();
  const auto& info_name = info.GetQualifiedName().Name().GetIdentifierName();
  const bool missing_ok =
    info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;

  bool dropped = false;
  switch (info.type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      dropped =
        catalog.DropTable(catalog::ActingAs(context), info_catalog, info_schema,
                          info_name, info.cascade, missing_ok);
      break;
    case INDEX_ENTRY:
      dropped =
        catalog.DropIndex(catalog::ActingAs(context), info_catalog, info_schema,
                          info_name, info.cascade, missing_ok);
      break;
    case VIEW_ENTRY:
      dropped =
        catalog.DropView(catalog::ActingAs(context), info_catalog, info_schema,
                         info_name, info.cascade, missing_ok);
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      dropped = info.has_func_args
                  ? DropFunctionOverload(catalog, context, info)
                  : DropFunctionByKind(context, catalog, info);
      if (!dropped && !missing_ok) {
        auto kind = info.is_procedure ? "procedure" : "function";
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a ", kind, " named \"", info_name, "\""));
      }
      break;
    case TYPE_ENTRY:
      dropped =
        catalog.DropType(catalog::ActingAs(context), info_catalog, info_schema,
                         info_name, info.cascade, missing_ok);
      break;
    case SEQUENCE_ENTRY:
      dropped =
        catalog.DropSequence(catalog::ActingAs(context), info_catalog,
                             info_schema, info_name, info.cascade, missing_ok);
      break;
    case SCHEMA_ENTRY:
      if (info_name == StaticStrings::kPgCatalogSchema ||
          info_name == StaticStrings::kInformationSchema) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
          ERR_MSG("cannot drop schema ", info_name,
                  " because it is required by the database system"));
      } else {
        dropped = catalog.DropSchema(catalog::ActingAs(context), info_catalog,
                                     info_name, info.cascade, missing_ok);
      }
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("DROP for this object type is not implemented: ",
                              duckdb::CatalogTypeToString(info.type)));
  }
  if (!dropped) {
    auto& ctx = GetSereneDBContext(context);
    if (info.type == duckdb::CatalogType::MACRO_ENTRY ||
        info.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
      ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
        ERR_MSG("function ", info_name, "() does not exist, skipping")));
    } else {
      ctx.AddNotice(
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                       ERR_MSG(pg::ToPgObjectTypeName(info.type), " \"",
                               info_name, "\" does not exist, skipping")));
    }
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
}

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db,
                                 ObjectId database_id)
  : duckdb::Catalog{db}, _database_id{database_id} {}

void SereneDBCatalog::Initialize(bool load_builtin) {}

duckdb::optional_idx SereneDBCatalog::GetCatalogVersion(
  duckdb::ClientContext& context) {
  auto* ctx = GetSereneDBContextPtr(context);
  if (!ctx) {
    return {};
  }
  return duckdb::optional_idx{ctx->CatalogSnapshot()->Version()};
}

duckdb::ErrorData SereneDBCatalog::SupportsCreateTable(
  duckdb::BoundCreateTableInfo& info) {
  auto& base = info.Base();
  if (!base.partition_keys.empty()) {
    return duckdb::ErrorData(duckdb::ExceptionType::CATALOG,
                             "PARTITIONED BY is not supported");
  }
  if (!base.sort_keys.empty()) {
    return duckdb::ErrorData(duckdb::ExceptionType::CATALOG,
                             "SORTED BY is not supported");
  }
  return {};
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  const auto& schema_name =
    info.GetQualifiedName().Schema().GetIdentifierName();
  // PG: schemas beginning with "pg_" are reserved for the system.
  if (absl::StartsWithIgnoreCase(schema_name, "pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
      ERR_MSG("unacceptable schema name \"", schema_name, "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
  }
  auto& client = transaction.GetContext();

  auto& system = duckdb::Catalog::GetSystemCatalog(client);
  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  if (system.GetSchema(client, info.GetQualifiedName().Schema(),
                       duckdb::OnEntryNotFound::RETURN_NULL)) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" already exists"));
  }

  // PG: CREATE SCHEMA requires CREATE on the current database -- enforced
  // inside Catalog::CreateSchema, which throws "permission denied for database
  // <name>" directly. The creator owns the schema (PG current_user).
  auto& catalog_impl = catalog::GetCatalog();
  const ObjectId owner = GetSereneDBContext(client).GetRoleId();
  auto schema = std::make_shared<catalog::Schema>(owner, GetDatabaseId(),
                                                  ObjectId{}, schema_name);
  if (!catalog_impl.CreateSchema(catalog::ActingAs(owner), GetDatabaseId(),
                                 std::move(schema), if_not_exists)) {
    return nullptr;
  }
  // New snapshot will have the schema; next LookupSchema will find it
  return nullptr;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SereneDBCatalog::LookupSchema(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& schema_lookup,
  duckdb::OnEntryNotFound if_not_found) {
  std::string_view schema_name = schema_lookup.GetEntryName();
  // DuckDB uses "main" as default schema; map to "public" for PG compat
  if (schema_name.empty() || schema_name == "main") {
    schema_name = "public";
  }
  // Get connection's snapshot and delegate to its cache
  auto snapshot =
    GetSereneDBContext(transaction.GetContext()).CatalogSnapshot();
  return snapshot->GetDuckDBEntryCache().EnsureSchema(*this, GetDatabaseId(),
                                                      schema_name, *snapshot);
}

void SereneDBCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  auto* ctx = GetSereneDBContextPtr(context);
  if (!ctx) {
    return;
  }
  auto snapshot = ctx->CatalogSnapshot();
  snapshot->GetDuckDBEntryCache().ScanSchemas(*this, GetDatabaseId(), callback,
                                              *snapshot);
}

void SereneDBCatalog::DropSchema(duckdb::ClientContext& context,
                                 duckdb::DropInfo& info) {
  info.SetCatalog(GetName());
  DropObject(context, info);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  auto& table_info = op.info->Base();

  // Search CTAS routes to the iresearch insert operator: it builds the
  // store-less facade and consumes the storage option in CreateCtasTable, so
  // this is a read-only probe of the WITH options.
  if (ReadStorageEngine(table_info.options) == catalog::TableEngine::Search) {
    auto& search_ctas = planner.Make<SereneDBSearchInsert>(
      std::move(op.info), op.schema, op.estimated_cardinality);
    search_ctas.children.push_back(plan);
    return search_ctas;
  }

  // Transactional CTAS. Planning is side-effect free (it can run more than once
  // per statement, e.g. on rebind): pre-allocate the table id and build the
  // operators only; the catalog entry is created at execution. The
  // pre-allocated id names the store table so the CTAS-variant insert can
  // encode it now.
  auto& schema_entry = op.schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();

  catalog::CreateTableOptions options;
  // facade name (before the retarget below)
  options.name = table_info.GetTableName().GetIdentifierName();
  // Consume the storage WITH-option (Transactional on this path) so the
  // unrecognized-parameter check does not reject it.
  ApplyStorageKind(context, options, table_info.options);

  if (!table_info.options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            table_info.options.begin()->first, "\""));
  }

  const auto table_id = catalog::NextId();
  // Capture before the store-table retarget below overwrites on_conflict to
  // ERROR_ON_CONFLICT. For CREATE OR REPLACE TABLE AS this is
  // REPLACE_ON_CONFLICT; the CTAS operator drops the pre-existing table at
  // execution.
  const auto on_conflict = table_info.on_conflict;

  for (auto& col : table_info.columns.Logical()) {
    catalog::Column sdb_col{
      {}, catalog::NextId(), col.Name().GetIdentifierName(), col.Type()};
    if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
    options.columns.push_back(std::move(sdb_col));
  }

  // Retarget the bound create-table info at the hidden store table: the
  // CTAS-variant PhysicalInsert creates it (under the second transaction)
  // and the columns are already bound, so no rebind is needed.
  table_info.SetCatalog(duckdb::Identifier{catalog::kStoreDatabaseName});
  table_info.SetSchema(duckdb::Identifier{"main"});
  table_info.SetTableName(duckdb::Identifier{catalog::StoreTableName(table_id)});
  table_info.on_conflict = duckdb::OnCreateConflict::ERROR_ON_CONFLICT;
  table_info.query.reset();

  auto& store_schema =
    duckdb::Catalog::GetCatalog(context,
                                duckdb::Identifier{catalog::kStoreDatabaseName})
      .GetSchema(context, duckdb::Identifier{"main"});
  auto store_info = duckdb::make_uniq<duckdb::BoundCreateTableInfo>(
    store_schema, std::move(op.info->base));

  // Mirror duckdb's own DuckCatalog::PlanCreateTableAs branch selection: a
  // partitionable, order-preserving load uses PhysicalBatchInsert, which
  // flushes and compresses row groups optimistically across worker threads
  // during the sink. Wrapping only PhysicalInsert forced the serial commit-time
  // flush and made CTAS from a parallel source ~7x slower than native.
  const bool parallel_streaming_insert =
    !duckdb::PhysicalPlanGenerator::PreserveInsertionOrder(context, plan);
  const bool use_batch_index =
    duckdb::PhysicalPlanGenerator::UseBatchIndex(context, plan);
  const auto num_threads =
    duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads();

  auto& insert = [&]() -> duckdb::PhysicalOperator& {
    if (!parallel_streaming_insert && use_batch_index) {
      return planner.Make<duckdb::PhysicalBatchInsert>(
        op, store_schema, std::move(store_info), op.estimated_cardinality);
    }
    const bool parallel = parallel_streaming_insert && num_threads > 1;
    return planner.Make<duckdb::PhysicalInsert>(
      op, store_schema, std::move(store_info), op.estimated_cardinality,
      parallel);
  }();

  auto& ctas = planner.Make<SereneDBPhysicalCTAS>(
    insert, database_id, GetName().GetIdentifierName(),
    op.schema.name.GetIdentifierName(), std::move(options), table_id,
    on_conflict, op.estimated_cardinality);
  ctas.children.push_back(plan);
  return ctas;
}

namespace {

std::vector<duckdb::idx_t> ComputeKeptViewPositions(
  const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    parsed_index_exprs,
  const duckdb::ViewColumnInfo& column_info) {
  std::vector<duckdb::idx_t> kept;
  auto add = [&](std::string_view name) {
    for (size_t i = 0; i < column_info.names.size(); ++i) {
      if (column_info.names[i].GetIdentifierName() == name) {
        kept.push_back(i);
        break;
      }
    }
  };
  auto collect = [&](this auto& self,
                     const duckdb::ParsedExpression& e) -> void {
    if (e.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      add(e.Cast<duckdb::ColumnRefExpression>()
            .GetColumnName()
            .GetIdentifierName());
      return;
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      e, [&](const duckdb::ParsedExpression& c) { self(c); });
  };
  for (const auto& expr : parsed_index_exprs) {
    collect(*expr);
  }
  absl::c_sort(kept);
  kept.erase(std::unique(kept.begin(), kept.end()), kept.end());
  return kept;
}

// Wrap `plan` in a LogicalProjection that enumerates only the kept view
// columns + PK plumbing. Optimizer rule RemoveUnusedColumns then treats this
// projection as a scope boundary and prunes the chain below to match. Need this
// as CREATE INDEX itself is not a prune boundary.
duckdb::unique_ptr<duckdb::LogicalOperator> InsertBackfillFilterProjection(
  duckdb::unique_ptr<duckdb::LogicalOperator> plan,
  const std::vector<duckdb::idx_t>& kept_view, duckdb::idx_t view_decl_size,
  duckdb::idx_t vcols_count, duckdb::TableIndex new_table_index) {
  plan->ResolveOperatorTypes();
  const auto top_bindings = plan->GetColumnBindings();
  const auto& top_types = plan->types;
  SDB_ASSERT(top_bindings.size() == view_decl_size + vcols_count,
             "chain top should expose view cols + PK plumbing: got ",
             top_bindings.size(), ", expected ", view_decl_size + vcols_count);

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> filter_exprs;
  filter_exprs.reserve(kept_view.size() + vcols_count);
  for (auto v : kept_view) {
    SDB_ASSERT(v < view_decl_size);
    filter_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      top_types[v], top_bindings[v]));
  }
  for (duckdb::idx_t i = 0; i < vcols_count; ++i) {
    auto p = view_decl_size + i;
    filter_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      top_types[p], top_bindings[p]));
  }

  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    new_table_index, std::move(filter_exprs));
  proj->children.push_back(std::move(plan));
  return proj;
}

// Retarget bound constraints onto the store mirror. The store catalog cannot
// bind CHECK constraints that reference facade-only types or functions, so the
// mirror table omits them; they were already bound against the facade entry, so
// carry them onto the store-bound set as engine-supplied extras (appended last,
// where DataTable's Verify{Append,Update}Constraints expect the surplus). This
// is what enforces such CHECKs on INSERT, UPDATE and upsert alike.
duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
RetargetStoreConstraints(
  duckdb::ClientContext& context, duckdb::DuckTableEntry& store_entry,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>& facade_bound) {
  auto store_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  for (auto& constraint : facade_bound) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      store_constraints.push_back(std::move(constraint));
    }
  }
  return store_constraints;
}

}  // namespace

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  // Search table: route to the iresearch insert operator. It has no store
  // table to compute defaults/generated columns downstream, so resolve them
  // into the plan here (ResolveDefaultsProjection two-passes STORED generated
  // columns -- incl. the generated PK -- over a storage-ordered chunk).
  if (sdb_table->GetEngine() == catalog::TableEngine::Search) {
    if (plan && !op.column_index_map.empty()) {
      plan = &planner.ResolveDefaultsProjection(op, *plan);
      op.column_index_map.clear();
    }
    auto& insert = planner.Make<SereneDBSearchInsert>(
      std::move(sdb_table), std::move(op.types), op.estimated_cardinality);
    if (plan) {
      insert.children.push_back(*plan);
    }
    return insert;
  }

  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();

  // Column layouts match by construction; upstream PlanInsert handles the
  // batch-vs-streaming branch and the store DuckTableEntry target (via
  // GetStorageTableEntry).
  op.bound_constraints =
    RetargetStoreConstraints(context, store_entry, op.bound_constraints);

  // Resolve defaults/generated columns (the shared upstream two-pass) BELOW the
  // progress wrapper, then clear the map so the delegated PlanInsert does not
  // project a second time.
  if (plan && !op.column_index_map.empty()) {
    plan = &planner.ResolveDefaultsProjection(op, *plan);
    op.column_index_map.clear();
  }
  return store_entry.ParentCatalog().Cast<duckdb::DuckCatalog>().PlanInsert(
    context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  if (sdb_table->GetEngine() == catalog::TableEngine::Search) {
    // TRUNCATE (autocommit): fast iresearch Clear marker. In-transaction
    // TRUNCATE has is_truncate but is not autocommit, so it falls through to
    // the row-wise SereneDBSearchDelete below.
    if (op.is_truncate && context.transaction.IsAutoCommit()) {
      return planner.Make<SereneDBSearchTruncate>(std::move(sdb_table),
                                                  op.estimated_cardinality);
    }

    // A Search table has no separate inverted indexes, so its scan appends only
    // the PK virtuals (BuildRowIdColumns): [real..., pk_0..pk_{n-1}] for
    // explicit-PK tables, or [real..., generated_pk] for generated-PK ones.
    const auto& pk_col_ids = sdb_table->PKColumns();
    const auto child_cols = plan.types.size();
    std::vector<duckdb::idx_t> pk_indices;
    if (pk_col_ids.empty()) {
      pk_indices.push_back(child_cols - 1);  // generated-PK slot is last
    } else {
      const auto num_pk = pk_col_ids.size();
      for (size_t i = 0; i < num_pk; ++i) {
        pk_indices.push_back(child_cols - num_pk + i);
      }
    }
    auto& search_del = planner.Make<SereneDBSearchDelete>(
      std::move(sdb_table), std::move(pk_indices), op.estimated_cardinality);
    search_del.children.push_back(plan);
    return search_del;
  }

  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();
  op.bound_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  return store_entry.ParentCatalog().Cast<duckdb::DuckCatalog>().PlanDelete(
    context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  if (sdb_table->GetEngine() == catalog::TableEngine::Search) {
    // Wrap `plan` with a PhysicalProjection that resolves VALUE_DEFAULT and
    // passes every projected new-row column through -- the SET values, duckdb's
    // recomputed STORED generated columns, and the old-value passthroughs that
    // BindUpdateConstraints added -- plus the PK virtuals, so
    // SereneDBSearchUpdate sees [resolved new-row vals, pk_virtuals]. A Search
    // table has no separate inverted indexes, so BuildRowIdColumns appends only
    // the PK virtuals: [real..., pk_0..pk_{n-1}] / [real..., generated_pk].
    const auto& pk_col_ids = sdb_table->PKColumns();
    const auto num_pk = pk_col_ids.size();
    const auto num_virtual = pk_col_ids.empty() ? 1 : num_pk;
    const auto child_cols = plan.types.size();

    const auto num_updates = op.expressions.size();
    duckdb::vector<duckdb::LogicalType> proj_types;
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
    proj_types.reserve(num_updates + num_virtual);
    proj_exprs.reserve(num_updates + num_virtual);

    for (duckdb::idx_t i = 0; i < num_updates; ++i) {
      auto& expr = op.expressions[i];
      if (expr->GetExpressionType() == duckdb::ExpressionType::VALUE_DEFAULT) {
        auto phys = op.columns[i].index;
        SDB_ASSERT(phys < op.bound_defaults.size());
        proj_types.push_back(op.bound_defaults[phys]->GetReturnType());
        proj_exprs.push_back(op.bound_defaults[phys]->Copy());
      } else {
        proj_types.push_back(expr->GetReturnType());
        proj_exprs.push_back(expr->Copy());
      }
    }

    // Passthrough virtual columns (PKs / generated PK).
    auto virt_start = child_cols - num_virtual;
    for (duckdb::idx_t i = virt_start; i < child_cols; ++i) {
      proj_types.push_back(plan.types[i]);
      proj_exprs.push_back(
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(plan.types[i], i));
    }

    auto& proj = planner.Make<duckdb::PhysicalProjection>(
      std::move(proj_types), std::move(proj_exprs), op.estimated_cardinality);
    proj.children.push_back(plan);

    std::vector<duckdb::idx_t> pk_indices;
    if (pk_col_ids.empty()) {
      // generated PK is the single virtual, after the SET vals.
      pk_indices.push_back(num_updates + num_virtual - 1);
    } else {
      pk_indices.reserve(num_pk);
      for (size_t i = 0; i < num_pk; ++i) {
        pk_indices.push_back(num_updates + i);
      }
    }

    auto& search_upd = planner.Make<SereneDBSearchUpdate>(
      std::move(sdb_table), std::move(pk_indices), std::move(op.columns),
      op.estimated_cardinality);
    search_upd.children.push_back(proj);
    return search_upd;
  }

  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();
  op.bound_constraints =
    RetargetStoreConstraints(context, store_entry, op.bound_constraints);
  return store_entry.ParentCatalog().Cast<duckdb::DuckCatalog>().PlanUpdate(
    context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanMergeInto(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  if (table_entry.GetSereneDBTable()->GetEngine() ==
      catalog::TableEngine::Search) {
    // MERGE INTO (and INSERT ... ON CONFLICT, which duckdb also lowers to
    // MergeInto) delegates each action to the store mirror, which bypasses the
    // iresearch index -- it silently corrupts the search index. Reject it with
    // a clear error until search-backed MERGE is implemented.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("MERGE INTO (and INSERT ... ON CONFLICT) is not yet supported on "
              "search-backed tables"));
  }
  // DuckDB routes INSERT ON CONFLICT through MergeInto as well. Retarget the
  // constraints onto the store mirror and delegate; upstream
  // DuckCatalog::PlanMergeInto builds each action against the store
  // DuckTableEntry via TableCatalogEntry::GetStorageTableEntry.
  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();
  op.bound_constraints =
    RetargetStoreConstraints(context, store_entry, op.bound_constraints);
  return store_entry.ParentCatalog().Cast<duckdb::DuckCatalog>().PlanMergeInto(
    context, planner, op, plan);
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindAlterAddIndex(
  duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
  duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) {
  // ADD PRIMARY KEY records the PK in the table's catalog (the PK columns
  // become the row identity), not ART index so discard the binder's
  // index plan and route the ALTER through LOGICAL_ALTER.
  return duckdb::make_uniq<duckdb::LogicalSimple>(
    duckdb::LogicalOperatorType::LOGICAL_ALTER, std::move(alter_info));
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::CatalogEntry& target,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  if (target.type != duckdb::CatalogType::VIEW_ENTRY) {
    auto& table_entry =
      RequireBaseTable(target.Cast<duckdb::TableCatalogEntry>());
    auto sdb_table = table_entry.GetSereneDBTable();
    RejectIfSearchTable(*sdb_table, "CREATE INDEX");
  }

  // View-backed indexes are STATIC -- captured at CREATE INDEX, no DML refresh.
  duckdb::optional_ptr<duckdb::TableCatalogEntry> resolved_table;
  bool view_backed = false;
  std::optional<ViewFastPath> view_fast_path;
  int64_t pinned_iceberg_snapshot_id = 0;
  std::optional<std::vector<duckdb::idx_t>> kept_view_positions;
  std::optional<std::vector<duckdb::column_t>> vcols_opt;
  if (target.type == duckdb::CatalogType::VIEW_ENTRY) {
    view_backed = true;
    auto is_fast_path_wrapper = [](duckdb::LogicalOperator& op) -> bool {
      switch (op.type) {
        case duckdb::LogicalOperatorType::LOGICAL_FILTER:
        case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        case duckdb::LogicalOperatorType::LOGICAL_LIMIT:
        case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
        case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
          return true;
        default:
          return false;
      }
    };
    auto snapshot = GetSereneDBContext(binder.context).CatalogSnapshot();
    auto relation_obj =
      snapshot->GetRelation(catalog::NoAccessCheck(), GetDatabaseId(),
                            target.ParentSchema().name.GetIdentifierName(),
                            target.name.GetIdentifierName());
    std::optional<ViewFastPath> fp;
    if (relation_obj &&
        relation_obj->GetType() == catalog::ObjectType::PgSqlView) {
      auto view =
        std::static_pointer_cast<const catalog::PgSqlView>(relation_obj);
      fp = ResolveViewFastPath(binder.context, *view);
    }
    duckdb::LogicalOperator* leaf_parent_chain_root = plan.get();
    duckdb::LogicalGet* leaf_get = nullptr;
    {
      duckdb::LogicalOperator* cur = plan.get();
      bool ok = true;
      while (cur && cur->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
        if (!is_fast_path_wrapper(*cur) || cur->children.size() != 1) {
          ok = false;
          break;
        }
        cur = cur->children[0].get();
      }
      if (ok && cur) {
        leaf_get = &cur->Cast<duckdb::LogicalGet>();
      }
    }
    if (fp && leaf_get) {
      view_fast_path = std::move(fp);
      vcols_opt = BackfillPkVirtualColumns(*view_fast_path);
      const auto& vcols = *vcols_opt;

      const auto leaf_orig_size = leaf_get->GetColumnIds().size();
      duckdb::vector<duckdb::LogicalType> pk_types;
      pk_types.reserve(vcols.size());
      for (auto vcol : vcols) {
        if (vcol == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
          pk_types.push_back(duckdb::LogicalType::UBIGINT);
        } else {
          pk_types.push_back(duckdb::LogicalType::BIGINT);
        }
      }
      leaf_get->types.clear();
      for (duckdb::idx_t i = 0; i < leaf_orig_size; ++i) {
        leaf_get->types.push_back(
          leaf_get->GetColumnType(leaf_get->GetColumnIds()[i]));
      }
      for (size_t i = 0; i < vcols.size(); ++i) {
        leaf_get->AddColumnId(vcols[i]);
        leaf_get->types.push_back(pk_types[i]);
        // Iceberg's get_virtual_columns omits file_index even though the
        // reader produces it -- patch the map.
        if (leaf_get->virtual_columns.find(vcols[i]) ==
            leaf_get->virtual_columns.end()) {
          if (vcols[i] ==
              duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
            leaf_get->virtual_columns.emplace(
              vcols[i],
              duckdb::TableColumn("file_index", duckdb::LogicalType::UBIGINT));
          } else if (vcols[i] == duckdb::MultiFileReader::
                                   COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
            leaf_get->virtual_columns.emplace(
              vcols[i], duckdb::TableColumn("file_row_number",
                                            duckdb::LogicalType::BIGINT));
          }
        }
      }
      auto thread_pk_through = [&](auto& self,
                                   duckdb::LogicalOperator& op) -> void {
        if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
          return;
        }
        SDB_ASSERT(op.children.size() == 1);
        self(self, *op.children[0]);
        if (op.type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
          return;
        }
        auto& proj = op.Cast<duckdb::LogicalProjection>();
        auto child_bindings = op.children[0]->GetColumnBindings();
        const auto child_orig_size = child_bindings.size() - vcols.size();
        for (size_t i = 0; i < vcols.size(); ++i) {
          proj.expressions.push_back(
            duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
              pk_types[i], child_bindings[child_orig_size + i]));
        }
      };
      thread_pk_through(thread_pk_through, *leaf_parent_chain_root);
      if (leaf_get->bind_data) {
        pinned_iceberg_snapshot_id =
          ExtractIcebergSnapshotId(*leaf_get->bind_data);
        EnableIcebergSort(leaf_get->bind_data.get());
      }

      auto& view_entry = target.Cast<duckdb::ViewCatalogEntry>();
      auto column_info = view_entry.GetColumnInfo();
      SDB_ASSERT(column_info,
                 "view must be bound by the time fp && leaf_get holds -- "
                 "the leaf get came from binding the view body");
      auto kept = ComputeKeptViewPositions(
        stmt.info->Cast<duckdb::CreateIndexInfo>().parsed_expressions,
        *column_info);
      if (kept.size() < column_info->names.size()) {
        plan = InsertBackfillFilterProjection(
          std::move(plan), kept, column_info->names.size(), vcols.size(),
          binder.GenerateTableIndex());
        kept_view_positions = std::move(kept);
      }
    } else {
      auto& view_entry = target.Cast<duckdb::ViewCatalogEntry>();
      if (auto column_info = view_entry.GetColumnInfo()) {
        auto kept = ComputeKeptViewPositions(
          stmt.info->Cast<duckdb::CreateIndexInfo>().parsed_expressions,
          *column_info);
        if (kept.size() < column_info->names.size()) {
          plan = InsertBackfillFilterProjection(
            std::move(plan), kept, column_info->names.size(),
            /*vcols_count=*/0, binder.GenerateTableIndex());
          kept_view_positions = std::move(kept);
        }
      }
    }
  } else {
    resolved_table = &target.Cast<duckdb::TableCatalogEntry>();
  }
  // IndexBinder casts bind_data to TableScanBindData -- doesn't fit ours.
  auto create_index_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
      std::move(stmt.info));

  // DuckDB defaults to "" or "ART"; PG defaults to "btree".
  {
    auto& idx_type = create_index_info->index_type;
    auto type = absl::AsciiStrToLower(idx_type);
    if (type.empty() || type == "art" || type == "btree") {
      create_index_info->index_type = "secondary";
    } else if (type == "secondary" || type == "inverted") {
      create_index_info->index_type = std::move(type);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("access method \"", idx_type, "\" does not exist"));
    }
  }

  if (view_backed && create_index_info->index_type == "secondary") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("plain indexes on views are not supported; use an inverted "
              "index instead"));
  }

  std::vector<std::pair<std::string, duckdb::LogicalType>> rel_columns;
  // Populated for base-table indexes; used below to drive the narrow
  // projection that BuildCreateIndexProjection computes. Stays null for
  // view-backed indexes (whose projection comes from the view body).
  std::shared_ptr<catalog::Table> sdb_table;
  if (view_backed) {
    auto& view_entry = target.Cast<duckdb::ViewCatalogEntry>();
    auto column_info = view_entry.GetColumnInfo();
    if (!column_info) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("view \"", target.name.GetIdentifierName(),
                              "\" must be bound before it can be indexed"));
    }
    if (kept_view_positions) {
      rel_columns.reserve(kept_view_positions->size());
      for (auto p : *kept_view_positions) {
        rel_columns.emplace_back(column_info->names[p].GetIdentifierName(),
                                 column_info->types[p]);
      }
    } else {
      rel_columns.assign_range(
        std::views::iota(size_t{0}, column_info->names.size()) |
        std::views::transform([&](size_t i) {
          return std::pair{column_info->names[i].GetIdentifierName(),
                           column_info->types[i]};
        }));
    }
  } else {
    auto& sdb_entry = RequireBaseTable(*resolved_table);
    sdb_table = sdb_entry.GetSereneDBTable();
    const auto& columns = sdb_table->Columns();
    rel_columns.assign_range(columns | std::views::transform([](const auto& c) {
                               return std::pair{std::string{c.GetName()},
                                                c.type};
                             }));
  }

  containers::FlatHashSet<duckdb::column_t> seen_columns;
  auto add_column = [&](std::string_view col_name) {
    for (size_t i = 0; i < rel_columns.size(); ++i) {
      if (absl::EqualsIgnoreCase(rel_columns[i].first, col_name)) {
        const auto col_id = static_cast<duckdb::column_t>(i);
        if (seen_columns.insert(col_id).second) {
          create_index_info->column_ids.emplace_back(col_id);
          create_index_info->scan_types.emplace_back(rel_columns[i].second);
        }
        break;
      }
    }
  };

  auto collect = [&](this auto& self,
                     const duckdb::ParsedExpression& e) -> void {
    if (e.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      add_column(e.Cast<duckdb::ColumnRefExpression>()
                   .GetColumnName()
                   .GetIdentifierName());
      return;
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      e, [&](const duckdb::ParsedExpression& child) { self(child); });
  };
  for (auto& expr : create_index_info->parsed_expressions) {
    collect(*expr);
  }
  create_index_info->scan_types.emplace_back(duckdb::LogicalType::ROW_TYPE);

  auto leaf_get_from_plan =
    [](duckdb::LogicalOperator& root) -> duckdb::LogicalGet& {
    auto* cur = &root;
    while (cur->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      cur = cur->children[0].get();
    }
    return cur->Cast<duckdb::LogicalGet>();
  };
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  if (!view_backed) {
    SDB_ASSERT(sdb_table);
    // Project only what the backfill actually needs: index columns + PK
    // columns (or ROW_ID for generated PK). Replaces the previous
    // "every non-PK column" default which made every CREATE INDEX scan
    // redundantly read the whole table.
    auto projection =
      BuildCreateIndexProjection(sdb_table->Columns(), sdb_table->PKColumns(),
                                 create_index_info->column_ids);
    auto& get = leaf_get_from_plan(*plan);
    if (get.GetColumnIds().empty()) {
      for (auto pos : projection) {
        get.AddColumnId(static_cast<duckdb::column_t>(pos));
      }
      get.types.clear();
      for (auto pos : projection) {
        get.types.push_back(rel_columns[pos].second);
      }
      get.AddColumnId(duckdb::COLUMN_IDENTIFIER_ROW_ID);
      get.types.push_back(duckdb::LogicalType::ROW_TYPE);
    }
    SDB_ASSERT(get.bind_data,
               "base-table LogicalGet missing SereneDB bind_data");
    create_index_info->names = get.names;
    create_index_info->SetSchema(resolved_table->schema.name);
    create_index_info->SetCatalog(resolved_table->catalog.GetName());

    duckdb::IndexBinder index_binder(binder, binder.context, resolved_table,
                                     create_index_info.get());
    for (auto& parsed : create_index_info->expressions) {
      expressions.emplace_back(index_binder.Bind(parsed));
    }
  } else {
    create_index_info->names.assign_range(
      rel_columns | std::views::keys |
      std::views::transform(
        [](const std::string& n) { return duckdb::Identifier{n}; }));
    create_index_info->SetSchema(target.ParentSchema().name);
    create_index_info->SetCatalog(target.ParentCatalog().GetName());
    if (view_fast_path) {
      switch (view_fast_path->pk_spec) {
        case catalog::PkSpec::DuckDBRowId:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("duckdb_rowid");
          break;
        case catalog::PkSpec::FileIndexPlusDuckDBRowId:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("file_index_plus_duckdb_rowid");
          break;
        default: {
          SDB_ASSERT(vcols_opt,
                     "view_fast_path set but vcols not populated -- the "
                     "two are produced together in the leaf-rewrite block");
          const auto& vcols = *vcols_opt;
          if (vcols.size() == 1) {
            create_index_info->options["_sdb_view_fast_path_pk"] =
              duckdb::Value("file_row_number");
          } else if (vcols.size() == 2) {
            create_index_info->options["_sdb_view_fast_path_pk"] =
              duckdb::Value("file_index_plus_row_number");
          }
          break;
        }
      }
      if (pinned_iceberg_snapshot_id != 0) {
        create_index_info->options["_sdb_iceberg_snapshot_id"] =
          duckdb::Value::BIGINT(pinned_iceberg_snapshot_id);
      }
    }
    if (kept_view_positions) {
      duckdb::vector<duckdb::Value> kept_values;
      kept_values.reserve(kept_view_positions->size());
      for (auto p : *kept_view_positions) {
        kept_values.emplace_back(duckdb::Value::UBIGINT(p));
      }
      create_index_info->options["_sdb_view_kept_positions"] =
        duckdb::Value::LIST(duckdb::LogicalType::UBIGINT,
                            std::move(kept_values));
    }
    // Remap col-ref bindings to (TableIndex(0), narrowed_position): the
    // resolver matches LOGICAL_CREATE_INDEX exprs against TableIndex(0), and
    // chunk positions follow kept_view_positions' (sorted) order.
    duckdb::IndexBinder index_binder(binder, binder.context, nullptr,
                                     create_index_info.get());
    expressions.reserve(create_index_info->expressions.size());
    for (auto& parsed : create_index_info->expressions) {
      auto bound = index_binder.Bind(parsed);
      auto remap = [&](this auto& self, duckdb::Expression& e) -> void {
        if (e.GetExpressionClass() ==
            duckdb::ExpressionClass::BOUND_COLUMN_REF) {
          auto& cref = e.Cast<duckdb::BoundColumnRefExpression>();
          auto col_idx = cref.Binding().column_index.GetIndex();
          if (kept_view_positions) {
            auto it = std::ranges::lower_bound(*kept_view_positions, col_idx);
            SDB_ASSERT(it != kept_view_positions->end() && *it == col_idx,
                       "view col ref references a non-kept position");
            col_idx = static_cast<duckdb::idx_t>(
              std::distance(kept_view_positions->begin(), it));
          }
          cref.BindingMutable() = duckdb::ColumnBinding(
            duckdb::TableIndex(0), duckdb::ProjectionIndex(col_idx));
        }
        duckdb::ExpressionIterator::EnumerateChildren(
          e, [&](duckdb::Expression& c) { self(c); });
      };
      remap(*bound);
      expressions.emplace_back(std::move(bound));
    }
  }

  auto& target_for_op = view_backed
                          ? static_cast<duckdb::CatalogEntry&>(target)
                          : static_cast<duckdb::CatalogEntry&>(*resolved_table);
  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(create_index_info), std::move(expressions), target_for_op,
    nullptr);
  result->children.push_back(std::move(plan));
  return result;
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  // Facade tables hold no row data themselves -- it lives in the hidden store
  // database. Report the store's size (the user never names the store).
  auto store = duckdb::DatabaseManager::Get(context).GetDatabase(
    context, duckdb::Identifier{catalog::kStoreDatabaseName});
  if (store) {
    return store->GetCatalog().GetDatabaseSize(context);
  }
  return {};
}

}  // namespace sdb::connector
