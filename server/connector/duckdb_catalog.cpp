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
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/operator/persistent/physical_delete.hpp>
#include <duckdb/execution/operator/persistent/physical_insert.hpp>
#include <duckdb/execution/operator/persistent/physical_merge_into.hpp>
#include <duckdb/execution/operator/persistent/physical_update.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
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
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/database_size.hpp>
#include <ranges>

#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/pk_spec.h"
#include "catalog/schema.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_progress.h"
#include "connector/duckdb_table_entry.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

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
Result DropFunctionOverload(catalog::Catalog& catalog,
                            duckdb::ClientContext& context,
                            duckdb::DropInfo& info) {
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info.catalog);
  if (!db) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(catalog::NoAccessCheck(), database_id,
                                        info.schema, info.name);
  if (!existing) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
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
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  // PG: DROP FUNCTION on a procedure (or vice versa) is an error.
  auto& matched = *macro_info.macros[match_idx];
  if (matched.is_procedure != info.is_procedure) {
    auto expect = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(info.name, "() is not a ", expect));
  }

  if (macro_info.macros.size() == 1) {
    // Last overload -- drop the whole function.
    return catalog.DropFunction(catalog::RequireOwnership(context),
                                info.catalog, info.schema, info.name,
                                info.cascade);
  }

  // Remove just the matched overload and update the stored function.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      macro_info.Copy());
  new_info->macros.erase(new_info->macros.begin() + match_idx);
  AlignMacroCatalogType(*new_info);

  auto function = std::make_shared<catalog::PgSqlFunction>(
    catalog::Permissions{}, ObjectId{}, ObjectId{}, info.name,
    std::move(new_info));
  return catalog.CreateFunction(catalog::RequireOwnership(context), database_id,
                                info.schema, function, true);
}

// DROP FUNCTION/PROCEDURE name -- drop overloads matching the drop kind.
// PG: DROP FUNCTION drops only function overloads, DROP PROCEDURE drops only
// procedure overloads. If mixed (func + proc under same name), keep the other.
Result DropFunctionByKind(duckdb::ClientContext& context,
                          catalog::Catalog& catalog,
                          const duckdb::DropInfo& info) {
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info.catalog);
  if (!db) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(catalog::NoAccessCheck(), database_id,
                                        info.schema, info.name);
  if (!existing) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
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
      ERR_MSG("could not find a ", kind, " named \"", info.name, "\""));
  }
  if (all_match) {
    return catalog.DropFunction(catalog::RequireOwnership(context),
                                info.catalog, info.schema, info.name,
                                info.cascade);
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
    catalog::Permissions{}, ObjectId{}, ObjectId{}, info.name,
    std::move(new_info));
  return catalog.CreateFunction(catalog::RequireOwnership(context), database_id,
                                info.schema, function, true);
}

}  // namespace

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info) {
  auto& catalog = catalog::GetCatalog();

  Result r;
  switch (info.type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      r = catalog.DropTable(catalog::RequireOwnership(context), info.catalog,
                            info.schema, info.name, info.cascade);
      if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop table ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
      }
      break;
    case INDEX_ENTRY:
      r = catalog.DropIndex(catalog::RequireOwnership(context), info.catalog,
                            info.schema, info.name, info.cascade);
      break;
    case VIEW_ENTRY:
      r = catalog.DropView(catalog::RequireOwnership(context), info.catalog,
                           info.schema, info.name, info.cascade);
      if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop view ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
      }
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      if (info.has_func_args) {
        r = DropFunctionOverload(catalog, context, info);
      } else {
        r = DropFunctionByKind(context, catalog, info);
      }
      if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop function ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
      }
      break;
    case TYPE_ENTRY:
      r = catalog.DropType(catalog::RequireOwnership(context), info.catalog,
                           info.schema, info.name, info.cascade);
      if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop type ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
      }
      break;
    case SEQUENCE_ENTRY: {
      bool if_exists =
        info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
      r = catalog.DropSequence(catalog::RequireOwnership(context), info.catalog,
                               info.schema, info.name, if_exists, info.cascade);
      if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop sequence ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP ... CASCADE to drop the dependent "
                   "objects too, or DROP TABLE on the owning table."));
      }
    } break;
    case SCHEMA_ENTRY:
      if (info.name == StaticStrings::kPgCatalogSchema ||
          info.name == StaticStrings::kInformationSchema) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
          ERR_MSG("cannot drop schema ", info.name,
                  " because it is required by the database system"));
      } else {
        r = catalog.DropSchema(catalog::RequireOwnership(context), info.catalog,
                               info.name, info.cascade);
        // TODO(mbkkt) better error handling
        if (!info.cascade && r.is(ERROR_BAD_PARAMETER)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ERR_MSG("cannot drop schema ", info.name,
                    " because other objects depend on it"),
            ERR_HINT(
              "Use DROP ... CASCADE to drop the dependent objects too."));
        }
      }
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("DROP for this object type is not implemented: ",
                              duckdb::CatalogTypeToString(info.type)));
  }
  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    // The error message from catalog contains the actual object type name
    auto actual_type = r.errorMessage();
    auto actual_name = absl::AsciiStrToLower(actual_type);
    auto object_name = pg::ToPgObjectTypeName(info.type);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", info.name, "\" is not ",
              basics::string_utils::GetArticle(object_name), " ", object_name),
      ERR_HINT("Use DROP ", absl::AsciiStrToUpper(actual_type), " to remove ",
               basics::string_utils::GetArticle(actual_name), " ", actual_name,
               "."));
  }
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    const bool missing_ok =
      info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
    auto& ctx = GetSereneDBContext(context);
    if (info.type == duckdb::CatalogType::MACRO_ENTRY ||
        info.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
      auto kind = info.is_procedure ? "procedure" : "function";
      if (!missing_ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a ", kind, " named \"", info.name, "\""));
      }
      ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
        ERR_MSG("function ", info.name, "() does not exist, skipping")));
    } else {
      if (!missing_ok) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG(pg::ToPgObjectTypeName(info.type), " \"",
                                info.name, "\" does not exist"));
      }
      ctx.AddNotice(
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                       ERR_MSG(pg::ToPgObjectTypeName(info.type), " \"",
                               info.name, "\" does not exist, skipping")));
    }
    r = {};
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db,
                                 ObjectId database_id)
  : duckdb::Catalog{db}, _database_id{database_id} {}

void SereneDBCatalog::Initialize(bool load_builtin) {}

duckdb::optional_idx SereneDBCatalog::GetCatalogVersion(
  duckdb::ClientContext& context) {
  return catalog::GetCatalog().GetCatalogSnapshot()->Version();
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
  // PG: schemas beginning with "pg_" are reserved for the system.
  if (absl::StartsWithIgnoreCase(info.schema, "pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
      ERR_MSG("unacceptable schema name \"", info.schema, "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
  }
  // SereneDBCatalog (unlike a DuckCatalog) has no SingleFileStorageManager, so
  // it is never driven by WAL replay / checkpoint -- CreateSchema is only
  // reached from a CREATE SCHEMA statement, which always carries a session.
  auto& client = transaction.GetContext();

  // PG: schemas pre-populated by the system catalog (e.g. information_schema)
  // shadow the user catalog. Reject creation if a system schema with the same
  // name already exists.
  auto& system = duckdb::Catalog::GetSystemCatalog(client);
  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  if (system.GetSchema(client, info.schema,
                       duckdb::OnEntryNotFound::RETURN_NULL)) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", info.schema, "\" already exists"));
  }

  // PG: CREATE SCHEMA requires CREATE on the current database -- enforced
  // inside Catalog::CreateSchema, which throws "permission denied for database
  // <name>" directly. The creator owns the schema (PG current_user).
  auto& catalog_impl = catalog::GetCatalog();
  const ObjectId owner = GetSereneDBContext(client).GetRoleId();
  auto schema = std::make_shared<catalog::Schema>(owner, GetDatabaseId(),
                                                  ObjectId{}, info.schema);
  auto r = catalog_impl.CreateSchema(catalog::RequireOwnership(owner),
                                     GetDatabaseId(), std::move(schema));
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", info.schema, "\" already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
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
    GetSereneDBContext(transaction.GetContext()).EnsureCatalogSnapshot();
  return snapshot->GetDuckDBEntryCache().EnsureSchema(*this, GetDatabaseId(),
                                                      schema_name, *snapshot);
}

void SereneDBCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  // Internal connections (catalog store, appenders) have no serenedb
  // session; error-path suggestion scans walk every attached catalog and
  // must see this one as empty instead of dying on the missing state.
  if (!context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    return;
  }
  auto snapshot = GetSereneDBContext(context).EnsureCatalogSnapshot();
  snapshot->GetDuckDBEntryCache().ScanSchemas(*this, GetDatabaseId(), callback,
                                              *snapshot);
}

void SereneDBCatalog::DropSchema(duckdb::ClientContext& context,
                                 duckdb::DropInfo& info) {
  info.catalog = GetName();
  DropObject(context, info);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  // CTAS always gets a generated PK (monotonic), no sort needed.
  auto& ctas = planner.Make<SereneDBPhysicalCTAS>(std::move(op.info), op.schema,
                                                  op.estimated_cardinality);
  ctas.children.push_back(plan);
  return ctas;
}

namespace {

// Defaults / generated-column projection for INSERT.
//
// Two passes so gen-col exprs can resolve their BoundRef(dep.storage_oid)
// leaves against a storage-ordered chunk:
//   Pass 1 (storage-oid order):
//     gen col      -> NULL placeholder
//     user-omitted -> bound_defaults[storage_idx]  (BoundRef-free)
//     user-set     -> BoundRef(mapped_index) into the user input chunk
//   Pass 2 (same layout):
//     gen col      -> bound_defaults[storage_idx]  (refs resolve vs pass 1)
//     other        -> BoundRef(storage_idx) passthrough
duckdb::PhysicalOperator& ResolveDefaultsWithGenerated(
  duckdb::PhysicalPlanGenerator& planner, duckdb::LogicalInsert& op,
  duckdb::PhysicalOperator& child) {
  SDB_ASSERT(!op.column_index_map.empty());

  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> pass1_exprs;
  bool has_stored_generated = false;
  for (auto& col : op.table.GetColumns().Physical()) {
    types.push_back(col.Type());
    if (col.Category() == duckdb::TableColumnType::GENERATED_STORED) {
      has_stored_generated = true;
      pass1_exprs.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value(col.Type())));
      continue;
    }
    auto mapped_index = op.column_index_map[col.Physical()];
    if (mapped_index == duckdb::DConstants::INVALID_INDEX) {
      auto storage_idx = col.StorageOid();
      pass1_exprs.push_back(std::move(op.bound_defaults[storage_idx]));
    } else {
      pass1_exprs.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        col.Type(), mapped_index));
    }
  }
  auto& pass1 = planner.Make<duckdb::PhysicalProjection>(
    types, std::move(pass1_exprs), child.estimated_cardinality);
  pass1.children.push_back(child);

  if (!has_stored_generated) {
    return pass1;
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> pass2_exprs;
  for (auto& col : op.table.GetColumns().Physical()) {
    auto storage_idx = col.StorageOid();
    if (col.Category() == duckdb::TableColumnType::GENERATED_STORED) {
      pass2_exprs.push_back(std::move(op.bound_defaults[storage_idx]));
    } else {
      pass2_exprs.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        col.Type(), storage_idx));
    }
  }
  auto& pass2 = planner.Make<duckdb::PhysicalProjection>(
    std::move(types), std::move(pass2_exprs), pass1.estimated_cardinality);
  pass2.children.push_back(pass1);
  return pass2;
}

std::vector<duckdb::idx_t> ComputeKeptViewPositions(
  const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>&
    parsed_index_exprs,
  const duckdb::ViewColumnInfo& column_info) {
  std::vector<duckdb::idx_t> kept;
  auto add = [&](std::string_view name) {
    for (size_t i = 0; i < column_info.names.size(); ++i) {
      if (column_info.names[i] == name) {
        kept.push_back(i);
        break;
      }
    }
  };
  auto collect = [&](this auto& self,
                     const duckdb::ParsedExpression& e) -> void {
    if (e.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      add(e.Cast<duckdb::ColumnRefExpression>().GetColumnName());
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

}  // namespace

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();

  // Two-pass projection: see ResolveDefaultsWithGenerated comment for why
  // we don't reuse upstream's single-pass ResolveDefaultsProjection here.
  if (!op.column_index_map.empty() && plan) {
    plan = &ResolveDefaultsWithGenerated(planner, op, *plan);
  }

  if (plan) {
    plan = &planner.Make<SereneDBPhysicalProgress>(
      *plan, table_entry.GetSereneDBTable()->GetId());
  }

  const auto on_conflict_action = op.on_conflict_info.action_type;

  const bool parallel_streaming_insert =
    plan &&
    !duckdb::PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan) &&
    !op.return_chunk && on_conflict_action == duckdb::OnConflictAction::THROW &&
    duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads() > 1;
  auto store_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  // Facade-bound CHECK constraints can reference facade-only functions
  // (bound with macros expanded); the store mirror demotes those, so they
  // ride the insert from here. Column layouts match by construction.
  for (auto& constraint : op.bound_constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      store_constraints.push_back(std::move(constraint));
    }
  }
  auto& insert = planner.Make<duckdb::PhysicalInsert>(
    op.types, store_entry, std::move(store_constraints),
    std::move(op.expressions), std::move(op.on_conflict_info.set_columns),
    std::move(op.on_conflict_info.set_types), op.estimated_cardinality,
    op.return_chunk, parallel_streaming_insert, on_conflict_action,
    std::move(op.on_conflict_info.on_conflict_condition),
    std::move(op.on_conflict_info.do_update_condition),
    std::move(op.on_conflict_info.on_conflict_filter),
    std::move(op.on_conflict_info.columns_to_fetch),
    op.on_conflict_info.update_is_del_and_insert);
  if (plan) {
    insert.children.push_back(*plan);
  }
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();

  auto& bound_ref = op.expressions[0]->Cast<duckdb::BoundReferenceExpression>();
  auto store_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  auto& del = planner.Make<duckdb::PhysicalDelete>(
    op.types, store_entry, store_entry.GetStorage(),
    std::move(store_constraints), bound_ref.index, op.estimated_cardinality,
    op.return_chunk, std::move(op.return_columns));
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();

  auto store_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  auto& update = planner.Make<duckdb::PhysicalUpdate>(
    op.types, store_entry, store_entry.GetStorage(), op.columns,
    std::move(op.expressions), std::move(op.bound_defaults),
    std::move(store_constraints), op.estimated_cardinality, op.return_chunk);
  auto& cast_update = update.Cast<duckdb::PhysicalUpdate>();
  cast_update.update_is_del_and_insert = op.update_is_del_and_insert;
  cast_update.children.push_back(plan);
  return update;
}

namespace {

duckdb::unique_ptr<duckdb::MergeIntoOperator> PlanStoreMergeIntoAction(
  duckdb::ClientContext& context, duckdb::LogicalMergeInto& op,
  duckdb::PhysicalPlanGenerator& planner, duckdb::BoundMergeIntoAction& action,
  duckdb::DuckTableEntry& store_entry) {
  auto result = duckdb::make_uniq<duckdb::MergeIntoOperator>();

  result->action_type = action.action_type;
  result->condition = std::move(action.condition);
  auto bound_constraints =
    duckdb::Binder::BindConstraints(context, store_entry.GetConstraints(),
                                    store_entry.name, store_entry.GetColumns());
  auto return_types = op.types;
  if (op.return_chunk) {
    // For RETURNING the last column is the merge_action, added by the merge
    // operator itself.
    return_types.pop_back();
  }

  auto cardinality = op.EstimateCardinality(context);
  switch (action.action_type) {
    case duckdb::MergeActionType::MERGE_UPDATE: {
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> defaults;
      for (auto& def : op.bound_defaults) {
        defaults.push_back(def->Copy());
      }
      result->op = planner.Make<duckdb::PhysicalUpdate>(
        std::move(return_types), store_entry, store_entry.GetStorage(),
        std::move(action.columns), std::move(action.expressions),
        std::move(defaults), std::move(bound_constraints), cardinality,
        op.return_chunk);
      auto& cast_update = result->op->Cast<duckdb::PhysicalUpdate>();
      cast_update.update_is_del_and_insert = action.update_is_del_and_insert;
      break;
    }
    case duckdb::MergeActionType::MERGE_DELETE: {
      duckdb::vector<duckdb::idx_t> return_columns = op.delete_return_columns;
      result->op = planner.Make<duckdb::PhysicalDelete>(
        std::move(return_types), store_entry, store_entry.GetStorage(),
        std::move(bound_constraints), op.row_id_start, cardinality,
        op.return_chunk, std::move(return_columns));
      break;
    }
    case duckdb::MergeActionType::MERGE_INSERT: {
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> set_expressions;
      duckdb::vector<duckdb::PhysicalIndex> set_columns;
      duckdb::vector<duckdb::LogicalType> set_types;
      duckdb::unordered_set<duckdb::column_t> on_conflict_filter;
      duckdb::vector<duckdb::column_t> columns_to_fetch;

      result->op = planner.Make<duckdb::PhysicalInsert>(
        std::move(return_types), store_entry, std::move(bound_constraints),
        std::move(set_expressions), std::move(set_columns),
        std::move(set_types), cardinality, op.return_chunk, !op.return_chunk,
        duckdb::OnConflictAction::THROW, nullptr, nullptr,
        std::move(on_conflict_filter), std::move(columns_to_fetch), false);
      // Transform expressions map merge-join output -> table columns.
      if (!action.column_index_map.empty()) {
        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> new_exprs;
        for (auto& col : op.table.GetColumns().Physical()) {
          auto storage_idx = col.StorageOid();
          auto mapped = action.column_index_map[col.Physical()];
          if (mapped == duckdb::DConstants::INVALID_INDEX) {
            new_exprs.push_back(op.bound_defaults[storage_idx]->Copy());
          } else {
            new_exprs.push_back(std::move(action.expressions[mapped]));
          }
        }
        action.expressions = std::move(new_exprs);
      }
      result->expressions = std::move(action.expressions);
      break;
    }
    case duckdb::MergeActionType::MERGE_ERROR:
      result->expressions = std::move(action.expressions);
      break;
    case duckdb::MergeActionType::MERGE_DO_NOTHING:
      break;
    default:
      SDB_THROW(ERROR_BAD_PARAMETER,
                "Unsupported MERGE INTO action type for SereneDB");
  }
  return result;
}

}  // namespace

duckdb::PhysicalOperator& SereneDBCatalog::PlanMergeInto(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) {
  // DuckDB routes INSERT ON CONFLICT through MergeInto as well; this is a
  // near-copy of DuckCatalog::PlanMergeInto targeting the store table.
  auto& table_entry = RequireBaseTable(op.table);
  auto& store_entry =
    table_entry.ResolveStoreEntry(context).Cast<duckdb::DuckTableEntry>();

  std::map<duckdb::MergeActionCondition,
           duckdb::vector<duckdb::unique_ptr<duckdb::MergeIntoOperator>>>
    actions;

  duckdb::idx_t append_count = 0;
  for (auto& [condition, action_list] : op.actions) {
    duckdb::vector<duckdb::unique_ptr<duckdb::MergeIntoOperator>>
      planned_actions;
    for (auto& action : action_list) {
      if (action->action_type == duckdb::MergeActionType::MERGE_INSERT) {
        ++append_count;
      }
      if (action->action_type == duckdb::MergeActionType::MERGE_UPDATE &&
          action->update_is_del_and_insert) {
        ++append_count;
      }
      planned_actions.push_back(
        PlanStoreMergeIntoAction(context, op, planner, *action, store_entry));
    }
    actions.emplace(condition, std::move(planned_actions));
  }

  const bool parallel = append_count <= 1 && !op.return_chunk;

  auto& merge = planner.Make<duckdb::PhysicalMergeInto>(
    op.types, std::move(actions), op.row_id_start, op.source_marker, parallel,
    op.return_chunk);
  merge.children.push_back(plan);
  return merge;
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::CatalogEntry& target,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
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
    auto snapshot = GetSereneDBContext(binder.context).EnsureCatalogSnapshot();
    auto relation_obj =
      snapshot->GetRelation(catalog::NoAccessCheck(), GetDatabaseId(),
                            target.ParentSchema().name, target.name);
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
                      ERR_MSG("view \"", target.name,
                              "\" must be bound before it can be indexed"));
    }
    if (kept_view_positions) {
      rel_columns.reserve(kept_view_positions->size());
      for (auto p : *kept_view_positions) {
        rel_columns.emplace_back(column_info->names[p], column_info->types[p]);
      }
    } else {
      rel_columns.assign_range(
        std::views::iota(size_t{0}, column_info->names.size()) |
        std::views::transform([&](size_t i) {
          return std::pair{column_info->names[i], column_info->types[i]};
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
      add_column(e.Cast<duckdb::ColumnRefExpression>().GetColumnName());
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
    create_index_info->schema = resolved_table->schema.name;
    create_index_info->catalog = resolved_table->catalog.GetName();

    duckdb::IndexBinder index_binder(binder, binder.context, resolved_table,
                                     create_index_info.get());
    for (auto& parsed : create_index_info->expressions) {
      expressions.emplace_back(index_binder.Bind(parsed));
    }
  } else {
    create_index_info->names.assign_range(rel_columns | std::views::keys);
    create_index_info->schema = target.ParentSchema().name;
    create_index_info->catalog = target.ParentCatalog().GetName();
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
          auto col_idx = cref.binding.column_index.GetIndex();
          if (kept_view_positions) {
            auto it = std::ranges::lower_bound(*kept_view_positions, col_idx);
            SDB_ASSERT(it != kept_view_positions->end() && *it == col_idx,
                       "view col ref references a non-kept position");
            col_idx = static_cast<duckdb::idx_t>(
              std::distance(kept_view_positions->begin(), it));
          }
          cref.binding = duckdb::ColumnBinding(
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
  return {};
}

}  // namespace sdb::connector
