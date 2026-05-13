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

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/operator/order/physical_order.hpp>
#include <duckdb/execution/operator/persistent/physical_merge_into.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/database_size.hpp>
#include <ranges>

#include "catalog/catalog.h"
#include "catalog/pk_spec.h"
#include "catalog/schema.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_delete.h"
#include "connector/duckdb_physical_insert.h"
#include "connector/duckdb_physical_sst_insert.h"
#include "connector/duckdb_physical_truncate.h"
#include "connector/duckdb_physical_update.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
namespace sdb::connector {
namespace {

// DROP FUNCTION name(type, ...) -- selective overload removal.
// Fetches the existing PgSqlFunction, finds the matching overload by
// parameter signature, and either removes just that overload (updating the
// stored function) or drops the whole function if it was the last one.
// DROP FUNCTION name(type, ...) -- selective overload removal.
Result DropFunctionOverload(catalog::LogicalCatalog& catalog,
                            duckdb::ClientContext& context,
                            duckdb::DropInfo& info) {
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info.catalog);
  if (!db) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(database_id, info.schema, info.name);
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
    return catalog.DropFunction(info.catalog, info.schema, info.name);
  }

  // Remove just the matched overload and update the stored function.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      macro_info.Copy());
  new_info->macros.erase(new_info->macros.begin() + match_idx);
  // Align catalog_type with the surviving macros: MACRO_ENTRY iff all
  // remaining macros are scalar, else TABLE_MACRO_ENTRY. Prevents a
  // mismatched catalog bucket (e.g. a TableMacroFunction left in a
  // MACRO_ENTRY bucket) which breaks Cast<>() at lookup time.
  bool all_scalar = !new_info->macros.empty();
  for (const auto& m : new_info->macros) {
    if (m->type != duckdb::MacroType::SCALAR_MACRO) {
      all_scalar = false;
      break;
    }
  }
  new_info->type = all_scalar ? duckdb::CatalogType::MACRO_ENTRY
                              : duckdb::CatalogType::TABLE_MACRO_ENTRY;

  auto function = std::make_shared<catalog::PgSqlFunction>(
    database_id, ObjectId{}, info.name, std::move(new_info));
  return catalog.CreateFunction(database_id, info.schema, function, true);
}

// DROP FUNCTION/PROCEDURE name -- drop overloads matching the drop kind.
// PG: DROP FUNCTION drops only function overloads, DROP PROCEDURE drops only
// procedure overloads. If mixed (func + proc under same name), keep the other.
Result DropFunctionByKind(catalog::LogicalCatalog& catalog,
                          const duckdb::DropInfo& info) {
  auto snapshot = catalog.GetCatalogSnapshot();
  auto db = snapshot->GetDatabase(info.catalog);
  if (!db) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  auto database_id = db->GetId();
  auto existing = snapshot->GetFunction(database_id, info.schema, info.name);
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
    return catalog.DropFunction(info.catalog, info.schema, info.name);
  }
  // Mixed: remove only matching overloads, keep the rest.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      existing->GetInfo().Copy());
  std::erase_if(new_info->macros, [&](const auto& m) {
    return m->is_procedure == info.is_procedure;
  });
  // Align catalog_type with the surviving macros (see DropFunctionOverload).
  bool all_scalar = !new_info->macros.empty();
  for (const auto& m : new_info->macros) {
    if (m->type != duckdb::MacroType::SCALAR_MACRO) {
      all_scalar = false;
      break;
    }
  }
  new_info->type = all_scalar ? duckdb::CatalogType::MACRO_ENTRY
                              : duckdb::CatalogType::TABLE_MACRO_ENTRY;

  auto function = std::make_shared<catalog::PgSqlFunction>(
    database_id, ObjectId{}, info.name, std::move(new_info));
  return catalog.CreateFunction(database_id, info.schema, function, true);
}

}  // namespace

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  Result r;
  switch (info.type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      r = catalog.DropTable(info.catalog, info.schema, info.name);
      break;
    case INDEX_ENTRY:
      r = catalog.DropIndex(info.catalog, info.schema, info.name);
      break;
    case VIEW_ENTRY:
      r = catalog.DropView(info.catalog, info.schema, info.name);
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      if (info.has_func_args) {
        r = DropFunctionOverload(catalog, context, info);
      } else {
        r = DropFunctionByKind(catalog, info);
      }
      break;
    case TYPE_ENTRY:
      r = catalog.DropType(info.catalog, info.schema, info.name);
      break;
    case SEQUENCE_ENTRY: {
      bool if_exists =
        info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
      r = catalog.DropSequence(info.catalog, info.schema, info.name, if_exists);
      if (r.is(ERROR_BAD_PARAMETER)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("cannot drop sequence ", info.name,
                  " because other objects depend on it"),
          ERR_DETAIL(r.errorMessage()),
          ERR_HINT("Use DROP TABLE on the owning table to drop the sequence "
                   "as a side-effect."));
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
        r = catalog.DropSchema(info.catalog, info.name, info.cascade);
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
  // PG: schemas pre-populated by the system catalog (e.g. information_schema)
  // shadow the user catalog. Reject creation if a system schema with the same
  // name already exists.
  if (transaction.HasContext()) {
    auto& system = duckdb::Catalog::GetSystemCatalog(*transaction.context);
    auto existing = system.GetSchema(*transaction.context, info.schema,
                                     duckdb::OnEntryNotFound::RETURN_NULL);
    bool if_not_exists =
      info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    if (existing) {
      if (if_not_exists) {
        return nullptr;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                      ERR_MSG("schema \"", info.schema, "\" already exists"));
    }
  }
  auto& catalog_impl = catalog::GetCatalog();
  auto schema = std::make_shared<catalog::Schema>(
    GetDatabaseId(), catalog::SchemaOptions{.name = info.schema});
  auto r = catalog_impl.CreateSchema(GetDatabaseId(), std::move(schema));
  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
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

// --- View-backed CREATE INDEX column pruning ---------------------------
//
// On the fast-path view-backed branch, the binder produces a plan that
// exposes every column the view declares. For a wide view that the index
// only partially uses, the leaf LogicalGet reads (and the underlying
// source fetches) columns the backfill never consumes. We prune those
// columns at two levels:
//   - the outermost LogicalProjection (the one whose output IS the view's
//     declared schema): drop expressions for view columns not in the
//     needed set.
//   - the leaf LogicalGet: drop leaf positions that no surviving
//     expression in the chain references.
// Compacting either level shifts positions, so wrapper bindings on the
// chain get rewritten in lockstep.
//
// v1 only handles the simple chain shape `LogicalProjection -> [filter]*
// -> LogicalGet`. Other shapes (ORDER BY/LIMIT above the projection,
// nested projections) fall through unchanged.

constexpr duckdb::idx_t kInvalidNewPos =
  std::numeric_limits<duckdb::idx_t>::max();

// Visit (mutable) every BoundColumnRefExpression in a single expression.
template<typename Visit>
void VisitLeafRefs(duckdb::unique_ptr<duckdb::Expression>& expr,
                   Visit&& visit) {
  if (!expr) {
    return;
  }
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundColumnRefExpression>(
    expr, [&](duckdb::BoundColumnRefExpression& ref,
              duckdb::unique_ptr<duckdb::Expression>&) { visit(ref); });
}

// Walk each expression of a fast-path wrapper. Restricted to the
// allowlist (FILTER/ORDER_BY/LIMIT/TOP_N/PROJECTION).
template<typename Visit>
void VisitWrapperExpressions(duckdb::LogicalOperator& op, Visit&& visit) {
  switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      for (auto& e : op.expressions) {
        visit(e);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& o : op.Cast<duckdb::LogicalOrder>().orders) {
        visit(o.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& o : op.Cast<duckdb::LogicalTopN>().orders) {
        visit(o.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_LIMIT:
      // No column-ref expressions of interest.
      break;
    default:
      SDB_ASSERT(false, "unexpected fast-path wrapper type: ",
                 static_cast<int>(op.type));
  }
}

// Returns the outermost LogicalProjection on the chain starting at
// `chain_root` and ending strictly above `leaf_get`. Returns nullptr if
// no projection exists in that span or the chain shape isn't single-child
// all the way down.
duckdb::LogicalProjection* FindOutermostViewProjection(
  duckdb::LogicalOperator& chain_root, duckdb::LogicalGet& leaf_get) {
  auto* cur = &chain_root;
  while (cur && cur != &leaf_get) {
    if (cur->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return &cur->Cast<duckdb::LogicalProjection>();
    }
    if (cur->children.size() != 1) {
      return nullptr;
    }
    cur = cur->children[0].get();
  }
  return nullptr;
}

// True iff a LogicalProjection appears strictly between `outer_proj` and
// `leaf_get`. v1 rejects nested projections.
bool HasNestedProjectionBelow(duckdb::LogicalProjection& outer_proj,
                              duckdb::LogicalGet& leaf_get) {
  if (outer_proj.children.empty()) {
    return false;
  }
  auto* cur = outer_proj.children[0].get();
  while (cur && cur != &leaf_get) {
    if (cur->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return true;
    }
    if (cur->children.size() != 1) {
      return true;  // unexpected shape
    }
    cur = cur->children[0].get();
  }
  return false;
}

// Prune unused view columns from the fast-path view-backed CREATE INDEX
// chain. Mutates the plan tree: compacts leaf_get's column_ids, removes
// expressions from the outermost projection, and rewrites leaf-bound
// column references everywhere they appear in surviving expressions.
//
// `index_view_positions`: positions into the view's declared column list
// that the index keys on.
// `view_decl_size`: total view-declared column count.
//
// Returns the sorted kept view positions on success. Returns nullopt if
// the chain shape isn't supported in v1 (no projection / nested
// projections / wrappers above the outermost projection) -- caller keeps
// all view positions in that case.
std::optional<std::vector<duckdb::idx_t>> PruneViewFastPathChain(
  duckdb::LogicalOperator& chain_root, duckdb::LogicalGet& leaf_get,
  std::span<const duckdb::idx_t> index_view_positions,
  duckdb::idx_t view_decl_size) {
  auto* outer_proj = FindOutermostViewProjection(chain_root, leaf_get);
  if (!outer_proj) {
    return std::nullopt;
  }
  if (HasNestedProjectionBelow(*outer_proj, leaf_get)) {
    return std::nullopt;
  }
  SDB_ASSERT(outer_proj->expressions.size() == view_decl_size,
             "outermost projection expressions count ",
             outer_proj->expressions.size(),
             " does not match view declared schema size ", view_decl_size);

  // Walks each wrapper strictly above outer_proj and applies `visit` once
  // per wrapper. Used for both collecting and rewriting view-pos refs in
  // ORDER BY / LIMIT / TOP_N above the projection (CREATE INDEX has no
  // WHERE clause today, so FILTER above the projection isn't producible
  // from SQL -- but if it ever is, the allowlist handles it for free).
  auto walk_above_outer = [&](auto&& visit) {
    auto* cur = &chain_root;
    while (cur != outer_proj) {
      visit(*cur);
      SDB_ASSERT(cur->children.size() == 1);
      cur = cur->children[0].get();
    }
  };

  // 1. Compute the needed view-position set.
  containers::FlatHashSet<duckdb::idx_t> needed_view;
  needed_view.reserve(index_view_positions.size());
  for (auto p : index_view_positions) {
    SDB_ASSERT(p < view_decl_size, "CREATE INDEX view position ", p,
               " >= view_decl_size ", view_decl_size);
    needed_view.insert(p);
  }
  // Wrappers above outer_proj reference view positions through
  // outer_proj->table_index; those positions must survive the prune.
  walk_above_outer([&](duckdb::LogicalOperator& op) {
    VisitWrapperExpressions(op, [&](duckdb::unique_ptr<duckdb::Expression>& e) {
      if (!e) {
        return;
      }
      duckdb::ExpressionIterator::VisitExpression<
        duckdb::BoundColumnRefExpression>(
        *e, [&](const duckdb::BoundColumnRefExpression& ref) {
          if (ref.binding.table_index == outer_proj->table_index) {
            needed_view.insert(ref.binding.column_index.GetIndex());
          }
        });
    });
  });

  // 2. Collect leaf refs from kept projection expressions + wrappers
  // below the projection (FILTER predicates etc.).
  containers::FlatHashSet<duckdb::idx_t> needed_leaf;
  auto collect_leaf_refs_in_expr = [&](const duckdb::Expression& expr) {
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundColumnRefExpression>(
      expr, [&](const duckdb::BoundColumnRefExpression& ref) {
        if (ref.binding.table_index == leaf_get.table_index) {
          needed_leaf.insert(ref.binding.column_index.GetIndex());
        }
      });
  };
  for (auto v : needed_view) {
    SDB_ASSERT(v < outer_proj->expressions.size());
    collect_leaf_refs_in_expr(*outer_proj->expressions[v]);
  }
  if (!outer_proj->children.empty()) {
    auto* cur = outer_proj->children[0].get();
    while (cur && cur != &leaf_get) {
      VisitWrapperExpressions(*cur,
                              [&](duckdb::unique_ptr<duckdb::Expression>& e) {
                                if (e) {
                                  collect_leaf_refs_in_expr(*e);
                                }
                              });
      if (cur->children.size() != 1) {
        break;
      }
      cur = cur->children[0].get();
    }
  }

  const auto leaf_orig_size = leaf_get.GetColumnIds().size();
  std::vector<duckdb::idx_t> kept_leaf;
  kept_leaf.reserve(needed_leaf.size());
  for (auto p : needed_leaf) {
    SDB_ASSERT(p < leaf_orig_size, "wrapper leaf ref ", p,
               " >= leaf_orig_size ", leaf_orig_size);
    kept_leaf.push_back(p);
  }
  absl::c_sort(kept_leaf);

  std::vector<duckdb::idx_t> kept_view;
  kept_view.reserve(needed_view.size());
  for (auto v : needed_view) {
    kept_view.push_back(v);
  }
  absl::c_sort(kept_view);

  // No-op when everything is kept.
  if (kept_leaf.size() == leaf_orig_size &&
      kept_view.size() == view_decl_size) {
    return std::nullopt;
  }

  // 3. Compact the leaf if needed.
  if (kept_leaf.size() < leaf_orig_size) {
    std::vector<duckdb::idx_t> leaf_old_to_new(leaf_orig_size, kInvalidNewPos);
    const auto orig_ids = leaf_get.GetColumnIds();  // copy
    duckdb::vector<duckdb::ColumnIndex> new_ids;
    duckdb::vector<duckdb::LogicalType> new_types;
    new_ids.reserve(kept_leaf.size());
    new_types.reserve(kept_leaf.size());
    for (duckdb::idx_t i = 0; i < kept_leaf.size(); ++i) {
      new_ids.push_back(orig_ids[kept_leaf[i]]);
      new_types.push_back(leaf_get.GetColumnType(orig_ids[kept_leaf[i]]));
      leaf_old_to_new[kept_leaf[i]] = i;
    }
    leaf_get.SetColumnIds(std::move(new_ids));
    leaf_get.types = std::move(new_types);

    auto rewrite = [&](duckdb::unique_ptr<duckdb::Expression>& e) {
      VisitLeafRefs(e, [&](duckdb::BoundColumnRefExpression& ref) {
        if (ref.binding.table_index != leaf_get.table_index) {
          return;
        }
        auto old_pos = ref.binding.column_index.GetIndex();
        SDB_ASSERT(old_pos < leaf_old_to_new.size());
        auto new_pos = leaf_old_to_new[old_pos];
        SDB_ASSERT(new_pos != kInvalidNewPos,
                   "leaf binding rewrite hit a dropped column at old "
                   "position ",
                   old_pos);
        ref.binding.column_index = duckdb::ProjectionIndex{new_pos};
      });
    };
    for (auto v : kept_view) {
      rewrite(outer_proj->expressions[v]);
    }
    if (!outer_proj->children.empty()) {
      auto* cur = outer_proj->children[0].get();
      while (cur && cur != &leaf_get) {
        VisitWrapperExpressions(*cur, rewrite);
        if (cur->children.size() != 1) {
          break;
        }
        cur = cur->children[0].get();
      }
    }
  }

  // 4. Prune projection expressions to kept view positions and rewrite
  // view-pos refs in wrappers above the projection.
  if (kept_view.size() < view_decl_size) {
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> new_exprs;
    new_exprs.reserve(kept_view.size());
    for (auto v : kept_view) {
      new_exprs.push_back(std::move(outer_proj->expressions[v]));
    }
    outer_proj->expressions = std::move(new_exprs);
    outer_proj->ResolveOperatorTypes();

    std::vector<duckdb::idx_t> view_old_to_new(view_decl_size, kInvalidNewPos);
    for (duckdb::idx_t i = 0; i < kept_view.size(); ++i) {
      view_old_to_new[kept_view[i]] = i;
    }
    walk_above_outer([&](duckdb::LogicalOperator& op) {
      VisitWrapperExpressions(
        op, [&](duckdb::unique_ptr<duckdb::Expression>& e) {
          VisitLeafRefs(e, [&](duckdb::BoundColumnRefExpression& ref) {
            if (ref.binding.table_index != outer_proj->table_index) {
              return;
            }
            auto old_pos = ref.binding.column_index.GetIndex();
            SDB_ASSERT(old_pos < view_old_to_new.size());
            auto new_pos = view_old_to_new[old_pos];
            SDB_ASSERT(new_pos != kInvalidNewPos,
                       "view-pos binding rewrite hit a dropped position ",
                       old_pos);
            ref.binding.column_index = duckdb::ProjectionIndex{new_pos};
          });
        });
    });
  }

  return kept_view;
}

}  // namespace

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  // Two-pass projection: see ResolveDefaultsWithGenerated comment for why
  // we don't reuse upstream's single-pass ResolveDefaultsProjection here.
  if (!op.column_index_map.empty()) {
    plan = &ResolveDefaultsWithGenerated(planner, op, *plan);
  }

  // Resolve on-conflict action: when no explicit ON CONFLICT clause was given
  // (action is THROW by default), override with the session-level
  // sdb_write_conflict_policy setting.
  // TODO(gnusi): use duckdb enum
  auto on_conflict_action = op.on_conflict_info.action_type;
  if (on_conflict_action == duckdb::OnConflictAction::THROW) {
    switch (GetSereneDBContext(context).GetWriteConflictPolicy()) {
      case WriteConflictPolicy::EmitError:
        on_conflict_action = duckdb::OnConflictAction::THROW;
        break;
      case WriteConflictPolicy::DoNothing:
        on_conflict_action = duckdb::OnConflictAction::NOTHING;
        break;
      case WriteConflictPolicy::Replace:
        on_conflict_action = duckdb::OnConflictAction::REPLACE;
        break;
    }
  }

  // Use SST bulk insert for COPY FROM / INSERT...SELECT (has child plan).
  // SST bypasses transactions -- no conflict detection or constraint checks.
  // Fall back to regular insert when there are constraints to enforce.
  const bool use_sst = plan != nullptr &&
                       on_conflict_action == duckdb::OnConflictAction::THROW &&
                       op.bound_constraints.empty();

  if (use_sst) {
    auto* sorted_plan = plan.get();

    // For explicit PKs, add a Sort by PK columns before SST insert.
    // SST requires keys in ascending order. Generated PKs are monotonic
    // (no sort needed).
    const auto& pk_col_ids = sdb_table->PKColumns();
    if (!pk_col_ids.empty()) {
      const auto& columns = sdb_table->Columns();
      duckdb::vector<duckdb::BoundOrderByNode> orders;
      duckdb::vector<duckdb::idx_t> projections;

      // Sort by PK columns (ascending, nulls first)
      for (auto pk_id : pk_col_ids) {
        for (size_t i = 0; i < columns.size(); ++i) {
          if (columns[i].id == pk_id) {
            auto col_expr =
              duckdb::make_uniq_base<duckdb::Expression,
                                     duckdb::BoundReferenceExpression>(
                columns[i].type, i);
            orders.emplace_back(duckdb::OrderType::ASCENDING,
                                duckdb::OrderByNullType::NULLS_FIRST,
                                std::move(col_expr));
            break;
          }
        }
      }

      // Project all input columns through the sort
      for (duckdb::idx_t i = 0; i < plan->types.size(); ++i) {
        projections.push_back(i);
      }

      auto& sort = planner.Make<duckdb::PhysicalOrder>(
        plan->types, std::move(orders), std::move(projections),
        op.estimated_cardinality, true);
      sort.children.push_back(*plan);
      sorted_plan = &sort;
    }

    auto& insert = planner.Make<SereneDBPhysicalSSTInsert>(
      std::move(sdb_table), std::move(op.types), op.estimated_cardinality);
    insert.children.push_back(*sorted_plan);
    return insert;
  }

  auto& insert = planner.Make<SereneDBPhysicalInsert>(
    std::move(sdb_table), std::move(op.types), op.estimated_cardinality,
    on_conflict_action, std::move(op.bound_constraints));
  if (plan) {
    insert.children.push_back(*plan);
  }
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  if (op.is_truncate && context.transaction.IsAutoCommit()) {
    return planner.Make<SereneDBPhysicalTruncate>(std::move(sdb_table),
                                                  op.estimated_cardinality);
  }

  // Child output layout (from GetRowIdColumns):
  //   [..., pk_0, pk_1, ..., idx_col_0, idx_col_1, ..., rowid]
  // rowid is last, before it are indexed columns, before those are PK columns.
  const auto& pk_col_ids = sdb_table->PKColumns();
  const auto& idx_col_indices = table_entry.GetIndexedColumnIndices();
  auto num_pk = pk_col_ids.size();

  // Count non-PK indexed columns
  const auto& columns = sdb_table->Columns();
  containers::FlatHashSet<size_t> pk_set;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        pk_set.insert(i);
        break;
      }
    }
  }
  std::vector<size_t> non_pk_idx;
  for (auto idx : idx_col_indices) {
    if (!pk_set.contains(idx)) {
      non_pk_idx.push_back(idx);
    }
  }
  auto num_idx = non_pk_idx.size();
  auto num_virtual = num_pk + num_idx + 1;  // +1 for rowid
  auto child_cols = plan.types.size();

  // PK columns at [child_cols - num_virtual .. child_cols - num_virtual +
  // num_pk - 1]
  std::vector<duckdb::idx_t> pk_indices;
  if (pk_col_ids.empty()) {
    // No explicit PK -- the rowid (last column) carries the generated PK.
    pk_indices.push_back(child_cols - 1);
  } else {
    for (size_t i = 0; i < num_pk; ++i) {
      pk_indices.push_back(child_cols - num_virtual + i);
    }
  }

  // Indexed columns at [child_cols - num_virtual + num_pk .. child_cols - 2]
  std::vector<duckdb::idx_t> indexed_indices;
  for (size_t i = 0; i < num_idx; ++i) {
    indexed_indices.push_back(child_cols - num_virtual + num_pk + i);
  }

  auto& del = planner.Make<SereneDBPhysicalDelete>(
    std::move(sdb_table), std::move(pk_indices), std::move(indexed_indices),
    op.estimated_cardinality);
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();

  // Wrap `plan` with a PhysicalProjection that resolves VALUE_DEFAULT and
  // passes through virtuals, so the update operator only sees:
  //   [resolved SET vals, pk_virtuals, idx_virtuals, rowid]
  const auto& pk_col_ids = sdb_table->PKColumns();
  const auto& idx_col_indices = table_entry.GetIndexedColumnIndices();
  auto num_pk = pk_col_ids.size();

  const auto& columns = sdb_table->Columns();
  containers::FlatHashSet<size_t> pk_set;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        pk_set.insert(i);
        break;
      }
    }
  }
  std::vector<size_t> non_pk_idx;
  for (auto idx : idx_col_indices) {
    if (!pk_set.contains(idx)) {
      non_pk_idx.push_back(idx);
    }
  }
  auto num_idx = non_pk_idx.size();
  auto num_virtual = num_pk + num_idx + 1;  // +1 for rowid
  auto child_cols = plan.types.size();

  const auto num_updates = op.expressions.size();
  duckdb::vector<duckdb::LogicalType> proj_types;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  proj_types.reserve(num_updates + num_virtual);
  proj_exprs.reserve(num_updates + num_virtual);

  // phys -> expression that yields its post-update value. Built once, used
  // by the gen-col branch below to rewrite BoundRef leaves in O(1).
  // Gen-col sentinels (neither VALUE_DEFAULT nor BOUND_REF) aren't indexed
  // here -- CheckBinder's transitive inlining guarantees gen-col bound
  // expressions never have other gen cols as leaves.
  duckdb::physical_index_map_t<const duckdb::Expression*> dep_source;
  for (duckdb::idx_t j = 0; j < op.columns.size(); ++j) {
    const auto t = op.expressions[j]->GetExpressionType();
    if (t == duckdb::ExpressionType::VALUE_DEFAULT) {
      dep_source[op.columns[j]] = op.bound_defaults[op.columns[j].index].get();
    } else if (t == duckdb::ExpressionType::BOUND_REF) {
      dep_source[op.columns[j]] = op.expressions[j].get();
    }
  }

  for (duckdb::idx_t i = 0; i < num_updates; ++i) {
    auto& expr = op.expressions[i];
    const auto t = expr->GetExpressionType();
    if (t == duckdb::ExpressionType::VALUE_DEFAULT) {
      auto phys = op.columns[i].index;
      SDB_ASSERT(phys < op.bound_defaults.size());
      proj_types.push_back(op.bound_defaults[phys]->return_type);
      proj_exprs.push_back(op.bound_defaults[phys]->Copy());
    } else if (t == duckdb::ExpressionType::BOUND_REF) {
      proj_types.push_back(expr->return_type);
      proj_exprs.push_back(expr->Copy());
    } else {
      // STORED gen-col recompute: placeholder from BindUpdateConstraints;
      // the real expression lives in bound_defaults. Rewrite each
      // BoundRef(dep_phys) leaf to the dep's post-update source.
      auto phys = op.columns[i].index;
      SDB_ASSERT(phys < op.bound_defaults.size());
      auto bound_copy = op.bound_defaults[phys]->Copy();
      duckdb::ExpressionIterator::VisitExpressionClassMutable(
        bound_copy, duckdb::ExpressionClass::BOUND_REF,
        [&](duckdb::unique_ptr<duckdb::Expression>& e) {
          auto dep = duckdb::PhysicalIndex(
            e->Cast<duckdb::BoundReferenceExpression>().index);
          auto it = dep_source.find(dep);
          SDB_ASSERT(it != dep_source.end());
          e = it->second->Copy();
        });
      proj_types.push_back(bound_copy->return_type);
      proj_exprs.push_back(std::move(bound_copy));
    }
  }

  // Passthrough virtual columns (PKs, indexed, rowid).
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
    // No explicit PK -- the rowid (last column after projection) carries the
    // generated PK. After projection: [SET_vals..., virtuals..., rowid].
    pk_indices.push_back(num_updates + num_virtual - 1);
  } else {
    pk_indices.reserve(num_pk);
    for (size_t i = 0; i < num_pk; ++i) {
      pk_indices.push_back(num_updates + i);
    }
  }
  std::vector<duckdb::idx_t> indexed_indices;
  indexed_indices.reserve(num_idx);
  for (size_t i = 0; i < num_idx; ++i) {
    indexed_indices.push_back(num_updates + num_pk + i);
  }

  auto& upd = planner.Make<SereneDBPhysicalUpdate>(
    std::move(sdb_table), std::move(pk_indices), std::move(op.columns),
    std::move(indexed_indices), op.estimated_cardinality,
    std::move(op.bound_constraints));
  upd.children.push_back(proj);
  return upd;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanMergeInto(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) {
  // DuckDB routes INSERT ON CONFLICT through MergeInto.
  // Create a PhysicalMergeInto that wraps our SereneDB operators.
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();
  auto cardinality = op.EstimateCardinality(context);

  std::map<duckdb::MergeActionCondition,
           duckdb::vector<duckdb::unique_ptr<duckdb::MergeIntoOperator>>>
    actions;

  for (auto& [condition, action_list] : op.actions) {
    duckdb::vector<duckdb::unique_ptr<duckdb::MergeIntoOperator>>
      planned_actions;
    for (auto& action : action_list) {
      auto result = duckdb::make_uniq<duckdb::MergeIntoOperator>();
      result->action_type = action->action_type;
      result->condition = std::move(action->condition);

      switch (action->action_type) {
        case duckdb::MergeActionType::MERGE_INSERT: {
          duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
            bound_constraints;
          for (auto& c : op.bound_constraints) {
            bound_constraints.push_back(c->Copy());
          }
          // Use NOTHING policy: MergeInto should filter matched rows, but our
          // INSERT handles duplicates gracefully as a fallback.
          auto table_copy = sdb_table;
          result->op = &planner.Make<SereneDBPhysicalInsert>(
            std::move(table_copy), op.types, cardinality,
            duckdb::OnConflictAction::NOTHING, std::move(bound_constraints));
          // Transform expressions map merge-join output -> table columns.
          if (!action->column_index_map.empty()) {
            duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> new_exprs;
            for (auto& col : op.table.GetColumns().Physical()) {
              auto storage_idx = col.StorageOid();
              auto mapped = action->column_index_map[col.Physical()];
              if (mapped == duckdb::DConstants::INVALID_INDEX) {
                new_exprs.push_back(op.bound_defaults[storage_idx]->Copy());
              } else {
                new_exprs.push_back(std::move(action->expressions[mapped]));
              }
            }
            action->expressions = std::move(new_exprs);
          }
          result->expressions = std::move(action->expressions);
          break;
        }
        case duckdb::MergeActionType::MERGE_DO_NOTHING:
          break;
        default:
          SDB_THROW(ERROR_BAD_PARAMETER,
                    "Unsupported MERGE INTO action type for SereneDB");
      }
      planned_actions.push_back(std::move(result));
    }
    actions.emplace(condition, std::move(planned_actions));
  }

  auto& merge = planner.Make<duckdb::PhysicalMergeInto>(
    op.types, std::move(actions), op.row_id_start, op.source_marker,
    /*parallel=*/false, op.return_chunk);
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
  // Set iff PruneViewFastPathChain narrowed the view's exposed schema.
  // Holds the kept view positions in ascending order; downstream code
  // (rel_columns / column_ids / scan_types / LogicalCreateIndex input
  // expressions / SereneDBPhysicalCreateIndex view_columns) operates on
  // this pruned schema.
  std::optional<std::vector<duckdb::idx_t>> kept_view_positions;
  // PK virtual columns for the resolved fast path. Populated alongside
  // view_fast_path; reused later when stamping the _sdb_view_fast_path_pk
  // option onto create_index_info. Optional so a future refactor that
  // skips the initial population trips an assert at the second use site
  // instead of silently using an empty vector.
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
    auto relation_obj = snapshot->GetRelation(
      GetDatabaseId(), target.ParentSchema().name, target.name);
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

      // Try to prune unused view columns from the chain before we extend
      // it with PK virtuals. On success, subsequent code operates on the
      // narrowed view schema; on no-op, behaviour matches the pre-patch
      // path exactly.
      auto& view_entry = target.Cast<duckdb::ViewCatalogEntry>();
      auto column_info = view_entry.GetColumnInfo();
      if (column_info) {
        auto& create_idx_parsed =
          stmt.info->Cast<duckdb::CreateIndexInfo>().parsed_expressions;
        std::vector<duckdb::idx_t> index_view_positions;
        for (auto& expr : create_idx_parsed) {
          if (expr->GetExpressionType() != duckdb::ExpressionType::COLUMN_REF) {
            continue;
          }
          auto col_name =
            expr->Cast<duckdb::ColumnRefExpression>().GetColumnName();
          for (size_t i = 0; i < column_info->names.size(); ++i) {
            if (column_info->names[i] == col_name) {
              index_view_positions.push_back(i);
              break;
            }
          }
        }
        kept_view_positions = PruneViewFastPathChain(
          *plan, *leaf_get, index_view_positions, column_info->names.size());
      }

      const auto leaf_orig_size = leaf_get->GetColumnIds().size();
      duckdb::vector<duckdb::LogicalType> pk_types;
      pk_types.reserve(vcols.size());
      if (view_fast_path->pk_spec == catalog::PkSpec::RocksDBExplicitPK) {
        const auto& base_cols = view_fast_path->base_table->Columns();
        for (auto vcol : vcols) {
          SDB_ASSERT(vcol < base_cols.size());
          pk_types.push_back(base_cols[vcol].type);
        }
      } else if (view_fast_path->pk_spec ==
                 catalog::PkSpec::RocksDBGeneratedRowId) {
        SDB_ASSERT(vcols.size() == 1);
        pk_types.push_back(duckdb::LogicalType::BIGINT);
      } else {
        for (auto vcol : vcols) {
          if (vcol == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
            pk_types.push_back(duckdb::LogicalType::UBIGINT);
          } else {
            pk_types.push_back(duckdb::LogicalType::BIGINT);
          }
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
      throw duckdb::CatalogException("access method \"%s\" does not exist",
                                     idx_type);
    }
  }

  std::vector<std::pair<std::string, duckdb::LogicalType>> rel_columns;
  bool use_generated_pk_rowid_col = false;
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
      // Pruned view schema: only the kept view positions appear in
      // rel_columns. column_ids resolution below indexes into this
      // narrowed list, so positions are intrinsically remapped.
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
                               return std::pair{c.name, c.type};
                             }));
    use_generated_pk_rowid_col = sdb_table->PKColumns().empty();
  }

  for (auto& expr : create_index_info->parsed_expressions) {
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      auto col_name = col_ref.GetColumnName();
      for (size_t i = 0; i < rel_columns.size(); ++i) {
        if (rel_columns[i].first == col_name) {
          create_index_info->column_ids.push_back(i);
          create_index_info->scan_types.push_back(rel_columns[i].second);
          break;
        }
      }
    }
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
    // redundantly read the whole table -- and broke when any of those
    // columns were sdb_indexonly (no main-storage data to read).
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
      if (use_generated_pk_rowid_col) {
        get.AddColumnId(duckdb::COLUMN_IDENTIFIER_ROW_ID);
        get.types.push_back(duckdb::LogicalType::BIGINT);
      }
    }
    // Mark the scan as a CREATE INDEX backfill so InitCommonState relaxes
    // the read-side check on sdb_indexonly columns (the inverted index may
    // include them as indexed columns, and the backfill is the legitimate
    // path that consumes their values).
    SDB_ASSERT(get.bind_data,
               "base-table LogicalGet missing SereneDB bind_data");
    get.bind_data->Cast<SereneDBScanBindData>().is_create_index = true;
    create_index_info->names = get.names;
    create_index_info->schema = resolved_table->schema.name;
    create_index_info->catalog = resolved_table->catalog.GetName();
    // expressions[] mirrors the projection: ProjectionIndex must match
    // position in get.GetColumnIds(), which now follows `projection`.
    for (size_t pos = 0; pos < projection.size(); ++pos) {
      expressions.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        rel_columns[projection[pos]].second,
        duckdb::ColumnBinding(get.table_index, duckdb::ProjectionIndex(pos))));
    }
  } else {
    create_index_info->names.assign_range(rel_columns | std::views::keys);
    create_index_info->schema = target.ParentSchema().name;
    create_index_info->catalog = target.ParentCatalog().GetName();
    if (view_fast_path) {
      switch (view_fast_path->pk_spec) {
        case catalog::PkSpec::RocksDBExplicitPK:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("rocksdb_explicit_pk");
          break;
        case catalog::PkSpec::RocksDBGeneratedRowId:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("rocksdb_rowid");
          break;
        default: {
          SDB_ASSERT(vcols_opt.has_value(),
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
      // Hand the pruned position list to SereneDBCreateIndexPlan so the
      // physical operator can build its view_columns over the same
      // narrowed schema.
      duckdb::vector<duckdb::Value> kept_values;
      kept_values.reserve(kept_view_positions->size());
      for (auto p : *kept_view_positions) {
        kept_values.emplace_back(duckdb::Value::UBIGINT(p));
      }
      create_index_info->options["_sdb_view_kept_positions"] =
        duckdb::Value::LIST(duckdb::LogicalType::UBIGINT,
                            std::move(kept_values));
    }
    // column_binding_resolver synthesises (TableIndex(0), i) for
    // LOGICAL_CREATE_INDEX.
    for (size_t i = 0; i < rel_columns.size(); ++i) {
      expressions.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        rel_columns[i].second,
        duckdb::ColumnBinding(duckdb::TableIndex(0),
                              duckdb::ProjectionIndex(i))));
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
