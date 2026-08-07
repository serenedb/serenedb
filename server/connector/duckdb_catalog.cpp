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

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/index/bound_index.hpp>
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
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_simple.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/database_size.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <ranges>
#include <utility>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/deferred_writes.h"
#include "catalog/foreign_server.h"
#include "catalog/inverted_index.h"
#include "catalog/pk_spec.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_dependency.h"
#include "connector/duckdb_global_catalog.h"
#include "connector/duckdb_index_entry.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_object_entry.h"
#include "connector/duckdb_object_index.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_truncate.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_static_schema.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/inverted_index_options_util.h"
#include "connector/pg_logical_types.h"
#include "connector/search_table_dispatch.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::connector {
namespace {

// DROP of a schema child whose entry is the object: resolve the schema the
// qualified name points at and hand the rest to the kind. `missing_ok` is the
// statement's IF EXISTS, except where the caller has already resolved the
// target and the only absence left is a lost race.
bool DropSchemaChild(duckdb::ClientContext& context, duckdb::CatalogType type,
                     const duckdb::DropInfo& info, bool missing_ok) {
  const auto& qualified = info.GetQualifiedName();
  catalog::JoinStoreTransaction(&context);
  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto database_id =
    FindDatabase(&context, qualified.Catalog().GetIdentifierName()).Id();
  const auto schema_id =
    database_id.isSet() ? FindSchemaId(&context, database_id,
                                       qualified.Schema().GetIdentifierName())
                        : ObjectId{};
  return DropEntryObject(catalog::ActingAs(context), type, database_id,
                         schema_id, qualified.Name().GetIdentifierName(),
                         info.cascade, missing_ok);
}

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
// Fetches the existing definition, finds the matching overload by
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
  const auto database_id = FindDatabase(&context, info_catalog).Id();
  if (!database_id.isSet()) {
    return false;
  }
  const auto schema_id = FindSchemaId(&context, database_id, info_schema);
  auto existing =
    schema_id.isSet() ? FindFunction(&context, schema_id, info_name) : nullptr;
  if (!existing) {
    return false;
  }

  // Resolve UNBOUND types from the DROP statement to concrete types.
  auto binder = duckdb::Binder::CreateBinder(context);
  for (auto& t : info.func_parameters) {
    binder->BindLogicalType(t);
  }

  const auto& macro_info = *existing;
  // Find the matching overload by parameter signature.
  ssize_t match_idx = -1;
  for (size_t i = 0; i < macro_info.macros.size(); ++i) {
    const auto& macro = *macro_info.macros[i];
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
  const auto& matched = *macro_info.macros[match_idx];
  if (matched.is_procedure != info.is_procedure) {
    auto expect = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(info_name, "() is not a ", expect));
  }

  if (macro_info.macros.size() == 1) {
    // Last overload -- drop the whole function.
    (void)DropSchemaChild(context, duckdb::CatalogType::MACRO_ENTRY, info,
                          /*missing_ok=*/true);
    return true;
  }

  // Remove just the matched overload and update the stored function.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      existing->GetInfo());
  new_info->macros.erase(new_info->macros.begin() + match_idx);
  AlignMacroCatalogType(*new_info);

  catalog.ReplaceFunction(
    context, database_id, info_schema, info_name,
    std::shared_ptr<duckdb::CreateMacroInfo>{new_info.release()});
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
  const auto database_id = FindDatabase(&context, info_catalog).Id();
  if (!database_id.isSet()) {
    return false;
  }
  const auto schema_id = FindSchemaId(&context, database_id, info_schema);
  auto existing =
    schema_id.isSet() ? FindFunction(&context, schema_id, info_name) : nullptr;
  if (!existing) {
    return false;
  }

  const auto& macros = existing->macros;
  bool all_match = true;
  bool any_match = false;
  for (const auto& m : macros) {
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
    (void)DropSchemaChild(context, duckdb::CatalogType::MACRO_ENTRY, info,
                          /*missing_ok=*/true);
    return true;
  }
  // Mixed: remove only matching overloads, keep the rest.
  auto new_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      existing->GetInfo());
  std::erase_if(new_info->macros, [&](const auto& m) {
    return m->is_procedure == info.is_procedure;
  });
  AlignMacroCatalogType(*new_info);

  catalog.ReplaceFunction(
    context, database_id, info_schema, info_name,
    std::shared_ptr<duckdb::CreateMacroInfo>{new_info.release()});
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
      dropped = DropSchemaChild(context, VIEW_ENTRY, info, missing_ok);
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
      dropped = DropSchemaChild(context, TYPE_ENTRY, info, missing_ok);
      break;
    case SEQUENCE_ENTRY:
      dropped = DropSchemaChild(context, SEQUENCE_ENTRY, info, missing_ok);
      break;
    case SCHEMA_ENTRY:
      if (info_name == StaticStrings::kPgCatalogSchema ||
          info_name == StaticStrings::kInformationSchema) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
          ERR_MSG("cannot drop schema ", info_name,
                  " because it is required by the database system"));
      } else {
        // Foreign servers are database children (PG shape): DROP SCHEMA can
        // never take one down, so no attachment cleanup is needed here.
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
}

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db,
                                 ObjectId database_id,
                                 catalog::HeldSchema public_schema)
  : duckdb::DuckCatalog{db},
    _database_id{database_id},
    _public_schema{std::move(public_schema)},
    // Case-sensitive for the same reason the schema sets are: serenedb folds
    // an unquoted identifier at parse time and then matches exactly.
    _schemas{*this, nullptr, /*case_sensitive=*/true},
    _foreign_servers{*this, nullptr, /*case_sensitive=*/true},
    _object_index{*this, nullptr, /*case_sensitive=*/true} {}

duckdb::CatalogTransaction SereneDBCatalog::CommittedRead() {
  auto transaction =
    duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  // A system transaction starts at 1 and would therefore see only the entries
  // boot created; what a contextless caller means is "whatever is committed
  // now", which is one below the first transaction id.
  transaction.start_time = duckdb::TRANSACTION_ID_START - 1;
  return transaction;
}

duckdb::optional_ptr<SereneDBSchemaEntry> SereneDBCatalog::TryGetSchemaEntry(
  duckdb::CatalogTransaction transaction, std::string_view schema_name) {
  auto entry = _schemas.GetEntry(transaction, duckdb::Identifier{schema_name});
  return entry ? &entry->Cast<SereneDBSchemaEntry>() : nullptr;
}

duckdb::optional_ptr<SereneDBSchemaEntry> SereneDBCatalog::TryGetSchemaEntry(
  std::string_view schema_name) {
  return TryGetSchemaEntry(CommittedRead(), schema_name);
}

bool SereneDBCatalog::CreateSchemaEntry(duckdb::CatalogTransaction transaction,
                                        std::string_view schema_name,
                                        catalog::HeldSchema schema) {
  duckdb::CreateSchemaInfo info;
  info.SetSchema(duckdb::Identifier{schema_name});
  auto entry = duckdb::make_uniq<SereneDBSchemaEntry>(*this, info);
  if (schema.first) {
    entry->SetDefinition(std::move(schema.first), std::move(schema.second));
  }
  return _schemas.CreateEntry(transaction, duckdb::Identifier{schema_name},
                              std::move(entry),
                              duckdb::LogicalDependencyList{});
}

void SereneDBCatalog::DropSchemaEntry(duckdb::CatalogTransaction transaction,
                                      std::string_view schema_name) {
  // No cascade: what a DROP SCHEMA takes with it was planned by serenedb's own
  // cascade planner, and the sets of everything under this schema die with the
  // entry that owns them.
  (void)_schemas.DropEntry(transaction, duckdb::Identifier{schema_name},
                           /*cascade=*/false);
}

void SereneDBCatalog::VisitSchemaEntries(
  absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor) {
  _schemas.Scan([&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBSchemaEntry>());
  });
}

void SereneDBCatalog::Initialize(bool /*load_builtin*/) {
  const auto system =
    duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  // pg_catalog and information_schema are generated, not created: nobody owns
  // them, no transaction can add to them, and they exist from the moment the
  // database is attached. Their entries carry the DefaultGenerators that mint
  // the static content.
  for (const auto& [name, oid] :
       {std::pair{StaticStrings::kPgCatalogSchema, id::kPgCatalogSchema},
        std::pair{StaticStrings::kInformationSchema,
                  id::kPgInformationSchema}}) {
    (void)CreateSchemaEntry(system, name, {});
    if (auto entry = TryGetSchemaEntry(name)) {
      // The oid pg_namespace reports for these two, which is fixed rather than
      // allocated: they have no definition to take one from.
      catalog::AdoptEntryIdentity(*entry, oid);
    }
  }
  // The public schema CREATE DATABASE wrote a moment ago, in the frame that
  // carries the database itself. Committed outright rather than versioned: the
  // record is already durable, and a rolled-back CREATE DATABASE takes the
  // whole attachment with it.
  if (auto schema = std::exchange(_public_schema, {}); schema.first) {
    const auto name = schema.first->GetName();
    const auto id = schema.first->GetId();
    // Stated separately, as every schema's are: the entry is mutated in place
    // rather than versioned, so no create call carries them.
    auto deps = EntryDependencies(*schema.first, schema.second);
    (void)CreateSchemaEntry(system, name, std::move(schema));
    SetEntryDependencies(nullptr, *this, id, deps);
    BumpSchemaGeneration();
  }
}

// The identity a cached plan is checked against
// (PreparedStatementData::RequireRebind): the session's sampled view of the
// catalog's mutation count, which is fixed for the statement's duration --
// duckdb asserts that two reads inside one statement agree.
duckdb::optional_idx SereneDBCatalog::GetCatalogVersion(
  duckdb::ClientContext& context) {
  auto* ctx = GetSereneDBContextPtr(context);
  return ctx == nullptr ? duckdb::optional_idx{}
                        : duckdb::optional_idx{ctx->CatalogEpoch()};
}

duckdb::CatalogEntryInfo SereneDBCatalog::GetDependencyInfo(
  const duckdb::CatalogEntry& entry) const {
  // A schema entry keeps its definition as shared side state -- it owns the
  // CatalogSets of its contents, so it is never versioned -- but its id
  // addresses its edges like any other.
  if (const auto* schema = dynamic_cast<const SereneDBSchemaEntry*>(&entry)) {
    if (auto definition = schema->Definition()) {
      return DependencyInfo(definition->GetId());
    }
  } else if (IsHostedEntry(entry)) {
    return DependencyInfo(catalog::IdOf(entry));
  }
  return duckdb::Catalog::GetDependencyInfo(entry);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::GetDependencyEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::CatalogEntryInfo& info) {
  const auto id = DependencyInfoId(info);
  return id.isSet() ? LookupEntryById(transaction, *this, id)
                    : duckdb::Catalog::GetDependencyEntry(transaction, info);
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
  // `internal` catches anything duckdb creates for itself, on a system
  // transaction that has no ClientContext -- which every step below needs.
  if (info.internal) {
    return duckdb::DuckCatalog::CreateSchema(transaction, info);
  }
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
  auto schema = std::make_shared<catalog::CreateSchemaInfo>(
    ObjectId{}, GetDatabaseId(), schema_name);
  if (!catalog_impl.CreateSchema(catalog::ActingAs(owner, client),
                                 GetDatabaseId(), std::move(schema),
                                 catalog::Permissions{owner}, if_not_exists)) {
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
  // Straight off the set, with no session required: the sets are the whole of
  // what this catalog holds now, and the paths with no session of their own --
  // the checkpoint reader, the WAL replay, the data store -- all name schemas
  // that are really there.
  auto entry = TryGetSchemaEntry(transaction, schema_name);
  if (!entry && if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("schema \"", schema_name, "\" does not exist"));
  }
  return entry.get();
}

void SereneDBCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  auto* ctx = GetSereneDBContextPtr(context);
  if (!ctx) {
    return;
  }
  // The static schemas are generated content, not schemas of this database:
  // duckdb's own system catalog already answers for those two names, and
  // listing ours beside them would double every information_schema row.
  _schemas.Scan(GetCatalogTransaction(context),
                [&](duckdb::CatalogEntry& entry) {
                  auto& schema = entry.Cast<SereneDBSchemaEntry>();
                  if (!schema.IsStatic()) {
                    callback(schema);
                  }
                });
}

void SereneDBCatalog::ScanSchemas(
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  duckdb::DuckCatalog::ScanSchemas(callback);
  VisitSchemaEntries([&](SereneDBSchemaEntry& entry) { callback(entry); });
}

duckdb::optional_ptr<duckdb::TableCatalogEntry>
SereneDBCatalog::LookupTableById(duckdb::CatalogTransaction transaction,
                                 duckdb::idx_t catalog_id) {
  auto entry = LookupEntryById(transaction, *this, ObjectId{catalog_id});
  if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &entry->Cast<duckdb::TableCatalogEntry>();
}

bool SereneDBCatalog::IsReplaying() const {
  return !duckdb::StorageManager::Get(const_cast<SereneDBCatalog&>(*this))
            .IsLoaded();
}

void SereneDBCatalog::CreateTableStorage(duckdb::CatalogTransaction transaction,
                                         duckdb::BoundCreateTableInfo& info) {
  auto entry = LookupTableById(transaction, info.Base().oid);
  if (!entry) {
    // The catalog log no longer names this table: it was dropped after the
    // record was written, and the drop is later in the same file.
    SDB_WARN(STARTUP, "replay: no relation ", info.Base().oid,
             " for the rows of \"",
             info.Base().GetQualifiedName().Name().GetIdentifierName(), "\"");
    return;
  }
  auto& table = entry->Cast<duckdb::DuckTableEntry>();
  if (table.TryGetStorage()) {
    return;
  }
  // Built at the shape the record describes, which is not the one the entry
  // projects: the catalog log settled the definition at its latest version, and
  // the records that got it there replay over these rows afterwards. A
  // throwaway entry is what turns a CreateInfo into a DataTable; only the rows
  // outlive it.
  duckdb::DuckTableEntry at_record{*this, info.schema, info};
  table.AdoptStorage(at_record.GetStorage().shared_from_this());
}

void SereneDBCatalog::Alter(duckdb::CatalogTransaction transaction,
                            duckdb::AlterInfo& info) {
  // A record from this database's own data file, replaying into a catalog that
  // already holds the definition. Resolved by identity before the base looks
  // the name up, because the name in the record is the pre-rename one.
  if (IsReplaying()) {
    AlterStorage(transaction, info, /*versioned=*/false);
    return;
  }
  duckdb::DuckCatalog::Alter(transaction, info);
}

void SereneDBCatalog::AlterStorage(duckdb::CatalogTransaction transaction,
                                   duckdb::AlterInfo& info, bool versioned) {
  // Only a table alter reshapes rows. The write that follows a statement's
  // reshape records the definition it settled on as a permissions alter, and a
  // rename, a comment and a grant are the same: the catalog log owns all of
  // them, and replaying one here would ask duckdb to redo a definition change
  // it never made.
  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    return;
  }
  auto& context = transaction.GetContext();
  auto entry = LookupTableById(transaction, info.oid);
  if (!entry) {
    return;
  }
  auto& table = entry->Cast<duckdb::DuckTableEntry>();
  if (!table.TryGetStorage()) {
    return;
  }
  // The record names the target twice, and only the identity above resolves
  // it: a store op carries no name at all, and a replayed one carries the name
  // from before a rename. Restate it so an error, and the entry-change record
  // this alter writes, name the relation as it is now.
  info.SetName(table.name);
  if (!versioned) {
    // Boot. The definition in front of the rows is already the one the catalog
    // log settled on, so the alter cannot be applied to it -- the column it
    // adds is in it twice over. What the record describes is a step the rows
    // have not taken yet, so it is applied to a stand-in built at the shape
    // they are actually in; only the rows it produces outlive it.
    auto& rows = table.GetStorage();
    auto shape = duckdb::make_uniq<duckdb::CreateTableInfo>(
      table.ParentSchema(), table.name);
    for (const auto& column : rows.Columns()) {
      shape->columns.AddColumn(column.Copy());
    }
    auto bound = duckdb::Binder::BindCreateTableCheckpoint(
      std::move(shape), table.ParentSchema());
    duckdb::DuckTableEntry at_rows{*this, table.ParentSchema(), *bound,
                                   rows.shared_from_this(), nullptr};
    if (auto reshaped = at_rows.AlterStorage(context, info)) {
      table.AdoptStorage(std::move(reshaped));
    }
    return;
  }
  // A statement: the reshape is an alter of this entry and goes through the
  // set, which is what puts a faithful record of it in this database's WAL --
  // the definition the statement decided arrives separately, at the write.
  auto altered = table.AlterEntry(context, info);
  if (!altered) {
    return;
  }
  auto& set = table.ParentSchema().Cast<SereneDBSchemaEntry>().GetCatalogSet(
    duckdb::CatalogType::TABLE_ENTRY);
  // The reshape hands the edges over rather than letting them be re-derived: an
  // alter that states none reads as one that breaks whatever depends on the
  // table, and duckdb refuses it. They are the same edges the entry already
  // has -- only the rows are changing here.
  info.new_dependencies =
    duckdb::make_uniq<duckdb::LogicalDependencyList>(table.dependencies);
  if (!set.AlterEntry(transaction, table.name, info, std::move(altered))) {
    // Same wording the store-op path uses for the same race: a reshape that
    // lost it is a concurrent update of the table, named so the user can see
    // which one.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of table \"",
              table.name.GetIdentifierName(), "\""));
  }
}

void SereneDBCatalog::DropSchema(duckdb::ClientContext& context,
                                 duckdb::DropInfo& info) {
  info.SetCatalog(GetName());
  // The entry retires with the batch's other entries, at the write: doing it
  // here would take the schema out of the set before the transaction commits.
  DropObject(context, info);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  auto& table_info = op.info->Base();

  // Search CTAS routes to the iresearch insert operator, which consumes the
  // storage option in CreateCtasTable -- so this is a read-only probe of the
  // WITH options.
  if (ReadStorageEngine(table_info.options) == catalog::TableEngine::Search) {
    auto& search_ctas = planner.Make<SereneDBSearchInsert>(
      std::move(op.info), op.schema, op.estimated_cardinality);
    search_ctas.children.push_back(plan);
    return search_ctas;
  }

  // Transactional CTAS. Planning is side-effect free (it can run more than once
  // per statement, e.g. on rebind): pre-allocate the table id and build the
  // operators only; the relation is created at execution, by the operator in
  // front of the load.
  auto& schema_entry = op.schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();

  auto options = std::make_shared<catalog::CreateTableInfo>();
  options->SetTableName(table_info.GetTableName());
  options->SetSchema(op.schema.name);
  // Consume the storage WITH-option (Transactional on this path) so the
  // unrecognized-parameter check does not reject it.
  ApplyStorageKind(context, *options, table_info.options);

  if (!table_info.options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            table_info.options.begin()->first, "\""));
  }

  const auto table_id = catalog::NextId();
  // Captured before the retarget below: for CREATE OR REPLACE TABLE AS this is
  // REPLACE_ON_CONFLICT, and the CTAS operator drops the pre-existing table at
  // execution.
  const auto on_conflict = table_info.on_conflict;

  const auto column_count = table_info.columns.LogicalColumnCount();
  for (duckdb::idx_t i = 0; i < column_count; ++i) {
    auto& col = table_info.columns.GetColumnMutable(duckdb::LogicalIndex{i});
    duckdb::ColumnDefinition column{col.Name(), col.Type()};
    column.SetCatalogOid(catalog::NextId().id());
    column.SetCompressionType(col.CompressionType());
    if (col.Generated()) {
      column.SetGeneratedExpression(col.GeneratedExpression().Copy(),
                                    duckdb::TableColumnType::GENERATED_STORED);
    } else if (col.HasDefaultValue()) {
      column.SetDefaultValue(col.DefaultValue().Copy());
    }
    options->columns.AddColumn(std::move(column));
  }

  // The CTAS-variant insert resolves its target by creating it; the operator in
  // front of it has already done so, so this create finds the relation and
  // hands the sink the entry to append into. The columns are bound, so no
  // rebind is needed.
  table_info.SetCatalog(GetName());
  table_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  table_info.query.reset();

  auto& load_schema = op.schema;
  auto load_info = duckdb::make_uniq<duckdb::BoundCreateTableInfo>(
    load_schema, std::move(op.info->base));

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
        op, load_schema, std::move(load_info), op.estimated_cardinality);
    }
    const bool parallel = parallel_streaming_insert && num_threads > 1;
    return planner.Make<duckdb::PhysicalInsert>(
      op, load_schema, std::move(load_info), op.estimated_cardinality,
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
  const duckdb::ParsedExpression* predicate,
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
  if (predicate) {
    collect(*predicate);
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

  // Search table: route to the iresearch insert operator. It has no store
  // table to compute defaults/generated columns downstream, so resolve them
  // into the plan here (ResolveDefaultsProjection two-passes STORED generated
  // columns -- incl. the generated PK -- over a storage-ordered chunk).
  if (table_entry.IsSearchTable()) {
    if (plan && !op.column_index_map.empty()) {
      plan = &planner.ResolveDefaultsProjection(op, *plan);
      op.column_index_map.clear();
    }
    auto& insert = planner.Make<SereneDBSearchInsert>(
      ResolveSearchWriteTarget(table_entry), std::move(op.types),
      op.estimated_cardinality, op.return_chunk);
    if (plan) {
      insert.children.push_back(*plan);
    }
    return insert;
  }

  // Resolve defaults/generated columns (the shared upstream two-pass) BELOW the
  // progress wrapper, then clear the map so the delegated PlanInsert does not
  // project a second time.
  if (plan && !op.column_index_map.empty()) {
    plan = &planner.ResolveDefaultsProjection(op, *plan);
    op.column_index_map.clear();
  }
  return duckdb::DuckCatalog::PlanInsert(context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);

  if (table_entry.IsSearchTable()) {
    // TRUNCATE (autocommit): fast iresearch Clear marker. In-transaction
    // TRUNCATE has is_truncate but is not autocommit, so it falls through to
    // the row-wise SereneDBSearchDelete below.
    if (op.is_truncate && context.transaction.IsAutoCommit()) {
      return planner.Make<SereneDBSearchTruncate>(table_entry.GetSearchData(),
                                                  op.estimated_cardinality);
    }

    // A Search table has no separate inverted indexes, so its scan appends only
    // the PK virtuals (BuildRowIdColumns): [real..., pk_0..pk_{n-1}] for
    // explicit-PK tables, or [real..., generated_pk] for generated-PK ones.
    const auto num_pk = table_entry.GetPKColumnIndexes().size();
    const auto child_cols = plan.types.size();
    std::vector<duckdb::idx_t> pk_indices;
    if (num_pk == 0) {
      pk_indices.push_back(child_cols - 1);  // generated-PK slot is last
    } else {
      for (size_t i = 0; i < num_pk; ++i) {
        pk_indices.push_back(child_cols - num_pk + i);
      }
    }
    // RETURNING: the binder already widened the scan to every column the clause
    // can name, and op.return_columns says which slot each of them arrived in.
    std::vector<duckdb::idx_t> column_map;
    if (op.return_chunk) {
      column_map.assign(op.return_columns.begin(), op.return_columns.end());
    }
    auto& search_del = planner.Make<SereneDBSearchDelete>(
      ResolveSearchWriteTarget(table_entry), std::move(pk_indices),
      std::move(op.types), std::move(column_map), op.estimated_cardinality);
    search_del.children.push_back(plan);
    return search_del;
  }

  return duckdb::DuckCatalog::PlanDelete(context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);

  if (table_entry.IsSearchTable()) {
    // Wrap `plan` with a PhysicalProjection that resolves VALUE_DEFAULT and
    // passes every projected new-row column through -- the SET values, duckdb's
    // recomputed STORED generated columns, and the old-value passthroughs that
    // BindUpdateConstraints added -- plus the PK virtuals, so
    // SereneDBSearchUpdate sees [resolved new-row vals, pk_virtuals]. A Search
    // table has no separate inverted indexes, so BuildRowIdColumns appends only
    // the PK virtuals: [real..., pk_0..pk_{n-1}] / [real..., generated_pk].
    const auto num_pk = table_entry.GetPKColumnIndexes().size();
    const auto num_virtual = num_pk == 0 ? 1 : num_pk;
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
    if (num_pk == 0) {
      // generated PK is the single virtual, after the SET vals.
      pk_indices.push_back(num_updates + num_virtual - 1);
    } else {
      pk_indices.reserve(num_pk);
      for (size_t i = 0; i < num_pk; ++i) {
        pk_indices.push_back(num_updates + i);
      }
    }

    auto& search_upd = planner.Make<SereneDBSearchUpdate>(
      ResolveSearchWriteTarget(table_entry), std::move(pk_indices),
      std::move(op.columns), std::move(op.types), op.estimated_cardinality,
      op.return_chunk);
    search_upd.children.push_back(proj);
    return search_upd;
  }

  return duckdb::DuckCatalog::PlanUpdate(context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanMergeInto(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) {
  auto& table_entry = RequireBaseTable(op.table);
  if (table_entry.IsSearchTable()) {
    // MERGE INTO (and INSERT ... ON CONFLICT, which duckdb also lowers to
    // MergeInto) delegates each action to the store mirror, which bypasses the
    // iresearch index -- it silently corrupts the search index. Reject it with
    // a clear error until search-backed MERGE is implemented.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("MERGE INTO (and INSERT ... ON CONFLICT) is not yet supported on "
              "search-backed tables"));
  }
  return duckdb::DuckCatalog::PlanMergeInto(context, planner, op, plan);
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindAlterAddIndex(
  duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
  duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) {
  // ADD PRIMARY KEY / ADD UNIQUE as the data store issues it: the constraint is
  // already in the definition, and what this statement is for is the ART over
  // the rows that are already there.
  if (IsStorageStatement(binder.context)) {
    return duckdb::DuckCatalog::BindAlterAddIndex(
      binder, table_entry, std::move(plan), std::move(create_info),
      std::move(alter_info));
  }
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
    auto& table = target.Cast<duckdb::TableCatalogEntry>();
    // The ART mirror of an index as the data store issues it: a plain duckdb
    // index build, so nothing here applies to it.
    if (IsStorageStatement(binder.context)) {
      return duckdb::DuckCatalog::BindCreateIndex(binder, stmt, target,
                                                  std::move(plan));
    }
    RejectIfSearchTable(RequireBaseTable(table).GetEngine(), "CREATE INDEX");
  }

  // View-backed indexes are STATIC -- captured at CREATE INDEX, no DML refresh.
  duckdb::optional_ptr<duckdb::TableCatalogEntry> resolved_table;
  bool view_backed = false;
  std::optional<ViewFastPath> view_fast_path;
  int64_t pinned_iceberg_snapshot_id = 0;
  std::optional<std::vector<duckdb::idx_t>> kept_view_positions;
  const auto partial_view_index =
    !!stmt.info->Cast<duckdb::CreateIndexInfo>().where_clause;
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
    const auto schema_id =
      FindSchemaId(&binder.context, GetDatabaseId(),
                   target.ParentSchema().name.GetIdentifierName());
    std::optional<ViewFastPath> fp;
    if (schema_id.isSet()) {
      if (const auto* view = FindView(&binder.context, schema_id,
                                      target.name.GetIdentifierName())) {
        auto key_cols = KeyColumnsFromOptions(
          stmt.info->Cast<duckdb::CreateIndexInfo>().options);
        auto info = view->GetInfo();
        fp = ResolveViewFastPath(
          binder.context, info->Cast<duckdb::CreateViewInfo>(), key_cols);
      }
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
      for (size_t i = 0; i < vcols.size(); ++i) {
        const auto vcol = vcols[i];
        if (vcol == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
          pk_types.push_back(duckdb::LogicalType::UBIGINT);
        } else if (view_fast_path->pk_spec ==
                   catalog::PkSpec::ExternalColumnKey) {
          // Real key columns of arbitrary types -- project their own types, not
          // a hardcoded BIGINT (which only fits the file/rowid int64 keys).
          pk_types.push_back(view_fast_path->key_columns[i].type);
        } else if (view_fast_path->pk_spec ==
                   catalog::PkSpec::ExternalPostgresCtid) {
          // The postgres scanner emits the ctid straight as the struct.
          pk_types.push_back(pg::CTID());
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
        if (view_fast_path->pk_spec == catalog::PkSpec::ExternalPostgresCtid &&
            vcols[i] == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
          leaf_get->virtual_columns.insert_or_assign(
            duckdb::COLUMN_IDENTIFIER_ROW_ID,
            duckdb::TableColumn("ctid", pg::CTID()));
        }
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
        stmt.info->Cast<duckdb::CreateIndexInfo>().where_clause.get(),
        *column_info);
      if (kept.size() < column_info->names.size() || partial_view_index) {
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
          stmt.info->Cast<duckdb::CreateIndexInfo>().where_clause.get(),
          *column_info);
        if (kept.size() < column_info->names.size() || partial_view_index) {
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

  if (create_index_info->index_type == "inverted") {
    for (const auto& [option, value] : create_index_info->options) {
      if (!absl::c_contains(kCreateInvertedOptions,
                            absl::AsciiStrToLower(option))) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("unrecognized parameter \"", option, "\""));
      }
    }
  } else if (!create_index_info->options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            create_index_info->options.begin()->first, "\""));
  }

  if (create_index_info->where_clause &&
      create_index_info->index_type != "inverted") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("partial indexes are only supported for inverted indexes"));
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
  SereneDBTableEntry* sdb_entry = nullptr;
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
    sdb_entry = &RequireBaseTable(*resolved_table);
    const auto& entry_columns = sdb_entry->GetColumns();
    rel_columns.reserve(entry_columns.LogicalColumnCount());
    for (const auto& column : entry_columns.Logical()) {
      rel_columns.emplace_back(std::string{column.Name().GetIdentifierName()},
                               column.Type());
    }
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
  if (create_index_info->where_clause) {
    collect(*create_index_info->where_clause);
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
  // Partial-index predicate for the backfill: a LogicalFilter over the scan
  // (built below) drops non-matching rows before they reach the create-index
  // sink. The operator still receives the predicate (via expressions) to
  // persist it for DML maintenance.
  duckdb::unique_ptr<duckdb::Expression> backfill_filter_predicate;
  auto bind_predicate = [&](duckdb::IndexBinder& index_binder) {
    auto parsed = create_index_info->where_clause->Copy();
    auto bound = index_binder.Bind(parsed);
    if (bound->GetReturnType() != duckdb::LogicalType::BOOLEAN) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("argument of WHERE must be type boolean, not type ",
                bound->GetReturnType().ToString()));
    }
    return bound;
  };
  if (!view_backed) {
    SDB_ASSERT(sdb_entry);
    // Project only what the backfill actually needs: index columns + PK
    // columns (or ROW_ID for generated PK). Replaces the previous
    // "every non-PK column" default which made every CREATE INDEX scan
    // redundantly read the whole table.
    auto projection = BuildCreateIndexProjection(
      sdb_entry->GetPKColumnIndexes(), create_index_info->column_ids);
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
    if (create_index_info->where_clause) {
      auto bound_where = bind_predicate(index_binder);
      backfill_filter_predicate = bound_where->Copy();
      expressions.emplace_back(std::move(bound_where));
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
        case catalog::PkSpec::ExternalPostgresCtid:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("external_postgres_ctid");
          break;
        case catalog::PkSpec::ExternalColumnKey:
          create_index_info->options["_sdb_view_fast_path_pk"] =
            duckdb::Value("external_struct_key");
          break;
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
    // Remap col-ref bindings to (TableIndex(0), narrowed_position): the
    // resolver matches LOGICAL_CREATE_INDEX exprs against TableIndex(0), and
    // chunk positions follow kept_view_positions' (sorted) order. Applied to
    // the index keys and the persisted partial-index predicate alike.
    auto remap = [&](this auto& self, duckdb::Expression& e) -> void {
      if (e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
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
    expressions.reserve(create_index_info->expressions.size());
    for (auto& parsed : create_index_info->expressions) {
      auto bound = index_binder.Bind(parsed);
      remap(*bound);
      expressions.emplace_back(std::move(bound));
    }
    if (create_index_info->where_clause) {
      auto bound_where = bind_predicate(index_binder);

      SDB_ASSERT(plan->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION);
      auto& proj = plan->Cast<duckdb::LogicalProjection>();
      auto filter =
        duckdb::make_uniq<duckdb::LogicalFilter>(bound_where->Copy());
      filter->children.push_back(std::move(proj.children[0]));
      proj.children[0] = std::move(filter);
      // Persisted copy is normalized like the index keys.
      remap(*bound_where);
      expressions.emplace_back(std::move(bound_where));
    }
  }

  auto& target_for_op = view_backed
                          ? static_cast<duckdb::CatalogEntry&>(target)
                          : static_cast<duckdb::CatalogEntry&>(*resolved_table);
  if (backfill_filter_predicate) {
    auto filter = duckdb::make_uniq<duckdb::LogicalFilter>(
      std::move(backfill_filter_predicate));
    filter->children.push_back(std::move(plan));
    plan = std::move(filter);
  }
  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(create_index_info), std::move(expressions), target_for_op,
    nullptr);
  result->children.push_back(std::move(plan));
  return result;
}

RelationStorageSize StoreTableDataSize(duckdb::ClientContext& context,
                                       const SereneDBTableEntry& table) {
  RelationStorageSize result;
  auto rows = const_cast<SereneDBTableEntry&>(table).TryGetStorage();
  if (!rows) {
    return result;
  }
  auto& storage = *rows;
  duckdb::QueryContext query_context(context);
  containers::FlatHashSet<duckdb::block_id_t> blocks;
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
                             const SereneDBTableEntry& table) {
  auto rows = const_cast<SereneDBTableEntry&>(table).TryGetStorage();
  if (!rows) {
    return 0;
  }
  auto& info = *rows->GetDataTableInfo();
  info.BindIndexes(context);
  int64_t total = 0;
  for (auto& index : info.GetIndexes().Indexes()) {
    if (!index.IsBound()) {
      continue;
    }
    total += static_cast<int64_t>(
      index.Cast<duckdb::BoundIndex>().GetAllocationSize());
  }
  return total;
}

int64_t SearchTableBytes(const SereneDBTableEntry& table) {
  const auto& data = table.GetSearchData();
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

int64_t RelationDataBytes(duckdb::ClientContext& context,
                          const SereneDBTableEntry& table) {
  return table.IsSearchTable() ? SearchTableBytes(table)
                               : StoreTableDataSize(context, table).bytes;
}

int64_t IndexEntryBytes(duckdb::ClientContext& context,
                        const SereneDBIndexEntry& index) {
  if (index.IsInverted()) {
    const auto& data = index.GetInvertedData();
    return data ? static_cast<int64_t>(data->GetStats().indexSize) : 0;
  }
  // A secondary index is an ART on the store table, so its size is the
  // allocation the store table reports for it.
  auto entry = catalog::GetStoreTableEntry(
    context, const_cast<duckdb::Catalog&>(index.ParentCatalog()),
    index.GetRelationId(), duckdb::OnEntryNotFound::RETURN_NULL);
  if (!entry) {
    return 0;
  }
  auto& info = *entry->GetStorage().GetDataTableInfo();
  info.BindIndexes(context);
  auto bound = info.GetIndexes().Find(index.name);
  return bound ? static_cast<int64_t>(bound->GetAllocationSize()) : 0;
}

int64_t TableIndexesTotalBytes(duckdb::ClientContext& context,
                               SereneDBTableEntry& table) {
  int64_t total =
    table.IsSearchTable() ? 0 : StoreTableIndexBytes(context, table);
  VisitRelationIndexEntries(&context, table.schema.Cast<SereneDBSchemaEntry>(),
                            catalog::IdOf(table),
                            [&](SereneDBIndexEntry& index) {
                              if (index.IsInverted()) {
                                total += IndexEntryBytes(context, index);
                              }
                            });
  return total;
}

duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         ObjectId database_id,
                                         std::string_view only_schema) {
  duckdb::DatabaseSize result;
  auto store = catalog::TryStoreDatabase(context, database_id);
  if (!store) {
    // PRAGMA database_size walks every attachment, and another session can have
    // dropped one since: a database that is no longer attached has no size to
    // report.
    return result;
  }
  if (store->HasStorageManager()) {
    result.block_size = store->GetStorageManager().GetDatabaseSize().block_size;
  }
  int64_t bytes = 0;
  int64_t blocks = 0;
  VisitCatalogSetEntries(
    context, database_id, duckdb::CatalogType::TABLE_ENTRY,
    [&](const catalog::CreateSchemaInfo& schema, duckdb::CatalogEntry& entry) {
      if (!only_schema.empty() && schema.GetName() != only_schema) {
        return;
      }
      // Views and the index-name-as-table wrappers share this set and own no
      // rows of their own; the cast is the filter.
      const auto* table = dynamic_cast<const SereneDBTableEntry*>(&entry);
      if (table == nullptr) {
        return;
      }
      if (table->IsSearchTable()) {
        bytes += SearchTableBytes(*table);
        return;
      }
      const auto data = StoreTableDataSize(context, *table);
      bytes += data.bytes + StoreTableIndexBytes(context, *table);
      blocks += data.persistent_blocks;
    });
  VisitCatalogSetEntries(
    context, database_id, duckdb::CatalogType::INDEX_ENTRY,
    [&](const catalog::CreateSchemaInfo& schema, duckdb::CatalogEntry& entry) {
      if (!only_schema.empty() && schema.GetName() != only_schema) {
        return;
      }
      auto* index = dynamic_cast<SereneDBIndexEntry*>(&entry);
      if (index != nullptr && index->IsInverted()) {
        bytes += IndexEntryBytes(context, *index);
      }
    });
  result.bytes = static_cast<duckdb::idx_t>(bytes);
  result.total_blocks = static_cast<duckdb::idx_t>(blocks);
  result.used_blocks = result.total_blocks;
  return result;
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return DatabaseStorageSize(context, _database_id);
}

}  // namespace sdb::connector
