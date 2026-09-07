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

#include "catalog/ddl/duckdb_catalog.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/execution/index/bound_index.hpp>
#include <duckdb/execution/operator/order/physical_order.hpp>
#include <duckdb/execution/operator/persistent/physical_batch_insert.hpp>
#include <duckdb/execution/operator/persistent/physical_delete.hpp>
#include <duckdb/execution/operator/persistent/physical_insert.hpp>
#include <duckdb/execution/operator/persistent/physical_merge_into.hpp>
#include <duckdb/execution/operator/persistent/physical_update.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
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
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
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
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <ranges>
#include <utility>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_index_scan_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/foreign_server.h"
#include "catalog/inverted_index.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"
#include "catalog/pk_spec.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_physical_ctas.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_truncate.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/duckdb_table_function.h"
#include "connector/file_manifest.h"
#include "connector/functions/system.h"
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

namespace sdb::catalog {
namespace {

// DROP of a schema child whose entry is the object. `missing_ok` is the
// statement's IF EXISTS, except where the caller has already resolved the
// target and the only absence left is a lost race.
bool DropSchemaChild(duckdb::ClientContext& context, duckdb::CatalogType type,
                     const duckdb::DropInfo& info, bool missing_ok) {
  const auto& qualified = info.GetQualifiedName();
  catalog::JoinStoreTransaction(&context);
  const auto* database =
    FindDatabase(&context, qualified.Catalog().GetIdentifierName());
  if (!database) {
    if (missing_ok) {
      return false;
    }
    pg::ThrowUndefinedObject(type, qualified.Name().GetIdentifierName());
  }
  const auto database_id = catalog::IdOf(*database);
  const auto schema_id =
    FindSchemaId(&context, database_id, qualified.Schema().GetIdentifierName());
  return DropEntryObject(catalog::ActingAs(context), type, database_id,
                         schema_id, qualified.Name().GetIdentifierName(),
                         info.cascade, missing_ok);
}

// What survives a DROP of one or more overloads: a rewrite of the function
// under the identity the owner already holds. Resolved again here -- the
// surgery above ran against an earlier read.
void InstallSurvivingOverloads(
  duckdb::ClientContext& context, ObjectId database_id, std::string_view schema,
  std::string_view name, duckdb::unique_ptr<duckdb::CreateMacroInfo> next) {
  const auto schema_id =
    catalog::TryFindSchemaId(&context, database_id, schema);
  if (!schema_id) {
    return;
  }
  const auto* existing = catalog::FindFunction(&context, *schema_id, name);
  if (!existing) {
    return;
  }
  const auto& perm = existing->permissions;
  // PG: only the owner may drop an overload, and what is left is a rewrite of
  // the function the owner holds -- so the ACL and the owner carry over.
  const auto fn_name = existing->name.GetIdentifierName();
  catalog::RequireOwner(&context, catalog::ActingAs(context).role, perm,
                        "function", fn_name);
  catalog::SetIdentity(*next, ObjectId{existing->oid},
                       ObjectId{existing->ParentSchema().oid});
  catalog::PutEntry(&context, fn_name, std::move(next), perm);
}

// The function a per-overload DROP names, with the database it lives in.
struct DropFunctionTarget {
  ObjectId database_id;
  const duckdb::MacroCatalogEntry* existing = nullptr;
};

DropFunctionTarget FindDropFunctionTarget(duckdb::ClientContext& context,
                                          const duckdb::DropInfo& info) {
  const auto database_id = catalog::FindDatabaseId(
    &context, info.GetQualifiedName().Catalog().GetIdentifierName());
  if (!database_id.isSet()) {
    return {};
  }
  const auto schema_id =
    FindSchemaId(&context, database_id,
                 info.GetQualifiedName().Schema().GetIdentifierName());
  return {database_id,
          schema_id.isSet()
            ? FindFunction(&context, schema_id,
                           info.GetQualifiedName().Name().GetIdentifierName())
            : nullptr};
}

bool DropFunctionOverload(duckdb::ClientContext& context,
                          duckdb::DropInfo& info) {
  const auto& info_schema =
    info.GetQualifiedName().Schema().GetIdentifierName();
  const auto& info_name = info.GetQualifiedName().Name().GetIdentifierName();
  const auto [database_id, existing] = FindDropFunctionTarget(context, info);
  if (!existing) {
    return false;
  }

  // Resolve UNBOUND types from the DROP statement to concrete types.
  auto binder = duckdb::Binder::CreateBinder(context);
  for (auto& t : info.func_parameters) {
    binder->BindLogicalType(t);
  }

  const auto matched = existing->FindOverload(info.func_parameters);
  if (!matched) {
    return false;
  }

  // PG: DROP FUNCTION on a procedure (or vice versa) is an error.
  if (matched->is_procedure != info.is_procedure) {
    auto expect = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(info_name, "() is not a ", expect));
  }

  auto next = existing->WithoutOverload(info.func_parameters);
  SDB_ASSERT(next);
  if (next->macros.empty()) {
    DropSchemaChild(context, duckdb::CatalogType::MACRO_ENTRY, info,
                    /*missing_ok=*/true);
    return true;
  }
  InstallSurvivingOverloads(context, database_id, info_schema, info_name,
                            std::move(next));
  return true;
}

// DROP FUNCTION/PROCEDURE name -- drop overloads matching the drop kind.
// PG: DROP FUNCTION drops only function overloads, DROP PROCEDURE drops only
// procedure overloads. If mixed (func + proc under same name), keep the other.
bool DropFunctionByKind(duckdb::ClientContext& context,
                        const duckdb::DropInfo& info) {
  const auto& info_schema =
    info.GetQualifiedName().Schema().GetIdentifierName();
  const auto& info_name = info.GetQualifiedName().Name().GetIdentifierName();
  const auto [database_id, existing] = FindDropFunctionTarget(context, info);
  if (!existing) {
    return false;
  }

  auto next = existing->WithoutKind(info.is_procedure);
  if (!next) {
    auto kind = info.is_procedure ? "procedure" : "function";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("could not find a ", kind, " named \"", info_name, "\""));
  }
  if (next->macros.empty()) {
    DropSchemaChild(context, duckdb::CatalogType::MACRO_ENTRY, info,
                    /*missing_ok=*/true);
    return true;
  }
  InstallSurvivingOverloads(context, database_id, info_schema, info_name,
                            std::move(next));
  return true;
}

}  // namespace

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info) {
  const auto& info_catalog =
    info.GetQualifiedName().Catalog().GetIdentifierName();
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier{info_catalog})
      .Cast<SereneDBCatalog>();
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
        catalog::DropTable(catalog::ActingAs(context), info_catalog,
                           info_schema, info_name, info.cascade, missing_ok);
      break;
    case INDEX_ENTRY:
    case VIEW_ENTRY:
    case TYPE_ENTRY:
    case SEQUENCE_ENTRY:
      dropped = DropSchemaChild(context, info.type, info, missing_ok);
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      dropped = info.has_func_args ? DropFunctionOverload(context, info)
                                   : DropFunctionByKind(context, info);
      if (!dropped && !missing_ok) {
        auto kind = info.is_procedure ? "procedure" : "function";
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a ", kind, " named \"", info_name, "\""));
      }
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
    auto& ctx = connector::GetSereneDBContext(context);
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
                                 ObjectId public_schema_id,
                                 catalog::Permissions owner)
  // Case-sensitive for the same reason the schema sets are: serenedb folds
  // an unquoted identifier at parse time and then matches exactly.
  : duckdb::DuckCatalog{db, /*case_sensitive_schemas=*/true},
    _database_id{database_id},
    _public_schema_id{public_schema_id},
    _public_schema_owner{std::move(owner)},
    _foreign_servers{*this, nullptr, /*case_sensitive=*/true} {
  _foreign_servers.EnableOidLookup(nullptr,
                                   duckdb::CatalogType::FOREIGN_SERVER_ENTRY);
}

duckdb::optional_ptr<duckdb::CatalogSet> SereneDBCatalog::RootEntrySet(
  duckdb::CatalogType slot) {
  if (slot == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    return &_foreign_servers;
  }
  return duckdb::DuckCatalog::RootEntrySet(slot);
}

duckdb::CatalogTransaction SereneDBCatalog::CommittedRead() {
  return duckdb::CatalogTransaction::GetCommittedTransaction(GetDatabase());
}

duckdb::optional_ptr<SereneDBSchemaEntry> SereneDBCatalog::TryGetSchemaEntry(
  duckdb::CatalogTransaction transaction, std::string_view schema_name) {
  auto entry = GetSchemaCatalogSet().GetEntry(transaction,
                                              duckdb::Identifier{schema_name});
  return entry ? &entry->Cast<SereneDBSchemaEntry>() : nullptr;
}

duckdb::optional_ptr<SereneDBSchemaEntry> SereneDBCatalog::TryGetSchemaEntry(
  std::string_view schema_name) {
  return TryGetSchemaEntry(CommittedRead(), schema_name);
}

bool SereneDBCatalog::CreateSchemaEntry(
  duckdb::CatalogTransaction transaction, std::string_view schema_name,
  ObjectId id, catalog::Permissions perm,
  const duckdb::LogicalDependencyList& deps) {
  duckdb::CreateSchemaInfo info;
  info.SetSchema(duckdb::Identifier{schema_name});
  auto entry =
    duckdb::make_uniq<SereneDBSchemaEntry>(*this, info, id, std::move(perm));
  return GetSchemaCatalogSet().CreateEntry(
    transaction, duckdb::Identifier{schema_name}, std::move(entry), deps);
}

duckdb::optional_ptr<SereneDBSchemaEntry>
SereneDBCatalog::TryGetSchemaEntryById(duckdb::CatalogTransaction transaction,
                                       ObjectId id) {
  auto entry = LookupEntryById(transaction, *this, id);
  if (!entry || entry->type != duckdb::CatalogType::SCHEMA_ENTRY) {
    return nullptr;
  }
  return &entry->Cast<SereneDBSchemaEntry>();
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
SereneDBCatalog::LookupSchemaById(duckdb::CatalogTransaction transaction,
                                  duckdb::idx_t catalog_id) {
  return TryGetSchemaEntryById(transaction, ObjectId{catalog_id}).get();
}

bool SereneDBCatalog::AlterSchemaEntry(
  duckdb::CatalogTransaction transaction, std::string_view old_name,
  std::string_view new_name, ObjectId id, catalog::Permissions perm,
  const duckdb::LogicalDependencyList& deps) {
  auto* current = TryGetSchemaEntry(transaction, old_name).get();
  if (current == nullptr) {
    return false;
  }
  duckdb::CreateSchemaInfo info;
  info.SetSchema(duckdb::Identifier{new_name});
  auto entry = current->AlteredEntry(info, id, std::move(perm));
  return GetSchemaCatalogSet().CreateOrReplaceEntry(
    transaction, duckdb::Identifier{old_name}, std::move(entry), deps);
}

void SereneDBCatalog::DropSchemaEntry(duckdb::CatalogTransaction transaction,
                                      std::string_view schema_name,
                                      bool cascade) {
  // duckdb's dependency walk owns containment: the entries' edges onto this
  // schema refuse a RESTRICT and dispatch every content through DropDependent
  // on a cascade; whatever the walk left -- the index-name wrappers -- dies
  // with the sets the entry owns.
  GetSchemaCatalogSet().DropEntry(transaction, duckdb::Identifier{schema_name},
                                  cascade);
}

void SereneDBCatalog::VisitSchemaEntries(
  absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor) {
  GetSchemaCatalogSet().Scan([&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBSchemaEntry>());
  });
}

void SereneDBCatalog::Initialize(bool load_builtin) {
  Initialize(nullptr, load_builtin);
}

void SereneDBCatalog::Initialize(
  duckdb::optional_ptr<duckdb::ClientContext> context, bool /*load_builtin*/) {
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
    // The oid pg_namespace reports for these two is fixed rather than
    // allocated: they have no definition to take one from.
    CreateSchemaEntry(system, name, oid, {}, duckdb::LogicalDependencyList{});
  }
  // The public schema is made here rather than replayed; a DROP of it is an
  // ordinary record that lands after this.
  if (_public_schema_id.isSet()) {
    const auto schema = catalog::MakeSchemaInfo(_public_schema_id, _database_id,
                                                StaticStrings::kPublic);
    CreateSchemaEntry(system, StaticStrings::kPublic, _public_schema_id,
                      _public_schema_owner, EntryDependencies(*schema));
  }
}

duckdb::CatalogEntryInfo SereneDBCatalog::GetDependencyInfo(
  const duckdb::CatalogEntry& entry) const {
  // The two static schemas are generated rather than created, so they have no
  // id of their own and no edges to address; every other schema's id addresses
  // its edges like any other entry's.
  if (const auto* schema = dynamic_cast<const SereneDBSchemaEntry*>(&entry)) {
    if (!schema->IsStatic()) {
      return DependencyInfo(catalog::IdOf(*schema));
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

void SereneDBCatalog::AlterDependent(duckdb::CatalogTransaction transaction,
                                     duckdb::CatalogEntry& dependent,
                                     duckdb::AlterInfo& info) {
  auto* context = transaction.context.get();
  const auto* table = dynamic_cast<const SereneDBTableEntry*>(&dependent);
  if (context == nullptr || table == nullptr) {
    duckdb::Catalog::AlterDependent(transaction, dependent, info);
    return;
  }
  const auto live = table->Definition();
  if (info.type == duckdb::AlterType::ALTER_TABLE &&
      info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
        duckdb::AlterTableType::REMOVE_COLUMN) {
    // The column-drop road, whole: the covering-index victims fall first and
    // the store indexes that block the reshape are recreated around it.
    const auto* column = catalog::ColumnByName(
      *live,
      info.Cast<duckdb::RemoveColumnInfo>().removed_column.GetIdentifierName());
    if (column != nullptr) {
      catalog::DropTableColumns(context, *table,
                                {ObjectId{column->CatalogOid()}});
    }
    return;
  }
  catalog::ApplyTableAlter(context, *live, info);
}

bool SereneDBCatalog::DropDependent(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& /*object*/,
  duckdb::CatalogEntry& dependent, bool /*cascade*/,
  const duckdb::vector<duckdb::DependencyPiece>& /*pieces*/) {
  auto* context = transaction.context.get();
  if (context == nullptr) {
    return false;
  }
  // Every victim takes the one per-kind removal road a statement's drop takes
  // (authority was checked on the seed): the entry, the store half, the
  // artifact sweep and the counter row are all its.
  switch (KindOf(dependent.type)) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      // The index-name wrapper shares this slot and is nobody's object; it
      // goes with its index's own drop.
      if (!EntryOf<SereneDBTableEntry>(&dependent)) {
        return true;
      }
      [[fallthrough]];
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
    case VIEW_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
    case TYPE_ENTRY:
    case TOKENIZER_ENTRY:
      catalog::GetCatalog().DropResolved(
        context, ParentIdOf(dependent), KindOf(dependent.type),
        catalog::IdOf(dependent), dependent.name.GetIdentifierName(),
        /*cascade=*/true);
      return true;
    default:
      return false;
  }
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

namespace {

// The role a new schema is to belong to: the one running the statement, or the
// one AUTHORIZATION named. PG lets a role hand a schema to another only if it
// could SET ROLE to it.
ObjectId SchemaOwner(duckdb::ClientContext& client,
                     const duckdb::CreateSchemaInfo& info, ObjectId creator) {
  const auto authorization = info.authorization.GetIdentifierName();
  if (authorization.empty()) {
    return creator;
  }
  auto role = catalog::FindRole(&client, authorization);
  if (!role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", authorization, "\" does not exist"));
  }
  const ObjectId owner = role->GetId();
  if (owner != creator && !auth::ClosureFor(&client, creator)->is_superuser &&
      !auth::ComputeSetRoleClosure(*auth::RolesOf(&client), creator)
         .contains(owner)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be able to SET ROLE \"", authorization, "\""));
  }
  return owner;
}

}  // namespace

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

  const ObjectId creator = connector::GetSereneDBContext(client).GetRoleId();
  const ObjectId owner = SchemaOwner(client, info, creator);
  const auto database_id = GetDatabaseId();

  // PG: CREATE SCHEMA requires CREATE on the current database of the role
  // running it, whoever ends up owning the schema.
  catalog::RequireDatabaseAccess(&client, creator,
                                 FindDatabase(&client, database_id),
                                 catalog::AclMode::Create);
  if (FindSchemaId(&client, database_id, schema_name)) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" already exists"));
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  PutSchema(
    &client, {},
    catalog::MakeSchemaInfo(catalog::NextId(), database_id, schema_name),
    catalog::Permissions{owner});
  // New snapshot will have the schema; next LookupSchema will find it
  return nullptr;
}

void SereneDBCatalog::RenameSchema(duckdb::CatalogTransaction transaction,
                                   const duckdb::RenameSchemaInfo& info) {
  const auto old_name = info.GetQualifiedName().Schema().GetIdentifierName();
  const auto new_name = info.new_name.GetIdentifierName();
  auto& client = transaction.GetContext();
  const auto* current = TryGetSchemaEntry(transaction, old_name).get();
  if (current == nullptr) {
    if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("schema \"", old_name, "\" does not exist"));
  }
  if (current->IsStatic()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to rename schema \"", old_name, "\""));
  }
  if (absl::StartsWithIgnoreCase(new_name, "pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
      ERR_MSG("unacceptable schema name \"", new_name, "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
  }
  const auto schema_id = catalog::IdOf(*current);
  auto perm = current->permissions;
  const auto database_id = GetDatabaseId();
  const auto ax = catalog::ActingAs(client);
  catalog::RequireOwner(&client, ax.role, perm, "schema", old_name);
  catalog::RequireDatabaseAccess(&client, ax.role,
                                 FindDatabase(&client, database_id),
                                 catalog::AclMode::Create);
  if (catalog::FindSchemaId(&client, database_id, new_name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", new_name, "\" already exists"));
  }
  catalog::PutSchema(&client, old_name,
                     catalog::MakeSchemaInfo(schema_id, database_id, new_name),
                     std::move(perm));
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
  auto* ctx = connector::GetSereneDBContextPtr(context);
  if (!ctx) {
    return;
  }
  // The static schemas are generated content, not schemas of this database:
  // duckdb's own system catalog already answers for those two names, and
  // listing ours beside them would double every information_schema row.
  // Collected first, called back after: the set's lock must not be held across
  // a callback -- one that resolves a transaction takes locks of its own. Safe
  // here because this road has a statement behind it, whose transaction pins
  // every version it can see.
  duckdb::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
  GetSchemaCatalogSet().Scan(GetCatalogTransaction(context),
                             [&](duckdb::CatalogEntry& entry) {
                               auto& schema = entry.Cast<SereneDBSchemaEntry>();
                               if (!schema.IsStatic()) {
                                 schemas.push_back(schema);
                               }
                             });
  for (auto& schema : schemas) {
    callback(schema.get());
  }
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
  if (info.type == duckdb::AlterType::ALTER_SCHEMA) {
    RenameSchema(transaction, info.Cast<duckdb::RenameSchemaInfo>());
    return;
  }
  // ALTER SEQUENCE ... RENAME TO arrives as a RenameTableInfo: the grammar
  // shares one RenameAlter across the kinds, so the statement's own kind is
  // gone by the time it gets here and the relation-namespace lookup the base
  // does would refuse it. duckdb makes the same exemption for ALTER FUNCTION
  // ... RENAME, which it cannot type at parse time either.
  if (transaction.HasContext() && info.type == duckdb::AlterType::ALTER_TABLE &&
      info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
        duckdb::AlterTableType::RENAME_TABLE) {
    auto* context = &transaction.GetContext();
    // Resolved the way the base resolves it, so an unqualified name lands in
    // the session's own schema rather than nowhere.
    auto& schema = GetSchema(transaction, info.GetQualifiedName().Schema());
    const auto schema_id = catalog::FindSchemaId(
      context, GetDatabaseId(), schema.name.GetIdentifierName());
    if (schema_id.isSet() &&
        catalog::Find<SereneDBSequenceEntry>(
          context, schema_id,
          info.GetQualifiedName().Name().GetIdentifierName())) {
      schema.Alter(transaction, info);
      return;
    }
  }
  duckdb::DuckCatalog::Alter(transaction, info);
}

namespace {

void ApplyStorageAlter(duckdb::ClientContext& context, ObjectId database_id,
                       duckdb::unique_ptr<duckdb::AlterInfo> info) {
  // On the statement's own transaction, not parked for its commit: the row
  // versions the reshape moves are that transaction's, and the entry version it
  // produces has to be the one the statement goes on to write.
  const store_op::Targeted op{.database_id = database_id,
                              .relation_id = ObjectId{info->oid},
                              .info = std::move(info)};
  const auto applied = GetDataStore().ApplyStoreOps(&context, {&op, 1});
  if (!applied.ok()) {
    THROW_SQL_ERROR(ERR_MSG(applied.message()));
  }
}

}  // namespace
namespace {

void ApplyTableAlterLocked(duckdb::ClientContext* context,
                           const duckdb::CreateTableInfo& table,
                           duckdb::AlterInfo& info) try {
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* current =
    catalog::Find<SereneDBTableEntry>(context, schema_id, table_id);
  if (current == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             table.GetTableName().GetIdentifierName());
  }
  const auto current_info = current->Definition();
  // duckdb's own alter is the whole step: the definition, the storage it
  // rebuilds or shares, the transaction's local rows, the entry version in
  // the set, the data WAL record -- and the catalog record, which the commit
  // walk writes off the version the set now holds (WriteCatalogChange).
  auto op = info.Copy();
  op->oid = table_id.id();
  ApplyStorageAlter(*context, catalog::SchemaDatabaseId(context, schema_id),
                    std::move(op));
  // A rename is read back by the name it moved to.
  const bool renamed = info.type == duckdb::AlterType::ALTER_TABLE &&
                       info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
                         duckdb::AlterTableType::RENAME_TABLE;
  const auto* altered =
    renamed
      ? catalog::Find<SereneDBTableEntry>(context, schema_id,
                                          info.Cast<duckdb::RenameTableInfo>()
                                            .new_table_name.GetIdentifierName())
      : catalog::Find<SereneDBTableEntry>(context, schema_id, table_id);
  SDB_ASSERT(altered);
  if (altered == current) {
    return;
  }
  const auto updated = altered->Definition();
  catalog::RefreshExpressionReferences(context, *updated);
  // The refreshed references go onto the version the commit walk records, and
  // into the dependency manager the RESTRICT gate reads, where what the
  // definition names by id is derived beside them.
  auto& placed = const_cast<SereneDBTableEntry&>(*altered);
  placed.dependencies = updated->dependencies;
  catalog::SetEntryDependencies(context, placed,
                                catalog::EntryDependencies(*updated));
  for (auto& new_idx : catalog::RelationIndexVersions(
         catalog::RelationIndexRecords(context, schema_id, table_id),
         *current_info, *updated)) {
    catalog::PutEntry(context, new_idx->GetName(), std::move(new_idx));
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

}  // namespace

void ApplyTableAlter(const AccessContext& ax,
                     const duckdb::CreateTableInfo& table,
                     duckdb::AlterInfo& info) {
  JoinStoreTransaction(ax.context);
  const auto* current = catalog::Find<SereneDBTableEntry>(
    ax.context, catalog::ParentIdOf(table), catalog::IdOf(table));
  if (current == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             table.GetTableName().GetIdentifierName());
  }
  RequireOwner(ax.context, ax.role, current->permissions, "table",
               current->name.GetIdentifierName());
  ApplyTableAlterLocked(ax.context, table, info);
}

void ApplyTableAlter(duckdb::ClientContext* context,
                     const duckdb::CreateTableInfo& table,
                     duckdb::AlterInfo& info) {
  ApplyTableAlterLocked(context, table, info);
}

duckdb::ColumnList& SereneDBCatalog::ReplayShape(
  uint64_t table_id, const duckdb::DataTable& rows) {
  auto [it, fresh] = _replay_shapes.try_emplace(
    table_id, /*allow_duplicate_names=*/false, /*case_sensitive=*/true);
  if (fresh) {
    for (const auto& column : rows.Columns()) {
      it->second.AddColumn(column.Copy());
    }
  }
  return it->second;
}

namespace {

// The recorded reshape of `column` into `type`, taken out of what the catalog
// log carried for this table. Null when the log recorded none, which is every
// reshape the two definitions already state between them.
duckdb::unique_ptr<duckdb::AlterInfo> TakeRowRecipe(
  std::vector<duckdb::unique_ptr<duckdb::AlterInfo>>& recipes,
  const duckdb::Identifier& column, const duckdb::LogicalType& type) {
  for (auto& recipe : recipes) {
    if (!recipe || recipe->type != duckdb::AlterType::ALTER_TABLE) {
      continue;
    }
    auto& table_info = recipe->Cast<duckdb::AlterTableInfo>();
    if (table_info.alter_table_type !=
        duckdb::AlterTableType::ALTER_COLUMN_TYPE) {
      continue;
    }
    auto& change = table_info.Cast<duckdb::ChangeColumnTypeInfo>();
    if (change.column_name == column && change.target_type == type) {
      return std::move(recipe);
    }
  }
  return nullptr;
}

}  // namespace

bool SereneDBCatalog::ReplayMissingRows(duckdb::DuckTableEntry& table) {
  auto info = table.GetInfo();
  auto& definition = info->Cast<duckdb::CreateTableInfo>();
  if (catalog::ReadTableEngineTag(definition.tags) !=
      TableEngine::Transactional) {
    return false;
  }
  // The definition is all there is of this table: the file it belongs to never
  // heard of it.
  auto bound = duckdb::Binder::BindCreateTableCheckpoint(
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
      std::move(info)),
    table.ParentSchema());
  duckdb::DuckTableEntry at_definition{*this, table.ParentSchema(), *bound};
  table.AdoptStorage(at_definition.GetStorage().shared_from_this());
  return true;
}

bool SereneDBCatalog::ReplayMissingReshapes(
  duckdb::CatalogTransaction transaction, duckdb::DuckTableEntry& table) {
  // The column identities are what the two sides are matched through: a rename
  // the file missed shows up as one column under two names, and only the
  // identity says it is the same column rather than a drop and an add.
  const auto same =
    [](
      const duckdb::ColumnList& columns,
      const duckdb::ColumnDefinition& like) -> const duckdb::ColumnDefinition* {
    const bool by_id = like.CatalogOid() != 0;
    for (const auto& column : columns.Logical()) {
      if (by_id && column.CatalogOid() != 0) {
        if (column.CatalogOid() == like.CatalogOid()) {
          return &column;
        }
        continue;
      }
      if (column.Name() == like.Name()) {
        return &column;
      }
    }
    return nullptr;
  };

  const auto& want = table.GetColumns();
  const auto& have = ReplayShape(table.oid, table.GetStorage());
  auto recipes = TakeRowRecipes(ObjectId{table.oid});
  std::vector<duckdb::unique_ptr<duckdb::AlterInfo>> steps;
  for (const auto& column : have.Logical()) {
    if (same(want, column) == nullptr) {
      steps.push_back(duckdb::make_uniq<duckdb::RemoveColumnInfo>(
        StoreTarget(), column.Name().GetIdentifierName(),
        /*if_column_exists=*/false, /*cascade=*/false));
    }
  }
  for (const auto& column : want.Logical()) {
    const auto* at_rows = same(have, column);
    if (at_rows == nullptr) {
      duckdb::ColumnDefinition definition{column.Name(), column.Type()};
      definition.SetCatalogOid(column.CatalogOid());
      definition.SetCompressionType(column.CompressionType());
      if (!column.Generated() && column.HasDefaultValue()) {
        definition.SetDefaultValue(column.DefaultValue().Copy());
      }
      steps.push_back(duckdb::make_uniq<duckdb::AddColumnInfo>(
        StoreTarget(), std::move(definition), /*if_column_not_exists=*/false));
      continue;
    }
    if (at_rows->Name() != column.Name()) {
      steps.push_back(duckdb::make_uniq<duckdb::RenameColumnInfo>(
        StoreTarget(), at_rows->Name(), column.Name()));
    }
    if (at_rows->Type() != column.Type()) {
      // The recipe the statement recorded says what the old values become; a
      // cast is what a type change means when it said nothing.
      auto recorded = TakeRowRecipe(recipes, column.Name(), column.Type());
      steps.push_back(
        recorded
          ? std::move(recorded)
          : duckdb::make_uniq<duckdb::ChangeColumnTypeInfo>(
              StoreTarget(), column.Name(), column.Type(),
              duckdb::make_uniq<duckdb::CastExpression>(
                column.Type(), duckdb::make_uniq<duckdb::ColumnRefExpression>(
                                 column.Name()))));
    }
  }
  for (auto& step : steps) {
    step->oid = table.oid;
    AlterStorage(transaction, *step, /*versioned=*/false);
  }
  return !steps.empty();
}

bool SereneDBCatalog::FinishStorageReplay(duckdb::ClientContext& context) {
  // The contextless scan, the one the checkpoint walks: boot has no client
  // state on its connection, and the other one answers nothing without it. The
  // tables are collected rather than reshaped on the spot, because a reshape
  // resolves the schema it is in and the scan is holding that set.
  std::vector<duckdb::reference<duckdb::DuckTableEntry>> tables;
  ScanSchemas([&](duckdb::SchemaCatalogEntry& schema) {
    schema.Scan(
      duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry& entry) {
        if (auto* table = dynamic_cast<duckdb::DuckTableEntry*>(&entry)) {
          tables.emplace_back(*table);
        }
      });
  });
  const auto transaction = GetCatalogTransaction(context);
  bool repaired = false;
  for (auto table : tables) {
    if (table.get().TryGetStorage()) {
      repaired |= ReplayMissingReshapes(transaction, table.get());
      continue;
    }
    repaired |= ReplayMissingRows(table.get());
  }
  _replay_shapes.clear();
  return repaired;
}

void SereneDBCatalog::AlterStorage(duckdb::CatalogTransaction transaction,
                                   duckdb::AlterInfo& info, bool versioned) {
  // Only a table alter reshapes rows (plus SET_COLUMN_COMMENT, which shares
  // the storage); every other kind is the catalog log's own, and replaying one
  // here would ask duckdb to redo a definition change it never made.
  if (info.type != duckdb::AlterType::ALTER_TABLE &&
      info.type != duckdb::AlterType::SET_COLUMN_COMMENT) {
    return;
  }
  auto& context = transaction.GetContext();
  auto entry = LookupTableById(transaction, info.oid);
  if (!entry) {
    return;
  }
  auto& table = entry->Cast<duckdb::DuckTableEntry>();
  // The record names the target twice, and only the identity above resolves
  // it: a store op carries no name at all, and a replayed one carries the name
  // from before a rename. Restate it so an error, and the entry-change record
  // this alter writes, name the relation as it is now.
  info.SetName(table.name);
  if (!versioned) {
    if (!table.TryGetStorage()) {
      return;
    }
    // Boot: the entry already holds the settled definition, so the record is
    // applied to a stand-in at the replay shape instead, walked forward record
    // by record -- a rename leaves no trace in the rows to read it back from.
    auto& rows = table.GetStorage();
    auto& shape = ReplayShape(info.oid, rows);
    auto stand_in = duckdb::make_uniq<duckdb::CreateTableInfo>(
      table.ParentSchema(), table.name);
    stand_in->columns = shape.Copy();
    auto bound = duckdb::Binder::BindCreateTableCheckpoint(
      std::move(stand_in), table.ParentSchema());
    duckdb::DuckTableEntry at_rows{*this, table.ParentSchema(), *bound,
                                   rows.shared_from_this(), nullptr};
    auto reshaped = at_rows.AlterEntry(context, info);
    if (!reshaped || reshaped->type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    auto& at_next = reshaped->Cast<duckdb::DuckTableEntry>();
    shape = at_next.GetColumns().Copy();
    if (auto next_rows = at_next.TryGetStorage(); next_rows.get() != &rows) {
      table.AdoptStorage(next_rows->shared_from_this());
    }
    return;
  }
  // A statement: the reshape is an alter of this entry and goes through the
  // set, which is what puts a faithful record of it in this database's WAL --
  // the definition the statement decided arrives separately, at the write. The
  // set also carries the alter to the commit walk, where the catalog log
  // records it ahead of that definition.
  auto altered = table.AlterEntry(context, info);
  if (!altered) {
    return;
  }
  auto& set = table.ParentSchema().Cast<SereneDBSchemaEntry>().GetCatalogSet(
    duckdb::CatalogType::TABLE_ENTRY);
  // The reshape hands the edges over rather than letting the alter arm's own
  // bind state a narrower set: they are the same edges the entry already has --
  // only the rows are changing here.
  info.new_dependencies = duckdb::make_uniq<duckdb::LogicalDependencyList>(
    catalog::EntryDependencies(*table.GetInfo()));
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
  if (connector::ReadStorageEngine(table_info.options) ==
      catalog::TableEngine::Search) {
    auto& search_ctas = planner.Make<connector::SereneDBSearchInsert>(
      std::move(op.info), op.schema, op.estimated_cardinality);
    search_ctas.children.push_back(plan);
    return search_ctas;
  }

  // Transactional CTAS: duckdb's own load operator creates the relation it
  // fills -- its create dispatches through schema.CreateTable to the serenedb
  // road (ids, serials, ownership, OR REPLACE) exactly as a plain CREATE
  // TABLE. The operator in front of the load only reports progress.
  auto& schema_entry = op.schema.Cast<SereneDBSchemaEntry>();
  const auto database_id = schema_entry.GetDatabaseId();

  table_info.SetCatalog(GetName());
  // The record must not carry the SELECT: the rows are the data plane's.
  table_info.query.reset();
  auto table_name = std::string{table_info.GetTableName().GetIdentifierName()};

  auto& load_schema = op.schema;
  auto load_info = duckdb::make_uniq<duckdb::BoundCreateTableInfo>(
    load_schema, std::move(op.info->base));

  // duckdb's own load-operator selection: a partitionable, order-free load
  // uses PhysicalBatchInsert; wrapping PhysicalInsert directly forces the
  // serial commit-time flush, ~7x slower for a parallel source.
  auto& insert = duckdb::DuckCatalog::PlanCreateTableAsInsert(
    context, planner, op, load_schema, std::move(load_info), plan,
    op.estimated_cardinality);

  auto& ctas = planner.Make<connector::SereneDBPhysicalCTAS>(
    insert, database_id, op.schema.name.GetIdentifierName(),
    std::move(table_name), op.estimated_cardinality);
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
    auto& insert = planner.Make<connector::SereneDBSearchInsert>(
      connector::ResolveSearchWriteTarget(context, table_entry),
      std::move(op.types), op.estimated_cardinality, op.return_chunk);
    if (plan) {
      insert.children.push_back(*plan);
    }
    return insert;
  }

  return duckdb::DuckCatalog::PlanInsert(context, planner, op, plan);
}

std::vector<duckdb::idx_t> SearchPkSlots(const SereneDBTableEntry& table,
                                         duckdb::idx_t child_cols) {
  const auto rowid_cols = table.GetRowIdColumns();
  SDB_ASSERT(rowid_cols.size() <= child_cols);
  const auto virt_start = child_cols - rowid_cols.size();
  const auto slot_of = [&](duckdb::column_t id) {
    const auto it = absl::c_find(rowid_cols, id);
    SDB_ASSERT(it != rowid_cols.end());
    return virt_start + static_cast<duckdb::idx_t>(it - rowid_cols.begin());
  };
  const auto pk_positions = table.GetPKColumnIndexes();
  std::vector<duckdb::idx_t> slots;
  if (pk_positions.empty()) {
    slots.push_back(slot_of(kColumnIdentifierGeneratedPk));
    return slots;
  }
  slots.reserve(pk_positions.size());
  for (const auto position : pk_positions) {
    slots.push_back(slot_of(PKVirtualColumnId(position.index)));
  }
  return slots;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  if (auto* index_entry =
        dynamic_cast<ViewInvertedIndexScanEntry*>(&op.table)) {
    // The remove side of a REINDEX pass: plans only on an internal connection
    // (only driver code issues statements there); the sink removes the matched
    // (file, row) pks on the live index. Wire sessions keep the DML-on-index
    // error -- view indexes have no user DML surface.
    auto* conn_ctx = connector::GetSereneDBContextPtr(context);
    if (!conn_ctx || conn_ctx->GetSendBuffer()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
        ERR_MSG("cannot open relation \"", op.table.name.GetIdentifierName(),
                "\""),
        ERR_DETAIL("This operation is not supported for indexes."));
    }
    auto storage =
      InvertedStorageIn(&context, index_entry->catalog, index_entry->IndexId());
    if (!storage) {
      ThrowConcurrentlyDropped(index_entry->IndexId());
    }
    // The encoding config the removes are written against, off the committed
    // definition and pinned for the operator's lifetime.
    auto index = FindInvertedIndex(GetDatabaseId(), index_entry->IndexId());
    std::shared_ptr<const irs::IndexFieldOptions> field_options;
    if (index) {
      field_options = std::shared_ptr<const irs::IndexFieldOptions>{
        index, &InvertedInfo(*index)};
    }
    // The scan's row-identity columns (file_index, row_number) are last.
    std::vector<duckdb::idx_t> pk_indices{plan.types.size() - 2,
                                          plan.types.size() - 1};
    auto& index_del = planner.Make<connector::SereneDBSearchDelete>(
      index_entry->IndexId(), std::move(storage), std::move(field_options),
      std::move(pk_indices), std::move(op.types), op.estimated_cardinality);
    index_del.children.push_back(plan);
    return index_del;
  }
  auto& table_entry = RequireBaseTable(op.table);

  if (table_entry.IsSearchTable()) {
    if (op.is_truncate) {
      return planner.Make<connector::SereneDBSearchTruncate>(
        table_entry.GetSearchData(), op.estimated_cardinality,
        context.transaction.IsAutoCommit());
    }

    auto pk_indices = SearchPkSlots(table_entry, plan.types.size());
    // RETURNING: the binder already widened the scan to every column the clause
    // can name, and op.return_columns says which slot each of them arrived in.
    std::vector<duckdb::idx_t> column_map;
    if (op.return_chunk) {
      column_map.assign(op.return_columns.begin(), op.return_columns.end());
    }
    auto& search_del = planner.Make<connector::SereneDBSearchDelete>(
      connector::ResolveSearchWriteTarget(context, table_entry),
      std::move(pk_indices), std::move(op.types), std::move(column_map),
      op.estimated_cardinality);
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
    // passes every projected new-row column through, plus the PK virtuals, so
    // SereneDBSearchUpdate sees [resolved new-row vals, pk_virtuals].
    const auto pk_slots = SearchPkSlots(table_entry, plan.types.size());
    const auto num_updates = op.expressions.size();
    duckdb::vector<duckdb::LogicalType> proj_types;
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
    proj_types.reserve(num_updates + pk_slots.size());
    proj_exprs.reserve(num_updates + pk_slots.size());

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

    for (const auto slot : pk_slots) {
      proj_types.push_back(plan.types[slot]);
      proj_exprs.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        plan.types[slot], slot));
    }

    auto& proj = planner.Make<duckdb::PhysicalProjection>(
      std::move(proj_types), std::move(proj_exprs), op.estimated_cardinality);
    proj.children.push_back(plan);

    std::vector<duckdb::idx_t> pk_indices(pk_slots.size());
    absl::c_iota(pk_indices, num_updates);

    auto& search_upd = planner.Make<connector::SereneDBSearchUpdate>(
      connector::ResolveSearchWriteTarget(context, table_entry),
      std::move(pk_indices), std::move(op.columns), std::move(op.types),
      op.estimated_cardinality, op.return_chunk);
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
  if (connector::IsStorageStatement(binder.context)) {
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
    // The rebuild of an index the catalog already holds, as boot replay and a
    // reshape issue it: a plain duckdb index build over the rows.
    if (connector::IsStorageStatement(binder.context)) {
      return duckdb::Catalog::BindCreateIndex(binder, stmt, target,
                                              std::move(plan));
    }
    // A Search table takes an inverted index over its own store, so this is a
    // narrower gate than RejectIfSearchTable: only the kind and the
    // index-existing-rows limit.
    connector::ValidateSearchTableCreateIndex(
      RequireBaseTable(table),
      stmt.info->Cast<duckdb::CreateIndexInfo>().index_type);
    // A plain index is duckdb's ART in full -- bind, plan, build -- whatever
    // serenedb calls the kind; SereneDBSchemaEntry::CreateIndex files the
    // record when the build lands. Only an inverted index needs the bind below.
    auto& index_info = stmt.info->Cast<duckdb::CreateIndexInfo>();
    if (!absl::EqualsIgnoreCase(index_info.index_type,
                                catalog::kInvertedIndexType)) {
      auto type = absl::AsciiStrToLower(index_info.index_type);
      if (!type.empty() && type != "art" && type != "btree" &&
          type != "secondary") {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG("access method \"", index_info.index_type,
                                "\" does not exist"));
      }
      if (!index_info.options.empty()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("unrecognized parameter \"",
                                index_info.options.begin()->first, "\""));
      }
      if (index_info.where_clause) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("partial indexes are only supported for inverted indexes"));
      }
      index_info.index_type = duckdb::ART::TYPE_NAME;
      return duckdb::Catalog::BindCreateIndex(binder, stmt, target,
                                              std::move(plan));
    }
  }

  // The functions the key expressions and the predicate name, recorded where
  // their binds look them up. Scoped strictly around those binds: a scan bound
  // under the callback would copy it into bind data that outlives the
  // statement.
  const auto with_collection = [&binder, this](duckdb::CreateInfo& into,
                                               auto&& bind_keys) {
    binder.EntryRetriever().SetCallback(
      [&into, self = this](duckdb::CatalogEntry& entry) {
        if (catalog::KindOf(entry.type) == duckdb::CatalogType::MACRO_ENTRY &&
            &entry.ParentCatalog() == self) {
          into.dependencies.AddDependency(entry);
        }
      });
    const absl::Cleanup reset = [&binder] {
      binder.EntryRetriever().SetCallback(nullptr);
    };
    bind_keys();
  };

  // A REINDEX pass arrives as a SereneDBCreateIndexInfo statement (only the
  // driver can construct that subclass -- the parser never does).
  const auto* reindex_pass =
    dynamic_cast<const connector::SereneDBCreateIndexInfo*>(stmt.info.get());
  if (reindex_pass && reindex_pass->Pass() ==
                        connector::SereneDBCreateIndexInfo::ReindexPass::None) {
    reindex_pass = nullptr;
  }
  // View-backed indexes are STATIC -- captured at CREATE INDEX, no DML refresh.
  duckdb::optional_ptr<duckdb::TableCatalogEntry> resolved_table;
  bool view_backed = false;
  bool delta_pass = false;
  std::optional<connector::ViewFastPath> view_fast_path;
  std::optional<search::FileManifest> captured_manifest;
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
    std::optional<connector::ViewFastPath> fp;
    if (schema_id.isSet()) {
      if (const auto* view = Find<duckdb::ViewCatalogEntry>(
            &binder.context, schema_id, target.name.GetIdentifierName())) {
        auto key_cols = connector::KeyColumnsFromOptions(
          stmt.info->Cast<duckdb::CreateIndexInfo>().options);
        auto info = view->GetInfo();
        fp = connector::ResolveViewFastPath(
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
      delta_pass = reindex_pass &&
                   reindex_pass->Pass() ==
                     connector::SereneDBCreateIndexInfo::ReindexPass::Delta;
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
        connector::EnableIcebergSort(leaf_get->bind_data.get());
      }
      if (delta_pass) {
        connector::NarrowScanToDelta(
          *leaf_get, stmt.info->Cast<connector::SereneDBCreateIndexInfo>(),
          vcols, leaf_orig_size);
      }
      if (connector::IsFilePkSpec(view_fast_path->pk_spec) && !delta_pass &&
          leaf_get->bind_data) {
        captured_manifest = connector::CaptureManifest(
          binder.context,
          leaf_get->bind_data->Cast<duckdb::MultiFileBindData>());
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
      if (kept.size() < column_info->names.size() || partial_view_index ||
          delta_pass) {
        plan = InsertBackfillFilterProjection(
          std::move(plan), kept, column_info->names.size(), vcols.size(),
          binder.GenerateTableIndex());
        if (delta_pass) {
          connector::AddDeltaFileBase(
            binder, plan->Cast<duckdb::LogicalProjection>(), vcols, kept.size(),
            reindex_pass->delta_file_base);
        }
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
  // The info travels on as a SereneDBCreateIndexInfo (reindex passes already
  // are one; plain user creates upgrade here) so the bind captures below ride
  // the statement to the plan hook.
  auto create_index_info = [&] {
    auto base =
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
        std::move(stmt.info));
    if (dynamic_cast<connector::SereneDBCreateIndexInfo*>(base.get())) {
      return duckdb::unique_ptr_cast<duckdb::CreateIndexInfo,
                                     connector::SereneDBCreateIndexInfo>(
        std::move(base));
    }
    return duckdb::make_uniq<connector::SereneDBCreateIndexInfo>(
      std::move(*base));
  }();

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
      if (!absl::c_contains(connector::kCreateInvertedOptions,
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
    // columns (or ROW_ID for generated PK).
    auto projection = connector::BuildCreateIndexProjection(
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
      // A Search-table index is storage-less and never backfilled here (the
      // table has to be empty), so it needs no base rowid to bridge doc-id
      // spaces.
      if (!sdb_entry->IsSearchTable()) {
        get.AddColumnId(duckdb::COLUMN_IDENTIFIER_ROW_ID);
        get.types.push_back(duckdb::LogicalType::ROW_TYPE);
      }
    }
    SDB_ASSERT(get.bind_data,
               "base-table LogicalGet missing SereneDB bind_data");
    create_index_info->names = get.names;
    create_index_info->SetSchema(resolved_table->ParentSchema().name);
    create_index_info->SetCatalog(resolved_table->catalog.GetName());

    duckdb::IndexBinder index_binder(binder, binder.context, resolved_table,
                                     create_index_info.get());
    with_collection(*create_index_info, [&] {
      for (auto& parsed : create_index_info->expressions) {
        expressions.emplace_back(index_binder.Bind(parsed));
      }
      if (create_index_info->where_clause) {
        auto bound_where = bind_predicate(index_binder);
        backfill_filter_predicate = bound_where->Copy();
        expressions.emplace_back(std::move(bound_where));
      }
    });
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
      if (!delta_pass) {
        // A delta pass binds the view narrowed to its scan files, so ITS
        // fast path claims a single-file pk -- the driver already put the
        // REAL pk type on the statement; keep it.
        create_index_info->generated_pk_type =
          view_fast_path->GeneratedPkType();
      }
      if (captured_manifest) {
        create_index_info->manifest =
          std::make_shared<search::FileManifest>(std::move(*captured_manifest));
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
    with_collection(*create_index_info, [&] {
      for (auto& parsed : create_index_info->expressions) {
        auto bound = index_binder.Bind(parsed);
        remap(*bound);
        expressions.emplace_back(std::move(bound));
      }
      if (create_index_info->where_clause) {
        auto bound_where = bind_predicate(index_binder);

        SDB_ASSERT(plan->type ==
                   duckdb::LogicalOperatorType::LOGICAL_PROJECTION);
        auto& proj = plan->Cast<duckdb::LogicalProjection>();
        auto filter =
          duckdb::make_uniq<duckdb::LogicalFilter>(bound_where->Copy());
        filter->children.push_back(std::move(proj.children[0]));
        proj.children[0] = std::move(filter);
        // Persisted copy is normalized like the index keys.
        remap(*bound_where);
        expressions.emplace_back(std::move(bound_where));
      }
    });
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

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  return DatabaseStorageSize(context, _database_id, {});
}

void SereneDBCatalog::OnDetach(duckdb::ClientContext& context) {
  auto state = context.registered_state->Get<connector::SereneDBClientState>(
    connector::kSereneDBClientStateKey);

  auto ax = catalog::NoAccessCheck(context);
  if (state) {
    ax.role = state->GetConnectionContext().GetRoleId();
  }

  // The bind context outlives nothing: its connection holds this attachment
  // and its ConnectionContext holds the catalog Database going away here.
  catalog::DataStore::ForgetDatabase(GetDatabaseId());

  // An edge is kept by its dependent's catalog, so the dependency graph dies
  // with this attachment; DropDatabase below retires the database entry's own
  // cluster-global edges.
  //
  // The foreign servers are read now, the last point anything can: each holds
  // an instance-global attachment the cascade does not touch, captured by
  // identity so the detach removes only what this drop actually saw.
  const auto servers = CatalogForeignServerNames(*this);
  std::vector<catalog::ForeignServerAttachment> detach_servers;
  detach_servers.reserve(servers.size());
  for (const auto& name : servers) {
    detach_servers.emplace_back(catalog::ForeignServerAttachment{
      name, catalog::ForeignServerAttachmentId(name)});
  }

  duckdb::shared_ptr<void> keep_alive = GetAttached().shared_from_this();
  catalog::GetCatalog().DropDatabase(ax, GetName().GetIdentifierName(),
                                     std::move(keep_alive));
  for (const auto& server : detach_servers) {
    catalog::DetachForeignServerAttachment(server.name, server.attachment_id);
  }
}

std::shared_ptr<const Index> InvertedDefinitionIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId index_id) {
  const auto* index =
    catalog::FindIn<SereneDBIndexEntry>(context, catalog, index_id);
  return index != nullptr ? index->DefinitionPtr() : nullptr;
}

std::shared_ptr<search::InvertedIndexStorage> InvertedStorageIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId index_id) {
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return nullptr;
  }
  const auto* index =
    catalog::FindIn<SereneDBIndexEntry>(context, catalog, index_id);
  return index != nullptr ? index->GetInvertedData() : nullptr;
}

std::shared_ptr<search::InvertedIndexStorage> InvertedStorageIn(
  duckdb::Catalog& catalog, ObjectId index_id) {
  return InvertedStorageIn(nullptr, catalog, index_id);
}

std::shared_ptr<search::InvertedIndexStorage> InvertedStorageOf(
  duckdb::ClientContext* context, ObjectId database_id, ObjectId index_id) {
  auto attachment = catalog::TryStoreDatabase(database_id);
  if (!attachment) {
    return nullptr;
  }
  return InvertedStorageIn(context, attachment->GetCatalog(), index_id);
}

std::shared_ptr<search::InvertedIndexStorage> InvertedStorageOf(
  ObjectId database_id, ObjectId index_id) {
  return InvertedStorageOf(nullptr, database_id, index_id);
}

std::vector<size_t> SereneDBCatalog::IndexedColumns(
  ObjectId relation_id) const {
  absl::ReaderMutexLock lock{&_indexed_columns_mutex};
  const auto it = _indexed_columns.find(relation_id.id());
  if (it == _indexed_columns.end()) {
    return {};
  }
  std::vector<size_t> united;
  for (const auto& [index_id, columns] : it->second) {
    united.insert(united.end(), columns.begin(), columns.end());
  }
  absl::c_sort(united);
  united.erase(std::unique(united.begin(), united.end()), united.end());
  return united;
}

void SereneDBCatalog::SetIndexColumns(ObjectId relation_id, ObjectId index_id,
                                      std::vector<size_t> columns) const {
  absl::MutexLock lock{&_indexed_columns_mutex};
  _indexed_columns[relation_id.id()][index_id.id()] = std::move(columns);
}

void SereneDBCatalog::RemoveIndexColumns(ObjectId relation_id,
                                         ObjectId index_id) const {
  absl::MutexLock lock{&_indexed_columns_mutex};
  const auto it = _indexed_columns.find(relation_id.id());
  if (it == _indexed_columns.end()) {
    return;
  }
  it->second.erase(index_id.id());
  if (it->second.empty()) {
    _indexed_columns.erase(it);
  }
}

void SereneDBCatalog::ReleaseIndexedColumns(ObjectId relation_id) const {
  absl::MutexLock lock{&_indexed_columns_mutex};
  _indexed_columns.erase(relation_id.id());
}

void SereneDBCatalog::ReplayCatalogEntry(
  duckdb::ClientContext& /*context*/, duckdb::CreateInfo& info,
  const duckdb::CatalogPermissions& permissions, bool dropped) {
  // Contextless: replay runs a transaction with no statement behind it, and a
  // write attributed to one wants the client state a statement would have.
  catalog::ReplayCatalogRecord(info.Copy(), permissions, dropped);
}

namespace {

// The reshape recipe the alter behind this commit record left in the undo
// buffer: how the rows of a reshaped table got to the shape the version states,
// for the one step the definition cannot express -- ALTER COLUMN ... TYPE ...
// USING says what the old values become, and nothing in the resulting
// definition does. Null for every other change: a create and a drop leave
// nothing behind the entry, and every other reshape is the difference between
// the two definitions.
duckdb::unique_ptr<duckdb::AlterInfo> UndoBufferRowRecipe(
  const duckdb::CatalogEntry& old_entry, duckdb::data_ptr_t extra_data) {
  // Two versions of the same kind is what tells an alter from a create, which
  // is also what says the undo record carries an AlterInfo behind the entry.
  if (old_entry.type != duckdb::CatalogType::TABLE_ENTRY ||
      old_entry.Parent().type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  duckdb::MemoryStream source{extra_data + sizeof(duckdb::idx_t),
                              duckdb::Load<duckdb::idx_t>(extra_data)};
  duckdb::BinaryDeserializer deserializer{source};
  deserializer.Begin();
  duckdb::string column_name;
  deserializer.ReadProperty(100, "column_name", column_name);
  auto info = deserializer.ReadProperty<duckdb::unique_ptr<duckdb::ParseInfo>>(
    101, "alter_info");
  deserializer.End();
  auto& alter = info->Cast<duckdb::AlterInfo>();
  if (alter.type != duckdb::AlterType::ALTER_TABLE ||
      alter.Cast<duckdb::AlterTableInfo>().alter_table_type !=
        duckdb::AlterTableType::ALTER_COLUMN_TYPE) {
    return nullptr;
  }
  return duckdb::unique_ptr_cast<duckdb::ParseInfo, duckdb::AlterInfo>(
    std::move(info));
}

}  // namespace

void SereneDBCatalog::WriteCatalogChange(
  duckdb::DuckTransaction& /*transaction*/, duckdb::CatalogEntry& old_entry,
  duckdb::data_ptr_t extra_data) {
  using enum duckdb::CatalogType;
  // Written where duckdb writes its own WAL record for the same change: one
  // record per change, in commit order, made durable by the flush that ends the
  // commit. An alter is the next version of the object rather than the
  // statement that produced it -- applying a version needs no statement behind
  // it, and the reshape of the rows is the data WAL's own record, save for the
  // recipe below.
  auto& new_entry = old_entry.Parent();
  const bool dropped = new_entry.type == DELETED_ENTRY;
  auto& version = dropped ? old_entry : new_entry;
  switch (version.type) {
    case SCHEMA_ENTRY:
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
    case TYPE_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
    case TOKENIZER_ENTRY:
    case FOREIGN_SERVER_ENTRY:
      break;
    default:
      // duckdb's own bookkeeping: its dependency edges and the marker a rename
      // leaves behind, both rebuilt from the records above.
      return;
  }
  // Every serenedb kind travels as the generic entry record: duckdb's per-kind
  // create records carry neither the stable id nor the owner and ACL. The
  // index-name wrapper is nobody's object; recording it would put the index in
  // the log twice and replay it as two.
  if (version.type == TABLE_ENTRY &&
      dynamic_cast<const SereneDBTableEntry*>(&version) == nullptr) {
    return;
  }
  if (!ClusterCatalogWal()) {
    return;
  }
  if (dropped) {
    BufferCatalogDrop(version.GetInfo());
    return;
  }
  // Ahead of the definition it belongs to: a recipe with no definition is
  // dropped at boot, a definition with no recipe reshapes by cast.
  if (auto recipe = UndoBufferRowRecipe(old_entry, extra_data)) {
    BufferCatalogRecipe(std::move(recipe));
  }
  BufferCatalogCreate(version.GetInfo(), version.permissions);
}

bool SereneDBCatalog::DropSchema(const AccessContext& ax,
                                 std::string_view database,
                                 std::string_view name, bool cascade,
                                 bool missing_ok) {
  JoinStoreTransaction(ax.context);

  const auto database_id = FindDatabaseId(ax.context, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  const auto* schema = catalog::FindSchema(ax.context, database_id, name);
  if (schema == nullptr) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  const std::optional schema_id{IdOf(*schema)};
  RequireOwner(ax.context, ax.role, schema->permissions, "schema",
               schema->name.GetIdentifierName());

  // Collected before the set drop: the walk that removes the contents is
  // holding one of its sets. The victim roads cover everything else -- an
  // index's store half and artifacts, a sequence's counter row -- but a
  // table's search artifacts are the schema drop's own.
  std::vector<const catalog::SereneDBTableEntry*> tables;
  catalog::Visit<catalog::SereneDBTableEntry>(
    ax.context, database_id, [&](const catalog::SereneDBTableEntry& table) {
      if (catalog::ParentIdOf(table) == *schema_id) {
        tables.push_back(&table);
      }
    });
  // The set drop first, whole: every content holds a containment edge onto the
  // schema, so duckdb's dependency walk refuses a RESTRICT here, and takes
  // every content and outside victim through DropDependent on a cascade.
  catalog::DropSchemaEntry(ax.context, database_id, name, cascade);
  // Check that SereneDB won't open this schema after reboot
  SDB_IF_FAILURE("crash_on_drop") { return true; }
  for (const auto* table : tables) {
    catalog::DropSearchTableArtifacts(ax.context, *table);
  }
  return true;
}

void SereneDBCatalog::ChangeColumnType(
  const AccessContext& ax, const duckdb::CreateTableInfo& table,
  std::string_view column, duckdb::LogicalType new_type,
  duckdb::unique_ptr<duckdb::ParsedExpression> using_expr) {
  JoinStoreTransaction(ax.context);
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* entry =
    catalog::Find<SereneDBTableEntry>(ax.context, schema_id, table_id);
  if (entry == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             table.GetTableName().GetIdentifierName());
  }
  const auto& perm = entry->permissions;
  const auto live = entry->Definition();
  RequireOwner(ax.context, ax.role, perm, "table",
               entry->name.GetIdentifierName());
  // A missing column falls through: duckdb's alter names it in its own error.
  const auto* col = catalog::ColumnByName(*live, column);
  const auto table_indexes =
    catalog::RelationIndexRecords(ax.context, schema_id, table_id);
  if (col != nullptr) {
    const ObjectId col_id{col->CatalogOid()};
    // The index stores values of the column's old type; a type change would
    // leave them inconsistent. Reject and let the user drop the index first.
    for (const auto& idx : table_indexes) {
      if (idx->ReferencesColumn(col_id)) {
        THROW_SQL_ERROR(ERR_MSG("cannot alter type of column \"", column,
                                "\" because index \"", idx->GetName(),
                                "\" depends on it; drop the index first"));
      }
    }
  }

  const auto db_id = catalog::SchemaDatabaseId(ax.context, schema_id);
  duckdb::unique_ptr<duckdb::CreateTableInfo> updated;
  if (catalog::ReadTableEngineTag(live->tags) == TableEngine::Transactional) {
    catalog::StoreAlter(
      ax.context, db_id, table_id,
      duckdb::make_uniq<duckdb::ChangeColumnTypeInfo>(
        StoreTarget(), duckdb::Identifier{std::string{column}}, new_type,
        using_expr ? using_expr->Copy() : nullptr));
    const auto* altered =
      catalog::Find<SereneDBTableEntry>(ax.context, schema_id, table_id);
    SDB_ASSERT(altered);
    updated = altered->Definition();
  } else {
    // A search table's rows live in iresearch, keyed by column id: duckdb has
    // nothing to rewrite, so only the definition changes and only this write
    // records it. Reached by the nested-field remaps alone -- a search table
    // refuses a plain ALTER TYPE.
    updated = catalog::ChangeColumnType(*live, column, new_type);
    SetIdentity(*updated, table_id, schema_id);
    catalog::PutEntry(ax.context, updated->GetTableName().GetIdentifierName(),
                      updated->Copy(), perm);
  }
  for (auto& new_idx :
       catalog::RelationIndexVersions(table_indexes, *live, *updated)) {
    catalog::PutEntry(ax.context, new_idx->GetName(), std::move(new_idx));
  }
}

bool SereneDBCatalog::CreateTokenizer(
  const AccessContext& ax, ObjectId database_id, std::string_view schema,
  std::shared_ptr<CreateTokenizerInfo> tokenizer, bool if_not_exists) {
  auto schema_id = TryFindSchemaId(ax.context, database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(ax.context, ax.role, *schema_id);
  const auto name = tokenizer->GetName();
  if (catalog::FindTokenizer(ax.context, *schema_id, name)) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" already exists"));
  }
  if (!tokenizer->GetId().isSet()) {
    tokenizer->SetId(NextId());
  }
  tokenizer->SetSchemaId(*schema_id);
  Permissions perm{ax.role};
  catalog::PutEntry(ax.context, /*old_name=*/{}, tokenizer->Copy(),
                    std::move(perm));
  return true;
}

bool SereneDBCatalog::CreateForeignServer(
  const AccessContext& ax, ObjectId database_id,
  std::shared_ptr<CreateForeignServerInfo> info, Permissions perm,
  bool if_not_exists) {
  // Gated on CREATE on the database, same as CREATE SCHEMA -- PG gates on FDW
  // USAGE instead, but serenedb has no foreign-data-wrapper catalog object to
  // hang an ACL on.
  RequireDatabaseAccess(ax.context, ax.role,
                        catalog::FindDatabase(ax.context, database_id),
                        AclMode::Create);
  const auto name = info->GetName();
  if (!IsSupportedFdw(info->GetFdwName())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("foreign-data wrapper \"", info->GetFdwName(),
                            "\" is not supported"),
                    ERR_HINT("Use one of: ", SupportedFdwList(), "."));
  }
  if (catalog::FindForeignServer(ax.context, database_id, name)) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("server \"", name, "\" already exists"));
  }
  // Catalog names are per-database, but the live attachment alias is
  // instance-global: a second same-named server would race the first for it
  // (nondeterministic boot winner; DROP DATABASE detaching the other
  // database's attachment). The attach cannot catch this -- it only collides
  // while the first server's attachment is live, not when its remote is down.
  std::vector<const catalog::SereneDBDatabaseEntry*> databases;
  catalog::VisitDatabases(ax.context,
                          [&](const catalog::SereneDBDatabaseEntry& db) {
                            databases.push_back(&db);
                          });
  for (const auto* db : databases) {
    // A database shares the alias namespace with foreign servers, so a server
    // named after one would make DROP SERVER's detach tear the database down.
    if (db->name.GetIdentifierName() == name) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("database \"", db->name.GetIdentifierName(),
                "\" already exists, so a server cannot take that name"),
        ERR_HINT("Foreign server attachment names are instance-wide; "
                 "choose a name not used by any database."));
    }
    if (catalog::IdOf(*db) != database_id &&
        catalog::FindForeignServer(ax.context, catalog::IdOf(*db), name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("server \"", name, "\" already exists in database \"",
                db->name.GetIdentifierName(), "\""),
        ERR_HINT("Foreign server attachment names are instance-wide; "
                 "choose a name not used by any database."));
    }
  }
  info->SetDatabaseId(database_id);
  const auto id = info->GetId().isSet() ? info->GetId() : NextId();
  info->SetId(id);
  // One definition, handed to the record and to the entry: nothing is derived
  // at append time.
  catalog::PutEntry(ax.context, /*old_name=*/{}, info->Copy(), perm);
  return true;
}

}  // namespace sdb::catalog
