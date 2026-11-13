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

#include "catalog/base_catalog.h"

#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_job_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/parser/parsed_data/create_tokenizer_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/database_size.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "catalog/dependency.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

[[noreturn]] void RefuseWrites(duckdb::Catalog& catalog) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("catalog \"", catalog.GetName().GetIdentifierName(),
                          "\" has no tables"));
}

int EntryErrcode(duckdb::CatalogType type, bool exists) {
  using enum duckdb::CatalogType;
  switch (type) {
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
      return exists ? ERRCODE_DUPLICATE_TABLE : ERRCODE_UNDEFINED_TABLE;
    case SCHEMA_ENTRY:
      return exists ? ERRCODE_DUPLICATE_SCHEMA : ERRCODE_UNDEFINED_SCHEMA;
    case DATABASE_ENTRY:
      return exists ? ERRCODE_DUPLICATE_DATABASE : ERRCODE_UNDEFINED_DATABASE;
    case TABLE_FUNCTION_ENTRY:
    case SCALAR_FUNCTION_ENTRY:
    case AGGREGATE_FUNCTION_ENTRY:
    case WINDOW_FUNCTION_ENTRY:
    case COPY_FUNCTION_ENTRY:
    case PRAGMA_FUNCTION_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return exists ? ERRCODE_DUPLICATE_FUNCTION : ERRCODE_UNDEFINED_FUNCTION;
    default:
      return exists ? ERRCODE_DUPLICATE_OBJECT : ERRCODE_UNDEFINED_OBJECT;
  }
}

std::string_view EntryNoun(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  switch (type) {
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
      return "relation";
    case SCHEMA_ENTRY:
      return "schema";
    case DATABASE_ENTRY:
      return "database";
    case TABLE_FUNCTION_ENTRY:
    case SCALAR_FUNCTION_ENTRY:
    case AGGREGATE_FUNCTION_ENTRY:
    case WINDOW_FUNCTION_ENTRY:
    case COPY_FUNCTION_ENTRY:
    case PRAGMA_FUNCTION_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return "function";
    case TYPE_ENTRY:
      return "type";
    case COLLATION_ENTRY:
      return "collation";
    case TOKENIZER_ENTRY:
      return "text search dictionary";
    case ROLE_ENTRY:
      return "role";
    case FOREIGN_SERVER_ENTRY:
      return "server";
    case SUBSCRIPTION_ENTRY:
      return "subscription";
    case TRIGGER_ENTRY:
      return "trigger";
    case JOB_ENTRY:
      return "job";
    case COORDINATE_SYSTEM_ENTRY:
      return "coordinate system";
    default:
      return "object";
  }
}

}  // namespace

std::shared_ptr<DependentIndex> SharedDependents(duckdb::AttachedDatabase& db) {
  if (db.IsSystem() || db.IsReadOnly()) {
    return std::make_shared<DependentIndex>();
  }
  return duckdb::Catalog::GetSystemCatalog(db.GetDatabase())
    .Cast<BaseCatalog>()
    .Oids()
    .SharedDependents();
}

void ThrowEntryExists(duckdb::CatalogType type,
                      const duckdb::Identifier& name) {
  THROW_SQL_ERROR(ERR_CODE(EntryErrcode(type, true)),
                  ERR_MSG(EntryNoun(type), " \"", name.GetIdentifierName(),
                          "\" already exists"));
}

void ThrowMissingEntry(duckdb::CatalogType type,
                       const duckdb::Identifier& name) {
  THROW_SQL_ERROR(ERR_CODE(EntryErrcode(type, false)),
                  ERR_MSG(EntryNoun(type), " \"", name.GetIdentifierName(),
                          "\" does not exist"));
}

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(), type);
}

duckdb::optional_ptr<duckdb::CatalogEntry> AlterEntry(
  duckdb::CatalogTransaction transaction, EntrySet& set,
  duckdb::CatalogEntry& entry, duckdb::AlterInfo& info) {
  auto altered = entry.AlterEntry(transaction, info);
  if (!altered) {
    return nullptr;
  }
  altered->oid = entry.oid;
  if (info.type != duckdb::AlterType::ALTER_PERMISSIONS) {
    altered->permissions = entry.permissions;
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> result = altered.get();
  const auto new_name = altered->name;
  auto alter = SerializeAlter(info);
  if (set.SameName(new_name, entry.name)) {
    set.Alter(transaction, std::move(altered), std::move(alter));
  } else if (!set.Rename(transaction, entry.name, std::move(altered),
                         std::move(alter))) {
    ThrowEntryExists(entry.type, new_name);
  }
  result->SetAsRoot(&transaction, &entry);
  return result;
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  if (!info.ParentSchemas().empty()) {
    const duckdb::EntryLookupInfo lookup{
      duckdb::CatalogType::SCHEMA_ENTRY,
      info.GetQualifiedName().Parent().Parent()};
    return LookupSchema(transaction, lookup,
                        duckdb::OnEntryNotFound::THROW_EXCEPTION)
      ->Cast<Schema>()
      .CreateSchema(transaction, info);
  }
  const auto& name = info.SchemaName();
  if (!info.internal && duckdb::DefaultSchemaGenerator::IsDefaultSchema(name)) {
    if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
      ThrowEntryExists(duckdb::CatalogType::SCHEMA_ENTRY, name);
    }
    return nullptr;
  }
  DeclareModified(transaction, *this);
  if (info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
    if (auto existing = _schemas.Get(transaction, name)) {
      DropDependents(transaction, *existing, false);
      _schemas.Drop(transaction, name);
    }
  }
  auto entry = duckdb::make_uniq<Schema>(*this, info);
  auto* result = entry.get();
  if (!_schemas.Create(transaction, std::move(entry))) {
    if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
      ThrowEntryExists(duckdb::CatalogType::SCHEMA_ENTRY, name);
    }
    return nullptr;
  }
  return result;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> BaseCatalog::LookupSchema(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& schema_lookup,
  duckdb::OnEntryNotFound if_not_found) {
  return LookupSchemaPath(transaction, schema_lookup, if_not_found,
                          [&](const duckdb::EntryLookupInfo& lookup) {
                            return _schemas.Get(transaction,
                                                lookup.GetEntryIdentifier());
                          });
}

void BaseCatalog::ScanSchemas(
  duckdb::ClientContext& context,
  std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
  const bool system_schemas =
    ConnectionContext::Get(context).lists_system_schemas;
  const auto transaction = GetCatalogTransaction(context);
  _schemas.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
    if (system_schemas || !entry.internal) {
      entry.Cast<Schema>().ScanSchemaTree(transaction, callback);
    }
  });
}

void BaseCatalog::ScanSchemas(absl::FunctionRef<void(Schema&)> callback) {
  _schemas.ScanCommitted([&](duckdb::CatalogEntry& entry) {
    entry.Cast<Schema>().ScanSchemaTree(
      [&](duckdb::SchemaCatalogEntry& schema) {
        callback(schema.Cast<Schema>());
      });
  });
}

void BaseCatalog::DropSchema(duckdb::ClientContext& context,
                             duckdb::DropInfo& info) {
  auto transaction = GetCatalogTransaction(context);
  const duckdb::EntryLookupInfo lookup{duckdb::CatalogType::SCHEMA_ENTRY,
                                       info.GetQualifiedName()};
  auto found = LookupSchema(transaction, lookup, info.if_not_found);
  if (!found) {
    return;
  }
  auto& schema = found->Cast<Schema>();
  if (schema.internal && !info.allow_drop_internal) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("Cannot drop entry \"", schema.name.GetIdentifierName(),
              "\" because it is an internal system entry"));
  }
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  DropDependents(transaction, schema, info.cascade);
  SchemaSet(transaction, schema).Drop(transaction, schema.name);
}

void BaseCatalog::AlterSchemaEntry(duckdb::CatalogTransaction transaction,
                                   duckdb::SchemaCatalogEntry& schema,
                                   duckdb::AlterInfo& info) {
  if (schema.internal && !info.allow_internal) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("Cannot alter entry \"", schema.name.GetIdentifierName(),
              "\" because it is an internal system entry"));
  }
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::ALTER_TABLE);
  auto& set = SchemaSet(transaction, schema);
  std::vector<PendingDefault> defaults;
  std::vector<duckdb::idx_t> tables;
  const auto old_name = schema.name;
  if (const auto new_name = info.GetNewName();
      new_name && !set.SameName(*new_name, schema.name) &&
      info.bind_mode != duckdb::AlterBindMode::SKIP_BINDING) {
    FollowSchemaRename(transaction, schema, *new_name, defaults);
    schema.Cast<Schema>()
      .Set(duckdb::CatalogType::TABLE_ENTRY)
      .Scan(transaction,
            [&](duckdb::CatalogEntry& table) { tables.push_back(table.oid); });
  }
  catalog::AlterEntry(transaction, set, schema, info);
  SetDefaults(transaction, *this, defaults);
  for (const auto oid : tables) {
    auto table = LookupOid(transaction, oid);
    FollowReferences(transaction, *table, old_name, table->name);
  }
}

void BaseCatalog::AlterSchema(duckdb::CatalogTransaction transaction,
                              duckdb::SchemaCatalogEntry& schema,
                              duckdb::AlterSchemaInfo& info) {
  if (info.alter_schema_type != duckdb::AlterSchemaType::RENAME_SCHEMA) {
    duckdb::Catalog::AlterSchema(transaction, schema, info);
    return;
  }
  AlterSchemaEntry(transaction, schema, info);
}

duckdb::optional_idx BaseCatalog::GetCatalogVersion(
  duckdb::ClientContext& context) {
  return duckdb::Transaction::Get(context, *this)
    .Cast<DatabaseTransaction>()
    .Session()
    .CatalogEpoch();
}

duckdb::PhysicalOperator& BaseCatalog::PlanCreateTableAs(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator&,
  duckdb::LogicalCreateTable&, duckdb::PhysicalOperator&) {
  RefuseWrites(*this);
}

duckdb::PhysicalOperator& BaseCatalog::PlanInsert(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator&,
  duckdb::LogicalInsert&, duckdb::optional_ptr<duckdb::PhysicalOperator>) {
  RefuseWrites(*this);
}

duckdb::PhysicalOperator& BaseCatalog::PlanDelete(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator&,
  duckdb::LogicalDelete&, duckdb::PhysicalOperator&) {
  RefuseWrites(*this);
}

duckdb::PhysicalOperator& BaseCatalog::PlanUpdate(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator&,
  duckdb::LogicalUpdate&, duckdb::PhysicalOperator&) {
  RefuseWrites(*this);
}

duckdb::DatabaseSize BaseCatalog::GetDatabaseSize(duckdb::ClientContext&) {
  return {};
}

void BaseCatalog::ScanSubjects(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& entry,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  catalog::ScanSubjects(
    transaction, entry,
    [&](duckdb::CatalogEntry& subject,
        const duckdb::DependencyDependentFlags&) { callback(subject); });
}

duckdb::catalog_entry_vector_t BaseCatalog::OwnedEntries(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& owner) {
  return catalog::OwnedEntries(transaction, owner);
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::OwnerOf(
  duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& owned) {
  return catalog::OwnerOf(transaction, owned);
}

duckdb::unique_ptr<duckdb::CatalogEntry> BaseCatalog::MakeTable(
  duckdb::CatalogTransaction, Schema&, duckdb::BoundCreateTableInfo&) {
  RefuseWrites(*this);
}

duckdb::unique_ptr<duckdb::CatalogEntry> BaseCatalog::MakeIndex(
  duckdb::CatalogTransaction, Schema&, duckdb::CreateIndexInfo&,
  duckdb::CatalogEntry&) {
  RefuseWrites(*this);
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::AddTo(
  EntrySet& set, duckdb::CatalogTransaction transaction,
  duckdb::unique_ptr<duckdb::CatalogEntry> entry,
  duckdb::OnCreateConflict on_conflict) {
  DeclareModified(transaction, *this);
  const auto type = entry->type;
  const auto name = entry->name;
  duckdb::optional_ptr<duckdb::CatalogEntry> result = entry.get();
  if (set.Create(transaction, std::move(entry))) {
    return result;
  }
  if (on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
    return nullptr;
  }
  ThrowEntryExists(type, name);
}

bool BaseCatalog::DropFrom(EntrySet& set,
                           duckdb::CatalogTransaction transaction,
                           duckdb::CatalogType type, duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  const auto& name = info.GetQualifiedName().Name();
  if (set.Drop(transaction, name)) {
    return true;
  }
  if (info.if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
    ThrowMissingEntry(type, name);
  }
  return false;
}

EntrySet& BaseCatalog::Set(duckdb::CatalogType type) {
  if (type != duckdb::CatalogType::SCHEMA_ENTRY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("Unsupported catalog type ", duckdb::CatalogTypeToString(type),
              " in catalog ", GetName().GetIdentifierName()));
  }
  return _schemas;
}

EntrySet& BaseCatalog::SchemaSet(duckdb::CatalogTransaction transaction,
                                 const duckdb::SchemaCatalogEntry& schema) {
  const auto& parent = schema.GetSchemaInfo()->parent;
  if (!parent) {
    return _schemas;
  }
  auto entry = LookupOid(transaction, parent->oid);
  SDB_ASSERT(entry);
  return entry->Cast<Schema>().Set(duckdb::CatalogType::SCHEMA_ENTRY);
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::LookupOid(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  if (auto entry = _oids.Find(transaction, oid)) {
    return entry;
  }
  _schemas.Scan(transaction, [&](duckdb::CatalogEntry& schema) {
    schema.Cast<Schema>().CreateDefaults(transaction);
  });
  return _oids.Find(transaction, oid);
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::LookupOwner(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  if (const auto owner = _oids.Owner(oid)) {
    return _oids.Find(transaction, owner);
  }
  return LookupOid(transaction, oid);
}

duckdb::optional_ptr<duckdb::CatalogEntry> BaseCatalog::FindRelation(
  duckdb::CatalogTransaction transaction, duckdb::idx_t oid) {
  auto entry = _oids.Find(transaction, oid);
  if (!entry || (entry->type != duckdb::CatalogType::TABLE_ENTRY &&
                 entry->type != duckdb::CatalogType::VIEW_ENTRY)) {
    return nullptr;
  }
  return entry;
}

void BaseCatalog::ReplayCreate(duckdb::ClientContext& context,
                               duckdb::unique_ptr<duckdb::CreateInfo> info) {
  switch (info->type) {
    case duckdb::CatalogType::SCHEMA_ENTRY:
      CreateSchema(context, info->Cast<duckdb::CreateSchemaInfo>());
      return;
    case duckdb::CatalogType::TABLE_ENTRY: {
      auto& schema = GetEntrySchema(GetCatalogTransaction(context),
                                    info->GetQualifiedName());
      auto bound =
        duckdb::Binder::BindCreateTableCheckpoint(std::move(info), schema);
      CreateTable(context, *bound);
      return;
    }
    case duckdb::CatalogType::INDEX_ENTRY: {
      auto& index = info->Cast<duckdb::CreateIndexInfo>();
      auto relation =
        FindRelation(GetCatalogTransaction(context), index.table_oid);
      SDB_ENSURE(relation, "replayed index ", index.oid,
                 " names a missing relation ", index.table_oid);
      index.table = relation->name;
      relation->ParentSchema(context).CreateIndex(context, index, *relation);
      return;
    }
    case duckdb::CatalogType::VIEW_ENTRY:
      CreateView(context, info->Cast<duckdb::CreateViewInfo>());
      return;
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      CreateSequence(context, info->Cast<duckdb::CreateSequenceInfo>());
      return;
    case duckdb::CatalogType::TYPE_ENTRY:
      CreateType(context, info->Cast<duckdb::CreateTypeInfo>());
      return;
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      CreateFunction(context, info->Cast<duckdb::CreateFunctionInfo>());
      return;
    case duckdb::CatalogType::TOKENIZER_ENTRY:
      CreateTokenizer(context, info->Cast<duckdb::CreateTokenizerInfo>());
      return;
    case duckdb::CatalogType::JOB_ENTRY:
      CreateJob(context, info->Cast<duckdb::CreateJobInfo>());
      return;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                      ERR_MSG("the log creates an unsupported entry of type ",
                              duckdb::CatalogTypeToString(info->type)));
  }
}

void BaseCatalog::ReplayDrop(duckdb::ClientContext& context,
                             duckdb::CatalogEntry& entry) {
  auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  if (entry.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    SchemaSet(transaction, entry.Cast<Schema>()).Drop(transaction, entry.name);
    return;
  }
  entry.ParentSchema(transaction).Cast<Schema>().Drop(transaction, entry);
}

}  // namespace sdb::catalog
