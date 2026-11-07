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

#include "catalog/schema.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/collate_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/coordinate_system_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_aggregate_function_info.hpp>
#include <duckdb/parser/parsed_data/create_collation_info.hpp>
#include <duckdb/parser/parsed_data/create_coordinate_system_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_pragma_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/create_window_function_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <ranges>
#include <span>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/dependency.h"
#include "catalog/entry/job.h"
#include "catalog/entry/tokenizer.h"
#include "catalog/record.h"
#include "pg/tsdictionary.h"
#include "scheduler/job_scheduler.h"

namespace sdb::catalog {
namespace {

bool IsRelation(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::TABLE_ENTRY:
    case duckdb::CatalogType::VIEW_ENTRY:
    case duckdb::CatalogType::INDEX_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
      return true;
    default:
      return false;
  }
}

bool InKind(const duckdb::CatalogEntry& entry, duckdb::CatalogType kind) {
  switch (kind) {
    case duckdb::CatalogType::TABLE_ENTRY:
      return entry.type == duckdb::CatalogType::TABLE_ENTRY ||
             entry.type == duckdb::CatalogType::VIEW_ENTRY;
    case duckdb::CatalogType::MACRO_ENTRY:
      return entry.type == duckdb::CatalogType::MACRO_ENTRY ||
             entry.type == duckdb::CatalogType::TABLE_MACRO_ENTRY;
    default:
      return entry.type == kind;
  }
}

void AlterAllInSchema(Schema& schema, duckdb::CatalogTransaction transaction,
                      duckdb::AlterPermissionsInfo& info) {
  const auto kind = info.entry_catalog_type;
  std::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  schema.Set(kind).Scan(transaction, [&](duckdb::CatalogEntry& entry) {
    if (!entry.internal && InKind(entry, kind)) {
      entries.emplace_back(entry);
    }
  });
  info.all_in_schema = false;
  for (auto& entry : entries) {
    info.entry_catalog_type =
      entry.get().type == duckdb::CatalogType::VIEW_ENTRY
        ? duckdb::CatalogType::TABLE_ENTRY
        : entry.get().type;
    info.SetQualifiedName(schema.GetQualifiedName(entry.get().name));
    schema.Alter(transaction, info);
  }
}

void ReownSequences(duckdb::CatalogTransaction transaction,
                    duckdb::CatalogEntry& table,
                    const duckdb::AlterPermissionsInfo& info) {
  for (auto& owned : OwnedEntries(transaction, table)) {
    auto& sequence = owned.get();
    if (sequence.type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      continue;
    }
    auto& schema = sequence.ParentSchema(transaction).Cast<Schema>();
    duckdb::AlterPermissionsInfo reown{duckdb::CatalogType::SEQUENCE_ENTRY,
                                       schema.GetQualifiedName(sequence.name)};
    reown.new_owner = info.new_owner;
    reown.new_owner_id = info.new_owner_id;
    schema.Alter(transaction, reown);
  }
}

void ResolveDependencyOids(duckdb::CatalogTransaction transaction,
                           duckdb::Catalog& catalog,
                           duckdb::CatalogEntry& entry) {
  if (entry.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    return;
  }
  auto& dependencies = entry.Cast<duckdb::StandardEntry>().dependencies;
  if (absl::c_none_of(dependencies.Set(),
                      [](const duckdb::LogicalDependency& dependency) {
                        return dependency.oid == 0;
                      })) {
    return;
  }
  const duckdb::IdentifierEquality same_name{catalog.IsCaseSensitive()};
  duckdb::LogicalDependencyList resolved;
  for (auto dependency : dependencies.Set()) {
    if (dependency.oid == 0 && !dependency.entry.schema_path.empty() &&
        (dependency.catalog.empty() ||
         same_name(dependency.catalog, catalog.GetName()))) {
      const auto name = duckdb::QualifiedName::FromCatalogSchema(
        catalog.GetName(), dependency.entry.schema_path, dependency.entry.name);
      if (auto schema = catalog.Cast<BaseCatalog>().LookupSchema(
            transaction,
            duckdb::EntryLookupInfo{duckdb::CatalogType::SCHEMA_ENTRY,
                                    name.Parent()},
            duckdb::OnEntryNotFound::RETURN_NULL)) {
        if (const auto subject = schema->Cast<Schema>()
                                   .Set(dependency.entry.type)
                                   .Get(transaction, dependency.entry.name)) {
          dependency.oid = subject->oid;
        }
      }
    }
    resolved.AddDependency(dependency);
  }
  dependencies = std::move(resolved);
}

}  // namespace

SchemaSets::SchemaSets(bool case_sensitive, OidIndex& oids)
  : tables{case_sensitive, oids, 0, true},
    indexes{case_sensitive, oids, 0, true},
    table_functions{case_sensitive, oids, 0, true},
    copy_functions{case_sensitive, oids, 0, true},
    pragma_functions{case_sensitive, oids, 0, true},
    functions{case_sensitive, oids, 0, true},
    sequences{case_sensitive, oids, 0, true},
    collations{case_sensitive, oids, 0, true},
    coordinate_systems{case_sensitive, oids, 0, true},
    types{case_sensitive, oids, 0, true},
    tokenizers{case_sensitive, oids, 0, true},
    jobs{case_sensitive, oids, 0, true},
    schemas{case_sensitive, oids} {}

Schema::Schema(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info,
               duckdb::optional_ptr<duckdb::SchemaCatalogEntry> parent,
               std::shared_ptr<SchemaSets> sets,
               duckdb::shared_ptr<duckdb::SchemaInfo> schema_info)
  : duckdb::SchemaCatalogEntry{catalog, info, parent, std::move(schema_info)},
    _sets{std::move(sets)} {
  if (!_sets) {
    _sets = std::make_shared<SchemaSets>(catalog.IsCaseSensitive(),
                                         catalog.Cast<BaseCatalog>().Oids());
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> Schema::AlterEntry(
  duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) {
  auto create = GetInfo();
  auto& schema_info = create->Cast<duckdb::CreateSchemaInfo>();
  if (const auto new_name = info.GetNewName()) {
    schema_info.SetQualifiedName(
      duckdb::QualifiedName{{*new_name}, duckdb::Identifier{}});
  }
  if (info.type == duckdb::AlterType::SET_COMMENT) {
    schema_info.comment = info.Cast<duckdb::SetCommentInfo>().comment_value;
  }
  if (info.type == duckdb::AlterType::ALTER_PERMISSIONS) {
    info.Cast<duckdb::AlterPermissionsInfo>().ApplyTo(schema_info.permissions,
                                                      type, nullptr);
  }
  auto result = duckdb::make_uniq<Schema>(catalog, schema_info, nullptr, _sets,
                                          GetSchemaInfo());
  result->_inherited = _inherited;
  return result;
}

EntrySet& Schema::Set(duckdb::CatalogType type) {
  auto& sets = *_sets;
  switch (type) {
    case duckdb::CatalogType::VIEW_ENTRY:
    case duckdb::CatalogType::TABLE_ENTRY:
      return sets.tables;
    case duckdb::CatalogType::INDEX_ENTRY:
      return sets.indexes;
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      return sets.table_functions;
    case duckdb::CatalogType::COPY_FUNCTION_ENTRY:
      return sets.copy_functions;
    case duckdb::CatalogType::PRAGMA_FUNCTION_ENTRY:
      return sets.pragma_functions;
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::WINDOW_FUNCTION_ENTRY:
      return sets.functions;
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return sets.sequences;
    case duckdb::CatalogType::COLLATION_ENTRY:
      return sets.collations;
    case duckdb::CatalogType::COORDINATE_SYSTEM_ENTRY:
      return sets.coordinate_systems;
    case duckdb::CatalogType::TYPE_ENTRY:
      return sets.types;
    case duckdb::CatalogType::TOKENIZER_ENTRY:
      return sets.tokenizers;
    case duckdb::CatalogType::JOB_ENTRY:
      return sets.jobs;
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return sets.schemas;
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("Unsupported catalog type ", duckdb::CatalogTypeToString(type),
                " in schema ", name.GetIdentifierName()));
  }
}

std::vector<EntrySet*> Schema::SiblingSets(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  if (type == TYPE_ENTRY) {
    return {&_sets->tables};
  }
  std::vector<EntrySet*> sets;
  for (auto* set : {&_sets->tables, &_sets->indexes, &_sets->sequences}) {
    if (set != &Set(type)) {
      sets.push_back(set);
    }
  }
  if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
    sets.push_back(&_sets->types);
  }
  return sets;
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::SiblingEntry(
  duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
  const duckdb::Identifier& name, bool types) {
  if (!catalog.IsCaseSensitive() || !IsRelation(type)) {
    return nullptr;
  }
  for (auto* set : SiblingSets(type)) {
    if (set == &_sets->types && !types) {
      continue;
    }
    if (auto taken = set->Get(transaction, name)) {
      return taken;
    }
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::NamespaceEntry(
  duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
  const duckdb::Identifier& name) {
  if (auto entry = Set(type).Get(transaction, name)) {
    return entry;
  }
  return SiblingEntry(transaction, type, name, false);
}

void Schema::CheckRelationName(duckdb::CatalogTransaction transaction,
                               duckdb::CatalogType type,
                               const duckdb::Identifier& entry_name) {
  if (auto taken = SiblingEntry(transaction, type, entry_name, true)) {
    ThrowEntryExists(
      type == duckdb::CatalogType::TYPE_ENTRY ? type : taken->type, entry_name);
  }
  if (!catalog.IsCaseSensitive() || !IsRelation(type)) {
    return;
  }
  for (auto* set : SiblingSets(type)) {
    set->CheckNameWritable(transaction, entry_name);
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::Add(
  duckdb::CatalogTransaction transaction,
  duckdb::unique_ptr<duckdb::CatalogEntry> entry,
  duckdb::OnCreateConflict on_conflict) {
  DeclareModified(transaction, catalog);
  const auto type = entry->type;
  const auto entry_name = entry->name;
  auto& set = Set(type);
  if (on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT ||
      on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
    auto old = NamespaceEntry(transaction, type, entry_name);
    if (old && on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    if (old) {
      if (type != duckdb::CatalogType::SCHEMA_ENTRY &&
          entry->Cast<duckdb::StandardEntry>().dependencies.Contains(*old)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
          ERR_MSG("CREATE OR REPLACE is not allowed to depend on itself"));
      }
      if (old->type != type) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
          ERR_MSG("Existing object ",
                  duckdb::SQLQuotedIdentifier::ToString(entry_name),
                  " is of type ", duckdb::CatalogTypeToString(old->type),
                  ", trying to replace with type ",
                  duckdb::CatalogTypeToString(type)));
      }
      entry->permissions = old->permissions;
      if (!old->internal) {
        DropDependents(transaction, *old, false);
      }
      Drop(transaction, *old);
    }
  }
  CheckRelationName(transaction, type, entry_name);
  ResolveDependencyOids(transaction, catalog, *entry);
  auto* result = entry.get();
  if (!set.Create(transaction, std::move(entry))) {
    if (on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
      const auto existing = set.Get(transaction, entry_name);
      ThrowEntryExists(existing ? existing->type : type, entry_name);
    }
    return nullptr;
  }
  result->SetAsRoot(&transaction, nullptr);
  return result;
}

bool Schema::AlterExisting(duckdb::CatalogTransaction transaction,
                           duckdb::CreateInfo& info,
                           const duckdb::Identifier& entry_name) {
  if (info.on_conflict != duckdb::OnCreateConflict::ALTER_ON_CONFLICT) {
    return false;
  }
  auto current = NamespaceEntry(transaction, info.type, entry_name);
  if (!current) {
    return false;
  }
  if (current->type != info.type) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("Existing object ",
              duckdb::SQLQuotedIdentifier::ToString(entry_name), " is of type ",
              duckdb::CatalogTypeToString(current->type),
              ", trying to replace with type ",
              duckdb::CatalogTypeToString(info.type)));
  }
  if (!current->internal &&
      InKind(*current, duckdb::CatalogType::MACRO_ENTRY)) {
    RefuseDependents(
      transaction, *current, [](const duckdb::CatalogEntry& dependent) {
        return dependent.type == duckdb::CatalogType::INDEX_ENTRY;
      });
  }
  auto alter = info.GetAlterInfo();
  Alter(transaction, *alter);
  return true;
}

void Schema::Scan(duckdb::ClientContext& context, duckdb::CatalogType type,
                  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  Scan(GetCatalogTransaction(context), type, callback);
}

void Schema::Scan(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogType type,
                  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  auto& own = Set(type);
  own.Scan(transaction, callback);
  for (const auto& inherited : _inherited) {
    if (!inherited.types_only && Inherits(inherited, type)) {
      inherited.schema->Scan(transaction, type,
                             [&](duckdb::CatalogEntry& entry) {
                               if (!own.Get(transaction, entry.name)) {
                                 callback(entry);
                               }
                             });
    }
  }
}

void Schema::Scan(duckdb::CatalogType type,
                  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  auto& own = Set(type);
  own.ScanCommitted(callback);
  for (const auto& inherited : _inherited) {
    if (!inherited.types_only && Inherits(inherited, type)) {
      inherited.schema->Scan(type, [&](duckdb::CatalogEntry& entry) {
        if (!own.GetCommitted(entry.name)) {
          callback(entry);
        }
      });
    }
  }
}

bool Schema::Inherits(const Inherited& inherited,
                      duckdb::CatalogType type) const {
  if (inherited.types_only) {
    return type == duckdb::CatalogType::TYPE_ENTRY;
  }
  switch (type) {
    case duckdb::CatalogType::INDEX_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return false;
    default:
      return true;
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  duckdb::CatalogEntry& relation = table;
  return CreateIndex(transaction, info, relation);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::CatalogEntry& relation) {
  if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      Set(duckdb::CatalogType::INDEX_ENTRY)
        .Get(transaction, info.GetIndexName())) {
    return nullptr;
  }
  info.table_oid = relation.oid;
  auto index =
    catalog.Cast<BaseCatalog>().MakeIndex(transaction, *this, info, relation);
  return Add(transaction, std::move(index), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  if (AlterExisting(transaction, info, info.GetFunctionName())) {
    return nullptr;
  }
  duckdb::unique_ptr<duckdb::StandardEntry> function;
  switch (info.type) {
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
      function = duckdb::make_uniq<duckdb::ScalarFunctionCatalogEntry>(
        catalog, *this, info.Cast<duckdb::CreateScalarFunctionInfo>());
      break;
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
      function = duckdb::make_uniq<duckdb::TableFunctionCatalogEntry>(
        catalog, *this, info.Cast<duckdb::CreateTableFunctionInfo>());
      break;
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      function = duckdb::MacroCatalogEntry::Create(
        catalog, *this, info.Cast<duckdb::CreateMacroInfo>());
      break;
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
      function = duckdb::make_uniq<duckdb::AggregateFunctionCatalogEntry>(
        catalog, *this, info.Cast<duckdb::CreateAggregateFunctionInfo>());
      break;
    case duckdb::CatalogType::WINDOW_FUNCTION_ENTRY:
      function = duckdb::make_uniq<duckdb::WindowFunctionCatalogEntry>(
        catalog, *this, info.Cast<duckdb::CreateWindowFunctionInfo>());
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("Unknown function type \"",
                              duckdb::CatalogTypeToString(info.type), "\""));
  }
  function->internal = info.internal;
  return Add(transaction, std::move(function), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateTable(
  duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo& info) {
  auto& base = info.Base();
  if (base.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      Set(duckdb::CatalogType::TABLE_ENTRY)
        .Get(transaction, base.GetTableName())) {
    return nullptr;
  }
  std::vector<std::pair<duckdb::SequenceCatalogEntry*, duckdb::Identifier>>
    serials;
  for (auto& serial : info.serial_sequences) {
    duckdb::CreateSequenceInfo sequence_info;
    sequence_info.SetQualifiedName(GetQualifiedName(serial.name));
    sequence_info.temporary = base.temporary;
    sequence_info.max_value =
      duckdb::Value::MaximumValue(base.columns.GetColumn(serial.column).Type())
        .GetValue<int64_t>();
    sequence_info.permissions = std::move(serial.permissions);
    auto& sequence = CreateSequence(transaction, sequence_info)
                       ->Cast<duckdb::SequenceCatalogEntry>();
    duckdb::LogicalDependency dependency{sequence};
    dependency.owned_by = true;
    for (const auto& dependent : serial.dependents) {
      dependency.subdependencies.insert(
        duckdb::SubDependency{duckdb::AlterTableType::SET_DEFAULT, dependent});
    }
    base.dependencies.AddDependency(dependency);
    serials.emplace_back(&sequence, serial.column);
  }
  auto table = catalog.Cast<BaseCatalog>().MakeTable(transaction, *this, info);
  const auto& columns = table->Cast<duckdb::TableCatalogEntry>();
  for (auto& [sequence, column] : serials) {
    if (const auto oid = columns.GetColumn(column).CatalogOid()) {
      sequence->owner_column = oid;
    }
  }
  return Add(transaction, std::move(table), base.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  if (AlterExisting(transaction, info, info.GetViewName())) {
    return nullptr;
  }
  return Add(transaction,
             duckdb::make_uniq<duckdb::ViewCatalogEntry>(catalog, *this, info),
             info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateSequence(
  duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo& info) {
  auto result =
    Add(transaction,
        duckdb::make_uniq<duckdb::SequenceCatalogEntry>(catalog, *this, info),
        info.on_conflict);
  if (result && !info.owned_by.empty()) {
    duckdb::ChangeOwnershipInfo ownership{
      duckdb::CatalogType::SEQUENCE_ENTRY,
      duckdb::AlterEntryData{GetQualifiedName(result->name),
                             duckdb::OnEntryNotFound::THROW_EXCEPTION},
      duckdb::Identifier{}, duckdb::Identifier{}};
    ownership.owner_path = info.owned_by;
    Alter(transaction, ownership);
  }
  return result;
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateTableFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateTableFunctionInfo& info) {
  auto entry =
    duckdb::make_uniq<duckdb::TableFunctionCatalogEntry>(catalog, *this, info);
  entry->internal = info.internal;
  return Add(transaction, std::move(entry), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateCopyFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateCopyFunctionInfo& info) {
  auto entry =
    duckdb::make_uniq<duckdb::CopyFunctionCatalogEntry>(catalog, *this, info);
  entry->internal = info.internal;
  return Add(transaction, std::move(entry), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreatePragmaFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreatePragmaFunctionInfo& info) {
  auto entry =
    duckdb::make_uniq<duckdb::PragmaFunctionCatalogEntry>(catalog, *this, info);
  entry->internal = info.internal;
  return Add(transaction, std::move(entry), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateCollation(
  duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo& info) {
  auto entry =
    duckdb::make_uniq<duckdb::CollateCatalogEntry>(catalog, *this, info);
  entry->internal = info.internal;
  return Add(transaction, std::move(entry), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateCoordinateSystem(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateCoordinateSystemInfo& info) {
  auto entry = duckdb::make_uniq<duckdb::CoordinateSystemCatalogEntry>(
    catalog, *this, info);
  entry->internal = info.internal;
  return Add(transaction, std::move(entry), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      Set(duckdb::CatalogType::TYPE_ENTRY)
        .Get(transaction, info.GetQualifiedName().Name())) {
    return nullptr;
  }
  return Add(transaction,
             duckdb::make_uniq<duckdb::TypeCatalogEntry>(catalog, *this, info),
             info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateTokenizer(
  duckdb::CatalogTransaction transaction, duckdb::CreateTokenizerInfo& info) {
  if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      Set(duckdb::CatalogType::TOKENIZER_ENTRY)
        .Get(transaction, info.GetQualifiedName().Name())) {
    return nullptr;
  }
  if (info.config.empty()) {
    pg::CompileTokenizer(transaction.GetContext(), info);
  }
  return Add(transaction,
             duckdb::make_uniq<TokenizerCatalogEntry>(catalog, *this, info),
             info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateJob(
  duckdb::CatalogTransaction transaction, duckdb::CreateJobInfo& info) {
  if (catalog.IsTemporaryCatalog()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Jobs are not supported by this catalog"));
  }
  if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      Set(duckdb::CatalogType::JOB_ENTRY)
        .Get(transaction, info.GetQualifiedName().Name())) {
    return nullptr;
  }
  VerifySchedule(info.schedule);
  auto job = duckdb::make_uniq<JobCatalogEntry>(catalog, *this, info,
                                                std::make_shared<JobState>());
  if (transaction.context) {
    job->ScheduleAtCommit(*transaction.context);
  }
  return Add(transaction, std::move(job), info.on_conflict);
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  const auto type = lookup_info.GetCatalogType();
  if (auto entry =
        Set(type).Get(transaction, lookup_info.GetEntryIdentifier())) {
    return entry;
  }
  for (const auto& inherited : _inherited) {
    if (Inherits(inherited, type)) {
      if (auto entry =
            inherited.schema->LookupEntry(transaction, lookup_info)) {
        return entry;
      }
    }
  }
  return nullptr;
}

void Schema::Drop(duckdb::CatalogTransaction transaction,
                  duckdb::CatalogEntry& entry) {
  DeclareModified(transaction, catalog,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  entry.BeforeDrop(transaction);
  if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
    std::vector<duckdb::Identifier> indexes;
    _sets->indexes.Scan(transaction, [&](duckdb::CatalogEntry& index) {
      if (index.Cast<duckdb::IndexCatalogEntry>().table_oid == entry.oid) {
        indexes.push_back(index.name);
      }
    });
    for (const auto& index : indexes) {
      _sets->indexes.Drop(transaction, index);
    }
    for (const auto& dependency :
         entry.Cast<duckdb::StandardEntry>().dependencies.Set()) {
      if (!dependency.owned_by) {
        continue;
      }
      if (auto owned = _sets->sequences.Get(transaction, dependency.entry.name);
          owned && owned->oid == dependency.oid) {
        Drop(transaction, *owned);
      }
    }
  }
  Set(entry.type).Drop(transaction, entry.name);
}

void Schema::DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) {
  auto transaction = GetCatalogTransaction(context);
  const auto& entry_name = info.GetQualifiedName().Name();
  auto entry = Set(info.type).Get(transaction, entry_name);
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("Failed to drop entry ",
                            duckdb::SQLQuotedIdentifier::ToString(entry_name),
                            " - entry could not be found"));
  }
  if (entry->type != info.type) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG(
        "Existing object ", duckdb::SQLQuotedIdentifier::ToString(entry_name),
        " is of type ", duckdb::CatalogTypeToString(entry->type),
        ", trying to drop type ", duckdb::CatalogTypeToString(info.type)));
  }
  if (entry->internal && !info.allow_drop_internal) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("Cannot drop entry \"", entry_name.GetIdentifierName(),
              "\" because it is an internal system entry"));
  }
  if (info.has_func_args) {
    auto create = entry->GetInfo();
    auto& macros = create->Cast<duckdb::CreateMacroInfo>().macros;
    const auto overload = absl::c_find_if(macros, [&](const auto& function) {
      return function->HasParameterTypes(info.func_parameters);
    });
    if (overload == macros.end()) {
      if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                      ERR_MSG("function ", entry_name.GetIdentifierName(), "(",
                              duckdb::MacroFunction::ParameterTypesToString(
                                info.func_parameters),
                              ") does not exist"));
    }
    if (macros.size() > 1) {
      macros.erase(overload);
      create->on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
      CreateFunction(transaction, create->Cast<duckdb::CreateFunctionInfo>());
      return;
    }
  }
  DropDependents(transaction, *entry, info.cascade);
  Drop(transaction, *entry);
}

void Schema::Alter(duckdb::CatalogTransaction transaction,
                   duckdb::AlterInfo& info) {
  if (info.type == duckdb::AlterType::ALTER_PERMISSIONS &&
      info.Cast<duckdb::AlterPermissionsInfo>().all_in_schema) {
    AlterAllInSchema(*this, transaction,
                     info.Cast<duckdb::AlterPermissionsInfo>());
    return;
  }
  auto type = info.GetCatalogType();
  const auto& entry_name = info.GetQualifiedName().Name();
  if (info.type == duckdb::AlterType::ALTER_SCALAR_FUNCTION &&
      info.Cast<duckdb::AlterScalarFunctionInfo>().alter_scalar_function_type ==
        duckdb::AlterScalarFunctionType::RENAME_SCALAR_FUNCTION &&
      !Set(type).Get(transaction, entry_name) &&
      Set(duckdb::CatalogType::TABLE_MACRO_ENTRY)
        .Get(transaction, entry_name)) {
    type = duckdb::CatalogType::TABLE_MACRO_ENTRY;
  }
  auto& set = Set(type);
  auto entry = set.Get(transaction, entry_name);
  if (!entry) {
    if (info.if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
      ThrowMissingEntry(type, entry_name);
    }
    return;
  }
  if (!info.allow_internal && entry->internal) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("Cannot alter entry \"", entry->name.GetIdentifierName(),
              "\" because it is an internal system entry"));
  }
  if (const auto new_schema = info.GetNewSchema()) {
    if (type == duckdb::CatalogType::VIEW_ENTRY &&
        entry->type != duckdb::CatalogType::VIEW_ENTRY) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
        ERR_MSG("\"", entry->name.GetIdentifierName(), "\" is not a view"));
    }
    auto target = catalog.Cast<BaseCatalog>()
                    .Set(duckdb::CatalogType::SCHEMA_ENTRY)
                    .Get(transaction, *new_schema);
    if (!target) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", new_schema->GetIdentifierName(),
                              "\" does not exist"));
    }
    if (target->oid == oid) {
      return;
    }
    if (entry->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
      if (const auto owner = OwnerOf(transaction, *entry)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("cannot move an owned sequence into another schema"),
          ERR_DETAIL("Sequence \"", entry->name.GetIdentifierName(),
                     "\" is linked to table \"",
                     owner->name.GetIdentifierName(), "\"."));
      }
    }
    DeclareModified(transaction, catalog,
                    duckdb::DatabaseModificationType::ALTER_TABLE);
    Move(transaction, set, type, *entry, target->Cast<Schema>(), info);
    return;
  }
  const bool replay = info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING;
  std::vector<PendingDefault> defaults;
  const auto new_name = info.GetNewName();
  if (new_name) {
    if (new_name->GetIdentifierName() == entry->name.GetIdentifierName()) {
      ThrowEntryExists(entry->type, *new_name);
    }
    if (!set.SameName(*new_name, entry->name)) {
      CheckRelationName(transaction, type, *new_name);
      if (!replay) {
        FollowRename(transaction, *entry, *new_name, defaults);
      }
    }
  }
  if (!replay && info.type == duckdb::AlterType::ALTER_TABLE &&
      info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
        duckdb::AlterTableType::RENAME_COLUMN) {
    const auto& rename = info.Cast<duckdb::RenameColumnInfo>();
    FollowColumnRename(transaction, *entry, rename.old_name, rename.new_name);
  }
  if (type == duckdb::CatalogType::VIEW_ENTRY &&
      entry->type != duckdb::CatalogType::VIEW_ENTRY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", entry->name.GetIdentifierName(), "\" is not a view"));
  }
  DeclareModified(transaction, catalog,
                  duckdb::DatabaseModificationType::ALTER_TABLE);
  auto result = catalog::AlterEntry(transaction, set, *entry, info);
  if (!replay) {
    SetDefaults(transaction, catalog, defaults);
  }
  if (!replay && result && new_name) {
    FollowReferences(transaction, *result, name, entry->name);
  }
  if (!replay && result && result->type == duckdb::CatalogType::TABLE_ENTRY &&
      info.type == duckdb::AlterType::ALTER_TABLE) {
    FollowColumnOids(transaction, entry->Cast<duckdb::TableCatalogEntry>(),
                     result->Cast<duckdb::TableCatalogEntry>());
  }
  if (!replay && result && result->type == duckdb::CatalogType::TABLE_ENTRY &&
      info.type == duckdb::AlterType::ALTER_PERMISSIONS &&
      !info.Cast<duckdb::AlterPermissionsInfo>().new_owner.empty()) {
    ReownSequences(transaction, *result,
                   info.Cast<duckdb::AlterPermissionsInfo>());
  }
  if (result && info.type == duckdb::AlterType::CHANGE_OWNERSHIP) {
    auto& owned = result->Cast<duckdb::StandardEntry>();
    auto& oids = catalog.Cast<BaseCatalog>().Oids();
    oids.RemoveDependents(owned, owned.oid);
    absl::Cleanup reindex = [&] { oids.AddDependents(owned, owned.oid); };
    Own(transaction, owned, info.Cast<duckdb::ChangeOwnershipInfo>());
  }
}

void Schema::Move(duckdb::CatalogTransaction transaction, EntrySet& set,
                  duckdb::CatalogType type, duckdb::CatalogEntry& entry,
                  Schema& target, const duckdb::AlterInfo& info) {
  const bool replay = info.bind_mode == duckdb::AlterBindMode::SKIP_BINDING;
  struct Moving {
    EntrySet& set;
    duckdb::CatalogType type;
    duckdb::CatalogEntry& entry;
  };
  std::vector<Moving> moving;
  if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
    auto& indexes = Set(duckdb::CatalogType::INDEX_ENTRY);
    indexes.Scan(transaction, [&](duckdb::CatalogEntry& index) {
      if (index.Cast<duckdb::IndexCatalogEntry>().table_oid == entry.oid) {
        moving.push_back({indexes, duckdb::CatalogType::INDEX_ENTRY, index});
      }
    });
  }
  for (auto& owned : OwnedEntries(transaction, entry)) {
    if (owned.get().type == duckdb::CatalogType::SEQUENCE_ENTRY &&
        owned.get().ParentSchema(transaction).oid == oid) {
      moving.push_back({Set(duckdb::CatalogType::SEQUENCE_ENTRY),
                        duckdb::CatalogType::SEQUENCE_ENTRY, owned});
    }
  }
  moving.push_back({set, type, entry});
  std::vector<duckdb::idx_t> oids;
  for (const auto& item : std::views::reverse(moving)) {
    target.CheckRelationName(transaction, item.type, item.entry.name);
    oids.push_back(item.entry.oid);
  }
  std::vector<PendingDefault> defaults;
  if (!replay) {
    for (const auto& item : moving) {
      FollowSchemaMove(transaction, item.entry, target.name, oids, defaults);
    }
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> result;
  for (const auto& item : moving) {
    auto moved = item.entry.Copy(*transaction.context);
    moved->Cast<duckdb::StandardEntry>().schema_info = target.GetSchemaInfo();
    moved->oid = item.entry.oid;
    moved->permissions = item.entry.permissions;
    result = moved.get();
    if (!item.set.Move(transaction, item.entry.name, target.Set(item.type),
                       std::move(moved),
                       &item.entry == &entry ? SerializeAlter(info) : "")) {
      ThrowEntryExists(item.entry.type, item.entry.name);
    }
    result->SetAsRoot(&transaction, &item.entry);
  }
  if (!replay) {
    SetDefaults(transaction, catalog, defaults);
    FollowReferences(transaction, *result, name, entry.name);
  }
}

void Schema::Own(duckdb::CatalogTransaction transaction,
                 duckdb::StandardEntry& owned,
                 const duckdb::ChangeOwnershipInfo& ownership) {
  duckdb::LogicalDependencyList kept;
  for (const auto& dependency : owned.dependencies.Set()) {
    if (!dependency.owned_by) {
      kept.AddDependency(dependency);
    }
  }
  owned.dependencies = std::move(kept);
  if (owned.type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    owned.Cast<duckdb::SequenceCatalogEntry>().owner_column = {};
  }
  const auto& owner = ownership.owner_path;
  if (owner.empty()) {
    if (ownership.owner_name.empty()) {
      return;
    }
    auto schema = ownership.owner_schema.empty()
                    ? duckdb::optional_ptr<duckdb::CatalogEntry>{this}
                    : catalog.Cast<BaseCatalog>()
                        .Set(duckdb::CatalogType::SCHEMA_ENTRY)
                        .Get(transaction, ownership.owner_schema);
    duckdb::optional_ptr<duckdb::CatalogEntry> entry;
    if (schema) {
      auto& sets = schema->Cast<Schema>();
      entry = sets.Set(duckdb::CatalogType::INDEX_ENTRY)
                .Get(transaction, ownership.owner_name);
      if (!entry) {
        entry = sets.Set(duckdb::CatalogType::TABLE_ENTRY)
                  .Get(transaction, ownership.owner_name);
      }
    }
    if (!entry) {
      ThrowMissingEntry(duckdb::CatalogType::TABLE_ENTRY, ownership.owner_name);
    }
    duckdb::LogicalDependency dependency{*entry};
    dependency.owned_by = true;
    owned.dependencies.AddDependency(dependency);
    return;
  }
  auto& sequence = owned.Cast<duckdb::SequenceCatalogEntry>();
  if (owner.size() < 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid OWNED BY option"),
      ERR_HINT("Specify OWNED BY table.column or OWNED BY NONE."));
  }
  const auto& column = owner.back();
  const auto& table_name = owner[owner.size() - 2];
  const auto schema_path = owned.ParentSchemaPath(transaction);
  const auto qualification = std::span{owner}.first(owner.size() - 2);
  const duckdb::IdentifierEquality equals{catalog.IsCaseSensitive()};
  const bool with_catalog = qualification.size() > schema_path.size() &&
                            equals(qualification.front(), catalog.GetName());
  if (!qualification.empty() &&
      !absl::c_equal(with_catalog ? qualification.subspan(1) : qualification,
                     schema_path, equals)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("sequence must be in same schema as table it is linked to"));
  }
  auto table =
    Set(duckdb::CatalogType::TABLE_ENTRY).Get(transaction, table_name);
  if (!table || table->type != duckdb::CatalogType::TABLE_ENTRY) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table_name.GetIdentifierName(),
                            "\" does not exist"));
  }
  if (sequence.permissions.owner != table->permissions.owner) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("sequence must have same owner as table it is linked to"));
  }
  const auto& columns = table->Cast<duckdb::TableCatalogEntry>();
  if (!columns.ColumnExists(column)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", column.GetIdentifierName(), "\" of relation \"",
              table_name.GetIdentifierName(), "\" does not exist"));
  }
  duckdb::LogicalDependency dependency{*table};
  dependency.owned_by = true;
  dependency.subdependencies.insert(
    duckdb::SubDependency{duckdb::AlterTableType::REMOVE_COLUMN, column});
  sequence.dependencies.AddDependency(dependency);
  if (const auto oid = columns.GetColumn(column).CatalogOid()) {
    sequence.owner_column = oid;
  }
}

void Schema::CreateDefaults(duckdb::CatalogTransaction transaction) {
  auto& sets = *_sets;
  for (auto* set : {&sets.tables, &sets.table_functions, &sets.functions,
                    &sets.coordinate_systems, &sets.types}) {
    set->CreateDefaults(transaction);
  }
}

void Schema::ScanMembers(
  duckdb::CatalogTransaction transaction,
  absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  auto& sets = *_sets;
  for (auto* set :
       {&sets.tables, &sets.indexes, &sets.table_functions,
        &sets.copy_functions, &sets.pragma_functions, &sets.functions,
        &sets.sequences, &sets.collations, &sets.coordinate_systems,
        &sets.types, &sets.tokenizers, &sets.jobs, &sets.schemas}) {
    set->Scan(transaction, callback);
  }
}

void Schema::ScanTree(duckdb::CatalogTransaction transaction,
                      absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  ScanMembers(transaction, [&](duckdb::CatalogEntry& member) {
    callback(member);
    if (member.type == duckdb::CatalogType::SCHEMA_ENTRY) {
      member.Cast<Schema>().ScanTree(transaction, callback);
    }
  });
}

duckdb::optional_ptr<duckdb::CatalogEntry> Schema::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  return Add(transaction, duckdb::make_uniq<Schema>(catalog, info, this),
             info.on_conflict);
}

}  // namespace sdb::catalog
