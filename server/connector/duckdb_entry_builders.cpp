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

#include "connector/duckdb_entry_builders.h"

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_dependency.h"
#include "connector/duckdb_index_entry.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_object_entry.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_view_entry.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

// The column definitions and constraints of a table entry, plus the positions
// an index covers. Shared between a table's own entry and the index-name-as-
// table wrapper, which advertises the same shape.
struct TableInfoAndIndices {
  duckdb::unique_ptr<duckdb::CreateTableInfo> info;
  std::vector<size_t> indexed_col_indices;
};

// Re-points every foreign key at the relation its identity names. The schema,
// the relation name and the referenced key are what they were when the
// constraint was written, and a rename or a column drop over there has moved
// all three -- so the durable identities are what the entry is built from and
// the names are derived here.
void RetargetForeignKeys(duckdb::CreateTableInfo& info,
                         const duckdb::CreateTableInfo& table,
                         duckdb::Catalog& catalog,
                         duckdb::ClientContext* context) {
  for (auto& constraint : info.constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    const ObjectId referenced_id{fk.host_referenced_id};
    const duckdb::CreateTableInfo* referenced = nullptr;
    catalog::TableInfoRef held;
    duckdb::Identifier referenced_schema = info.GetQualifiedName().Schema();
    duckdb::Identifier referenced_name = info.GetTableName();
    if (referenced_id.isSet() && referenced_id != catalog::IdOf(table)) {
      const auto* found = FindTableIn(context, catalog, referenced_id);
      if (found == nullptr) {
        continue;
      }
      held = found->Definition();
      referenced = held.get();
      auto schema = FindSchema(context, catalog::ParentIdOf(*held));
      referenced_schema = duckdb::Identifier{
        schema ? catalog::SchemaNameOf(*schema) : std::string_view{}};
      referenced_name = duckdb::Identifier{catalog::TableNameOf(*held)};
    } else {
      referenced = &table;
    }
    fk.info.schema = referenced_schema;
    fk.info.table = referenced_name;
    fk.pk_columns = catalog::ReferencedKeyNames(fk, referenced);
    fk.info.pk_keys.clear();
    for (const auto& name : fk.pk_columns) {
      const auto* column =
        catalog::ColumnByName(*referenced, name.GetIdentifierName());
      fk.info.pk_keys.emplace_back(column == nullptr ? 0
                                                     : column->Logical().index);
    }
  }
}

// The referenced half of every foreign key pointing at this table. duckdb
// drives delete-side enforcement off a FK_TYPE_PRIMARY_KEY_TABLE constraint on
// the referenced table -- with none there, a DELETE verifies nothing and
// silently breaks the key. duckdb adds it by altering the referenced entry
// when the referencing table is created; a serenedb entry is rebuilt from its
// definition on every write, so it has to be derived here instead, from the
// foreign-key edges the referencing tables recorded against this one.
void AddReferencedForeignKeys(duckdb::CreateTableInfo& info,
                              const duckdb::CreateTableInfo& table,
                              duckdb::Catalog& catalog,
                              duckdb::ClientContext* context) {
  const DependencyView dependents{context};
  for (const auto& dependent : dependents.Dependents(catalog::IdOf(table))) {
    if (dependent.type != duckdb::CatalogType::TABLE_ENTRY) {
      continue;
    }
    const auto* referencing_entry = FindTableIn(context, catalog, dependent.id);
    if (referencing_entry == nullptr) {
      continue;
    }
    const auto referencing = referencing_entry->Definition();
    auto schema = FindSchema(context, catalog::ParentIdOf(*referencing));
    for (const auto& constraint : referencing->constraints) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
      if (ObjectId{fk.host_referenced_id} != catalog::IdOf(table)) {
        continue;
      }
      duckdb::ForeignKeyInfo mirror;
      mirror.type = duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
      mirror.schema = duckdb::Identifier{schema ? catalog::SchemaNameOf(*schema)
                                                : std::string_view{}};
      mirror.table = duckdb::Identifier{catalog::TableNameOf(*referencing)};
      auto pk_columns = catalog::ReferencedKeyNames(fk, &table);
      for (const auto& name : pk_columns) {
        const auto* column =
          catalog::ColumnByName(table, name.GetIdentifierName());
        mirror.pk_keys.emplace_back(
          column == nullptr ? 0 : column->Logical().index);
      }
      for (const auto& name : fk.fk_columns) {
        const auto* column =
          catalog::ColumnByName(*referencing, name.GetIdentifierName());
        mirror.fk_keys.emplace_back(
          column == nullptr ? 0 : column->Logical().index);
      }
      auto built = duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
        pk_columns, fk.fk_columns, std::move(mirror));
      built->constraint_name = fk.constraint_name;
      built->oid = fk.oid;
      built->host_referenced_id = fk.host_referenced_id;
      info.constraints.push_back(std::move(built));
    }
  }
}

TableInfoAndIndices BuildTableInfoAndIndices(
  std::string_view name, SereneDBSchemaEntry& schema, duckdb::Catalog& catalog,
  const duckdb::CreateTableInfo& table, duckdb::ClientContext* context) {
  TableInfoAndIndices out;
  out.info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(
      table.Copy());
  out.info->SetTableName(duckdb::Identifier{name});
  out.info->SetSchema(schema.name);
  RetargetForeignKeys(*out.info, table, catalog, context);
  AddReferencedForeignKeys(*out.info, table, catalog, context);

  containers::FlatHashSet<size_t> idx_set;
  VisitRelationIndexEntries(
    context, schema, catalog::IdOf(table), [&](SereneDBIndexEntry& index) {
      for (auto col_id : index.Definition()->GetReferencedColumns()) {
        if (const auto* column = catalog::ColumnById(table, col_id)) {
          idx_set.insert(column->Logical().index);
        }
      }
    });
  out.indexed_col_indices.assign(idx_set.begin(), idx_set.end());
  std::sort(out.indexed_col_indices.begin(), out.indexed_col_indices.end());
  return out;
}

}  // namespace

duckdb::unique_ptr<duckdb::CatalogEntry> MakeTableEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, catalog::TableInfoRef table,
  catalog::Permissions perm, duckdb::ClientContext* context,
  duckdb::shared_ptr<duckdb::DataTable> storage,
  std::shared_ptr<catalog::TableRuntime> runtime,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers) {
  auto built =
    BuildTableInfoAndIndices(entry_name, schema, catalog, *table, context);
  return duckdb::make_uniq<SereneDBTableEntry>(
    catalog, schema, *built.info, std::move(table), std::move(perm),
    std::move(storage), std::move(runtime),
    std::move(built.indexed_col_indices), std::move(inherited_triggers));
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  catalog::IndexInfoRef index, duckdb::ClientContext* context) {
  // The relation an index covers is a table or a view, and both are entries in
  // the catalog this one is being built for -- placed ahead of the indexes that
  // project them.
  std::string_view relation_name;
  if (const auto* view = FindViewIn(context, catalog, index->GetRelationId())) {
    relation_name = view->name.GetIdentifierName();
  } else if (const auto* relation =
               FindTableIn(context, catalog, index->GetRelationId())) {
    relation_name = relation->name.GetIdentifierName();
  }
  // The definition's own info, which already carries the name, the index type,
  // the UNIQUE constraint and the comment. Only the relation this entry hangs
  // off is added here: the info names it by id, the entry by name.
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, catalog::CreateIndexInfoBase>(
      index->Copy());
  info->SetSchema(schema.name);
  info->table = duckdb::Identifier{relation_name};
  auto table_name = info->table.GetIdentifierName();
  return duckdb::make_uniq<SereneDBIndexEntry>(
    catalog, schema, *info, std::move(index), std::move(table_name));
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeSequenceEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const duckdb::CreateSequenceInfo& sequence,
  std::shared_ptr<catalog::SequenceCounter> counter,
  catalog::Permissions perm) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateSequenceInfo>(
      sequence.Copy());
  info->SetSchema(schema.name);
  // The counter and its durable horizon are serenedb's, written to the catalog
  // log; duckdb neither persists nor reclaims anything for this entry.
  return duckdb::make_uniq<SereneDBSequenceEntry>(
    catalog, schema, *info, std::move(counter), std::move(perm));
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeViewEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, const duckdb::CreateViewInfo& view,
  catalog::Permissions perm) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      view.Copy());
  info->SetSchema(schema.name);
  // The stored CREATE VIEW text keeps the name the view was defined under, so
  // a renamed view would otherwise key its CatalogSet chain under the old one.
  info->SetViewName(duckdb::Identifier{entry_name});
  info->temporary = false;
  info->internal = false;
  return duckdb::make_uniq<SereneDBViewEntry>(catalog, schema, *info,
                                              std::move(perm));
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacroEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, bool internal,
  const duckdb::CreateMacroInfo& func, catalog::Permissions perm) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      func.Copy());
  info->SetSchema(schema.name);
  info->SetFunctionName(duckdb::Identifier{entry_name});
  info->temporary = false;
  info->internal = internal;
  if (info->type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    return duckdb::make_uniq_base<duckdb::CatalogEntry,
                                  SereneDBTableMacroEntry>(
      catalog, schema, *info, std::move(perm));
  }
  return duckdb::make_uniq_base<duckdb::CatalogEntry, SereneDBScalarMacroEntry>(
    catalog, schema, *info, std::move(perm));
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexScanEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, catalog::IndexInfoRef index,
  duckdb::ClientContext* context) {
  // In the catalog the entry is being built for rather than the session's: an
  // attach reads these before the attachment is in the database manager.
  if (const auto* view = FindViewIn(context, catalog, index->GetRelationId())) {
    const auto& view_perm = view->permissions;
    const auto view_columns = view->GetColumnInfo();
    if (!view_columns) {
      return nullptr;
    }
    const auto& vinfo = *view_columns;
    auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
    info->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                       /*case_sensitive=*/true);
    info->SetTableName(duckdb::Identifier{entry_name});
    info->SetSchema(schema.name);
    for (size_t i = 0; i < vinfo.names.size(); ++i) {
      info->columns.AddColumn(
        duckdb::ColumnDefinition(vinfo.names[i], vinfo.types[i]));
    }
    const auto& col_ids = index->GetColumns();
    std::vector<size_t> indexed_col_indices(col_ids.begin(), col_ids.end());
    if (index->IsInverted()) {
      IndexedRelation relation{
        .id = ObjectId{view->oid},
        .type = duckdb::CatalogType::VIEW_ENTRY,
        .name = std::string{view->name.GetIdentifierName()},
        .perm = view_perm};
      return duckdb::make_uniq<ViewInvertedIndexScanEntry>(
        catalog, schema, *info, *view, std::move(relation),
        std::move(indexed_col_indices), std::move(index));
    }
    // CREATE INDEX rejects a plain (secondary) index on a view at bind time.
    return nullptr;
  }

  const auto* table_entry =
    FindTableIn(context, catalog, index->GetRelationId());
  if (table_entry == nullptr) {
    return nullptr;
  }
  const auto& table_perm = table_entry->permissions;
  const auto table = table_entry->Definition();
  auto built =
    BuildTableInfoAndIndices(entry_name, schema, catalog, *table, context);
  IndexedRelation relation{.id = catalog::IdOf(*table),
                           .type = duckdb::CatalogType::TABLE_ENTRY,
                           .name = std::string{catalog::TableNameOf(*table)},
                           .perm = table_perm,
                           .column_acls = table_perm.column_acl};

  if (index->IsInverted()) {
    return duckdb::make_uniq<TableInvertedIndexScanEntry>(
      catalog, schema, *built.info, std::move(relation),
      std::move(built.indexed_col_indices), std::move(index));
  }

  // Secondary index: native ART on the store table; identity is the index id.
  const bool unique =
    basics::downCast<const catalog::CreateSecondaryIndexInfo>(*index)
      .IsUnique();
  return duckdb::make_uniq<TableSecondaryIndexScanEntry>(
    catalog, schema, *built.info, std::move(relation), *index,
    std::move(built.indexed_col_indices), unique);
}

namespace {

template<typename Info>
std::shared_ptr<const Info> InfoAs(
  const std::shared_ptr<const duckdb::CreateInfo>& info) {
  return std::static_pointer_cast<const Info>(info);
}

}  // namespace

duckdb::unique_ptr<duckdb::StandardEntry> MakeEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  const std::shared_ptr<const duckdb::CreateInfo>& info,
  const catalog::Permissions& perm, duckdb::CatalogType slot,
  duckdb::ClientContext* context,
  duckdb::optional_ptr<duckdb::CatalogEntry> superseded) {
  const auto name = catalog::NameOf(*info);
  duckdb::unique_ptr<duckdb::CatalogEntry> built;
  switch (info->type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY: {
      auto table = InfoAs<duckdb::CreateTableInfo>(info);
      // A table's trigger set is shared by every version of it rather than
      // versioned with the definition, and the rows carry on under the next
      // version, so each rewrite has to inherit what its predecessor held.
      duckdb::shared_ptr<duckdb::CatalogSet> triggers;
      duckdb::shared_ptr<duckdb::DataTable> storage;
      std::shared_ptr<catalog::TableRuntime> runtime;
      if (auto* previous =
            dynamic_cast<SereneDBTableEntry*>(superseded.get())) {
        triggers = previous->GetTriggerSet();
        runtime = previous->Runtime();
        if (auto held = previous->TryGetStorage()) {
          storage = held->shared_from_this();
        }
      }
      // A table on either side of having no columns at all is rebuilt rather
      // than inherited: ALTER refuses to remove the last column, so the reshape
      // is a drop and a create, and rows cannot survive a shape with nowhere to
      // put them. Every other transition hands its rows over.
      if (storage && (storage->Columns().empty() !=
                      (table->columns.PhysicalColumnCount() == 0))) {
        storage.reset();
      }
      built = MakeTableEntry(catalog, schema, name, std::move(table), perm,
                             context, std::move(storage), std::move(runtime),
                             std::move(triggers));
      break;
    }
    case VIEW_ENTRY:
      built = MakeViewEntry(catalog, schema, name,
                            *InfoAs<duckdb::CreateViewInfo>(info), perm);
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      if (slot != info->type) {
        return nullptr;
      }
      built = MakeMacroEntry(catalog, schema, name, /*internal=*/false,
                             *InfoAs<duckdb::CreateMacroInfo>(info), perm);
      break;
    case INDEX_ENTRY: {
      auto index = InfoAs<catalog::CreateIndexInfoBase>(info);
      built = slot == INDEX_ENTRY
                ? MakeIndexEntry(catalog, schema, std::move(index), context)
                : MakeIndexScanEntry(catalog, schema, name, std::move(index),
                                     context);
      break;
    }
    case SEQUENCE_ENTRY: {
      // A rewrite is the same sequence behind the same counter: a value the
      // superseded version handed out must never be handed out again.
      std::shared_ptr<catalog::SequenceCounter> counter;
      if (const auto* previous =
            dynamic_cast<const SereneDBSequenceEntry*>(superseded.get())) {
        counter = previous->Counter();
      }
      built = MakeSequenceEntry(catalog, schema,
                                *InfoAs<duckdb::CreateSequenceInfo>(info),
                                std::move(counter), perm);
      break;
    }
    case TYPE_ENTRY: {
      auto type = InfoAs<duckdb::CreateTypeInfo>(info);
      auto copied =
        duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
          type->Copy());
      copied->SetSchema(schema.name);
      built =
        duckdb::make_uniq<SereneDBTypeEntry>(catalog, schema, *copied, perm);
      break;
    }
    default:
      built = duckdb::make_uniq<SereneDBTokenizerEntry>(
        catalog, schema, InfoAs<catalog::CreateTokenizerInfo>(info), perm);
      break;
  }
  if (!built) {
    return nullptr;
  }
  return duckdb::unique_ptr_cast<duckdb::CatalogEntry, duckdb::StandardEntry>(
    std::move(built));
}

namespace {

// The comment a CreateInfo currently carries, in the form COMMENT ON compares
// against: NULL and the empty string both mean "no comment".
std::string_view CommentText(const duckdb::Value& value) {
  return value.IsNull() ? std::string_view{} : duckdb::StringValue::Get(value);
}

// The info with the new name and comment applied to a copy of it, or null when
// it already says that. For every kind whose name and comment are fields of
// duckdb's own CreateInfo, which is all of them but a sequence, an index and a
// table.
duckdb::unique_ptr<duckdb::CreateInfo> ChangedInfo(
  const duckdb::CreateInfo& info, std::string_view name,
  std::optional<std::string_view> comment) {
  const bool renames = !name.empty();
  const bool recomments = comment && CommentText(info.comment) != *comment;
  if (!renames && !recomments) {
    return nullptr;
  }
  auto copied = info.Copy();
  if (renames) {
    copied->SetName(duckdb::Identifier{name});
  }
  if (recomments) {
    copied->comment =
      comment->empty() ? duckdb::Value{} : duckdb::Value{std::string{*comment}};
  }
  return copied;
}

template<typename Info>
std::shared_ptr<const Info> Reshaped(
  duckdb::unique_ptr<duckdb::CreateInfo> info) {
  return std::shared_ptr<const Info>{
    duckdb::unique_ptr_cast<duckdb::CreateInfo, Info>(std::move(info))
      .release()};
}

std::shared_ptr<const duckdb::CreateInfo> Shared(
  duckdb::unique_ptr<duckdb::CreateInfo> info) {
  return std::shared_ptr<const duckdb::CreateInfo>{info.release()};
}

std::shared_ptr<const duckdb::CreateInfo> RewrittenSequence(
  const duckdb::CreateSequenceInfo& current, std::string_view name,
  std::optional<std::string_view> comment) {
  const auto current_comment =
    current.comment.IsNull()
      ? std::string_view{}
      : std::string_view{duckdb::StringValue::Get(current.comment)};
  const bool recomments = comment && current_comment != *comment;
  if (name.empty() && !recomments) {
    return nullptr;
  }
  auto options = catalog::SequenceOptionsOf(current);
  if (!name.empty()) {
    options.name = std::string{name};
  }
  if (recomments) {
    options.comment = std::string{*comment};
  }
  // The counter is not the definition and is not carried here: the entry built
  // for this version inherits its predecessor's, so a nextval running against
  // either advances the one they share.
  return catalog::MakeSequenceInfo(
    catalog::IdOf(current), catalog::ParentIdOf(current), std::move(options));
}

std::shared_ptr<const duckdb::CreateInfo> RewrittenIndex(
  duckdb::ClientContext* context, const catalog::IndexInfoRef& index,
  std::string_view name, std::optional<std::string_view> comment) {
  auto next = index;
  if (!name.empty()) {
    next = catalog::RenamedIndex(*next, name);
  }
  if (comment && next->Comment() != *comment) {
    next = catalog::CommentedIndex(*next, *comment);
  }
  if (next == index) {
    return nullptr;
  }
  return catalog::NextIndexVersion(context, next);
}

std::shared_ptr<const duckdb::CreateInfo> RewrittenTable(
  duckdb::ClientContext* context, const catalog::TableInfoRef& table,
  std::string_view name, std::optional<std::string_view> comment) {
  catalog::TableInfoRef info;
  if (!name.empty()) {
    auto renamed = catalog::Clone(*table);
    renamed->SetTableName(duckdb::Identifier{name});
    info = std::move(renamed);
  }
  if (comment) {
    if (auto commented = catalog::SetComment(info ? *info : *table, *comment)) {
      info = std::move(commented);
    }
  }
  if (!info) {
    return nullptr;
  }
  return catalog::NextTableVersion(context, catalog::IdOf(*table),
                                   catalog::ParentIdOf(*table),
                                   std::move(info));
}

}  // namespace

std::shared_ptr<const duckdb::CreateInfo> EntryDefinition(
  const duckdb::CatalogEntry& entry) {
  switch (entry.type) {
    using enum duckdb::CatalogType;
    case VIEW_ENTRY:
      return Shared(entry.GetInfo());
    case INDEX_ENTRY:
      return entry.Cast<SereneDBIndexEntry>().Definition();
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return Shared(entry.GetInfo());
    case SEQUENCE_ENTRY:
      return entry.Cast<SereneDBSequenceEntry>().Definition();
    case TYPE_ENTRY:
      return Shared(entry.GetInfo());
    case TOKENIZER_ENTRY:
      return entry.Cast<SereneDBTokenizerEntry>().Definition();
    case FOREIGN_SERVER_ENTRY:
      return entry.Cast<SereneDBForeignServerEntry>().Definition();
    default:
      return entry.Cast<SereneDBTableEntry>().Definition();
  }
}

std::shared_ptr<const duckdb::CreateInfo> RewrittenDefinition(
  duckdb::ClientContext* context, const duckdb::CatalogEntry& entry,
  std::string_view name, std::optional<std::string_view> comment) {
  switch (entry.type) {
    using enum duckdb::CatalogType;
    case VIEW_ENTRY: {
      auto info = ChangedInfo(*entry.GetInfo(), name, comment);
      if (!info) {
        return nullptr;
      }
      return catalog::NextViewVersion(
        context, Reshaped<duckdb::CreateViewInfo>(std::move(info)));
    }
    case INDEX_ENTRY:
      return RewrittenIndex(
        context, entry.Cast<SereneDBIndexEntry>().Definition(), name, comment);
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY: {
      auto info = ChangedInfo(*entry.GetInfo(), name, comment);
      if (!info) {
        return nullptr;
      }
      return catalog::NextFunctionVersion(
        context, Reshaped<duckdb::CreateMacroInfo>(std::move(info)));
    }
    case SEQUENCE_ENTRY:
      return RewrittenSequence(
        *entry.Cast<SereneDBSequenceEntry>().Definition(), name, comment);
    case TYPE_ENTRY:
      return Shared(ChangedInfo(*entry.GetInfo(), name, comment));
    case TOKENIZER_ENTRY:
      return Shared(ChangedInfo(
        entry.Cast<SereneDBTokenizerEntry>().Tokenizer(), name, comment));
    case FOREIGN_SERVER_ENTRY:
      return Shared(
        ChangedInfo(entry.Cast<SereneDBForeignServerEntry>().ForeignServer(),
                    name, comment));
    default:
      return RewrittenTable(
        context, entry.Cast<SereneDBTableEntry>().Definition(), name, comment);
  }
}

void RefreshEntrySiblings(duckdb::ClientContext* context,
                          const duckdb::CatalogEntry& entry) {
  switch (entry.type) {
    using enum duckdb::CatalogType;
    case VIEW_ENTRY: {
      // An index over a view projects the view's shape and its grants, so a
      // change to the view has to reach the index's entries.
      RefreshRelationIndexEntries(context, ObjectId{entry.ParentSchema().oid},
                                  ObjectId{entry.oid});
      return;
    }
    case INDEX_ENTRY: {
      // The relation's own entry advertises a virtual column per indexed
      // column, so adding one reshapes it.
      const auto& index = entry.Cast<SereneDBIndexEntry>();
      RefreshRelationEntry(context, index.Definition()->GetSchemaId(),
                           index.GetRelationId());
      return;
    }
    case TABLE_ENTRY: {
      // An index over this relation advertises the relation's columns and its
      // grants, so every rewrite of the relation has to put its wrappers back
      // in step. Not the other way round: the index write refreshes the
      // relation. The index-name wrapper shares this slot and reshapes
      // nothing.
      const auto* table = dynamic_cast<const SereneDBTableEntry*>(&entry);
      if (table == nullptr) {
        return;
      }
      const auto& definition = table->Definition();
      RefreshRelationIndexEntries(context, catalog::ParentIdOf(*definition),
                                  catalog::IdOf(*definition));
      RefreshForeignKeyTargets(context, *definition);
      return;
    }
    default:
      return;
  }
}

}  // namespace sdb::connector
