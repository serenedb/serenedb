////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/function_ref.h>

#include <string_view>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {
namespace {

// CREATE INDEX requires ownership of the relation the index is built on; an
// index has no independent owner of its own.
duckdb::unique_ptr<CreateIndexInfo> CreateIndexOnRelation(
  const AccessContext& ax, const duckdb::CatalogEntry& relation,
  std::string_view name, const std::vector<CreateIndexColumn>& columns,
  CreateIndexOperationOptions operation_options,
  absl::FunctionRef<duckdb::unique_ptr<CreateIndexInfo>(ObjectId)> author) {
  if (columns.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Cannot create index without columns"));
  }
  JoinStoreTransaction(ax.context);
  catalog::Catalog::MutationScope lock{catalog::GetCatalog()};
  const auto schema_id = catalog::ParentIdOf(relation);
  // The noun the refusal names is the relation's own kind: a view and a table
  // are both indexable and postgres says which one it refused.
  catalog::RequireOwner(ax.context, ax.role, relation.permissions,
                        pg::ToPgObjectTypeName(relation.type),
                        relation.name.GetIdentifierName());
  if (operation_options.if_not_exists &&
      catalog::FindRelation(ax.context, schema_id, name)) {
    return {};
  }
  auto index = author(schema_id);
  CreateIndexImpl(ax.context, *index, operation_options);
  return index;
}

}  // namespace

std::shared_ptr<const Index> CreateInvertedIndex(
  const AccessContext& ax, duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema, const duckdb::CatalogEntry& relation,
  std::string name, std::vector<CreateIndexColumn>&& columns,
  InvertedIndexOptions options, ExpressionData predicate,
  CreateIndexOperationOptions operation_options) {
  SDB_ASSERT(ax.context == &context);
  const auto created = CreateIndexOnRelation(
    ax, relation, name, columns, operation_options, [&](ObjectId schema_id) {
      return duckdb::make_uniq<CreateIndexInfo>(
        std::shared_ptr<const Index>{NewInvertedIndex(
          context, database_id, schema, schema_id, ObjectId{0},
          catalog::IdOf(relation), std::move(name), std::move(columns),
          std::move(options), std::move(predicate))});
    });
  return created ? created->GetIndex() : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndexImpl(
  duckdb::ClientContext* context, CreateIndexInfo& index,
  CreateIndexOperationOptions operation_options) {
  const auto schema_id = index.GetSchemaId();
  if (catalog::FindRelation(context, schema_id, index.GetName())) {
    ThrowDuplicateName(NameKind::Relation, index.GetName());
  }
  // A key constraint's index is filed under the constraint's own name and goes
  // on the same list as this one, so the two share a namespace -- which is what
  // postgres says too, where a constraint's index is a relation.
  if (const auto* relation = catalog::Find<SereneDBTableEntry>(
        context, schema_id, index.GetRelationId())) {
    for (const auto& constraint : relation->GetConstraints()) {
      if (constraint->type == duckdb::ConstraintType::UNIQUE &&
          constraint->constraint_name == index.GetName()) {
        ThrowDuplicateName(NameKind::Relation, index.GetName());
      }
    }
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  auto dependencies = operation_options.dependencies;
  if (index.IsInverted()) {
    for (const auto tokenizer_id : index.GetIndex()->GetTokenizers()) {
      if (tokenizer_id.isSet()) {
        dependencies.AddDependency(duckdb::LogicalDependency{
          nullptr, catalog::DependencyInfo(tokenizer_id),
          duckdb::Identifier{}});
      }
    }
  }
  index.dependencies = std::move(dependencies);
  const auto db_id = catalog::SchemaDatabaseId(context, schema_id);
  SDB_ASSERT(db_id.isSet());
  // Opened from the definition, which is all it takes, and handed to the entry
  // placed below -- the build (GetGlobalSinkState) and every reader after it
  // take the handle from there.
  auto storage = index.IsInverted()
                   ? search::InvertedIndexStorage::Create(
                       db_id, InvertedInfo(*index.GetIndex()), /*is_new=*/true)
                   : nullptr;
  const auto* entry = catalog::Find<SereneDBTableEntry>(context, schema_id,
                                                        index.GetRelationId());
  auto table = entry != nullptr ? entry->Definition() : nullptr;
  // Only an inverted index has a store half to publish from here: a plain ART
  // is built by the statement itself, and rebuilt by the replay and reshape
  // roads that state their own store op.
  auto store_index =
    table && index.IsInverted() ? MakeStoreIndexInfo(*table, index) : nullptr;
  if (store_index) {
    catalog::StoreCreateIndex(context, db_id, std::move(store_index),
                              std::move(table), index.GetRelationId(),
                              index.GetIndex());
  }
  // After the store op, so a relation another transaction has already dropped
  // is refused by the rows rather than by the set -- the store names the
  // relation, and the set can only say that something clashed.
  const auto placed = catalog::PutEntry(context, /*old_name=*/{}, index.Copy());
  if (storage) {
    // The handle the object keeps, on the entry every version of it hands over
    // to the next: the build below and every reader after it take it from
    // there.
    if (const auto* index_entry = EntryOf<SereneDBIndexEntry>(placed)) {
      index_entry->SetInvertedData(std::move(storage));
    }
  }
  // The relation is stated again (postgres likewise holds it for the CREATE's
  // duration) so a concurrent drop of it collides here rather than leaving this
  // index behind on a table that is gone. Read again rather than reusing what
  // the store op was built from: the same statement may have just rewritten it.
  if (const auto* held = catalog::Find<SereneDBTableEntry>(
        context, schema_id, index.GetRelationId())) {
    const auto current = held->Definition();
    catalog::PutEntry(context, current->GetTableName().GetIdentifierName(),
                      current->Copy(), held->permissions);
  }
  return placed;
}

void RenameIndex(duckdb::ClientContext* context, const CreateIndexInfo& index,
                 std::string_view new_name) {
  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto schema_id = index.GetSchemaId();
  auto renamed = RenamedIndexRecord(index, new_name);
  const auto db_id = catalog::SchemaDatabaseId(context, schema_id);
  const auto* entry = catalog::Find<SereneDBTableEntry>(context, schema_id,
                                                        index.GetRelationId());
  const auto table = entry ? entry->Definition() : nullptr;
  if (table && MakeStoreIndexInfo(*table, index)) {
    catalog::StoreRenameIndex(context, db_id, index.GetRelationId(),
                              index.GetName(), new_name);
  }
  catalog::PutEntry(context, index.GetName(), std::move(renamed));
}

void DropIndexLocked(duckdb::ClientContext* context, ObjectId database_id,
                     const CreateIndexInfo& index,
                     std::shared_ptr<search::InvertedIndexStorage> storage,
                     bool cascade) {
  catalog::DropIndexEntry(context, index.GetSchemaId(), index.GetName());
  // Store-side index drop is synchronous: UNIQUE enforcement must stop when
  // DROP INDEX commits, not when the artifact half runs.
  catalog::StoreDropIndex(context, database_id, index.GetRelationId(),
                          index.GetName());

  // Check that SereneDB won't open this index after reboot
  SDB_IF_FAILURE("crash_on_drop") { return; }

  DropIndexArtifacts(context, database_id, index, std::move(storage));
}

void DropIndexArtifacts(duckdb::ClientContext* context, ObjectId database_id,
                        const CreateIndexInfo& index,
                        std::shared_ptr<search::InvertedIndexStorage> storage) {
  if (!index.IsInverted()) {
    return;
  }
  DeferDropAction(
    context, [database_id, schema_id = index.GetSchemaId(),
              relation_id = index.GetRelationId(), index_id = index.GetId(),
              storage = std::move(storage)] {
      // Marking the storage dropped ends the handle for every version of the
      // index at once -- they all hold the same object; a reader that took it
      // before this keeps the directory alive until it is done, and its release
      // removes it.
      if (storage) {
        storage->MarkDropped();
        return;
      }
      // A failed build's compensating drop can arrive before the directory was
      // ever bound to an entry; the one the build created is removed directly.
      search::RemoveDroppedStorageDir(
        search::InvertedIndexStorage::GetPath(database_id, schema_id,
                                              relation_id, index_id),
        3);
    });
}

}  // namespace sdb::catalog
