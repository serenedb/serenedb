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

#include <absl/strings/str_cat.h>

#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/duckdb_primary_key.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/search_table.h"

namespace sdb::catalog {

const SereneDBTableEntry* CreateTable(
  const AccessContext& ax, ObjectId database_id, std::string_view schema,
  duckdb::unique_ptr<duckdb::CreateTableInfo> info,
  std::vector<SerialSequence> sequence_specs,
  CreateTableOperationOptions operation_options) {
  const auto name = std::string{info->GetTableName().GetIdentifierName()};
  // Uniqueness keys are enforced by the store table's DuckDB ART, which cannot
  // index nested types. Reject a nested-type key column up front with a clear
  // error instead of silently creating the table with the constraint dropped.
  for (const auto& constraint : info->constraints) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    const std::string_view what =
      unique.IsPrimaryKey() ? "primary key" : "unique constraint";
    for (const auto& key : unique.GetColumnNames()) {
      const auto* column =
        catalog::ColumnByName(*info, key.GetIdentifierName());
      if (column != nullptr && column->Type().IsNested()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG(what, " column \"", column->Name().GetIdentifierName(),
                  "\" has unsupported nested type ",
                  column->Type().ToString()));
      }
    }
  }

  JoinStoreTransaction(ax.context);
  auto schema_id = TryFindSchemaId(ax.context, database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(ax.context, ax.role, *schema_id);
  if (operation_options.if_not_exists &&
      catalog::FindRelation(ax.context, *schema_id, name)) {
    return nullptr;
  }

  // REFERENCES on the referenced table's key columns, which is what postgres
  // asks of the creator of a foreign key.
  for (const auto& constraint : info->constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    const ObjectId referenced_id{fk.host_referenced_id};
    if (!catalog::StatesForeignKey(fk) || !referenced_id.isSet()) {
      continue;
    }
    const auto* ref = catalog::FindIn<SereneDBTableEntry>(
      ax.context, database_id, referenced_id);
    if (ref == nullptr) {
      continue;
    }
    const auto& ref_perm = ref->permissions;
    std::vector<AclView> ref_acls;
    ref_acls.reserve(fk.host_pk_column_ids.size());
    for (const auto column_id : fk.host_pk_column_ids) {
      ref_acls.push_back(ref->GetColumnAcl(ObjectId{column_id}));
    }
    if (!auth::ClosureFor(ax.context, ax.role)
           ->CanColumns(ref_perm, AclMode::References, ref_acls)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for table ", ref->name.GetIdentifierName()));
    }
  }

  // PG mangles `<table>_<col>_seq` with a numeric suffix on collision. Done
  // under the mutex so concurrent CREATE TABLEs can't race on it. Every half of
  // the relation namespace is asked, because a sequence shares it.
  const auto pick_unique_name = [&](std::string_view base) {
    const auto taken = [&](std::string_view candidate) {
      return catalog::FindRelation(ax.context, *schema_id, candidate) !=
             nullptr;
    };
    std::string candidate{base};
    for (size_t i = 1; taken(candidate); ++i) {
      candidate = absl::StrCat(base, i);
    }
    return candidate;
  };

  const auto make_nextval_default = [](std::string_view qualified) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> args;
    args.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value{std::string{qualified}}));
    return duckdb::make_uniq<duckdb::FunctionExpression>("nextval",
                                                         std::move(args));
  };

  // A caller-provided id means CTAS mode: the plan pre-allocated the id the
  // relation is created under.
  const bool ctas_mode = operation_options.table_id.id() != 0;
  const auto table_id = ctas_mode ? operation_options.table_id : NextId();

  // Generated serial/PK sequences are owned by the table owner too (PG: ALTER
  // TABLE OWNER TO cascades to them, so they must start matching).
  const ObjectId owner = ax.role;
  const Permissions perm{owner};
  const Permissions sequence_perm{owner};
  std::vector<duckdb::unique_ptr<duckdb::CreateSequenceInfo>> sequences;
  sequences.reserve(sequence_specs.size() + 1);
  const auto make_sequence = [&](SequenceOptions opts) {
    return catalog::MakeSequenceInfo(NextId(), *schema_id, std::move(opts));
  };
  for (const auto& spec : sequence_specs) {
    const auto* column = catalog::ColumnById(*info, spec.column_id);
    SDB_ASSERT(column != nullptr);
    auto resolved = pick_unique_name(
      absl::StrCat(name, "_", column->Name().GetIdentifierName(), "_seq"));
    info->columns.GetColumnMutable(column->Logical())
      .SetDefaultValue(
        make_nextval_default(absl::StrCat(schema, ".", resolved)));
    auto seq_opts = spec.options;
    seq_opts.name = resolved;
    seq_opts.owner_table_id = table_id.id();
    sequences.push_back(make_sequence(std::move(seq_opts)));
  }

  // Tables without an explicit PK get an auto-PK owned sequence, whose id the
  // definition holds directly so the insert path does not have to look for it.
  ObjectId generated_pk_seq_id;
  if (TablePrimaryKey(info->constraints) == nullptr) {
    SequenceOptions opts;
    opts.name = pick_unique_name(absl::StrCat(name, "_pk_seq"));
    opts.cache = 65536;
    opts.owner_table_id = table_id.id();
    auto generated_pk_seq = make_sequence(std::move(opts));
    generated_pk_seq_id = catalog::IdOf(*generated_pk_seq);
    sequences.push_back(std::move(generated_pk_seq));
  }
  catalog::SetTableTags(*info, catalog::ReadTableEngineTag(info->tags),
                        catalog::ReadSearchOptionTags(info->tags),
                        generated_pk_seq_id);

  SetIdentity(*info, table_id, *schema_id);
  const auto table = std::move(info);
  // Runtime state, bound onto the entry once it is placed: the shard the rows
  // of a search table live in, and the counter the insert path reserves from.
  std::shared_ptr<search::SearchTable> search_data;
  if (catalog::ReadTableEngineTag(table->tags) == TableEngine::Search) {
    // The declared key columns are term-indexed under their own ids, so the
    // shard needs them to build its merged config.
    std::vector<ColumnId> pk_columns;
    for (const auto& pk : catalog::duckdb_primary_key::BuildPKColumns(*table)) {
      pk_columns.emplace_back(
        table->columns.GetColumn(duckdb::LogicalIndex{pk.input_col_idx})
          .CatalogOid());
    }
    search_data = search::SearchTable::Create(
      database_id, *schema_id, table_id,
      /*is_new=*/true, catalog::ReadSearchOptionTags(table->tags),
      std::move(pk_columns));
  }

  // Checked before anything is placed. Two generated sequences can collide with
  // each other (pick_unique_name only sees the transaction's view), so the
  // batch's own names are tracked too. Sequences share the relation namespace,
  // not its errcode.
  {
    std::vector<std::string_view> registering;
    registering.reserve(sequences.size() + 1);
    const auto taken = [&](std::string_view candidate) {
      return catalog::FindRelation(ax.context, *schema_id, candidate) ||
             absl::c_linear_search(registering, candidate);
    };
    if (taken(name)) {
      ThrowDuplicateName(NameKind::Relation, name);
    }
    registering.push_back(name);
    for (const auto& seq : sequences) {
      if (taken(seq->GetSequenceName().GetIdentifierName())) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
          ERR_MSG("relation \"", seq->GetSequenceName().GetIdentifierName(),
                  "\" already exists"));
      }
      registering.push_back(seq->GetSequenceName().GetIdentifierName());
    }
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  // Ahead of the records: the table's SERIAL columns name these sequences in
  // their DEFAULT, and the resolution the table's record carries is taken while
  // the entry is written -- so the entries have to be there to be resolved to,
  // or the table lands with no edge to its own sequence and DROP SEQUENCE stops
  // being refused. Their records ride the table's own, one frame below.
  for (const auto& seq : sequences) {
    auto placed = catalog::PutEntry(ax.context, /*old_name=*/{}, seq->Copy(),
                                    sequence_perm);
    // Seeded from START: a create has no predecessor to inherit a counter
    // from, and the insert path reserves off the entry's.
    if (const auto* entry =
          dynamic_cast<const catalog::SereneDBSequenceEntry*>(placed.get())) {
      entry->AdoptCounter(
        NewCounter(catalog::IdOf(*seq), catalog::SequenceOptionsOf(*seq)));
    }
  }
  // The seed is a floor, never a rewind: a nextval issued earlier in this
  // transaction has already carried the horizon past it.
  for (const auto& seq : sequences) {
    GetCatalogStore().AdvanceSequenceValue(
      catalog::IdOf(*seq), catalog::SequenceOptionsOf(*seq).Seed());
  }
  catalog::RefreshExpressionReferences(ax.context, *table);
  const auto placed =
    catalog::PutEntry(ax.context, /*old_name=*/{}, table->Copy(), perm);
  const auto* entry =
    dynamic_cast<const catalog::SereneDBTableEntry*>(placed.get());
  if (entry != nullptr && search_data) {
    entry->SetSearchData(std::move(search_data));
  }
  return entry;
}

void ChangeTableOwner(const AccessContext& ax,
                      const duckdb::CreateTableInfo& table,
                      duckdb::CatalogType type, ObjectId new_owner,
                      std::string_view new_owner_name) {
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* live =
    catalog::Find<SereneDBTableEntry>(ax.context, schema_id, table_id);
  if (live == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             table.GetTableName().GetIdentifierName());
  }
  const auto& perm = live->permissions;
  const auto name = live->name.GetIdentifierName();
  // The definition the record carries: the owner moves, the table does not.
  const auto definition = live->Definition();
  RequireOwnerTransfer(ax, schema_id, perm, new_owner, new_owner_name,
                       pg::ToPgObjectTypeName(type), name);

  auto updated_perm = auth::TransferredOwner(perm, new_owner);
  const auto database_id = catalog::SchemaDatabaseId(ax.context, schema_id);
  // A table's SERIAL sequences follow its owner, and are rewritten as their own
  // definitions in the same frame.
  auto sequences = catalog::DatabaseSequences(ax.context, database_id);
  std::erase_if(sequences, [&](const catalog::SereneDBSequenceEntry* seq) {
    return seq->GetOwnerTableId() != table_id;
  });
  std::vector<
    std::pair<duckdb::unique_ptr<duckdb::CreateSequenceInfo>, Permissions>>
    rewritten;
  rewritten.reserve(sequences.size());
  for (const auto* sequence : sequences) {
    rewritten.emplace_back(
      sequence->Definition(),
      auth::TransferredOwner(sequence->permissions, new_owner));
  }
  // One frame for the table and everything that follows its owner: a table and
  // the sequences its SERIAL columns own must never end up owned by different
  // roles, which one append per object allows a crash to do.
  catalog::PutEntry(ax.context, name, definition->Copy(), updated_perm);
  for (auto& [sequence, sequence_perm] : rewritten) {
    const auto sequence_name =
      std::string{sequence->GetSequenceName().GetIdentifierName()};
    catalog::PutEntry(ax.context, sequence_name, sequence->Copy(),
                      std::move(sequence_perm));
  }
}

bool DropTable(const AccessContext& ax, std::string_view database,
               std::string_view schema, std::string_view name, bool cascade,
               bool missing_ok) {
  JoinStoreTransaction(ax.context);

  const auto database_id = FindDatabaseId(ax.context, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto schema_id = TryFindSchemaId(ax.context, database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto target = catalog::RequireDropTarget(
    ax, duckdb::CatalogType::TABLE_ENTRY, *schema_id, name, missing_ok);
  if (!target) {
    return false;
  }
  // A TABLE-typed entry that is not serenedb's -- pg_catalog's generated
  // content -- is not a table this road can take apart.
  const auto* table = dynamic_cast<const SereneDBTableEntry*>(target.get());
  if (table == nullptr) {
    if (missing_ok) {
      return false;
    }
    pg::ThrowUndefinedObject(duckdb::CatalogType::TABLE_ENTRY, name);
  }
  const auto table_id = ObjectId{table->oid};

  // The sequences this table owns, read off the sets before anything is
  // removed: nothing else names them.
  std::vector<ObjectId> owned_sequence_ids;
  std::vector<std::string> owned_sequence_names;
  catalog::Visit<SereneDBSequenceEntry>(
    ax.context, database_id,
    [&](const catalog::SereneDBSequenceEntry& sequence) {
      if (sequence.GetOwnerTableId() == table_id) {
        owned_sequence_ids.push_back(ObjectId{sequence.oid});
        owned_sequence_names.emplace_back(sequence.name.GetIdentifierName());
      }
    });
  // The set drops first, whole: duckdb's dependency walk refuses a RESTRICT
  // with dependents there, and takes each victim through DropDependent -- the
  // indexes on this table whatever was asked for, everything else on a cascade.
  // The owned entries dropped after it no longer see the table as a live
  // dependent of their own.
  catalog::DropEntryOfKind(ax.context, duckdb::CatalogType::TABLE_ENTRY,
                           *schema_id, name, cascade);
  for (const auto& sequence_name : owned_sequence_names) {
    catalog::DropEntryOfKind(ax.context, duckdb::CatalogType::SEQUENCE_ENTRY,
                             *schema_id, sequence_name, cascade);
  }
  // Counters live outside the definition tree.
  for (const auto seq_id : owned_sequence_ids) {
    catalog::DeferDropAction(
      ax.context, [seq_id] { GetCatalogStore().DropSequence(seq_id); });
  }
  // Check that SereneDB won't open this table after reboot
  SDB_IF_FAILURE("crash_on_drop") { return true; }
  SDB_IF_FAILURE("compact_inside_drop") {
    // The artifact half runs after the commit, so a compaction has to be able
    // to run in between.
    catalog::DeferDropAction(ax.context,
                             [] { GetCatalogStore().CompactNow(); });
  }
  catalog::DropSearchTableArtifacts(ax.context, *table);
  return true;
}

void DropSearchTableArtifacts(duckdb::ClientContext* context,
                              const SereneDBTableEntry& table) {
  if (const auto& data = table.GetSearchData()) {
    DeferDropAction(context, [data] { data->MarkDropped(); });
  }
}

void DropTableColumns(duckdb::ClientContext* context,
                      const SereneDBTableEntry& table,
                      std::vector<ObjectId> dropped_columns) {
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto live = table.Definition();
  const auto db_id = catalog::SchemaDatabaseId(context, schema_id);
  auto indexes = catalog::RelationIndexRecords(context, schema_id, table_id);
  // PG's column->index cascade: an index covering a dropped column goes too.
  std::erase_if(indexes, [&](const duckdb::unique_ptr<CreateIndexInfo>& index) {
    const auto covers = absl::c_any_of(dropped_columns, [&](ObjectId col) {
      return index->ReferencesColumn(col);
    });
    if (covers) {
      DropIndexResolved(
        context, db_id, *index,
        catalog::InvertedStorageOf(context, db_id, index->GetId()),
        /*cascade=*/true);
    }
    return covers;
  });
  const bool transactional =
    catalog::ReadTableEngineTag(live->tags) == TableEngine::Transactional;
  if (live->columns.LogicalColumnCount() <= dropped_columns.size()) {
    // Dropping the last column is refused by duckdb's alter; rebuild the rows
    // instead (PG keeps the zero-column table).
    auto final_table = catalog::Clone(*live);
    final_table->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                              /*case_sensitive=*/true);
    final_table->constraints.clear();
    SetIdentity(*final_table, table_id, schema_id);
    auto perm = table.permissions;
    perm.column_acl.clear();
    catalog::RefreshExpressionReferences(context, *final_table);
    catalog::PutEntry(context, final_table->GetTableName().GetIdentifierName(),
                      final_table->Copy(), std::move(perm));
    return;
  }
  if (transactional) {
    // Surviving store indexes block the reshape whenever they cover a column
    // positioned after a dropped one; recreate them around it.
    for (const auto& idx : indexes) {
      catalog::StoreDropIndex(context, db_id, idx->GetRelationId(),
                              idx->GetName());
    }
  }
  const duckdb::AlterEntryData at{live->GetQualifiedName(),
                                  duckdb::OnEntryNotFound::THROW_EXCEPTION};
  for (const auto col_id : dropped_columns) {
    const auto* column = catalog::ColumnById(*live, col_id);
    if (column == nullptr) {
      continue;
    }
    duckdb::RemoveColumnInfo op{at,
                                std::string{column->Name().GetIdentifierName()},
                                /*if_column_exists=*/false, /*cascade=*/true};
    catalog::ApplyTableAlter(context, *live, op);
  }
  if (!transactional) {
    return;
  }
  const auto* altered = catalog::FindSessionTableEntry(*context, table_id);
  if (altered == nullptr) {
    return;
  }
  const auto updated = altered->Definition();
  for (const auto& idx : indexes) {
    if (auto store_info = MakeStoreIndexInfo(*updated, *idx)) {
      // A reshape re-states indexes that already exist: their committed
      // entries carry the directory handle, so the op resolves it there.
      catalog::StoreCreateIndex(context, db_id, std::move(store_info),
                                catalog::Clone(*updated), idx->GetRelationId(),
                                idx->GetIndex(), /*storage=*/nullptr);
    }
  }
}

void DropTableColumn(const AccessContext& ax, ObjectId database_id,
                     const duckdb::CreateTableInfo& table,
                     std::string_view column, bool if_exists) {
  JoinStoreTransaction(ax.context);
  const auto table_id = catalog::IdOf(table);
  const auto* entry = catalog::Find<SereneDBTableEntry>(
    ax.context, catalog::ParentIdOf(table), table_id);
  if (entry == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             table.GetTableName().GetIdentifierName());
  }
  const auto& perm = entry->permissions;
  const auto live = entry->Definition();
  RequireOwner(ax.context, ax.role, perm, "table",
               entry->name.GetIdentifierName());
  const auto* col = catalog::ColumnByName(*live, column);
  if (col == nullptr) {
    if (if_exists) {
      return;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", column, "\" of relation \"",
              live->GetTableName().GetIdentifierName(), "\" does not exist"));
  }
  const ObjectId col_id{col->CatalogOid()};

  DropTableColumns(ax.context, *entry, {col_id});
}

}  // namespace sdb::catalog
