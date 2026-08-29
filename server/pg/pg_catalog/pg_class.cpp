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

#include "pg/pg_catalog/pg_class.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <deque>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <string>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"
#include "query/config_variable_names.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgClass::oid),
  GetIndex(&PgClass::relname),
  GetIndex(&PgClass::relnamespace),
  GetIndex(&PgClass::reltype),
  GetIndex(&PgClass::reloftype),
  GetIndex(&PgClass::relowner),
  GetIndex(&PgClass::relam),
  GetIndex(&PgClass::relfilenode),
  GetIndex(&PgClass::reltablespace),
  GetIndex(&PgClass::relpages),
  GetIndex(&PgClass::reltuples),
  GetIndex(&PgClass::relallvisible),
  GetIndex(&PgClass::relallfrozen),
  GetIndex(&PgClass::reltoastrelid),
  GetIndex(&PgClass::relhasindex),
  GetIndex(&PgClass::relisshared),
  GetIndex(&PgClass::relpersistence),
  GetIndex(&PgClass::relkind),
  GetIndex(&PgClass::relnatts),
  GetIndex(&PgClass::relchecks),
  GetIndex(&PgClass::relhasrules),
  GetIndex(&PgClass::relhastriggers),
  GetIndex(&PgClass::relhassubclass),
  GetIndex(&PgClass::relrowsecurity),
  GetIndex(&PgClass::relforcerowsecurity),
  GetIndex(&PgClass::relispopulated),
  GetIndex(&PgClass::relreplident),
  GetIndex(&PgClass::relispartition),
  GetIndex(&PgClass::relrewrite),
  GetIndex(&PgClass::relfrozenxid),
  GetIndex(&PgClass::relminmxid),
  GetIndex(&PgClass::relacl),
  GetIndex(&PgClass::reloptions),
});

}  // namespace

// Indexes have no owner of their own, so the caller passes the underlying
// table's owner (PG semantics).
PgClass MakeBaseRow(ObjectId schema_id, ObjectId oid, std::string_view name,
                    ObjectId owner) {
  return {
    .oid = oid.id(),
    .relname = name,
    .relnamespace = schema_id.id(),
    .reltype = 0,
    .reloftype = 0,
    .relowner = owner.id(),
    .relam = 0,
    .relfilenode = 0,
    .reltablespace = 0,
    .relpages = 0,
    .reltuples = -1,
    .relallvisible = 0,
    .relallfrozen = 0,
    .reltoastrelid = 0,
    .relhasindex = false,
    .relisshared = false,
    .relpersistence = PgClass::Relpersistence::Permanent,
    .relkind = PgClass::Relkind::OrdinaryTable,
    .relnatts = 0,
    .relchecks = 0,
    .relhasrules = false,
    .relhastriggers = false,
    .relhassubclass = false,
    .relrowsecurity = false,
    .relforcerowsecurity = false,
    .relispopulated = true,
    .relreplident = PgClass::Relreplident::Default,
    .relispartition = false,
    .relrewrite = 0,
    .relfrozenxid = 0,
    .relminmxid = 0,
  };
}

// reloptions: the persisted storage parameters as a text[] of k=v. Options
// always hold concrete values (resolved from WITH / session settings when
// they were set), so every option is rendered; segment_docs_max=0 means
// unlimited.
std::vector<std::string> RenderInvertedIndexOptions(
  const catalog::InvertedIndexOptions& options) {
  std::vector<std::string> rendered;
  const auto add = [&](std::string_view name, uint64_t value) {
    rendered.push_back(absl::StrCat(name, "=", value));
  };
  add(kRowGroupSizeSetting, options.row_group_size);
  add(kRefreshIntervalSetting, options.refresh_interval_ms);
  add(kReindexIntervalSetting, options.reindex_interval_ms);
  add(kCompactionIntervalSetting, options.compaction_interval_ms);
  add(kCleanupIntervalStepSetting, options.cleanup_interval_step);
  add(kSegmentMemoryMaxSetting, options.segment_memory_max);
  add(kSegmentDocsMaxSetting, options.segment_docs_max);
  add(kCompactionMaxSegmentsSetting, options.compaction_max_segments);
  add(kCompactionMaxSegmentsBytesSetting,
      options.compaction_max_segments_bytes);
  add(kCompactionFloorSegmentBytesSetting,
      options.compaction_floor_segment_bytes);
  return rendered;
}

void RetrieveObjects(ObjectId database_id, std::vector<PgClass>& values,
                     std::deque<std::string>& pk_index_names,
                     std::deque<std::string>& uq_index_names,
                     std::vector<std::vector<std::string>>& reloptions_storage,
                     std::vector<std::vector<Text>>& reloptions_views,
                     duckdb::ClientContext& context) {
  // reltuples is read from the relation's own row-group metadata
  // (DataTable::GetTotalRows), never a count(*) query: pg_catalog must not scan
  // data. Off the entry this walk already holds, and nothing else: resolving a
  // second entry here would re-enter the catalog sets this walk is inside.
  auto count_store_rows = [](catalog::SereneDBTableEntry& table) -> float {
    auto storage = table.TryGetStorage();
    return storage ? static_cast<float>(storage->GetTotalRows()) : 0.0F;
  };
  // The two facts a relation's row needs from outside its own definition:
  // whether anything indexes it, and -- for the index rows below -- who owns
  // the relation the index hangs off, since an index has no owner of its own.
  // Both come off the same sets the rows do, so the whole projection answers
  // from one place.
  std::vector<const catalog::SereneDBIndexEntry*> indexes;
  containers::FlatHashSet<ObjectId> indexed_relations;
  catalog::Visit<catalog::SereneDBIndexEntry>(
    &context, database_id, [&](const catalog::SereneDBIndexEntry& entry) {
      indexed_relations.insert(entry.GetRelationId());
      indexes.push_back(&entry);
    });
  containers::FlatHashMap<ObjectId, ObjectId> relation_owners;
  // The tables in set order, for the synthetic key-index rows below, and the
  // sequences that feed a synthetic primary key -- serenedb's own machinery,
  // which postgres has no relation for.
  std::vector<std::pair<ObjectId, const catalog::SereneDBTableEntry*>> tables;
  containers::FlatHashSet<ObjectId> generated_pk_sequences;

  catalog::VisitCatalogSetEntries(
    context, database_id, duckdb::CatalogType::TABLE_ENTRY,
    [&](const catalog::SereneDBSchemaEntry& schema,
        duckdb::CatalogEntry& entry) {
      // The index-name-as-table wrappers share this set: their shape is the
      // relation's and pg_class already has that relation's row, so only a
      // table and a view are rows of their own here.
      const auto schema_id = catalog::IdOf(schema);
      auto* table = dynamic_cast<catalog::SereneDBTableEntry*>(&entry);
      if (table != nullptr) {
        relation_owners.emplace(catalog::IdOf(*table),
                                ObjectId{table->permissions.owner});
        tables.emplace_back(schema_id, table);
        if (table->GetGeneratedPkSeqId().isSet()) {
          generated_pk_sequences.insert(table->GetGeneratedPkSeqId());
        }
        auto row = MakeBaseRow(schema_id, catalog::IdOf(*table),
                               table->name.GetIdentifierName(),
                               ObjectId{table->permissions.owner});
        row.relkind = PgClass::Relkind::OrdinaryTable;
        row.relnatts =
          static_cast<int16_t>(table->GetColumns().LogicalColumnCount());
        // Postgres counts CHECK constraints here and nothing else: NOT NULL is
        // a pg_constraint row of its own but not one of these.
        row.relchecks = static_cast<int16_t>(std::ranges::count_if(
          table->GetConstraints(), [](const auto& constraint) {
            return constraint->type == duckdb::ConstraintType::CHECK;
          }));
        row.relhasindex = indexed_relations.contains(catalog::IdOf(*table));
        row.reltuples = count_store_rows(*table);
        row.relacl = {table->permissions.acl};
        values.push_back(std::move(row));
        return;
      }
      const auto* view_entry =
        dynamic_cast<const catalog::SereneDBViewEntry*>(&entry);
      if (view_entry == nullptr) {
        return;
      }
      const auto view_id = ObjectId{view_entry->oid};
      relation_owners.emplace(view_id, ObjectId{view_entry->permissions.owner});
      auto row =
        MakeBaseRow(schema_id, view_id, view_entry->name.GetIdentifierName(),
                    ObjectId{view_entry->permissions.owner});
      row.relkind = PgClass::Relkind::View;
      row.relacl = {view_entry->permissions.acl};
      values.push_back(std::move(row));
    });

  for (const auto* entry : indexes) {
    const auto record = entry->GetInfo();
    const auto& index = record->Cast<catalog::CreateIndexInfo>();
    const auto owner = relation_owners.find(index.GetRelationId());
    if (owner == relation_owners.end()) {
      continue;
    }
    // The entry's own name, which outlives the record read above.
    auto row = MakeBaseRow(index.GetSchemaId(), index.GetId(),
                           entry->name.GetIdentifierName(), owner->second);
    row.relkind = PgClass::Relkind::Index;
    row.relnatts = static_cast<int16_t>(index.GetColumns().size());
    if (index.IsInverted()) {
      row.relam = id::kPgAmInverted.id();
      auto rendered = RenderInvertedIndexOptions(
        catalog::InvertedInfo(*index.GetIndex()).GetOptions());
      if (!rendered.empty()) {
        const auto& strings =
          reloptions_storage.emplace_back(std::move(rendered));
        auto& views = reloptions_views.emplace_back();
        views.reserve(strings.size());
        for (const auto& option : strings) {
          views.emplace_back(option);
        }
        row.reloptions = views;
      }
    } else {
      row.relam = id::kPgAmSecondary.id();
    }
    values.push_back(std::move(row));
  }

  catalog::Visit<catalog::SereneDBSequenceEntry>(
    &context, database_id, [&](const catalog::SereneDBSequenceEntry& sequence) {
      // The synthetic primary-key sequence of a table declaring none is
      // serenedb's own machinery, like the column it feeds: postgres has no
      // such relation and neither does pg_class. A SERIAL's sequence is a real
      // one and is listed, as PG lists it.
      const auto& perm = sequence.permissions;
      if (generated_pk_sequences.contains(ObjectId{sequence.oid})) {
        return;
      }
      auto row = MakeBaseRow(
        ObjectId{sequence.ParentSchema().oid}, ObjectId{sequence.oid},
        sequence.name.GetIdentifierName(), ObjectId{perm.owner});
      row.relkind = PgClass::Relkind::Sequence;
      row.relacl = {catalog::AclView{perm.acl}};
      values.push_back(std::move(row));
    });

  catalog::Visit<catalog::SereneDBTypeEntry>(
    &context, database_id, [&](const duckdb::TypeCatalogEntry& type) {
      if (type.user_type.id() != duckdb::LogicalTypeId::STRUCT) {
        return;
      }
      auto row = MakeBaseRow(ObjectId{type.ParentSchema().oid},
                             ObjectId{type.oid}, type.name.GetIdentifierName(),
                             ObjectId{type.permissions.owner});
      row.relkind = PgClass::Relkind::CompositeType;
      row.relnatts = static_cast<int16_t>(
        duckdb::StructType::GetChildTypes(type.user_type).size());
      values.push_back(std::move(row));
    });

  // Synthetic pg_class entries for the indexes backing key constraints (PG
  // semantics): one per PRIMARY KEY, then one per UNIQUE. The relation is the
  // constraint's own name and backing-index id, and the owner is the table's,
  // as for any index. Primary keys first so the rows stay grouped.
  for (const auto primary : {true, false}) {
    for (const auto& [schema_id, table] : tables) {
      for (const auto& constraint : table->GetConstraints()) {
        if (constraint->type != duckdb::ConstraintType::UNIQUE) {
          continue;
        }
        const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (unique.IsPrimaryKey() != primary) {
          continue;
        }
        auto& names = primary ? pk_index_names : uq_index_names;
        names.push_back(unique.constraint_name);
        auto row =
          MakeBaseRow(schema_id, ObjectId{unique.host_index_id}, names.back(),
                      ObjectId{table->permissions.owner});
        row.relkind = PgClass::Relkind::Index;
        row.relnatts = static_cast<int16_t>(
          catalog::KeyConstraintAttnums(*table, unique).size());
        values.push_back(std::move(row));
      }
    }
  }
}

template<>
catalog::MaterializedData SystemTableSnapshot<PgClass>::GetTableData() {
  std::vector<PgClass> values;
  std::deque<std::string> pk_index_names;
  std::deque<std::string> uq_index_names;
  std::vector<std::vector<std::string>> reloptions_storage;
  std::vector<std::vector<Text>> reloptions_views;
  RetrieveObjects(GetDatabaseId(), values, pk_index_names, uq_index_names,
                  reloptions_storage, reloptions_views,
                  _config.GetClientContext());

  {
    VisitSystemTables([&](const catalog::VirtualTable& table, Oid schema_oid) {
      auto row_type = table.RowType();
      int16_t natts = row_type.id() == duckdb::LogicalTypeId::STRUCT
                        ? static_cast<int16_t>(
                            duckdb::StructType::GetChildTypes(row_type).size())
                        : 0;
      // pg_hba_file_rules is a view in PostgreSQL (relkind 'v'), not a base
      // table, so relkind='r' queries must not list it.
      const auto relkind = table.GetName() == "pg_hba_file_rules"
                             ? PgClass::Relkind::View
                             : PgClass::Relkind::OrdinaryTable;
      PgClass row{
        .oid = table.Id().id(),
        .relname = table.GetName(),
        .relnamespace = schema_oid,
        .reltype = 0,
        .reloftype = 0,
        .relowner = id::kRootUser.id(),
        .relam = 0,
        .relfilenode = 0,
        .reltablespace = 0,
        .relpages = 0,
        .reltuples = -1,
        .relallvisible = 0,
        .relallfrozen = 0,
        .reltoastrelid = 0,
        .relhasindex = false,
        .relisshared = false,
        .relpersistence = PgClass::Relpersistence::Permanent,
        .relkind = relkind,
        .relnatts = natts,
        .relchecks = 0,
        .relhasrules = false,
        .relhastriggers = false,
        .relhassubclass = false,
        .relrowsecurity = false,
        .relforcerowsecurity = false,
        .relispopulated = true,
        .relreplident = PgClass::Relreplident::Default,
        .relispartition = false,
        .relrewrite = 0,
        .relfrozenxid = 0,
        .relminmxid = 0,
      };
      values.push_back(std::move(row));
    });
  }

  {
    VisitSystemViews([&](const StaticView& view, Oid schema_oid) {
      PgClass row{
        .oid = catalog::IdOf(*view.first).id(),
        .relname = view.first->GetViewName().GetIdentifierName(),
        .relnamespace = schema_oid,
        .reltype = 0,
        .reloftype = 0,
        .relowner = id::kRootUser.id(),
        .relam = 0,
        .relfilenode = 0,
        .reltablespace = 0,
        .relpages = 0,
        .reltuples = -1,
        .relallvisible = 0,
        .relallfrozen = 0,
        .reltoastrelid = 0,
        .relhasindex = false,
        .relisshared = false,
        .relpersistence = PgClass::Relpersistence::Permanent,
        .relkind = PgClass::Relkind::View,
        .relnatts = 0,
        .relchecks = 0,
        .relhasrules = false,
        .relhastriggers = false,
        .relhassubclass = false,
        .relrowsecurity = false,
        .relforcerowsecurity = false,
        .relispopulated = true,
        .relreplident = PgClass::Relreplident::Default,
        .relispartition = false,
        .relrewrite = 0,
        .relfrozenxid = 0,
        .relminmxid = 0,
      };
      values.push_back(std::move(row));
    });
  }

  auto result = CreateColumns<PgClass>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
