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

#pragma once

#include <absl/functional/function_ref.h>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "connector/duckdb_schema_entry.h"

namespace sdb::connector {

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info);

// The storage-extension key, i.e. what Catalog::GetCatalogType() answers for a
// serenedb database. The one way to tell a serenedb attachment from an
// attached foreign one.
inline constexpr std::string_view kSereneDBCatalogType = "serenedb";

// Derives from DuckCatalog for its write lock, schema CatalogSet and
// undo-buffer wiring: CatalogSet only accepts a DuckCatalog and every mutation
// reaches DuckTransactionManager, which throws for a foreign manager. The
// The schema entries and their sets are serenedb's own, so the inherited
// schema set and dependency manager stay empty -- the overrides below keep
// every duckdb path that would consult them on today's behaviour.
class SereneDBCatalog final : public duckdb::DuckCatalog {
 public:
  // `public_schema` is set only by CREATE DATABASE: the schema shares the
  // database's log frame, so it is written before this catalog exists and
  // Initialize is what puts it in the set.
  SereneDBCatalog(duckdb::AttachedDatabase& db, ObjectId database_id,
                  catalog::HeldSchema public_schema);

  ObjectId GetDatabaseId() const { return _database_id; }

  // Storage-extension key, not DuckCatalog's "duckdb".
  std::string GetCatalogType() final { return "serenedb"; }
  std::string GetDefaultSchema() const final { return "public"; }
  // Unquoted identifiers are folded at parse time and then matched exactly, as
  // postgres does, so "A" and "a" are two columns and two relations.
  bool MatchesNamesExactly() const final { return true; }

  // The index the user asked for is a serenedb entry; the ART built over the
  // rows gets none, so this catalog is the only one that can say a name is
  // taken -- and it has already said so by the time the build runs.
  bool OwnsIndexNames() const final { return true; }
  duckdb::optional_idx GetCatalogVersion(duckdb::ClientContext& context) final;
  void Initialize(bool load_builtin) final;

  // Dependency edges are addressed by the object's stable id, so a rename never
  // rewrites one. Entries with no serenedb id keep duckdb's name-keyed form.
  duckdb::CatalogEntryInfo GetDependencyInfo(
    const duckdb::CatalogEntry& entry) const final;
  duckdb::optional_ptr<duckdb::CatalogEntry> GetDependencyEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::CatalogEntryInfo& info) final;

  // A drop plans its own cascade -- a catalog record and a store op per victim
  // -- and has already refused a RESTRICT drop by the time an entry is removed.
  bool CascadeDropsThroughDependencies() const final { return false; }

  // A rename breaks nothing here: a view records the columns it selects by
  // position, an index its keys by storage position, and a sequence is bound to
  // the table rather than to a name in it -- so postgres lets a column be
  // renamed under all three, and so do we. Every other alter keeps duckdb's
  // answer, which is what refuses an ADD PRIMARY KEY under a live index.
  bool AlterBreaksDependent(const duckdb::AlterInfo& info,
                            duckdb::CatalogType) const final {
    if (info.type != duckdb::AlterType::ALTER_TABLE) {
      return true;
    }
    const auto action = info.Cast<duckdb::AlterTableInfo>().alter_table_type;
    return action != duckdb::AlterTableType::RENAME_COLUMN &&
           action != duckdb::AlterTableType::RENAME_TABLE;
  }

  duckdb::ErrorData SupportsCreateTable(
    duckdb::BoundCreateTableInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) final;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& schema_lookup,
    duckdb::OnEntryNotFound if_not_found) final;

  void ScanSchemas(
    duckdb::ClientContext& context,
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) final;

  // The contextless scan the checkpoint runs on. Both the store schema and the
  // SereneDB schema entries are visited: a SereneDB entry that owns storage has
  // to be reachable here or the checkpoint never sees its rows.
  void ScanSchemas(
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) final;

  duckdb::optional_ptr<duckdb::TableCatalogEntry> LookupTableById(
    duckdb::CatalogTransaction transaction, duckdb::idx_t catalog_id) final;

  // The rows of the table `catalog_id` names, and nothing else. The definition
  // is this catalog's own and has already been replayed from the catalog log,
  // so a record in the data file describes a reshape of the rows in front of it
  // -- resolved by identity, because the name in that record is the one the
  // alter named and a later rename has since moved it.
  //
  // Both are the load's, gated on the storage manager not being loaded yet: a
  // live statement goes the other way, through the catalog and then to here.
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  // `versioned` puts the reshape in the table's CatalogSet as the alter it is,
  // which is what records it in this database's WAL. False for boot, where the
  // definition is already final and the rows move under it in place.
  void AlterStorage(duckdb::CatalogTransaction transaction,
                    duckdb::AlterInfo& info, bool versioned);
  void CreateTableStorage(duckdb::CatalogTransaction transaction,
                          duckdb::BoundCreateTableInfo& info);
  // Whether this catalog is still opening its data file, i.e. whether a record
  // arriving now is a replay rather than a statement.
  bool IsReplaying() const;

  void DropSchema(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  // A transaction reading the latest committed catalog, for the paths that
  // have no transaction of their own -- boot, the background drop tasks, and
  // the by-name schema lookups the entry projection performs.
  duckdb::CatalogTransaction CommittedRead();

  // The entry of a schema that exists as of `transaction`, or null.
  duckdb::optional_ptr<SereneDBSchemaEntry> TryGetSchemaEntry(
    duckdb::CatalogTransaction transaction, std::string_view schema_name);
  duckdb::optional_ptr<SereneDBSchemaEntry> TryGetSchemaEntry(
    std::string_view schema_name);

  // Puts a schema in the set. `schema` is null for the two static schemas,
  // which are generated at Initialize rather than created by anyone.
  bool CreateSchemaEntry(duckdb::CatalogTransaction transaction,
                         std::string_view schema_name,
                         catalog::HeldSchema schema);
  void DropSchemaEntry(duckdb::CatalogTransaction transaction,
                       std::string_view schema_name);

  // Every committed schema entry.
  void VisitSchemaEntries(
    absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor);

  // Foreign servers are database children, as they are in postgres, so their
  // version chain hangs off the catalog rather than off a schema entry.
  duckdb::CatalogSet& GetForeignServerSet() { return _foreign_servers; }

  // Where the entry of each stable id in this database currently lives, keyed
  // on the id. Every by-oid reader -- pg_relation_size, has_table_privilege,
  // the drop planner -- resolves through this rather than walking the schemas.
  duckdb::CatalogSet& GetObjectIndexSet() { return _object_index; }

  void OnDetach(duckdb::ClientContext& context) final;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanInsert(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalInsert& op,
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) final;

  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanUpdate(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalUpdate& op,
                                       duckdb::PhysicalOperator& plan) final;

  duckdb::PhysicalOperator& PlanMergeInto(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndex(
    duckdb::Binder& binder, duckdb::CreateStatement& stmt,
    duckdb::CatalogEntry& table,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan) final;

  duckdb::unique_ptr<duckdb::LogicalOperator> BindAlterAddIndex(
    duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
    duckdb::unique_ptr<duckdb::LogicalOperator> plan,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
    duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) final;

  duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext& context) final;

  bool InMemory() final { return false; }

 private:
  ObjectId _database_id;
  catalog::HeldSchema _public_schema;
  // Every schema of this database, the two static ones included. Created and
  // dropped, never versioned: a schema entry owns the CatalogSets of everything
  // under it, so a second version would strand the first's contents and every
  // table entry's reference to it. What a schema's own definition changes --
  // owner and ACL -- is shared side state on the entry (SetDefinition).
  duckdb::CatalogSet _schemas;
  duckdb::CatalogSet _foreign_servers;
  duckdb::CatalogSet _object_index;
};

struct RelationStorageSize {
  int64_t bytes = 0;
  int64_t persistent_blocks = 0;
};

class SereneDBIndexEntry;
class SereneDBTableEntry;

RelationStorageSize StoreTableDataSize(duckdb::ClientContext& context,
                                       const SereneDBTableEntry& table);
int64_t StoreTableIndexBytes(duckdb::ClientContext& context,
                             const SereneDBTableEntry& table);
// The rows of `table` plus every index over them: the ARTs on the store table
// and the iresearch directories of its inverted indexes.
int64_t TableIndexesTotalBytes(duckdb::ClientContext& context,
                               SereneDBTableEntry& table);
int64_t IndexEntryBytes(duckdb::ClientContext& context,
                        const SereneDBIndexEntry& index);
int64_t SearchTableBytes(const SereneDBTableEntry& table);
// The rows of one relation, whatever engine owns them.
int64_t RelationDataBytes(duckdb::ClientContext& context,
                          const SereneDBTableEntry& table);
duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         ObjectId database_id,
                                         std::string_view only_schema = {});

}  // namespace sdb::connector
