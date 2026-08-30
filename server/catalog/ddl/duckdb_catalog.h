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
#include <absl/synchronization/mutex.h>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_schema_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

void DropObject(duckdb::ClientContext& context, duckdb::DropInfo& info);

// The storage of one inverted index, off the entry that carries it, read
// through `context` when there is one so a statement sees the index it created
// itself. Null when that database is not attached, when nothing there carries
// that id, or when the index is not inverted.
std::shared_ptr<search::InvertedIndexStorage> InvertedStorageOf(
  duckdb::ClientContext* context, ObjectId database_id, ObjectId index_id);
std::shared_ptr<search::InvertedIndexStorage> InvertedStorageIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId index_id);
// The same read committed, for the callers with no statement to read through.
// A caller holding one passes it: an index its own transaction created is not
// committed yet, and its handle is on a version only that statement can see.
std::shared_ptr<search::InvertedIndexStorage> InvertedStorageOf(
  ObjectId database_id, ObjectId index_id);
std::shared_ptr<search::InvertedIndexStorage> InvertedStorageIn(
  duckdb::Catalog& catalog, ObjectId index_id);
// The definition of one inverted index, out of the catalog holding it, read
// through `context` when there is one so a statement sees its own uncommitted
// index. Null when nothing there carries that id.
std::shared_ptr<const Index> InvertedDefinitionIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId index_id);

// The storage-extension key, i.e. what Catalog::GetCatalogType() answers for a
// serenedb database. The one way to tell a serenedb attachment from an
// attached foreign one.
inline constexpr std::string_view kSereneDBCatalogType = "serenedb";

// Derives from DuckCatalog for its write lock, schema CatalogSet, undo-buffer
// wiring and dependency manager; every entry's edges land in the manager keyed
// by the stable id (GetDependencyInfo below).
class SereneDBCatalog final : public duckdb::DuckCatalog {
 public:
  // Every database has a public schema from the moment it exists, made when
  // this catalog is opened the way duckdb makes its own default schema. It is
  // not a record of its own: the database states its id, so every open agrees
  // on what pg_namespace reports.
  SereneDBCatalog(duckdb::AttachedDatabase& db, ObjectId database_id,
                  ObjectId public_schema_id, catalog::Permissions owner);

  ObjectId GetDatabaseId() const { return _database_id; }

  // Storage-extension key, not DuckCatalog's "duckdb".
  std::string GetCatalogType() final { return "serenedb"; }
  bool SupportsForeignDependencies() const final { return true; }
  std::string GetDefaultSchema() const final { return "public"; }
  // Unquoted identifiers are folded at parse time and then matched exactly, as
  // postgres does, so "A" and "a" are two columns and two relations.
  bool MatchesNamesExactly() const final { return true; }

  // The index the user asked for is a serenedb entry; the ART built over the
  // rows gets none, so this catalog is the only one that can say a name is
  // taken -- and it has already said so by the time the build runs.
  bool OwnsIndexNames() const final { return true; }
  void Initialize(bool load_builtin) final;
  // The context overload is the one that matters: CREATE DATABASE reaches here
  // with the statement that made it, and the new database's public schema is
  // that statement's entry -- which is what records it and what a rollback
  // takes away again.
  void Initialize(duckdb::optional_ptr<duckdb::ClientContext> context,
                  bool load_builtin) final;

  // Dependency edges are addressed by the object's stable id, so a rename never
  // rewrites one. Entries with no serenedb id keep duckdb's name-keyed form.
  duckdb::CatalogEntryInfo GetDependencyInfo(
    const duckdb::CatalogEntry& entry) const final;
  duckdb::optional_ptr<duckdb::CatalogEntry> GetDependencyEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::CatalogEntryInfo& info) final;

  // A cascade victim of this catalog needing more than the entry removed: an
  // index adds the store half and the artifact sweep, a sequence its counter
  // row.
  bool DropDependent(
    duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& object,
    duckdb::CatalogEntry& dependent, bool cascade,
    const duckdb::vector<duckdb::DependencyPiece>& pieces) final;

  // The trim's one application point: duckdb translated the piece into the
  // alter, this runs it whole -- definition, storage reshape and the store
  // mirror -- with no statement behind it (authority was checked on the seed).
  void AlterDependent(duckdb::CatalogTransaction transaction,
                      duckdb::CatalogEntry& dependent,
                      duckdb::AlterInfo& info) final;

  // No edge records which column a dependent binds, and a view re-resolves by
  // name at next use, so a rename or a dropped piece degrades a dependent here
  // rather than blocking the alter -- as in postgres. Every alter a dependent
  // cannot re-resolve after keeps duckdb's answer.
  bool DependentsResolveByName() const final { return true; }

  // An index, a constraint or a default is a piece of the relation it hangs off
  // rather than a separate binding: a reshape rebuilds it (AlterDependent), so
  // it does not stand in the way of one. ALTER COLUMN TYPE on an indexed column
  // rebuilds the index instead of being refused.
  bool ReshapesOwnedDependents() const final { return true; }

  duckdb::ErrorData SupportsCreateTable(
    duckdb::BoundCreateTableInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) final;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& schema_lookup,
    duckdb::OnEntryNotFound if_not_found) final;

  using duckdb::DuckCatalog::ScanSchemas;
  void ScanSchemas(
    duckdb::ClientContext& context,
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) final;

  duckdb::optional_ptr<duckdb::TableCatalogEntry> LookupTableById(
    duckdb::CatalogTransaction transaction, duckdb::idx_t catalog_id) final;

  // During load, a data-file record reshapes rows behind an already-replayed
  // definition, resolved by identity: the name it carries may predate a rename.
  // A live statement goes the other way, through the catalog and then to here.
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  // ALTER SCHEMA ... RENAME TO. The schema is not inside a schema, so the
  // catalog answers it rather than handing it down like every other alter.
  void RenameSchema(duckdb::CatalogTransaction transaction,
                    const duckdb::RenameSchemaInfo& info);

  // `versioned` puts the reshape in the table's CatalogSet as the alter it is,
  // which is what records it in this database's WAL. False for boot, where the
  // definition is already final and the rows move under it in place.
  void AlterStorage(duckdb::CatalogTransaction transaction,
                    duckdb::AlterInfo& info, bool versioned);

  // The catalog log is flushed before this file, so a table's rows can only be
  // behind its definition; each one still behind is walked up to it here. True
  // when a table was repaired, which the file must learn before any write.
  bool FinishStorageReplay(duckdb::ClientContext& context);

  // The shape this replay has walked `table_id`'s rows to, starting from the
  // one the file itself describes.
  duckdb::ColumnList& ReplayShape(uint64_t table_id,
                                  const duckdb::DataTable& rows);

  // The rows of a table the file has nothing of, built from the definition
  // that outlived it.
  bool ReplayMissingRows(duckdb::DuckTableEntry& table);

  // The reshapes that take one table's rows from the shape the file left them
  // in to the one its definition states, derived from the difference between
  // the two and applied on the spot.
  bool ReplayMissingReshapes(duckdb::CatalogTransaction transaction,
                             duckdb::DuckTableEntry& table);

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

  // Puts a schema in the set. `id` is unset for the two static schemas, which
  // are generated at Initialize rather than created by anyone.
  bool CreateSchemaEntry(duckdb::CatalogTransaction transaction,
                         std::string_view schema_name, ObjectId id,
                         catalog::Permissions perm,
                         const duckdb::LogicalDependencyList& deps);
  // Chains a new version of an existing schema entry: a rename, an owner
  // transfer or a GRANT. `old_name` is the name the version this supersedes is
  // filed under, which a rename moves the set's key away from. The new entry
  // takes the schema's whole contents over, so nothing under it moves.
  // False when the set refuses the write -- a lost race with concurrent DDL.
  bool AlterSchemaEntry(duckdb::CatalogTransaction transaction,
                        std::string_view old_name, std::string_view new_name,
                        ObjectId id, catalog::Permissions perm,
                        const duckdb::LogicalDependencyList& deps);
  void DropSchemaEntry(duckdb::CatalogTransaction transaction,
                       std::string_view schema_name, bool cascade);

  // The schema entry of one id, which is how everything the schema holds is
  // found: an object's location names its schema by id, so a rename of the
  // schema moves nothing under it. The data file's own records resolve the
  // same way, through the duckdb hook below.
  duckdb::optional_ptr<SereneDBSchemaEntry> TryGetSchemaEntryById(
    duckdb::CatalogTransaction transaction, ObjectId id);
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchemaById(
    duckdb::CatalogTransaction transaction, duckdb::idx_t catalog_id) final;

  // Every committed schema entry.
  void VisitSchemaEntries(
    absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor);

  // Foreign servers are database children, as they are in postgres, so their
  // version chain hangs off the catalog rather than off a schema entry.
  duckdb::CatalogSet& GetForeignServerSet() { return _foreign_servers; }

  // The catalog-level sets the by-id map's root locations resolve against:
  // duckdb's own schemas set answers for schemas, and this catalog adds one
  // set duckdb has no concept of.
  duckdb::optional_ptr<duckdb::CatalogSet> RootEntrySet(
    duckdb::CatalogType slot) final;

  // The positions of a relation's columns some index covers, in ascending
  // order -- what a search relation identifies its rows by. No entry version
  // can carry this, and the readers (duckdb's virtual-column API) have no
  // context to resolve it live with.
  std::vector<size_t> IndexedColumns(ObjectId relation_id) const;
  void SetIndexColumns(ObjectId relation_id, ObjectId index_id,
                       std::vector<size_t> columns) const;
  void RemoveIndexColumns(ObjectId relation_id, ObjectId index_id) const;
  // The relation is gone; its indexes went with it.
  void ReleaseIndexedColumns(ObjectId relation_id) const;

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

  // One committed catalog change, from the commit walk over this transaction's
  // undo records: the serenedb log is written here rather than ahead of the
  // entry, so what it records is exactly the versions the commit published.
  void ReplayCatalogEntry(duckdb::ClientContext& context,
                          duckdb::CreateInfo& info,
                          const duckdb::CatalogPermissions& permissions,
                          bool dropped) final;

  void WriteCatalogChange(duckdb::DuckTransaction& transaction,
                          duckdb::CatalogEntry& entry,
                          duckdb::data_ptr_t extra_data) final;

  // Taken before the catalog and set locks of the entry being committed, so the
  // cluster-wide log lock is never acquired under them.
  void PrepareCatalogChange(duckdb::DuckTransaction& transaction) final;

  // The per-database DDL surface: these live here rather than on
  // catalog::Catalog because the database they act on is this catalog.
  // The owner is `ax.role`: a dictionary has no ACL of its own, so its
  // permissions are the creator and nothing else.
  bool CreateTokenizer(const AccessContext& ax, ObjectId database_id,
                       std::string_view schema,
                       std::shared_ptr<CreateTokenizerInfo> tokenizer,
                       bool if_not_exists);

  // Foreign servers are database children, like PG (no schema). Returns false
  // for the if_not_exists no-op. The live ATTACH happens afterwards in the
  // command layer, compensated by a drop on failure -- so a denied or invalid
  // CREATE never touches the network.
  bool CreateForeignServer(const AccessContext& ax, ObjectId database_id,
                           std::shared_ptr<CreateForeignServerInfo> info,
                           Permissions perm, bool if_not_exists);

  // The caller resolves the table, so a name that turns out to hold something
  // else is its error to phrase.
  void ChangeColumnType(
    const AccessContext& ax, const duckdb::CreateTableInfo& table,
    std::string_view column, duckdb::LogicalType new_type,
    duckdb::unique_ptr<duckdb::ParsedExpression> using_expr);

  bool DropSchema(const AccessContext& ax, std::string_view database,
                  std::string_view name, bool cascade, bool missing_ok);

 private:
  // The shape the rows of each table are in while this database's file is being
  // read back: its own, not the catalog's. Seeded from what the file says and
  // walked forward by every record replayed against it, which is the only way a
  // rename in the middle of that run is accounted for. Empty once loaded.
  containers::NodeHashMap<uint64_t, duckdb::ColumnList> _replay_shapes;

  ObjectId _database_id;
  ObjectId _public_schema_id;
  catalog::Permissions _public_schema_owner;
  mutable absl::Mutex _indexed_columns_mutex;
  mutable containers::FlatHashMap<
    uint64_t, containers::FlatHashMap<uint64_t, std::vector<size_t>>>
    _indexed_columns ABSL_GUARDED_BY(_indexed_columns_mutex);

  duckdb::CatalogSet _foreign_servers;
};

}  // namespace sdb::catalog
