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

#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "auth/acl.h"
#include "catalog/deferred_writes.h"
#include "catalog/duckdb_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/entry.h"
#include "catalog/fwd.h"
#include "catalog/store/wal_entry.h"

namespace duckdb {

class Catalog;
class ClientContext;

class CatalogEntry;

}  // namespace duckdb
namespace sdb::catalog {

class CreateRoleInfo;
class CreateDatabaseInfo;
class CreateTokenizerInfo;
struct AccessContext;
class CreateIndexInfoBase;

}  // namespace sdb::catalog
namespace sdb::catalog {

class SereneDBCatalog;
class SereneDBIndexEntry;
class SereneDBSchemaEntry;
class SereneDBSequenceEntry;
class SereneDBTableEntry;
class SereneDBTypeEntry;

// The one write path for every kind whose duckdb entry IS the object. The
// entry reaches every slot its kind occupies through the schema's inherited
// AddEntryInternal -- which is where the catalog-log record of this version is
// appended, from the entry that is about to land, so a mutation states its
// change exactly once and the record cannot drift from what is in the set.
// Then the object index learns where the object now is.
//
// `old_name` is the name the version being superseded is filed under: empty
// for a create, the current name for a rewrite, the previous one for a rename.
// A replacement is therefore an alter of the version it supersedes rather than
// a drop and a create, and it is what says the record's mode.
//
// Nothing is recorded inside a Catalog::RecordedScope: the caller has already
// written this version's record into a frame of its own.
duckdb::optional_ptr<duckdb::CatalogEntry> PutEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  std::shared_ptr<const duckdb::CreateInfo> info,
  catalog::Permissions perm = {});

// GRANT / REVOKE and ALTER ... OWNER TO on one entry-backed kind. Neither
// changes the definition, so both are the same write: the object the entry
// already holds, put back under new permissions. `parent_id` is the schema for
// a schema child and the database for a foreign server.
void ChangeEntryAcl(const catalog::AccessContext& ax, duckdb::CatalogType type,
                    ObjectId parent_id, std::string_view name,
                    auth::AclMutator mutate);
void ChangeEntryOwner(const catalog::AccessContext& ax,
                      duckdb::CatalogType type, ObjectId parent_id,
                      std::string_view name, ObjectId new_owner,
                      std::string_view new_owner_name);

// COMMENT ON. Every kind but a table keeps its comment in its own definition,
// so this is a new version of the object; a no-op when it already says that.
void SetEntryComment(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     std::string_view comment);
// COMMENT ON COLUMN of a view, whose columns ride its own definition.
void SetViewColumnComment(const catalog::AccessContext& ax, ObjectId schema_id,
                          std::string_view name, std::string_view column,
                          std::string_view comment);

// ALTER ... RENAME TO. The entry moves under duckdb's rename path, so the
// version chain -- and the rows hanging off it -- carry on; the caller has
// resolved the target and checked the new name against the namespaces its kind
// shares.
void RenameEntry(duckdb::ClientContext* context, duckdb::CatalogType type,
                 ObjectId parent_id, std::string_view name,
                 std::string_view new_name);

// DROP of one entry-backed kind. Returns false for the IF EXISTS no-op; every
// other refusal is thrown. The cross-tree fixups the drop implies are planned
// first, against the pre-mutation catalog, and the removal is recorded beside
// them.
bool DropEntryObject(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId database_id, ObjectId parent_id,
                     std::string_view name, bool cascade, bool missing_ok);

// Who may ALTER or DROP an index. An index has no owner of its own -- postgres
// derives relowner from the relation it covers -- so authority over it is the
// relation's, and the error names the index, which is what the statement said.
void RequireIndexOwner(const catalog::AccessContext& ax,
                       const catalog::CreateIndexInfoBase& index);

// Every entry in one duckdb catalog set of `database`, per schema. The
// enumeration goes through SereneDBSchemaEntry::Scan, the same seam the binder
// takes, so pg_catalog and a query resolving a name answer from the one place:
// the schema's CatalogSet, read through the caller's own transaction.
//
// The readers here want facts that are the entry's own: its ColumnList, its
// constraints, and the owner and ACL the entry carries. Every entry in the

// Entry lookup, once, for every kind whose object is its entry. The duckdb
// CatalogType an entry class lives under is a property of the class, so the
// class is the only thing that varies -- there is nothing per-kind left to
// write. `LookupSlots` is the one place a kind that spans two duckdb sets (a
// macro) is spelled out.
//
// A lookup that finds an entry of another kind under the same name returns
// null rather than the wrong type: the cast is what decides, exactly as the
// per-kind functions did.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, duckdb::CatalogType type,
  std::string_view name);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSession(
  duckdb::ClientContext& context, ObjectId id);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInCatalog(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId id);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInDatabase(
  duckdb::ClientContext* context, ObjectId database, ObjectId id);

// The entry itself, for a kind whose object needs nothing beside it: the id,
// the owner and the ACL are all on the entry.
template<typename Entry>
const Entry* EntryOf(duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  return dynamic_cast<const Entry*>(entry.get());
}

template<typename Entry>
const Entry* Find(duckdb::ClientContext* context, ObjectId schema_id,
                  std::string_view name) {
  return EntryOf<Entry>(
    LookupInSchema(context, schema_id, Entry::kCatalogType, name));
}

template<typename Entry>
const Entry* Find(duckdb::ClientContext* context, ObjectId schema_id,
                  ObjectId id) {
  return EntryOf<Entry>(LookupInSchema(context, schema_id, id));
}

// The entry `id` names anywhere the statement can see, for the readers that
// hold an id and no parent.
template<typename Entry>
const Entry* FindSession(duckdb::ClientContext& context, ObjectId id) {
  return EntryOf<Entry>(LookupInSession(context, id));
}

template<typename Entry>
const Entry* FindIn(duckdb::ClientContext* context, duckdb::Catalog& catalog,
                    ObjectId id) {
  return EntryOf<Entry>(LookupInCatalog(context, catalog, id));
}

template<typename Entry>
const Entry* FindIn(duckdb::ClientContext* context, ObjectId database,
                    ObjectId id) {
  return EntryOf<Entry>(LookupInDatabase(context, database, id));
}

// Every entry of one kind in `database`, in every schema of it. The scan walks
// whichever duckdb sets the kind lives in; entries of another kind sharing
// those sets are skipped by the same cast Find uses.
// The catalog of one database -- the home of everything that is not
// cluster-wide. Null when the database is not attached here.
duckdb::optional_ptr<SereneDBCatalog> TryDatabaseCatalog(
  duckdb::ClientContext* context, ObjectId database);

// The same, for a caller that has already established the database exists.
SereneDBCatalog& DatabaseCatalog(duckdb::ClientContext* context,
                                 ObjectId database);

void ScanDatabase(duckdb::ClientContext* context, ObjectId database,
                  duckdb::CatalogType type,
                  absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor);

template<typename Entry>
void Visit(duckdb::ClientContext* context, ObjectId database,
           absl::FunctionRef<void(const Entry&)> visitor) {
  ScanDatabase(context, database, Entry::kCatalogType,
               [&](duckdb::CatalogEntry& entry) {
                 if (const auto* found = EntryOf<Entry>(&entry)) {
                   visitor(*found);
                 }
               });
}

template<typename Entry>
using DefinitionRefOf =
  std::decay_t<decltype(std::declval<const Entry&>().Definition())>;

// The same walk, handing over what a caller outside the catalog wants: the
// definition the entry publishes and the permissions beside it.
template<typename Entry>
void VisitDefinitions(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const DefinitionRefOf<Entry>&, const Permissions&)>
    visitor) {
  Visit<Entry>(context, database, [&](const Entry& entry) {
    visitor(entry.Definition(), entry.permissions);
  });
}

// set is offered, including the ones with no definition of their own.
void VisitCatalogSetEntries(
  duckdb::ClientContext& context, ObjectId database, duckdb::CatalogType set,
  absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                         duckdb::CatalogEntry&)>
    visitor);

// The base tables of one database, as the entries in front of their rows.
// pg_class, pg_attribute, pg_attrdef, pg_constraint and pg_index all want
// exactly this subset of TABLE_ENTRY.
void VisitTableEntries(duckdb::ClientContext& context, ObjectId database,
                       absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                                              const SereneDBTableEntry&)>
                         visitor);

// The relation `schema`.`name` names in `database`, as the entry in front of
// it -- a base table, a view, or the index-as-table wrapper, which all share
// postgres' relation namespace and therefore one duckdb set. Read through the
// caller's transaction, so a relation the statement itself created is visible;
// a null `context`, or one with no open transaction, reads what is committed.
duckdb::optional_ptr<duckdb::CatalogEntry> FindRelationEntry(
  duckdb::ClientContext* context, ObjectId database, std::string_view schema,
  std::string_view name);

// The same narrowed to a base table, for the readers whose facts are a table's
// -- its columns, its constraints. Null for a view or an index wrapper.
SereneDBTableEntry* FindTableEntry(duckdb::ClientContext* context,
                                   ObjectId database, std::string_view schema,
                                   std::string_view name);

// The base table `id` names in the session's database, by oid alone -- what a
// reader holding nothing but a pg_class.oid has. Null when the id names no
// object there, or one that is not a base table.
SereneDBTableEntry* FindSessionTableEntry(duckdb::ClientContext& context,
                                          ObjectId id);

// The same in a named database rather than the session's, for the callers whose
// database is not the one they are connected to -- boot, which is connected to
// none at all, and the attach paths. A null `context` reads what is committed.
//
// Null for a view relation as well as for a missing one: telling the two apart
// is what says whether an index over the relation has rows that still move.
SereneDBTableEntry* FindTableEntryIn(duckdb::ClientContext* context,
                                     ObjectId database, ObjectId id);

// The indexes built on one relation, from the INDEX_ENTRY set of the schema the
// relation lives in. An index shares its relation's schema, so the walk is
// bounded by that schema rather than by the database.
//
// A null `context` reads what is committed: boot and the background paths.
void VisitRelationIndexEntries(
  duckdb::ClientContext* context, SereneDBSchemaEntry& schema,
  ObjectId relation_id, absl::FunctionRef<void(SereneDBIndexEntry&)> visitor);

// Roles live in the cluster-global attachment rather than in a database. The
// mutators write its ROLE_ENTRY set directly, through the statement's own
// transaction, and queue the matching catalog-log record beside it.
//
// A null `context` reads what is committed -- boot, compaction and the
// background paths, which have no transaction of their own.
void VisitRoles(
  duckdb::ClientContext* context,
  absl::FunctionRef<void(const catalog::CreateRoleInfo&)> visitor);
std::shared_ptr<const catalog::CreateRoleInfo> FindRole(
  duckdb::ClientContext* context, std::string_view name);
std::shared_ptr<const catalog::CreateRoleInfo> FindRole(
  duckdb::ClientContext* context, ObjectId id);

// Whether this transaction has written a role it has not committed. Its own
// version is the one it must read, so it neither uses nor fills the shared
// closure cache while this holds.
bool HasUncommittedRoles(duckdb::ClientContext& context);

// Fixes this transaction's view of the cluster-global sets, and keeps what that
// view still needs from being reclaimed. Called at the transaction's first
// catalog write, where the overlay pins everything else -- the two halves of
// one read view. A transaction that writes no catalog never calls it and never
// pays a transaction on that attachment.
void PinClusterGlobalReadView(duckdb::ClientContext& context);

// Refuses the statement with 40001 when the version of `name` it just resolved
// is a committed one that a committed read no longer finds: another transaction
// dropped the role and committed while this one was open, and writing the new
// version would resurrect it. Returns normally otherwise -- including for a
// role this transaction created itself, which is uncommitted by construction.
void RequireRoleNotVanished(duckdb::ClientContext* context,
                            std::string_view name);

// Puts `role` in the set. `old_name` is the name the entry currently holds --
// empty for a create, different for a rename. Throws 40001 when another
// transaction is holding, or has committed, a version of the same role.
void PutRole(duckdb::ClientContext* context, std::string_view old_name,
             std::shared_ptr<const catalog::CreateRoleInfo> role);
void DropRoleEntry(duckdb::ClientContext* context, std::string_view name);

// A database as the DATABASE_ENTRY set holds it: the definition plus the owner
// and ACL the entry carries beside it. Null `info` means "no such database".
struct DatabaseRef {
  std::shared_ptr<const catalog::CreateDatabaseInfo> info;
  catalog::Permissions perm;

  explicit operator bool() const noexcept { return info != nullptr; }
  ObjectId Id() const noexcept;
  std::string_view Name() const noexcept;
};

// Databases are the second kind whose duckdb entry IS the object. The set is
// cluster-global like the roles', and the by-id and by-name reads go through a
// process-wide cache that is rebuilt only when database DDL bumps its
// generation -- a DDL-free workload pays one relaxed load and a hash lookup.
//
// A null `context` reads what is committed; a transaction that has written a
// database bypasses the cache and reads its own version out of the set.
void VisitDatabases(duckdb::ClientContext* context,
                    absl::FunctionRef<void(const DatabaseRef&)> visitor);
DatabaseRef FindDatabase(duckdb::ClientContext* context, std::string_view name);
DatabaseRef FindDatabase(duckdb::ClientContext* context, ObjectId id);
// The name alone, for the many callers that only need it to reach an
// attachment. Empty when no database carries the id.
std::string DatabaseName(duckdb::ClientContext* context, ObjectId id);

// Puts `database` in the set. `old_name` is the name the entry currently holds
// -- empty for a create, different for a rename.
void PutDatabase(duckdb::ClientContext* context, std::string_view old_name,
                 std::shared_ptr<const catalog::CreateDatabaseInfo> database,
                 catalog::Permissions perm);
void DropDatabaseEntry(duckdb::ClientContext* context, std::string_view name);

// Invalidates the cluster-wide database cache. Called by the mutators, and
// again when a transaction that wrote a database ends: what is committed is
// what the cache holds, and that only changes at the commit, not at the write.
void BumpDatabaseGeneration() noexcept;

// The schemas of one database, from the catalog's schema set. pg_catalog and
// information_schema are not in it: they are generated content, not schemas
// anyone created, and their rows are synthesized by the projection.
//
// Schemas are the third kind whose duckdb entry IS the object. The entry is
// never versioned -- it owns the CatalogSets of everything under it, so a
// second version would strand its whole contents -- but it is created and
// dropped through the set like any other, so a transaction still sees its own
// uncommitted CREATE/DROP SCHEMA and two concurrent ones still refuse each
// other. Owner and ACL are side state on the entry (SetDefinition).
//
// The by-name and by-id reads go through a process-wide cache rebuilt only
// when schema DDL bumps its generation, exactly as the database list does: a
// DDL-free workload pays one relaxed load and a hash lookup. A null `context`
// reads what is committed; a transaction that has written a schema bypasses
// the cache and reads its own version out of the set.
void VisitSchemas(duckdb::ClientContext* context, ObjectId database,
                  absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                                         const catalog::Permissions&)>
                    visitor);
catalog::SchemaRef FindSchema(duckdb::ClientContext* context, ObjectId database,
                              std::string_view name,
                              catalog::Permissions* perm = nullptr);
// By stable id. A schema id is unique cluster-wide, so this needs no database.
catalog::SchemaRef FindSchema(duckdb::ClientContext* context, ObjectId id,
                              catalog::Permissions* perm = nullptr);
// The id alone, for the resolution every schema-qualified lookup starts with.
ObjectId FindSchemaId(duckdb::ClientContext* context, ObjectId database,
                      std::string_view name);
// The database a schema belongs to, unset when no schema carries the id.
ObjectId SchemaDatabaseId(duckdb::ClientContext* context, ObjectId schema_id);

// Puts `schema` in the set of its database's catalog. `old_name` is the name
// the entry currently holds -- empty for a create, different for a rename.
// A schema entry is created and dropped, never replaced: a write that finds
// the entry already there sets its definition in place.
void PutSchema(duckdb::ClientContext* context, std::string_view old_name,
               std::shared_ptr<const duckdb::CreateSchemaInfo> schema,
               catalog::Permissions perm);
void DropSchemaEntry(duckdb::ClientContext* context, ObjectId database,
                     std::string_view name);

// The schemas of one database as the shared infos their entries hold, for the
// callers that must outlive the walk.
std::vector<catalog::HeldSchema> DatabaseSchemas(duckdb::ClientContext* context,
                                                 ObjectId database);

// Invalidates the cluster-wide schema cache. Called by the mutators, and again
// when a transaction that wrote a schema ends: what is committed is what the
// cache holds, and that only changes at the commit, not at the write.
void BumpSchemaGeneration() noexcept;

catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, std::string_view name,
                                    catalog::Permissions* perm = nullptr);
catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, ObjectId id,
                                    catalog::Permissions* perm = nullptr);

// By stable id, out of one database's own catalog rather than the session's.
// The inverted-index feed resolves its dictionaries while its database may
// still be attaching, where nothing can look that database up by id.
// `context` is the committing transaction when there is one, so a dictionary
// that transaction created itself is seen -- and a reader keeps seeing the
// dictionary its own index version names after another session has dropped
// both, which is what the entry's version chain answers.
catalog::TokenizerRef FindTokenizerIn(duckdb::ClientContext* context,
                                      duckdb::Catalog& catalog, ObjectId id);

// The same, for the database this session is connected to: the one that holds
// the index naming the dictionary. The Visit form is what the tokenize paths
// take -- they resolve a whole index's dictionaries at once, so it is one walk
// rather than one per id.
void VisitSessionTokenizers(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(catalog::TokenizerRef)> visitor);
catalog::TokenizerRef FindSessionTokenizer(duckdb::ClientContext& context,
                                           ObjectId id);

// The dictionaries of one database as the shared infos their entries hold.
std::vector<catalog::HeldTokenizer> DatabaseTokenizers(
  duckdb::ClientContext* context, ObjectId database);

// The stable id of everything one schema entry holds, straight off its sets.
// A DROP SCHEMA takes those sets in one step, so their entries reach no
// DropEntry of their own and nothing else retires what they referenced -- and
// the edges are the catalog's, which outlives the schema entry.
std::vector<ObjectId> SchemaEntryContentIds(SereneDBSchemaEntry& schema);
void RetireEntryEdges(duckdb::ClientContext* context, duckdb::Catalog& owner,
                      std::span<const ObjectId> ids);

// The same, collected, for the callers that walk them more than once.
std::vector<const duckdb::TypeCatalogEntry*> DatabaseTypes(
  duckdb::ClientContext* context, ObjectId database);

// The SQL functions of one database, from the two macro sets of each of its
// schemas. Same shape as the types above, and for the same reasons: the sixth
// kind whose duckdb entry IS the object, with no cache in front of it.
void VisitFunctions(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const duckdb::MacroCatalogEntry&)> visitor);
const duckdb::MacroCatalogEntry* FindFunction(duckdb::ClientContext* context,
                                              ObjectId schema_id,
                                              std::string_view name);
const duckdb::MacroCatalogEntry* FindFunction(duckdb::ClientContext* context,
                                              ObjectId schema_id, ObjectId id);
const duckdb::MacroCatalogEntry* FindSessionFunction(
  duckdb::ClientContext& context, ObjectId id);

// The same, collected, for the callers that walk them more than once.
std::vector<const SereneDBSequenceEntry*> DatabaseSequences(
  duckdb::ClientContext* context, ObjectId database);

// The indexes built on one relation, by the relation's id. The walk is bounded
// by the schema the relation lives in: an index shares it.
std::vector<catalog::IndexInfoRef> RelationIndexes(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id);
// The same out of one database's own catalog, for the attach paths.
std::vector<catalog::IndexInfoRef> RelationIndexesIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  ObjectId relation_id);

void DropIndexEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name);

// Rebuilds the entries of every index on `relation_id`. The relation-namespace
// wrapper projects the relation's columns and its grants, and an index is no
// longer written alongside the relation, so every table and view mutation has
// to put the wrappers back in step with what it wrote.
void RefreshRelationIndexEntries(duckdb::ClientContext* context,
                                 ObjectId schema_id, ObjectId relation_id);

// Rebuilds every table of `database` that a foreign key points at, so it
// carries the referenced half of that key. Run once all of them are in the set:
// the half is derived from the referencing table's edges, which a parent placed
// ahead of its child cannot see.
void RefreshForeignKeyReferents(duckdb::ClientContext* context,
                                ObjectId database);

// The same for the tables one table points at. A referenced table carries the
// half of the foreign key that makes a DELETE against it look for children, and
// that half is derived from the referencing table's edges rather than stored --
// so both ends of a table's life, the write and the drop, have to re-derive
// it. duckdb stores the half instead and unstores it in DuckSchemaEntry::
// DropEntry; with nothing stored there is nothing for that pass to undo.
void RefreshForeignKeyTargets(duckdb::ClientContext* context,
                              const duckdb::CreateTableInfo& table);

// Whether the version of `name` this statement resolved is a committed one
// that a committed read no longer finds: another transaction dropped the table
// and committed while this one was open. False for a version this transaction
// created itself, which is uncommitted by construction.
bool TableVanished(duckdb::ClientContext* context, ObjectId schema_id,
                   std::string_view name);

// Rebuilds the entry of the relation `relation_id` names, without touching the
// indexes over it. A relation entry advertises a virtual column per indexed
// column, so an index create or drop reshapes it; the reverse direction is
// RefreshRelationIndexEntries, and running both would recurse.
void RefreshRelationEntry(duckdb::ClientContext* context, ObjectId schema_id,
                          ObjectId relation_id);

// The foreign servers of one database -- the eighth kind whose duckdb entry IS
// the object, and the first database-scoped one: a foreign server is a database
// child as it is in postgres, so its set is the catalog's own and no schema is
// in the path.
void VisitForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const catalog::CreateForeignServerInfo&,
                         const catalog::Permissions&)>
    visitor);
// `perm`, when given, is filled with the owner and ACL the entry carries -- the
// one home of both. Left alone when nothing is found.
catalog::ForeignServerRef FindForeignServer(
  duckdb::ClientContext* context, ObjectId database, std::string_view name,
  catalog::Permissions* perm = nullptr);
catalog::ForeignServerRef FindForeignServer(
  duckdb::ClientContext* context, ObjectId database, ObjectId id,
  catalog::Permissions* perm = nullptr);

// By name alone, across every attached serenedb catalog's set. The attach alias
// a foreign server holds is instance-global while the object is a database
// child, so a name is unique cluster-wide and the query-path USAGE check -- run
// from whichever database the session is in -- has to reach the one that holds
// it.
catalog::ForeignServerRef FindForeignServerAnywhere(
  duckdb::ClientContext* context, std::string_view name,
  catalog::Permissions* perm = nullptr);

// The servers one catalog holds, straight off its set, for the callers that
// must capture them before the attachment -- and the set it owns -- goes away.
std::vector<catalog::HeldForeignServer> CatalogForeignServers(
  SereneDBCatalog& catalog);

// The servers of one database, as the shared definitions their entries hold --
// for the callers that must outlive the walk: the checkpoint writer, and boot's
// re-attach pass, which runs a whole statement per server.
std::vector<catalog::HeldForeignServer> DatabaseForeignServers(
  duckdb::ClientContext* context, ObjectId database);

// The name the object `id` answers to under `parent_id`, empty when nothing
// there holds it. `parent_id` is the schema for a schema child, the database
// for a schema or a foreign server, and unread for a role or a database.
std::string EntryNameOfKind(duckdb::ClientContext* context,
                            duckdb::CatalogType type, ObjectId parent_id,
                            ObjectId id);

// Takes the entry `parent_id`.`name` out of the set its kind lives in -- the
// one place that knows which set that is, for the callers that hold a kind
// rather than a definition: boot replay, and the drops.
void DropEntryOfKind(duckdb::ClientContext* context, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name);

// Takes a table out of its schema's set and re-derives the referenced half of
// every foreign key it held, which its edges no longer state.
void DropTableEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name);

// Puts one definition record back where it belongs, as boot reads the catalog
// log: the record's own kind picks the set, and a replace supersedes whatever
// name its id holds now -- a rename record has already moved it, and reading
// the new name back would leave the old entry behind under the old one.
//
// Nothing is recorded: these are the records being read.
void ReplayEntryRecord(const catalog::wal::PutEntry& entry);
void ReplayTableRecord(const catalog::wal::PutTable& table);

}  // namespace sdb::catalog
