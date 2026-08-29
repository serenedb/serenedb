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
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
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
#include "catalog/entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/fwd.h"
#include "catalog/log/duckdb_global_catalog.h"

namespace duckdb {

class Catalog;
class ClientContext;

class CatalogEntry;

}  // namespace duckdb
namespace sdb::catalog {

class Role;
class CreateDatabaseInfo;
class CreateTokenizerInfo;
struct AccessContext;
class Index;

}  // namespace sdb::catalog
namespace sdb::catalog {

class SereneDBCatalog;
class SereneDBDatabaseEntry;
class SereneDBIndexEntry;
class SereneDBRoleEntry;
class SereneDBSchemaEntry;
class SereneDBForeignServerEntry;
class SereneDBSequenceEntry;
class SereneDBTableEntry;

// The one write path for every kind whose duckdb entry IS the object: the
// catalog-log record is appended in the schema's AddEntryInternal from the
// entry about to land, so the record cannot drift from the set. `old_name` is
// the name the superseded version is filed under (empty for a create); a
// replacement is an alter of that version, not a drop and a create.
duckdb::optional_ptr<duckdb::CatalogEntry> PutEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  duckdb::unique_ptr<duckdb::CreateInfo> info, catalog::Permissions perm = {});

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

// COMMENT ON an index, whose comment rides its own definition rather than the
// entry field duckdb's SET_COMMENT alter writes -- so the version is rebuilt.
// Every other kind goes through ApplyEntryAlter.
void SetEntryComment(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     std::string_view comment);

// The fork's one spelling of a DDL write-write conflict, thrown where duckdb's
// TransactionException would otherwise surface raw.
[[noreturn]] void ThrowConcurrentDdl();

// One entry-level ALTER (a comment, a rename), done by duckdb's own alter: the
// entry builds its next version, the set places it, and the catalog record is
// the commit walk's. The caller has resolved the target's kind and checked the
// new name against the namespaces the kind shares; owner and existence are
// re-checked here, under the mutation scope.
void ApplyEntryAlter(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     duckdb::AlterInfo& info);

// The resolve every DROP road starts with: the entry of `type` under `name`,
// owner-checked -- or the road's own miss. A wrong-kind hit in the relation
// namespace throws PG's mismatch (IF EXISTS does not cover it, exactly as in
// postgres); an absent name throws or returns null by `missing_ok`.
duckdb::optional_ptr<duckdb::CatalogEntry> RequireDropTarget(
  const catalog::AccessContext& ax, duckdb::CatalogType type,
  ObjectId parent_id, std::string_view name, bool missing_ok);

// DROP of one entry-backed kind. Returns false for the IF EXISTS no-op; every
// other refusal is thrown.
bool DropEntryObject(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId database_id, ObjectId parent_id,
                     std::string_view name, bool cascade, bool missing_ok);

// Who may ALTER or DROP an index. An index has no owner of its own -- postgres
// derives relowner from the relation it covers -- so authority over it is the
// relation's, and the error names the index, which is what the statement said.
void RequireIndexOwner(const catalog::AccessContext& ax,
                       const catalog::CreateIndexInfo& index);

// Entry lookup for every kind whose object is its entry, in the set the
// schema's own kind-to-set map answers with -- one set for both macro kinds, as
// postgres has one function namespace. A lookup that finds an entry of another
// kind under the same name returns null rather than the wrong type: the cast is
// what decides.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, duckdb::CatalogType type,
  std::string_view name);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id);

// Whatever holds the name in the relation namespace -- a table, a view, a
// sequence or an index -- or null when the name is free.
const duckdb::CatalogEntry* FindRelation(duckdb::ClientContext* context,
                                         ObjectId schema_id,
                                         std::string_view name);

// The same resolution with the kind the object answers as, so a statement that
// can name any half of the relation namespace asks once instead of probing
// kind by kind. `kind` is INVALID when the name is free, and an index answers
// with its own entry rather than the wrapper that holds its name among the
// relations.
struct RelationTarget {
  duckdb::CatalogEntry* entry;
  duckdb::CatalogType kind;
};
RelationTarget FindRelationTarget(duckdb::ClientContext* context,
                                  ObjectId schema_id, std::string_view name);
// The entry `id` names in `catalog`, or null when this database holds no such
// object: duckdb's by-id map, read through `transaction`.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::CatalogTransaction transaction, SereneDBCatalog& catalog,
  ObjectId id);
// The same for a caller that knows which database to ask, and for one whose
// object is looked up in the database the session is connected to.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId id);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId id);
// The nullable-context forms: a null context reads what is committed, as boot
// and the background roads do.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId id);
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryIn(
  duckdb::ClientContext* context, ObjectId database, ObjectId id);

// The entry itself, for a kind whose object needs nothing beside it: the id,
// the owner and the ACL are all on the entry.
template<typename Entry>
const Entry* EntryOf(duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  return dynamic_cast<const Entry*>(entry.get());
}

// Every entry class names the set family it is looked up under -- duckdb's own
// as `Type`, and the two kinds duckdb has no class for the same way. The slot
// a kind shares with another is resolved where the set is taken, so a view
// asks for VIEW_ENTRY and lands in the tables set.
template<typename Entry>
const Entry* Find(duckdb::ClientContext* context, ObjectId schema_id,
                  std::string_view name) {
  return EntryOf<Entry>(LookupInSchema(context, schema_id, Entry::Type, name));
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
  return EntryOf<Entry>(LookupEntryById(context, id));
}

template<typename Entry>
const Entry* FindIn(duckdb::ClientContext* context, duckdb::Catalog& catalog,
                    ObjectId id) {
  return EntryOf<Entry>(LookupEntryIn(context, catalog, id));
}

template<typename Entry>
const Entry* FindIn(duckdb::ClientContext* context, ObjectId database,
                    ObjectId id) {
  return EntryOf<Entry>(LookupEntryIn(context, database, id));
}

// The same, for a caller that has already established the database exists.
SereneDBCatalog& DatabaseCatalog(duckdb::ClientContext* context,
                                 ObjectId database);

void ScanDatabase(duckdb::ClientContext* context, ObjectId database,
                  duckdb::CatalogType type,
                  absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor);

template<typename Entry>
void Visit(duckdb::ClientContext* context, ObjectId database,
           absl::FunctionRef<void(const Entry&)> visitor) {
  ScanDatabase(context, database, Entry::Type,
               [&](duckdb::CatalogEntry& entry) {
                 if (const auto* found = EntryOf<Entry>(&entry)) {
                   visitor(*found);
                 }
               });
}

// Every entry in one duckdb catalog set of `database`, per schema, through
// SereneDBSchemaEntry::Scan -- the same seam the binder takes -- read through
// the caller's own transaction. Every entry in the set is offered, including
// the ones with no definition of their own.
void VisitCatalogSetEntries(
  duckdb::ClientContext& context, ObjectId database, duckdb::CatalogType set,
  absl::FunctionRef<void(const SereneDBSchemaEntry&, duckdb::CatalogEntry&)>
    visitor);

// The base tables of one database, as the entries in front of their rows.
// pg_class, pg_attribute, pg_attrdef, pg_constraint and pg_index all want
// exactly this subset of TABLE_ENTRY.
void VisitTableEntries(
  duckdb::ClientContext& context, ObjectId database,
  absl::FunctionRef<void(const SereneDBSchemaEntry&, const SereneDBTableEntry&)>
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

void VisitRelationIndexEntries(
  duckdb::ClientContext* context, SereneDBSchemaEntry& schema,
  ObjectId relation_id, absl::FunctionRef<void(SereneDBIndexEntry&)> visitor);

// The object an entry hangs off, as the log files it: the instance for a role
// or a database, the database for a schema or a foreign server, and the schema
// for everything else.
ObjectId RecordParentOf(const duckdb::CatalogEntry& entry);

// The role entries a checkpoint reaches through their set rather than through
// a schema: what it writes is the entry itself -- the definition and the
// permissions beside it are both on it.
void VisitRoleEntries(duckdb::ClientContext* context,
                      absl::FunctionRef<void(SereneDBRoleEntry&)> visitor);

// Roles live in the cluster-global attachment rather than in a database; the
// mutators write its ROLE_ENTRY set directly and queue the matching
// catalog-log record beside it. A null `context` reads what is committed.
void VisitRoles(duckdb::ClientContext* context,
                absl::FunctionRef<void(const catalog::Role&)> visitor);
const catalog::Role* FindRole(duckdb::ClientContext* context,
                              std::string_view name);
const catalog::Role* FindRole(duckdb::ClientContext* context, ObjectId id);

// Whether this transaction has written a role it has not committed. Its own
// version is the one it must read, so it neither uses nor fills the shared
// closure cache while this holds.
bool HasUncommittedRoles(duckdb::ClientContext& context);

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
             std::shared_ptr<const catalog::Role> role);
void DropRoleEntry(duckdb::ClientContext* context, std::string_view name);

// A database's duckdb entry IS the object; the set is cluster-global like the
// roles'. A null `context` reads what is committed.
void VisitDatabases(duckdb::ClientContext* context,
                    absl::FunctionRef<void(SereneDBDatabaseEntry&)> visitor);
const SereneDBDatabaseEntry* FindDatabase(duckdb::ClientContext* context,
                                          std::string_view name);
const SereneDBDatabaseEntry* FindDatabase(duckdb::ClientContext* context,
                                          ObjectId id);
// The id alone, for the callers that resolve a database by name and have no use
// for the entry. Unset when no database carries the name -- which is how a
// statement naming one that does not exist is refused rather than crashed.
ObjectId FindDatabaseId(duckdb::ClientContext* context, std::string_view name);
// The name alone, for the many callers that only need it to reach an
// attachment. Empty when no database carries the id.
std::string DatabaseName(duckdb::ClientContext* context, ObjectId id);

// Puts `database` in the set. `old_name` is the name the entry currently holds
// -- empty for a create, different for a rename.
void PutDatabase(duckdb::ClientContext* context, std::string_view old_name,
                 duckdb::unique_ptr<catalog::CreateDatabaseInfo> database,
                 catalog::Permissions perm);
void DropDatabaseEntry(duckdb::ClientContext* context, std::string_view name);

// The schemas of one database, from the catalog's schema set. pg_catalog and
// information_schema are not in it: they are generated content, not schemas
// anyone created. A rename, an owner transfer or a GRANT chains a new entry
// version, and the schema's contents stay behind on the sets the versions
// share. A null `context` reads what is committed.
void VisitSchemas(duckdb::ClientContext* context, ObjectId database,
                  absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor);
SereneDBSchemaEntry* FindSchema(duckdb::ClientContext* context,
                                ObjectId database, std::string_view name);
// By stable id. A schema id is unique cluster-wide, so this needs no database.
SereneDBSchemaEntry* FindSchema(duckdb::ClientContext* context, ObjectId id);
// The id alone, for the resolution every schema-qualified lookup starts with.
ObjectId FindSchemaId(duckdb::ClientContext* context, ObjectId database,
                      std::string_view name);
// The database a schema belongs to, unset when no schema carries the id.
ObjectId SchemaDatabaseId(duckdb::ClientContext* context, ObjectId schema_id);

// Puts `schema` in the set of its database's catalog. `old_name` is the name
// the entry currently holds -- empty for a create, different for a rename.
void PutSchema(duckdb::ClientContext* context, std::string_view old_name,
               duckdb::unique_ptr<duckdb::CreateSchemaInfo> schema,
               catalog::Permissions perm);
void DropSchemaEntry(duckdb::ClientContext* context, ObjectId database,
                     std::string_view name, bool cascade);

catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, std::string_view name);

// By stable id, out of one database's own catalog rather than the session's:
// the inverted-index feed resolves its dictionaries while its database may
// still be attaching. `context` is the committing transaction when there is
// one, so a dictionary that transaction created itself is seen.
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

// The SQL functions of one database, from the two macro sets of each of its
// schemas -- a kind whose duckdb entry IS the object, with no cache in front
// of it.
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

// Every inverted index in one database. Off the entries, not their
// definitions: the iresearch storage an inverted index owns is the entry's,
// shared by every version of the definition.
std::vector<const SereneDBIndexEntry*> DatabaseInvertedIndexes(
  duckdb::ClientContext* context, ObjectId database);

// The indexes built on one relation, by the relation's id. The walk is bounded
// by the schema the relation lives in: an index shares it.
std::vector<std::shared_ptr<const catalog::Index>> RelationInvertedIndexes(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id);

// Every index over the relation, as the catalog records them: an inverted one
// carries its object, a plain ART is duckdb's own fields.
std::vector<duckdb::unique_ptr<catalog::CreateIndexInfo>> RelationIndexRecords(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id);

// The same out of one database's own catalog, for the attach paths.
std::vector<std::shared_ptr<const catalog::Index>> RelationInvertedIndexesIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  ObjectId relation_id);

void DropIndexEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name);

// A foreign server is a database child as it is in postgres, so its set is the
// catalog's own and no schema is in the path.
// The names of a catalog's foreign servers, for the one caller that needs to
// reach their attachments after the catalog is gone.
std::vector<std::string> CatalogForeignServerNames(SereneDBCatalog& catalog);

void VisitForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const SereneDBForeignServerEntry&)> visitor);

// The entry itself: owner, ACL and every fact a foreign server states are the
// entry's, and the version a reader resolved is the one it keeps reading.
const SereneDBForeignServerEntry* FindForeignServer(
  duckdb::ClientContext* context, ObjectId database, std::string_view name);

// By name alone, across every attached serenedb catalog's set. The attach alias
// a foreign server holds is instance-global while the object is a database
// child, so a name is unique cluster-wide and the query-path USAGE check -- run
// from whichever database the session is in -- has to reach the one that holds
// it.
const SereneDBForeignServerEntry* FindForeignServerAnywhere(
  duckdb::ClientContext* context, std::string_view name);

// Takes the entry `parent_id`.`name` out of the set its kind lives in -- the
// one place that knows which set that is, for the callers that hold a kind
// rather than a definition: boot replay, and the drops. `cascade` reaches
// duckdb's dependency walk, which takes each victim through DropDependent.
void DropEntryOfKind(duckdb::ClientContext* context, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name, bool cascade);

// Puts one record back where it belongs, as boot reads the catalog log: a
// version whose id a set already holds supersedes whatever name it is filed by
// now -- a rename earlier in the log has already moved it. Nothing is
// recorded: these are the records being read.
void ReplayCatalogRecord(duckdb::unique_ptr<duckdb::CreateInfo> info,
                         catalog::Permissions perm, bool dropped);

}  // namespace sdb::catalog
