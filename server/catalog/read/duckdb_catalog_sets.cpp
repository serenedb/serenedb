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

#include "catalog/read/duckdb_catalog_sets.h"

#include <array>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/string_utils.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/foreign_server.h"
#include "catalog/index.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_dependency.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_storage_extension.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::catalog {

// duckdb refuses the second writer of an entry another transaction is holding,
// which is the same answer serenedb gives for every other object; the two just
// have to say it the same way.
void ThrowConcurrentDdl() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                  ERR_MSG("could not serialize access due to concurrent DDL on "
                          "the same object"));
}

namespace {

// One referenced table's next version, and the permissions it keeps.
struct ReferencedKeyVersion {
  duckdb::unique_ptr<duckdb::CreateTableInfo> info;
  catalog::Permissions perm;
};

// The next version of every table whose referenced half of a foreign key this
// mutation changes: `before` is what the referencing table stated and `after`
// what it now states, either being null for a create or a drop. A referenced
// table carries the half that makes a DELETE against it look for children.
std::vector<ReferencedKeyVersion> ReferencedKeyVersions(
  duckdb::ClientContext* context, const duckdb::CreateTableInfo* before,
  const duckdb::CreateTableInfo* after);

// Takes a table out of its schema's set and re-derives the referenced half of
// every foreign key it held, which its edges no longer state.
void DropTableEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name, bool cascade);

// The catalog of `database`, or null when it is not (or no longer) a serenedb
// attachment. A null `context` is boot, compaction or a background path: they
// see what is committed and have no transaction of their own.
duckdb::optional_ptr<SereneDBCatalog> DatabaseCatalogOf(
  duckdb::ClientContext* context, ObjectId database) {
  auto attached = context != nullptr
                    ? catalog::TryStoreDatabase(*context, database)
                    : catalog::TryStoreDatabase(database);
  if (!attached) {
    return nullptr;
  }
  auto& duck_catalog = attached->GetCatalog();
  if (duck_catalog.GetCatalogType() != kSereneDBCatalogType) {
    return nullptr;
  }
  return &duck_catalog.Cast<SereneDBCatalog>();
}

[[noreturn]] void ThrowConcurrentDdlOn(std::string_view noun,
                                       std::string_view name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent DDL on ", noun, " \"",
            name, "\""));
}

// Puts one built entry in a set the catalog owns rather than a schema -- a
// role, a database, a foreign server. `old_name` is the name the version this
// supersedes is filed under; a rewrite is an alter rather than a drop followed
// by a create, whose tombstone DependencyManager::VerifyCommitDrop refuses at
// commit when any edge on the object committed after the transaction started.
void PutSetEntry(duckdb::CatalogSet& set,
                 const duckdb::CatalogTransaction& transaction,
                 std::string_view old_name,
                 duckdb::unique_ptr<duckdb::CatalogEntry> entry,
                 const duckdb::LogicalDependencyList& deps) {
  // Owning copies: a refused write destroys the entry it took, which is the
  // last reference to the definition a string_view name would point into.
  const duckdb::Identifier name = entry->name;
  const auto type = entry->type;
  const duckdb::Identifier from{old_name};
  bool written = false;
  try {
    written =
      old_name.empty()
        ? set.CreateEntry(transaction, name, std::move(entry), deps)
        : set.CreateOrReplaceEntry(transaction, from, std::move(entry), deps);
  } catch (const duckdb::CatalogException&) {
    // A rename whose target name was taken between the mutator's own
    // duplicate check and here. None of these entries is `internal`, so that
    // is the only CatalogException an alter of one can raise, and it is said
    // the way every other lost race is.
  }
  if (!written) {
    ThrowConcurrentDdlOn(pg::ToPgObjectTypeName(type),
                         name.GetIdentifierName());
  }
}

// One schema entry, the catalog holding it, and the transaction to read or
// write it through. Empty when the database is not attached yet or is gone, or
// when a DROP took the schema and its sets with it.
struct SchemaAt {
  duckdb::optional_ptr<SereneDBCatalog> catalog;
  duckdb::optional_ptr<SereneDBSchemaEntry> schema;
  std::optional<duckdb::CatalogTransaction> transaction;

  explicit operator bool() const noexcept { return schema != nullptr; }

  duckdb::CatalogSet& Set(duckdb::CatalogType slot) {
    return schema->GetCatalogSet(slot);
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> Lookup(duckdb::CatalogType slot,
                                                    std::string_view name) {
    return Set(slot).GetEntry(*transaction, duckdb::Identifier{name});
  }
};

SchemaAt OpenSchema(duckdb::ClientContext* context, ObjectId schema_id,
                    bool for_write) {
  SchemaAt at;
  auto* schema = FindSchema(context, schema_id);
  if (schema == nullptr) {
    return at;
  }
  at.catalog = DatabaseCatalogOf(context, schema->GetDatabaseId());
  if (!at.catalog) {
    return at;
  }
  at.transaction = context != nullptr
                     ? at.catalog->GetCatalogTransaction(*context)
                     : at.catalog->CommittedRead();
  at.schema = schema;
  if (at.schema && for_write && context != nullptr) {
    duckdb::MetaTransaction::Get(*context).ModifyDatabase(
      at.catalog->GetAttached(), duckdb::DatabaseModificationType::ALTER_TABLE);
  }
  return at;
}

// The same for the one kind that is a database child rather than a schema
// child, as in postgres: the set is the catalog's own, so no schema is in the
// path.
struct CatalogAt {
  duckdb::optional_ptr<SereneDBCatalog> catalog;
  std::optional<duckdb::CatalogTransaction> transaction;

  explicit operator bool() const noexcept { return catalog != nullptr; }
};

CatalogAt OpenCatalog(duckdb::ClientContext* context, ObjectId database_id,
                      bool for_write) {
  CatalogAt at;
  at.catalog = DatabaseCatalogOf(context, database_id);
  if (!at.catalog) {
    return at;
  }
  at.transaction = context != nullptr
                     ? at.catalog->GetCatalogTransaction(*context)
                     : at.catalog->CommittedRead();
  if (for_write && context != nullptr) {
    duckdb::MetaTransaction::Get(*context).ModifyDatabase(
      at.catalog->GetAttached(), duckdb::DatabaseModificationType::ALTER_TABLE);
  }
  return at;
}

// A function is in whichever of duckdb's two macro sets its own declaration
// puts it, so either entry class answers for one.
const duckdb::MacroCatalogEntry* FunctionOf(
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  if (const auto* scalar = EntryOf<duckdb::ScalarMacroCatalogEntry>(entry)) {
    return scalar;
  }
  return EntryOf<duckdb::TableMacroCatalogEntry>(entry);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupSchemaEntryById(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id) {
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  return at ? LookupEntryById(*at.transaction, *at.catalog, id) : nullptr;
}

// No schema entry can add a foreign server, which is why this is the one place
// besides a schema's AddEntryInternal that appends a record.
void PlaceForeignServer(duckdb::ClientContext* context,
                        std::string_view old_name,
                        const duckdb::CreateInfo& info,
                        const catalog::Permissions& perm) {
  auto at = OpenCatalog(context, catalog::ParentIdOf(info), /*for_write=*/true);
  if (!at) {
    return;
  }
  auto& set = at.catalog->GetForeignServerSet();
  auto entry = duckdb::make_uniq<SereneDBForeignServerEntry>(
    *at.catalog, basics::downCast<const catalog::CreateForeignServerInfo>(info),
    perm);
  const auto deps = EntryDependencies(info);
  PutSetEntry(set, *at.transaction, old_name, std::move(entry), deps);
}

namespace {

// One record's entry, built by the kind its info states. This is the log's
// record dispatch -- the counterpart of duckdb's replay switching on its own
// record types -- and the construction knowledge lives with each entry class
// (their Make factories), not here.
duckdb::unique_ptr<duckdb::StandardEntry> MakeEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  const duckdb::CreateInfo& info, const catalog::Permissions& perm,
  duckdb::CatalogType slot, duckdb::ClientContext* context,
  duckdb::optional_ptr<duckdb::CatalogEntry> superseded) {
  const auto name = info.GetQualifiedName().Name().GetIdentifierName();
  duckdb::unique_ptr<duckdb::CatalogEntry> built;
  switch (info.type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      built = SereneDBTableEntry::Make(
        catalog, schema, name,
        basics::downCast<const duckdb::CreateTableInfo>(info), perm, context,
        superseded);
      break;
    case VIEW_ENTRY:
      built = MakeViewEntry(
        catalog, schema, name,
        basics::downCast<const duckdb::CreateViewInfo>(info), perm);
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      built = MakeMacroEntry(
        catalog, schema, name, /*internal=*/false,
        basics::downCast<const duckdb::CreateMacroInfo>(info), perm);
      break;
    case INDEX_ENTRY:
      built = SereneDBIndexEntry::Make(
        catalog, schema, basics::downCast<const catalog::CreateIndexInfo>(info),
        context);
      break;
    case SEQUENCE_ENTRY:
      built = SereneDBSequenceEntry::Make(
        catalog, schema,
        basics::downCast<const duckdb::CreateSequenceInfo>(info), perm,
        superseded);
      break;
    case TYPE_ENTRY: {
      auto copied =
        duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
          info.Copy());
      copied->SetSchema(schema.name);
      built =
        duckdb::make_uniq<duckdb::TypeCatalogEntry>(catalog, schema, *copied);
      catalog::AdoptEntryIdentity(*built, ObjectId{copied->oid}, perm);
      break;
    }
    default:
      built = duckdb::make_uniq<SereneDBTokenizerEntry>(
        catalog, schema,
        basics::downCast<const catalog::CreateTokenizerInfo>(info), perm);
      break;
  }
  if (!built) {
    return nullptr;
  }
  return duckdb::unique_ptr_cast<duckdb::CatalogEntry, duckdb::StandardEntry>(
    std::move(built));
}

// The info with the comment applied to a copy of it, or null when it already
// says that.
duckdb::unique_ptr<duckdb::CreateInfo> RecommentedInfo(
  const duckdb::CreateInfo& info, std::string_view comment) {
  if (CommentText(info.comment) == comment) {
    return nullptr;
  }
  // An index carries its own comment, the way it carries its own name: the
  // record duckdb builds around it states what the index says, so the change
  // goes to the index rather than to the record.
  if (info.type == duckdb::CatalogType::INDEX_ENTRY) {
    return catalog::RecommentedIndexRecord(
      basics::downCast<const catalog::CreateIndexInfo>(info), comment);
  }
  auto copied = info.Copy();
  copied->comment = CommentValue(comment);
  return copied;
}

// Fixes this transaction's view of the cluster-global sets, and keeps what that
// view still needs from being reclaimed. Called at every catalog write, where
// the overlay pins everything else -- the two halves of one read view. A
// no-op outside a transaction and before the attach: a write with neither pays
// no transaction on that attachment.
void PinClusterGlobalReadView(duckdb::ClientContext& context) {
  // Nothing to pin without a transaction to fix a view for: boot, background
  // drops and teardown write inline and hold no claim.
  if (!context.transaction.HasActiveTransaction()) {
    return;
  }
  auto global = TryGlobalCatalog(context);
  if (!global) {
    return;
  }
  // Taking the transaction is the pin: it fixes what this transaction sees of
  // the cluster-global sets and, because the attachment now has an active
  // reader, keeps the versions that view still needs from being reclaimed.
  global->GetCatalogTransaction(context);
}

}  // namespace

// One version of one object in every slot it occupies. Nothing is recorded
// here: the commit walks what this transaction published and writes the log
// from that. Returns the primary entry now in the set, or null when there was
// nowhere left to put it.
duckdb::optional_ptr<duckdb::CatalogEntry> PlaceEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  duckdb::unique_ptr<duckdb::CreateInfo> owned,
  const catalog::Permissions& perm) try {
  SDB_ASSERT(owned);
  const auto& info = *owned;
  const auto type = info.type;
  if (type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    PlaceForeignServer(context, old_name, info, perm);
    return nullptr;
  }
  auto at = OpenSchema(context, catalog::ParentIdOf(info), /*for_write=*/true);
  if (!at) {
    // The parent went with a DROP and its sets with it: there is nothing left
    // to put this in.
    return nullptr;
  }
  // An owning name: a refused write destroys the entry it took, which is the
  // last reference to the definition a string_view name would point into.
  const duckdb::Identifier name{
    std::string{info.GetQualifiedName().Name().GetIdentifierName()}};
  const auto noun = pg::ToPgObjectTypeName(type);
  // What this version replaces, under the name it is still filed by -- which a
  // rename is about to move.
  const auto superseded =
    at.Lookup(type, old_name.empty() ? name.GetIdentifierName() : old_name);
  // A replace whose superseded name that slot does not hold still says Replace:
  // it is the record's mode, and a function leaving one macro set for the other
  // would otherwise be read back as a create that leaves the old entry behind.
  const duckdb::Identifier from{old_name};
  const auto on_conflict = old_name.empty()
                             ? duckdb::OnCreateConflict::IGNORE_ON_CONFLICT
                             : duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  auto entry = MakeEntry(*at.catalog, *at.schema, info, perm, EntrySlot(type),
                         context, superseded);
  if (!entry) {
    return nullptr;
  }
  const auto deps = EntryDependencies(info);
  auto placed =
    at.schema->AddEntryInternal(*at.transaction, std::move(entry), on_conflict,
                                deps, old_name.empty() ? nullptr : &from);
  if (!placed) {
    ThrowConcurrentDdlOn(noun, name.GetIdentifierName());
  }
  if (context != nullptr) {
    PinClusterGlobalReadView(*context);
  }
  return placed;
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void ScanForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  auto at = OpenCatalog(context, database, /*for_write=*/false);
  if (at) {
    at.catalog->GetForeignServerSet().Scan(*at.transaction, visitor);
  }
}

void DropSchemaObject(duckdb::ClientContext* context, duckdb::CatalogType type,
                      ObjectId schema_id, std::string_view name,
                      bool cascade) try {
  auto at = OpenSchema(context, schema_id, /*for_write=*/true);
  if (!at) {
    return;
  }
  at.Set(EntrySlot(type))
    .DropEntry(*at.transaction, duckdb::Identifier{name}, cascade);
  if (context != nullptr) {
    PinClusterGlobalReadView(*context);
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropForeignServerEntry(duckdb::ClientContext* context, ObjectId database,
                            std::string_view name) try {
  auto at = OpenCatalog(context, database, /*for_write=*/true);
  if (!at) {
    return;
  }
  auto& set = at.catalog->GetForeignServerSet();
  set.DropEntry(*at.transaction, duckdb::Identifier{name}, /*cascade=*/false);
  if (context != nullptr) {
    PinClusterGlobalReadView(*context);
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

// The object of one entry-backed kind under `parent_id`.`name`, whatever slot
// its kind is filed in -- the one place that knows which that is.
duckdb::CatalogEntry* FindEntryOfKind(duckdb::ClientContext* context,
                                      duckdb::CatalogType type,
                                      ObjectId parent_id,
                                      std::string_view name) {
  if (!parent_id.isSet()) {
    return nullptr;
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> object;
  if (type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    auto at = OpenCatalog(context, parent_id, /*for_write=*/false);
    object = at ? at.catalog->GetForeignServerSet().GetEntry(
                    *at.transaction, duckdb::Identifier{name})
                : nullptr;
  } else {
    object = LookupInSchema(context, parent_id, type, name);
  }
  // The relation namespace puts tables, views and sequences in one set, so a
  // name can be held by something else.
  if (object == nullptr || KindOf(object->type) != KindOf(type)) {
    return nullptr;
  }
  return object.get();
}

// The entry of `parent_id`.`name` an ALTER is about to rewrite, refused the way
// postgres names an absent one of this kind. A concurrent drop reaches the same
// throw: the statement resolved the name before it took the mutation scope, and
// this is the read that happens under it.
duckdb::CatalogEntry& RequireEntryOfKind(duckdb::ClientContext* context,
                                         duckdb::CatalogType type,
                                         ObjectId parent_id,
                                         std::string_view name) {
  auto* entry = FindEntryOfKind(context, type, parent_id, name);
  if (entry == nullptr) [[unlikely]] {
    pg::ThrowUndefinedObject(type, name);
  }
  return *entry;
}

// Who may ALTER or DROP this. An index has no owner of its own -- postgres
// derives relowner from the relation it covers -- so authority over it is that
// relation's, and every other kind answers with the owner on its entry.
void RequireEntryOwner(const catalog::AccessContext& ax,
                       duckdb::CatalogType type,
                       const duckdb::CatalogEntry& entry) {
  if (type == duckdb::CatalogType::INDEX_ENTRY) {
    const auto record = entry.Cast<SereneDBIndexEntry>().GetInfo();
    RequireIndexOwner(ax, record->Cast<catalog::CreateIndexInfo>());
    return;
  }
  catalog::RequireOwner(ax.context, ax.role, entry.permissions,
                        pg::ToPgObjectTypeName(type),
                        entry.name.GetIdentifierName());
}

}  // namespace

// The data WAL files rows under DataTableInfo::GetCatalogId and the dependency
// edges under DependencyInfo's id, so a rename between the write and the read
// cannot move them; the id resolves under the reader's own snapshot.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::CatalogTransaction transaction, SereneDBCatalog& catalog,
  ObjectId id) {
  return id.isSet() ? catalog.GetEntryById(transaction, id.id()) : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId id) {
  auto sdb_catalog = DatabaseCatalogOf(&context, database_id);
  return sdb_catalog
           ? LookupEntryById(sdb_catalog->GetCatalogTransaction(context),
                             *sdb_catalog, id)
           : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryById(
  duckdb::ClientContext& context, ObjectId id) {
  return LookupEntryById(
    context, connector::GetSereneDBContext(context).GetDatabaseId(), id);
}

namespace {

// One cluster-global set (roles or databases) and the transaction to read or
// write it through.
struct GlobalSet {
  duckdb::optional_ptr<SereneDBGlobalCatalog> catalog;
  duckdb::optional_ptr<duckdb::CatalogSet> set;
  duckdb::CatalogTransaction transaction;

  explicit operator bool() const noexcept { return set != nullptr; }
};

GlobalSet OpenGlobalSet(duckdb::ClientContext* context,
                        duckdb::CatalogType type, bool for_write) {
  auto global =
    context != nullptr ? TryGlobalCatalog(*context) : TryGlobalCatalog();
  if (!global) {
    // Callers test the set before touching the transaction; the instance is
    // reachable without a context, which boot and shutdown paths carry none.
    return {nullptr, nullptr,
            duckdb::CatalogTransaction::GetSystemTransaction(
              sdb::DuckDBEngine::Instance().instance())};
  }
  auto set = global->TryGetCatalogSet(type);
  SDB_ASSERT(set);
  if (context == nullptr) {
    return {global, set,
            duckdb::CatalogTransaction::GetCommittedTransaction(
              global->GetDatabase())};
  }
  if (for_write) {
    // A cluster-global kind lives in its own attachment, so a write has to be
    // attributed to it rather than to whichever database the statement runs
    // in. Catalog and nothing else: no rows of that attachment move, which is
    // what lets a statement write a role and a table in one transaction.
    ModifyGlobalDatabase(
      *context, duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
    if (type == duckdb::CatalogType::ROLE_ENTRY) {
      connector::GetSereneDBContext(*context).wrote_roles = true;
    }
  }
  return {global, set, global->GetCatalogTransaction(*context)};
}

GlobalSet OpenRoleSet(duckdb::ClientContext* context, bool for_write) {
  return OpenGlobalSet(context, duckdb::CatalogType::ROLE_ENTRY, for_write);
}

// The entry `id` names in one of the two cluster-global sets, through duckdb's
// by-id map: both are filed under a root location of their own kind.
duckdb::optional_ptr<duckdb::CatalogEntry> GlobalEntryById(
  GlobalSet& at, duckdb::CatalogType type, ObjectId id) {
  if (!id.isSet()) {
    return nullptr;
  }
  auto entry = at.catalog->GetEntryById(at.transaction, id.id());
  return entry && entry->type == type ? entry : nullptr;
}

// The role graph is cached per generation and filled from a committed read, so
// a bump has to follow the write becoming visible: it rides the commit, except
// for a contextless write, which has no transaction to hang a hook on.
void BumpRolesAfterVisible(duckdb::ClientContext* context) {
  if (context == nullptr) {
    auth::BumpRoleGeneration();
  }
}

}  // namespace

ObjectId RecordParentOf(const duckdb::CatalogEntry& entry) {
  switch (entry.type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
    case DATABASE_ENTRY:
      return id::kInstance;
    case SCHEMA_ENTRY:
      return entry.Cast<SereneDBSchemaEntry>().GetDatabaseId();
    case FOREIGN_SERVER_ENTRY:
      return entry.ParentCatalog().Cast<SereneDBCatalog>().GetDatabaseId();
    default:
      return ObjectId{
        basics::downCast<const duckdb::StandardEntry>(entry).Schema().oid};
  }
}

// A set scan whose callback may take locks of its own -- resolving a name,
// opening a transaction -- which the set's lock must not be held across.
// Releasing it is only safe while something keeps the entries alive: the
// statement's own transaction pins every version it can see, the way duckdb's
// own enumerations rely on. A contextless read pins nothing, so it scans under
// the lock, where the callbacks are the engine's own and take none.
void ScanEntries(duckdb::CatalogSet& set,
                 duckdb::CatalogTransaction transaction, bool pinned,
                 absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  if (!pinned) {
    set.Scan(transaction, visitor);
    return;
  }
  duckdb::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  set.Scan(transaction,
           [&](duckdb::CatalogEntry& entry) { entries.push_back(entry); });
  for (auto& entry : entries) {
    visitor(entry.get());
  }
}

void VisitRoleEntries(duckdb::ClientContext* context,
                      absl::FunctionRef<void(SereneDBRoleEntry&)> visitor) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return;
  }
  // Under the set's lock, which the scan holds for the callback: a role is the
  // entry's own, so a visitor reads it where the entry is still held. It must
  // take nothing of its own -- a privilege check reaches back into this very
  // set, and the lock is not recursive -- which is why the readers that resolve
  // anything collect first and resolve after.
  roles.set->Scan(roles.transaction, [&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBRoleEntry>());
  });
}

void VisitRoles(
  duckdb::ClientContext* context,
  absl::FunctionRef<void(const catalog::SereneDBRoleEntry&)> visitor) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return;
  }
  roles.set->Scan(roles.transaction, [&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBRoleEntry>());
  });
}

const SereneDBRoleEntry* FindRole(duckdb::ClientContext* context,
                                  std::string_view name) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return nullptr;
  }
  auto entry = roles.set->GetEntry(roles.transaction, duckdb::Identifier{name});
  return entry ? &entry->Cast<SereneDBRoleEntry>() : nullptr;
}

const SereneDBRoleEntry* FindRole(duckdb::ClientContext* context, ObjectId id) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return nullptr;
  }
  auto entry = GlobalEntryById(roles, duckdb::CatalogType::ROLE_ENTRY, id);
  return entry ? &entry->Cast<SereneDBRoleEntry>() : nullptr;
}

bool HasUncommittedRoles(duckdb::ClientContext& context) {
  auto* state = connector::GetSereneDBContextPtr(context);
  return state != nullptr && state->wrote_roles;
}

void RequireRoleNotVanished(duckdb::ClientContext* context,
                            std::string_view name) {
  if (context == nullptr) {
    return;
  }
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return;
  }
  if (roles.set->CommittedVersionVanished(roles.transaction,
                                          duckdb::Identifier{name})) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent delete of \"", name,
              "\""));
  }
}

namespace {

// The roles a role's own definition names: every grantee and grantor in its
// default-privileges entries, and itself for each entry it owns -- postgres
// refuses to drop either side while the entry stands.
duckdb::LogicalDependencyList DefaultAclReferences(
  const catalog::CreateRoleInfo& role) {
  duckdb::LogicalDependencyList out;
  const auto add = [&](ObjectId id) {
    // PUBLIC (id 0) and an unset id name no droppable object.
    if (id.isSet()) {
      out.AddDependency(duckdb::LogicalDependency{nullptr, DependencyInfo(id),
                                                  duckdb::Identifier{}});
    }
  };
  for (const auto& entry : role.DefaultAcls()) {
    add(role.GetId());
    for (const auto& item : entry.acl) {
      add(ObjectId{item.grantee});
      add(ObjectId{item.grantor});
    }
  }
  return out;
}

}  // namespace

void PutRole(duckdb::ClientContext* context, std::string_view old_name,
             duckdb::unique_ptr<catalog::CreateRoleInfo> role) try {
  auto roles = OpenRoleSet(context, /*for_write=*/true);
  if (!roles) {
    return;
  }
  const auto deps = DefaultAclReferences(*role);
  PutSetEntry(
    *roles.set, roles.transaction, old_name,
    duckdb::make_uniq<SereneDBRoleEntry>(*roles.catalog, std::move(*role)),
    deps);
  BumpRolesAfterVisible(context);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropGlobalEntry(duckdb::ClientContext* context, duckdb::CatalogType type,
                     std::string_view name) try {
  auto at = OpenGlobalSet(context, type, /*for_write=*/true);
  if (!at) {
    return;
  }
  at.set->DropEntry(at.transaction, duckdb::Identifier{name},
                    /*cascade=*/false);
  if (type == duckdb::CatalogType::ROLE_ENTRY) {
    BumpRolesAfterVisible(context);
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropRoleEntry(duckdb::ClientContext* context, std::string_view name) {
  DropGlobalEntry(context, duckdb::CatalogType::ROLE_ENTRY, name);
}

namespace {

GlobalSet OpenDatabaseSet(duckdb::ClientContext* context, bool for_write) {
  return OpenGlobalSet(context, duckdb::CatalogType::DATABASE_ENTRY, for_write);
}

}  // namespace

void VisitDatabases(duckdb::ClientContext* context,
                    absl::FunctionRef<void(SereneDBDatabaseEntry&)> visitor) {
  auto databases = OpenDatabaseSet(context, /*for_write=*/false);
  if (!databases) {
    return;
  }
  ScanEntries(*databases.set, databases.transaction, context != nullptr,
              [&](duckdb::CatalogEntry& entry) {
                visitor(entry.Cast<SereneDBDatabaseEntry>());
              });
}

const SereneDBDatabaseEntry* FindDatabase(duckdb::ClientContext* context,
                                          std::string_view name) {
  auto databases = OpenDatabaseSet(context, /*for_write=*/false);
  if (!databases) {
    return nullptr;
  }
  auto entry =
    databases.set->GetEntry(databases.transaction, duckdb::Identifier{name});
  return entry ? &entry->Cast<SereneDBDatabaseEntry>() : nullptr;
}

const SereneDBDatabaseEntry* FindDatabase(duckdb::ClientContext* context,
                                          ObjectId id) {
  auto databases = OpenDatabaseSet(context, /*for_write=*/false);
  if (!databases) {
    return nullptr;
  }
  auto entry =
    GlobalEntryById(databases, duckdb::CatalogType::DATABASE_ENTRY, id);
  return entry ? &entry->Cast<SereneDBDatabaseEntry>() : nullptr;
}

ObjectId FindDatabaseId(duckdb::ClientContext* context, std::string_view name) {
  const auto* database = FindDatabase(context, name);
  return database != nullptr ? catalog::IdOf(*database) : ObjectId{};
}

std::string DatabaseName(duckdb::ClientContext* context, ObjectId id) {
  const auto* database = FindDatabase(context, id);
  return database != nullptr ? std::string{database->name.GetIdentifierName()}
                             : std::string{};
}

void PutDatabase(duckdb::ClientContext* context, std::string_view old_name,
                 duckdb::unique_ptr<catalog::CreateDatabaseInfo> database,
                 catalog::Permissions perm) try {
  auto databases = OpenDatabaseSet(context, /*for_write=*/true);
  if (!databases) {
    return;
  }
  auto deps = EntryDependencies(*database);
  PutSetEntry(*databases.set, databases.transaction, old_name,
              duckdb::make_uniq<SereneDBDatabaseEntry>(
                *databases.catalog, *database, std::move(perm)),
              deps);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropDatabaseEntry(duckdb::ClientContext* context, std::string_view name) {
  DropGlobalEntry(context, duckdb::CatalogType::DATABASE_ENTRY, name);
}

void VisitSchemas(duckdb::ClientContext* context, ObjectId database,
                  absl::FunctionRef<void(SereneDBSchemaEntry&)> visitor) {
  if (context != nullptr) {
    if (auto catalog = DatabaseCatalogOf(context, database)) {
      catalog->ScanSchemas(*context, [&](duckdb::SchemaCatalogEntry& entry) {
        visitor(entry.Cast<SereneDBSchemaEntry>());
      });
    }
  } else if (auto catalog = DatabaseCatalogOf(nullptr, database)) {
    // Not duckdb's context-free ScanSchemas: that one also walks the inherited
    // schema set, whose only member is the store schema -- a plain
    // DuckSchemaEntry, and not a schema of ours at all.
    catalog->VisitSchemaEntries([&](SereneDBSchemaEntry& entry) {
      // pg_catalog and information_schema are generated at startup, not
      // created: nobody owns them and there is no version of them to read.
      // The transactional walk skips them itself.
      if (!entry.IsStatic()) {
        visitor(entry);
      }
    });
  }
}

SereneDBSchemaEntry* FindSchema(duckdb::ClientContext* context,
                                ObjectId database, std::string_view name) {
  auto catalog = DatabaseCatalogOf(context, database);
  if (!catalog) {
    return nullptr;
  }
  auto transaction = context != nullptr
                       ? catalog->GetCatalogTransaction(*context)
                       : catalog->CommittedRead();
  auto* entry = catalog->TryGetSchemaEntry(transaction, name).get();
  return entry != nullptr && !entry->IsStatic() ? entry : nullptr;
}

SereneDBSchemaEntry* FindSchema(duckdb::ClientContext* context, ObjectId id) {
  // The database is not part of the id, so the walk is over every attachment
  // until one's by-id map owns it.
  std::vector<ObjectId> databases;
  VisitDatabases(context, [&](const SereneDBDatabaseEntry& ref) {
    databases.push_back(catalog::IdOf(ref));
  });
  for (const auto database_id : databases) {
    auto catalog = DatabaseCatalogOf(context, database_id);
    if (!catalog) {
      continue;
    }
    const auto transaction = context != nullptr
                               ? catalog->GetCatalogTransaction(*context)
                               : catalog->CommittedRead();
    if (auto found = catalog->TryGetSchemaEntryById(transaction, id)) {
      return found.get();
    }
  }
  return nullptr;
}

ObjectId FindSchemaId(duckdb::ClientContext* context, ObjectId database,
                      std::string_view name) {
  const auto* schema = FindSchema(context, database, name);
  return schema != nullptr ? catalog::IdOf(*schema) : ObjectId{};
}

ObjectId SchemaDatabaseId(duckdb::ClientContext* context, ObjectId schema_id) {
  const auto* schema = FindSchema(context, schema_id);
  return schema != nullptr ? schema->GetDatabaseId() : ObjectId{};
}

void PutSchema(duckdb::ClientContext* context, std::string_view old_name,
               duckdb::unique_ptr<duckdb::CreateSchemaInfo> schema,
               catalog::Permissions perm) try {
  const auto database_id = catalog::ParentIdOf(*schema);
  auto catalog = DatabaseCatalogOf(context, database_id);
  if (!catalog) {
    // The database went with a DROP and its catalog with it: there is nothing
    // left to put this in.
    return;
  }
  auto transaction = context != nullptr
                       ? catalog->GetCatalogTransaction(*context)
                       : catalog->CommittedRead();
  if (context != nullptr) {
    duckdb::MetaTransaction::Get(*context).ModifyDatabase(
      catalog->GetAttached(), duckdb::DatabaseModificationType::ALTER_TABLE);
  }
  const auto name = schema->GetQualifiedName().Schema().GetIdentifierName();
  const auto entry_id = catalog::IdOf(*schema);
  const auto deps = EntryDependencies(*schema);
  // The name the version this supersedes is filed under: a rename moves the
  // set's key away from it, and every other write finds the entry under the
  // name it already has.
  const auto from = old_name.empty() ? name : old_name;
  const bool written =
    catalog->TryGetSchemaEntry(transaction, from) != nullptr
      ? catalog->AlterSchemaEntry(transaction, from, name, entry_id, perm, deps)
      : catalog->CreateSchemaEntry(transaction, name, entry_id, perm, deps);
  if (!written) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent DDL on schema \"",
              name, "\""));
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropSchemaEntry(duckdb::ClientContext* context, ObjectId database,
                     std::string_view name, bool cascade) try {
  auto catalog = DatabaseCatalogOf(context, database);
  if (!catalog) {
    return;
  }
  auto transaction = context != nullptr
                       ? catalog->GetCatalogTransaction(*context)
                       : catalog->CommittedRead();
  if (context != nullptr) {
    duckdb::MetaTransaction::Get(*context).ModifyDatabase(
      catalog->GetAttached(), duckdb::DatabaseModificationType::ALTER_TABLE);
  }
  catalog->DropSchemaEntry(transaction, name, cascade);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

// The analyzer behind a tokenizer entry.
catalog::TokenizerRef TokenizerOf(
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  const auto* tokenizer = EntryOf<SereneDBTokenizerEntry>(entry.get());
  return tokenizer == nullptr ? nullptr : tokenizer->GetTokenizer();
}

void VisitTokenizersIn(duckdb::ClientContext* context, duckdb::Catalog& catalog,
                       absl::FunctionRef<void(catalog::TokenizerRef)> visitor) {
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return;
  }
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  // The asking transaction's view when it has one, so a dictionary it created
  // itself is seen and one another session dropped since is still there; what
  // is committed otherwise. The feed reaches here from WAL replay and from the
  // tail of a commit, neither of which has a transaction to read through.
  const auto transaction =
    context != nullptr && context->transaction.HasActiveTransaction()
      ? sdb_catalog.GetCatalogTransaction(*context)
      : sdb_catalog.CommittedRead();
  sdb_catalog.VisitSchemaEntries([&](SereneDBSchemaEntry& schema) {
    ScanEntries(schema.GetCatalogSet(duckdb::CatalogType::TOKENIZER_ENTRY),
                transaction, context != nullptr,
                [&](duckdb::CatalogEntry& entry) {
                  visitor(entry.Cast<SereneDBTokenizerEntry>().GetTokenizer());
                });
  });
}

catalog::TokenizerRef FindTokenizerIn(duckdb::ClientContext* context,
                                      duckdb::Catalog& catalog, ObjectId id) {
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return {};
  }
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  const auto transaction =
    context != nullptr && context->transaction.HasActiveTransaction()
      ? sdb_catalog.GetCatalogTransaction(*context)
      : sdb_catalog.CommittedRead();
  return TokenizerOf(LookupEntryById(transaction, sdb_catalog, id));
}

void VisitSessionTokenizers(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(catalog::TokenizerRef)> visitor) {
  if (auto catalog = DatabaseCatalogOf(
        &context, connector::GetSereneDBContext(context).GetDatabaseId())) {
    VisitTokenizersIn(&context, *catalog, visitor);
  }
}

catalog::TokenizerRef FindSessionTokenizer(duckdb::ClientContext& context,
                                           ObjectId id) {
  auto catalog = DatabaseCatalogOf(
    &context, connector::GetSereneDBContext(context).GetDatabaseId());
  return catalog ? FindTokenizerIn(&context, *catalog, id) : nullptr;
}

catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, std::string_view name) {
  return TokenizerOf(LookupInSchema(
    context, schema_id, duckdb::CatalogType::TOKENIZER_ENTRY, name));
}

SereneDBCatalog& DatabaseCatalog(duckdb::ClientContext* context,
                                 ObjectId database) {
  auto found = DatabaseCatalogOf(context, database);
  SDB_ENSURE(found != nullptr, "database ", database.id(), " is not attached");
  return *found;
}

void ScanDatabase(duckdb::ClientContext* context, ObjectId database,
                  duckdb::CatalogType type,
                  absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  auto sdb_catalog = DatabaseCatalogOf(context, database);
  if (!sdb_catalog) {
    return;
  }
  const auto transaction = context != nullptr
                             ? sdb_catalog->GetCatalogTransaction(*context)
                             : sdb_catalog->CommittedRead();
  VisitSchemas(context, database, [&](SereneDBSchemaEntry& schema) {
    ScanEntries(schema.GetCatalogSet(type), transaction, context != nullptr,
                visitor);
  });
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, duckdb::CatalogType type,
  std::string_view name) {
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  return at ? at.Lookup(type, name) : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id) {
  return LookupSchemaEntryById(context, schema_id, id);
}

RelationTarget FindRelationTarget(duckdb::ClientContext* context,
                                  ObjectId schema_id, std::string_view name) {
  if (!schema_id.isSet()) {
    return {nullptr, duckdb::CatalogType::INVALID};
  }
  // Tables, views and sequences are one set; an index shares their namespace
  // but lives in the set of its own kind.
  auto object =
    LookupInSchema(context, schema_id, duckdb::CatalogType::TABLE_ENTRY, name);
  if (object) {
    return {object.get(), KindOf(object->type)};
  }
  auto index =
    LookupInSchema(context, schema_id, duckdb::CatalogType::INDEX_ENTRY, name);
  return {index.get(), index ? duckdb::CatalogType::INDEX_ENTRY
                             : duckdb::CatalogType::INVALID};
}

const duckdb::CatalogEntry* FindRelation(duckdb::ClientContext* context,
                                         ObjectId schema_id,
                                         std::string_view name) {
  return FindRelationTarget(context, schema_id, name).entry;
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId id) {
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return nullptr;
  }
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  const auto transaction =
    context != nullptr && context->transaction.HasActiveTransaction()
      ? sdb_catalog.GetCatalogTransaction(*context)
      : sdb_catalog.CommittedRead();
  return LookupEntryById(transaction, sdb_catalog, id);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntryIn(
  duckdb::ClientContext* context, ObjectId database, ObjectId id) {
  auto sdb_catalog = DatabaseCatalogOf(context, database);
  return sdb_catalog ? LookupEntryIn(context, *sdb_catalog, id) : nullptr;
}

void VisitFunctions(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const duckdb::MacroCatalogEntry&)> visitor) {
  ScanDatabase(context, database, duckdb::CatalogType::MACRO_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (const auto* found = FunctionOf(&entry)) {
                   visitor(*found);
                 }
               });
}

const duckdb::MacroCatalogEntry* FindFunction(duckdb::ClientContext* context,
                                              ObjectId schema_id,
                                              std::string_view name) {
  return FunctionOf(
    LookupInSchema(context, schema_id, duckdb::CatalogType::MACRO_ENTRY, name));
}

const duckdb::MacroCatalogEntry* FindFunction(duckdb::ClientContext* context,
                                              ObjectId schema_id, ObjectId id) {
  return FunctionOf(LookupSchemaEntryById(context, schema_id, id));
}

const duckdb::MacroCatalogEntry* FindSessionFunction(
  duckdb::ClientContext& context, ObjectId id) {
  return FunctionOf(LookupEntryById(context, id));
}

std::vector<const SereneDBSequenceEntry*> DatabaseSequences(
  duckdb::ClientContext* context, ObjectId database) {
  std::vector<const SereneDBSequenceEntry*> found;
  ScanDatabase(context, database, duckdb::CatalogType::SEQUENCE_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (const auto* seq = EntryOf<SereneDBSequenceEntry>(&entry)) {
                   found.push_back(seq);
                 }
               });
  return found;
}

std::vector<const SereneDBIndexEntry*> DatabaseInvertedIndexes(
  duckdb::ClientContext* context, ObjectId database) {
  std::vector<const SereneDBIndexEntry*> found;
  ScanDatabase(context, database, duckdb::CatalogType::INDEX_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 const auto* index = EntryOf<SereneDBIndexEntry>(&entry);
                 if (index != nullptr && index->IsInverted()) {
                   found.push_back(index);
                 }
               });
  return found;
}

std::vector<std::shared_ptr<const catalog::Index>> RelationInvertedIndexes(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id) {
  std::vector<std::shared_ptr<const catalog::Index>> found;
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  if (!at) {
    return found;
  }
  at.Set(duckdb::CatalogType::INDEX_ENTRY)
    .Scan(*at.transaction, [&](duckdb::CatalogEntry& entry) {
      auto* index = dynamic_cast<SereneDBIndexEntry*>(&entry);
      // A plain ART has no serenedb object to hand out: it is duckdb's.
      if (index != nullptr && index->GetRelationId() == relation_id &&
          index->IsInverted()) {
        found.push_back(index->DefinitionPtr());
      }
    });
  return found;
}

std::vector<duckdb::unique_ptr<catalog::CreateIndexInfo>> RelationIndexRecords(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id) {
  std::vector<duckdb::unique_ptr<catalog::CreateIndexInfo>> found;
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  if (!at) {
    return found;
  }
  VisitRelationIndexEntries(
    context, *at.schema, relation_id, [&](SereneDBIndexEntry& index) {
      found.push_back(
        duckdb::unique_ptr_cast<duckdb::CreateInfo, catalog::CreateIndexInfo>(
          index.GetInfo()));
    });
  return found;
}

std::vector<std::shared_ptr<const catalog::Index>> RelationInvertedIndexesIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  ObjectId relation_id) {
  std::vector<std::shared_ptr<const catalog::Index>> found;
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return found;
  }
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  // Off this catalog's own schema entries: an attach reads them before the
  // attachment is in the database manager, so nothing can resolve it by id yet.
  sdb_catalog.VisitSchemaEntries([&](SereneDBSchemaEntry& schema) {
    VisitRelationIndexEntries(context, schema, relation_id,
                              [&](SereneDBIndexEntry& index) {
                                if (index.IsInverted()) {
                                  found.push_back(index.DefinitionPtr());
                                }
                              });
  });
  return found;
}

namespace {

void DropTableEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name, bool cascade) {
  const auto* entry = Find<SereneDBTableEntry>(context, schema_id, name);
  const auto previous = entry != nullptr ? entry->Definition() : nullptr;
  DropSchemaObject(context, duckdb::CatalogType::TABLE_ENTRY, schema_id, name,
                   cascade);
  if (entry != nullptr) {
    entry->ParentCatalog().Cast<SereneDBCatalog>().ReleaseIndexedColumns(
      catalog::IdOf(*entry));
  }
  // The keys this table stated are gone with it, so the tables it pointed at
  // stop stating their half.
  for (const auto& version :
       ReferencedKeyVersions(context, previous.get(), nullptr)) {
    PutEntry(context, version.info->GetTableName().GetIdentifierName(),
             version.info->Copy(), version.perm);
  }
}

}  // namespace

void DropIndexEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name) {
  ObjectId index_id;
  ObjectId relation_id;
  const duckdb::Catalog* owner = nullptr;
  if (const auto* previous = Find<SereneDBIndexEntry>(context, schema_id, name);
      previous != nullptr) {
    index_id = ObjectId{previous->oid};
    relation_id = previous->GetRelationId();
    owner = &previous->ParentCatalog();
  }
  DropSchemaObject(context, duckdb::CatalogType::INDEX_ENTRY, schema_id, name,
                   /*cascade=*/false);
  // What the relation identifies its rows by stops counting this index the
  // moment the drop is written, the way duckdb takes an index off the
  // relation's own list rather than rewriting the entry in front of it.
  if (owner) {
    owner->Cast<SereneDBCatalog>().RemoveIndexColumns(relation_id, index_id);
  }
}

void VisitForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const SereneDBForeignServerEntry&)> visitor) {
  ScanForeignServers(context, database, [&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBForeignServerEntry>());
  });
}

const SereneDBForeignServerEntry* FindForeignServer(
  duckdb::ClientContext* context, ObjectId database, std::string_view name) {
  auto at = OpenCatalog(context, database, /*for_write=*/false);
  return at ? EntryOf<SereneDBForeignServerEntry>(
                at.catalog->GetForeignServerSet().GetEntry(
                  *at.transaction, duckdb::Identifier{name}))
            : nullptr;
}

const SereneDBForeignServerEntry* FindForeignServerAnywhere(
  duckdb::ClientContext* context, std::string_view name) {
  std::vector<ObjectId> databases;
  VisitDatabases(context, [&](const SereneDBDatabaseEntry& ref) {
    databases.push_back(catalog::IdOf(ref));
  });
  for (const auto database_id : databases) {
    if (const auto* found = FindForeignServer(context, database_id, name)) {
      return found;
    }
  }
  return nullptr;
}

std::vector<std::string> CatalogForeignServerNames(SereneDBCatalog& catalog) {
  std::vector<std::string> found;
  catalog.GetForeignServerSet().Scan([&](duckdb::CatalogEntry& entry) {
    found.emplace_back(entry.name.GetIdentifierName());
  });
  return found;
}

duckdb::optional_ptr<duckdb::CatalogEntry> PutEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  duckdb::unique_ptr<duckdb::CreateInfo> info, catalog::Permissions perm) {
  const auto schema_id = catalog::ParentIdOf(*info);
  const auto id = catalog::IdOf(*info);
  // Read before the write: what this version of the table stops stating is the
  // difference against the one it replaces. The keys it states are read off it
  // before it is handed over.
  duckdb::unique_ptr<duckdb::CreateTableInfo> before;
  duckdb::unique_ptr<duckdb::CreateTableInfo> stated;
  if (info->type == duckdb::CatalogType::TABLE_ENTRY) {
    if (const auto* table = Find<SereneDBTableEntry>(context, schema_id, id)) {
      before = table->Definition();
    }
    stated =
      catalog::Clone(basics::downCast<const duckdb::CreateTableInfo>(*info));
  }
  auto placed = PlaceEntry(context, old_name, std::move(info), perm);
  if (!placed || !stated) {
    return placed;
  }
  // The half of each key that lives on the table it points at. Those versions
  // differ from their predecessor only in that half, which is not a key they
  // state themselves, so none of them carries this any further.
  for (const auto& version :
       ReferencedKeyVersions(context, before.get(), stated.get())) {
    PutEntry(context, version.info->GetTableName().GetIdentifierName(),
             version.info->Copy(), version.perm);
  }
  return placed;
}

void RequireIndexOwner(const catalog::AccessContext& ax,
                       const catalog::CreateIndexInfo& index) {
  catalog::Permissions perm;
  const auto relation_id = index.GetRelationId();
  const auto schema_id = index.GetSchemaId();
  if (const auto* table =
        Find<SereneDBTableEntry>(ax.context, schema_id, relation_id)) {
    perm = table->permissions;
  } else if (const auto* view = Find<duckdb::ViewCatalogEntry>(
               ax.context, schema_id, relation_id)) {
    perm = view->permissions;
  }
  if (perm.owner == 0) {
    return;
  }
  catalog::RequireOwner(ax.context, ax.role, perm, "index", index.GetName());
}

void ChangeEntryAcl(const catalog::AccessContext& ax, duckdb::CatalogType type,
                    ObjectId parent_id, std::string_view name,
                    auth::AclMutator mutate) {
  // The caller has already checked the grant itself; what is enforced here is
  // only that the object is still there.
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  auto perm = auth::MutatedAcl(entry.permissions, KindOf(entry.type), mutate);
  // The definition does not change, so what is written back is the one the
  // entry already answers with.
  PutEntry(ax.context, name, entry.GetInfo(), std::move(perm));
}

void ChangeEntryOwner(const catalog::AccessContext& ax,
                      duckdb::CatalogType type, ObjectId parent_id,
                      std::string_view name, ObjectId new_owner,
                      std::string_view new_owner_name) {
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  const auto& perm = entry.permissions;
  catalog::RequireOwnerTransfer(ax, parent_id, perm, new_owner, new_owner_name,
                                pg::ToPgObjectTypeName(type), name);
  PutEntry(ax.context, name, entry.GetInfo(),
           auth::TransferredOwner(perm, new_owner));
}

void SetEntryComment(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     std::string_view comment) {
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  RequireEntryOwner(ax, type, entry);
  if (auto recommented = RecommentedInfo(*entry.GetInfo(), comment)) {
    PutEntry(ax.context, name, std::move(recommented), entry.permissions);
  }
}

void ApplyEntryAlter(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     duckdb::AlterInfo& info) try {
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  RequireEntryOwner(ax, type, entry);
  auto& sdb_catalog = entry.ParentCatalog().Cast<SereneDBCatalog>();
  const auto transaction = sdb_catalog.GetCatalogTransaction(*ax.context);
  auto& set =
    entry.ParentSchema().Cast<SereneDBSchemaEntry>().GetCatalogSet(entry.type);
  // duckdb's own alter is the whole step: the entry builds its next version,
  // the set places it, and the catalog record is the commit walk's
  // (WriteCatalogChange).
  if (!set.AlterEntry(transaction, entry.name, info)) {
    ThrowConcurrentDdlOn(pg::ToPgObjectTypeName(type), name);
  }
  PinClusterGlobalReadView(*ax.context);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

duckdb::optional_ptr<duckdb::CatalogEntry> RequireDropTarget(
  const catalog::AccessContext& ax, duckdb::CatalogType type,
  ObjectId parent_id, std::string_view name, bool missing_ok) {
  auto* object = FindEntryOfKind(ax.context, type, parent_id, name);
  if (object != nullptr) {
    RequireEntryOwner(ax, type, *object);
    return object;
  }
  // Another kind of the relation namespace still answering for the name is
  // PG's kind mismatch, not a missing object -- and IF EXISTS does not cover
  // it, exactly as in postgres.
  const auto in_relation_namespace =
    type == duckdb::CatalogType::TABLE_ENTRY ||
    type == duckdb::CatalogType::VIEW_ENTRY ||
    type == duckdb::CatalogType::SEQUENCE_ENTRY ||
    type == duckdb::CatalogType::INDEX_ENTRY;
  if (in_relation_namespace && parent_id.isSet()) {
    if (const auto relation = FindRelationTarget(ax.context, parent_id, name);
        relation.entry) {
      const auto kind = pg::ToPgObjectTypeName(type);
      const auto actual = pg::ToPgObjectTypeName(relation.kind);
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
        ERR_MSG("\"", name, "\" is not ",
                basics::string_utils::GetArticle(kind), " ", kind),
        ERR_HINT("Use DROP ", absl::AsciiStrToUpper(actual), " to remove ",
                 basics::string_utils::GetArticle(actual), " ", actual, "."));
    }
  }
  if (missing_ok) {
    return nullptr;
  }
  pg::ThrowUndefinedObject(type, name);
}

bool DropEntryObject(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId database_id, ObjectId parent_id,
                     std::string_view name, bool cascade, bool missing_ok) {
  auto object = RequireDropTarget(ax, type, parent_id, name, missing_ok);
  if (!object) {
    return false;
  }
  catalog::GetCatalog().DropResolved(ax.context, parent_id,
                                     KindOf(object->type),
                                     catalog::IdOf(*object), name, cascade);
  return true;
}

namespace {

// The name the object `id` answers to under `parent_id`, empty when nothing
// there holds it.
std::string EntryNameOfKind(duckdb::ClientContext* context,
                            duckdb::CatalogType type, ObjectId parent_id,
                            ObjectId id) {
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
    case DATABASE_ENTRY: {
      auto at = OpenGlobalSet(context, type, /*for_write=*/false);
      entry = at ? GlobalEntryById(at, type, id) : nullptr;
      break;
    }
    // A schema and a foreign server are children of the database, so the
    // parent is the catalog that holds them.
    case SCHEMA_ENTRY:
    case FOREIGN_SERVER_ENTRY:
      entry = LookupEntryIn(context, parent_id, id);
      break;
    case TOKENIZER_ENTRY:
    case TYPE_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
    case VIEW_ENTRY:
    case SEQUENCE_ENTRY:
    case TABLE_ENTRY:
    case INDEX_ENTRY:
      entry = LookupSchemaEntryById(context, parent_id, id);
      break;
    default:
      return {};
  }
  return entry ? std::string{entry->name.GetIdentifierName()} : std::string{};
}

}  // namespace

void DropEntryOfKind(duckdb::ClientContext* context, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name, bool cascade) {
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
      DropRoleEntry(context, name);
      return;
    case DATABASE_ENTRY:
      DropDatabaseEntry(context, name);
      return;
    case SCHEMA_ENTRY:
      DropSchemaEntry(context, parent_id, name, cascade);
      return;
    case TABLE_ENTRY:
      DropTableEntry(context, parent_id, name, cascade);
      return;
    case INDEX_ENTRY:
      DropIndexEntry(context, parent_id, name);
      return;
    case FOREIGN_SERVER_ENTRY:
      DropForeignServerEntry(context, parent_id, name);
      return;
    case TOKENIZER_ENTRY:
    case TYPE_ENTRY:
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
    case VIEW_ENTRY:
    case SEQUENCE_ENTRY:
      DropSchemaObject(context, type, parent_id, name, cascade);
      return;
    default:
      return;
  }
}

namespace {

void ReplaySequenceRecord(ObjectId id, std::string_view old_name,
                          const duckdb::CreateSequenceInfo& info,
                          const catalog::Permissions& perm) {
  const auto options = catalog::SequenceOptionsOf(info);
  auto placed = PutEntry(nullptr, old_name, info.Copy(), perm);
  // A rewrite inherited its predecessor's counter when the entry was built; a
  // sequence the log is meeting for the first time gets one seeded from the
  // durable value the authoritative records after it will fold in.
  if (const auto* seq =
        dynamic_cast<const SereneDBSequenceEntry*>(placed.get());
      seq != nullptr && !seq->Counter()) {
    seq->AdoptCounter(catalog::ReloadedCounter(id, options));
  }
}

}  // namespace

void ReplayCatalogRecord(duckdb::unique_ptr<duckdb::CreateInfo> info,
                         catalog::Permissions perm, bool dropped) {
  const auto type = info->type;
  const ObjectId parent_id = catalog::ParentIdOf(*info);
  // A schema is the one kind whose name is the qualified name's schema half,
  // not its name half -- that is where CreateSchemaInfo puts it.
  const auto& qualified = info->GetQualifiedName();
  const auto name = type == duckdb::CatalogType::SCHEMA_ENTRY
                      ? qualified.Schema().GetIdentifierName()
                      : qualified.Name().GetIdentifierName();
  if (dropped) {
    catalog::DropEntryOfKind(nullptr, type, parent_id, name,
                             /*cascade=*/false);
    if (type == duckdb::CatalogType::DATABASE_ENTRY) {
      // A live DROP DATABASE detaches through duckdb, which is not replaying
      // here: the attachment this log made a moment ago is what the record
      // takes back out, along with the database it was for.
      connector::DiscardDatabaseAttachment(name);
    }
    return;
  }
  // What the record replaces, if anything: an id already in a set means this
  // version supersedes the one under whatever name that set files it by, which
  // a rename in the same log may already have moved.
  const auto old_name =
    EntryNameOfKind(nullptr, type, parent_id, catalog::IdOf(*info));
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
      PutRole(
        nullptr, old_name,
        duckdb::unique_ptr_cast<duckdb::CreateInfo, catalog::CreateRoleInfo>(
          info->Copy()));
      return;
    case DATABASE_ENTRY: {
      const auto& database =
        basics::downCast<const catalog::CreateDatabaseInfo>(*info);
      PutDatabase(
        nullptr, old_name,
        duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                catalog::CreateDatabaseInfo>(database.Copy()),
        perm);
      // The attachment everything the log says about this database needs. Its
      // catalog alone: the file is opened once the whole log is in, so the data
      // WAL replays against a catalog that is already whole.
      connector::AttachDatabaseCatalog(database.GetId(), database.GetName());
      return;
    }
    case SCHEMA_ENTRY:
      PutSchema(
        nullptr, old_name,
        duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateSchemaInfo>(
          info->Copy()),
        perm);
      return;
    case SEQUENCE_ENTRY:
      ReplaySequenceRecord(
        catalog::IdOf(*info), old_name,
        basics::downCast<const duckdb::CreateSequenceInfo>(*info), perm);
      return;
    case INDEX_ENTRY:
      // An index has no owner and no ACL: every privilege decision reads the
      // relation it is built on.
      PutEntry(nullptr, old_name, info->Copy());
      return;
    default:
      PutEntry(nullptr, old_name, info->Copy(), perm);
  }
}

namespace {

// The keys `table` states against other tables, by the constraint id each is
// filed under -- which is what tells one key from another across a rewrite.
containers::FlatHashMap<ObjectId, const duckdb::ForeignKeyConstraint*>
StatedForeignKeys(const duckdb::CreateTableInfo* table) {
  containers::FlatHashMap<ObjectId, const duckdb::ForeignKeyConstraint*> found;
  if (table == nullptr) {
    return found;
  }
  for (const auto& constraint : table->constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
      continue;
    }
    const ObjectId target{fk.host_referenced_id};
    if (target.isSet() && target != catalog::IdOf(*table)) {
      found.emplace(ObjectId{fk.oid}, &fk);
    }
  }
  return found;
}

// The referenced half of one key, as the table it points at states it.
duckdb::unique_ptr<duckdb::Constraint> MirrorOf(
  const duckdb::ForeignKeyConstraint& fk,
  const duckdb::CreateTableInfo& referencing,
  const duckdb::CreateTableInfo& referenced, std::string_view schema_name) {
  duckdb::ForeignKeyInfo mirror;
  mirror.type = duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
  mirror.schema = duckdb::Identifier{schema_name};
  mirror.table =
    duckdb::Identifier{referencing.GetTableName().GetIdentifierName()};
  auto pk_columns = catalog::ReferencedKeyNames(fk, &referenced);
  for (const auto& name : pk_columns) {
    const auto* column =
      catalog::ColumnByName(referenced, name.GetIdentifierName());
    mirror.pk_keys.emplace_back(column == nullptr ? 0
                                                  : column->Logical().index);
  }
  for (const auto& name : fk.fk_columns) {
    const auto* column =
      catalog::ColumnByName(referencing, name.GetIdentifierName());
    mirror.fk_keys.emplace_back(column == nullptr ? 0
                                                  : column->Logical().index);
  }
  auto built = duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
    pk_columns, fk.fk_columns, std::move(mirror));
  built->constraint_name = fk.constraint_name;
  built->oid = fk.oid;
  built->host_referenced_id = catalog::IdOf(referencing).id();
  built->host_pk_column_ids = fk.host_pk_column_ids;
  return built;
}

std::vector<ReferencedKeyVersion> ReferencedKeyVersions(
  duckdb::ClientContext* context, const duckdb::CreateTableInfo* before,
  const duckdb::CreateTableInfo* after) {
  const auto stated_before = StatedForeignKeys(before);
  const auto stated_after = StatedForeignKeys(after);
  if (stated_before.empty() && stated_after.empty()) {
    return {};
  }
  // What each referenced table has to stop stating and start stating, keyed by
  // the table so one of them pointed at twice is rewritten once.
  struct Pending {
    containers::FlatHashSet<ObjectId> removed;
    std::vector<const duckdb::ForeignKeyConstraint*> added;
  };
  containers::FlatHashMap<ObjectId, Pending> pending;
  for (const auto& [id, fk] : stated_before) {
    if (!stated_after.contains(id)) {
      pending[ObjectId{fk->host_referenced_id}].removed.insert(id);
    }
  }
  for (const auto& [id, fk] : stated_after) {
    if (!stated_before.contains(id)) {
      pending[ObjectId{fk->host_referenced_id}].added.push_back(fk);
    }
  }

  const auto* referencing = after != nullptr ? after : before;
  const auto* referencing_schema =
    FindSchema(context, catalog::ParentIdOf(*referencing));
  const auto database_id = referencing_schema != nullptr
                             ? referencing_schema->GetDatabaseId()
                             : ObjectId{};
  const std::string_view referencing_schema_name =
    referencing_schema != nullptr ? referencing_schema->name.GetIdentifierName()
                                  : std::string_view{};

  std::vector<ReferencedKeyVersion> versions;
  versions.reserve(pending.size());
  for (const auto& [target, change] : pending) {
    const auto* held = FindIn<SereneDBTableEntry>(context, database_id, target);
    if (held == nullptr) {
      // The referenced table went with the same statement -- a cascade taking
      // both ends -- so there is nothing left over there to state the key.
      continue;
    }
    const auto referenced = held->Definition();
    auto next = catalog::Clone(*referenced);
    std::erase_if(next->constraints, [&](const auto& constraint) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        return false;
      }
      const auto& fk =
        constraint->template Cast<duckdb::ForeignKeyConstraint>();
      return fk.info.type ==
               duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE &&
             change.removed.contains(ObjectId{fk.oid});
    });
    for (const auto* fk : change.added) {
      next->constraints.push_back(
        MirrorOf(*fk, *referencing, *referenced, referencing_schema_name));
    }
    catalog::SetIdentity(*next, target, catalog::ParentIdOf(*referenced));
    versions.push_back({.info = std::move(next), .perm = held->permissions});
  }
  return versions;
}

}  // namespace

void VisitRelationIndexEntries(
  duckdb::ClientContext* context, SereneDBSchemaEntry& schema,
  ObjectId relation_id, absl::FunctionRef<void(SereneDBIndexEntry&)> visitor) {
  // The ART a serenedb index is mirrored by is storage, not a catalog object:
  // it shares this set because duckdb builds it there, and a catalog walk has
  // nothing to say about it.
  const auto match = [&](duckdb::CatalogEntry& entry) -> SereneDBIndexEntry* {
    auto* index = dynamic_cast<SereneDBIndexEntry*>(&entry);
    return index != nullptr && index->GetRelationId() == relation_id ? index
                                                                     : nullptr;
  };
  if (context == nullptr) {
    // Nothing pins what a contextless read sees, so the visitor runs where the
    // entries are still held: under the set's own lock.
    schema.Scan(duckdb::CatalogType::INDEX_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  if (auto* index = match(entry)) {
                    visitor(*index);
                  }
                });
    return;
  }
  std::vector<SereneDBIndexEntry*> found;
  schema.Scan(*context, duckdb::CatalogType::INDEX_ENTRY,
              [&](duckdb::CatalogEntry& entry) {
                if (auto* index = match(entry)) {
                  found.push_back(index);
                }
              });
  for (auto* index : found) {
    visitor(*index);
  }
}

void VisitCatalogSetEntries(
  duckdb::ClientContext& context, ObjectId database, duckdb::CatalogType set,
  absl::FunctionRef<void(const SereneDBSchemaEntry&, duckdb::CatalogEntry&)>
    visitor) {
  VisitSchemas(&context, database, [&](SereneDBSchemaEntry& schema) {
    // Collected first, visited after: the set's lock must not be held across a
    // visitor -- one that resolves entries or transactions takes locks of its
    // own. Safe here because this road always has a statement behind it, whose
    // transaction pins every version it can see.
    duckdb::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
    schema.Scan(context, set, [&](duckdb::CatalogEntry& object_entry) {
      entries.push_back(object_entry);
    });
    for (auto& entry : entries) {
      visitor(schema, entry.get());
    }
  });
}

void VisitTableEntries(
  duckdb::ClientContext& context, ObjectId database,
  absl::FunctionRef<void(const SereneDBSchemaEntry&, const SereneDBTableEntry&)>
    visitor) {
  VisitCatalogSetEntries(
    context, database, duckdb::CatalogType::TABLE_ENTRY,
    [&](const SereneDBSchemaEntry& schema, duckdb::CatalogEntry& object_entry) {
      // Views and sequences share this set; neither is a SereneDBTableEntry,
      // so the cast is the filter.
      if (const auto* table =
            dynamic_cast<const SereneDBTableEntry*>(&object_entry)) {
        visitor(schema, *table);
      }
    });
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindRelationEntry(
  duckdb::ClientContext* context, ObjectId database, std::string_view schema,
  std::string_view name) {
  // A caller with no open transaction -- the wire layer sizing a COPY response
  // before the statement binds -- reads what is committed, as boot does.
  if (context != nullptr && !context->transaction.HasActiveTransaction()) {
    context = nullptr;
  }
  auto sdb_catalog = DatabaseCatalogOf(context, database);
  if (!sdb_catalog) {
    return nullptr;
  }
  const auto transaction = context != nullptr
                             ? sdb_catalog->GetCatalogTransaction(*context)
                             : sdb_catalog->CommittedRead();
  auto schema_entry = sdb_catalog->TryGetSchemaEntry(transaction, schema);
  if (!schema_entry) {
    return nullptr;
  }
  return schema_entry->GetCatalogSet(duckdb::CatalogType::TABLE_ENTRY)
    .GetEntry(transaction, duckdb::Identifier{name});
}

SereneDBTableEntry* FindTableEntry(duckdb::ClientContext* context,
                                   ObjectId database, std::string_view schema,
                                   std::string_view name) {
  auto entry = FindRelationEntry(context, database, schema, name);
  return entry ? dynamic_cast<SereneDBTableEntry*>(entry.get()) : nullptr;
}

SereneDBTableEntry* FindSessionTableEntry(duckdb::ClientContext& context,
                                          ObjectId id) {
  auto entry = LookupEntryById(context, id);
  return entry ? dynamic_cast<SereneDBTableEntry*>(entry.get()) : nullptr;
}

}  // namespace sdb::catalog
