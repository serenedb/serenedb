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

#include "catalog/duckdb_catalog_sets.h"

#include <array>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/deferred_writes.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_dependency.h"
#include "catalog/duckdb_entry_builders.h"
#include "catalog/duckdb_global_catalog.h"
#include "catalog/duckdb_index_entry.h"
#include "catalog/duckdb_index_scan_entry.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/duckdb_object_index.h"
#include "catalog/duckdb_schema_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/duckdb_view_entry.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_storage_extension.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::catalog {
namespace {

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

// duckdb refuses the second writer of an entry another transaction is holding,
// which is the same answer serenedb gives for every other object; the two just
// have to say it the same way.
[[noreturn]] void ThrowConcurrentDdl() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                  ERR_MSG("could not serialize access due to concurrent DDL on "
                          "the same object"));
}

[[noreturn]] void ThrowConcurrentDdlOn(std::string_view noun,
                                       std::string_view name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent DDL on ", noun, " \"",
            name, "\""));
}

// Puts one built entry in a set the catalog owns rather than a schema -- the
// three kinds that hang off no schema: a role, a database, a foreign server.
// A schema child takes this route inside DuckSchemaEntry::AddEntryInternal.
//
// `old_name` is the name the version this supersedes is filed under, and a
// rewrite is the alter it is rather than a drop followed by a create: the
// tombstone a drop leaves reaches DependencyManager::VerifyCommitDrop at
// commit, which refuses it when any edge on the object committed after the
// transaction started -- so a concurrent CREATE by a role made every ALTER
// ROLE on that role fail. An alter also hands the object's edges over rather
// than retiring and re-adding them, and takes duckdb's rename path, which
// knows a rename from a drop.
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
  if (!old_name.empty() && set.GetEntry(transaction, from) != nullptr) {
    duckdb::SetPermissionsInfo info{
      duckdb::PermissionsAlterType::REPLACE_DEFINITION, type,
      duckdb::QualifiedName{from}, entry->permissions.owner};
    info.new_dependencies =
      duckdb::make_uniq<duckdb::LogicalDependencyList>(deps);
    try {
      written = set.AlterEntry(transaction, from, info, std::move(entry));
    } catch (const duckdb::CatalogException&) {
      // A rename whose target name was taken between the mutator's own
      // duplicate check and here. None of these entries is `internal`, so that
      // is the only CatalogException an alter of one can raise, and it is said
      // the way every other lost race is.
    }
  } else {
    written = set.CreateEntry(transaction, name, std::move(entry), deps);
  }
  if (!written) {
    ThrowConcurrentDdlOn(pg::ToPgObjectTypeName(type),
                         name.GetIdentifierName());
  }
}

// Every edge one version states: the roles it names as owner, grantee and
// grantor, plus what its body resolved to when it was written. A table adds the
// grants its columns carry and the ids its declaration names directly.
void RecordVersion(duckdb::ClientContext* context,
                   const std::shared_ptr<const duckdb::CreateInfo>& info,
                   const catalog::Permissions& perm, bool replace) {
  // Nothing inside a RecordedScope: a batched mutator, a refresh and boot
  // replay have all said their piece already.
  if (catalog::Catalog::RecordedScope::Open()) {
    return;
  }
  const auto mode =
    replace ? catalog::wal::PutMode::Replace : catalog::wal::PutMode::Create;
  if (info->type == duckdb::CatalogType::TABLE_ENTRY) {
    // A table's record carries the store table's shape and the sequences a
    // create hands it, so it is the one kind whose record is not the plain one.
    catalog::GetCatalog().RecordTable(
      context, *std::static_pointer_cast<const duckdb::CreateTableInfo>(info),
      mode, perm);
    return;
  }
  catalog::GetCatalog().RecordEntry(context, catalog::ParentIdOf(*info),
                                    KindOf(info->type), catalog::IdOf(*info),
                                    mode, info, perm);
}

// Every object one version names: the roles its owner and ACL name, plus what
// its body resolved to when it was written. A table is the one kind whose
// references are not a flat list -- which column, which constraint and
// therefore what a DROP of the target does to it all come off the definition.
duckdb::LogicalDependencyList EntryEdges(
  const std::shared_ptr<const duckdb::CreateInfo>& info,
  const catalog::Permissions& perm) {
  if (info->type == duckdb::CatalogType::TABLE_ENTRY) {
    return catalog::TableDependencies(
      *std::static_pointer_cast<const duckdb::CreateTableInfo>(info), perm);
  }
  return EntryDependencies(*info, perm);
}

// The catalog-log record of the write now running, appended before anything
// reaches a set. For the three hand-written puts -- a role, a database and a
// schema -- whose entry answers to no schema's AddEntryInternal; every other
// kind records itself there. Nothing inside a RecordedScope: that caller has
// already written this version's record.
void RecordPut(duckdb::ClientContext* context, std::string_view old_name,
               ObjectId parent_id, duckdb::CatalogType type, ObjectId id,
               std::shared_ptr<const duckdb::CreateInfo> info,
               const catalog::Permissions& perm) {
  if (catalog::Catalog::RecordedScope::Open()) {
    return;
  }
  catalog::GetCatalog().RecordEntry(context, parent_id, type, id,
                                    old_name.empty()
                                      ? catalog::wal::PutMode::Create
                                      : catalog::wal::PutMode::Replace,
                                    std::move(info), perm);
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
  const auto schema = FindSchema(context, schema_id);
  if (!schema) {
    return at;
  }
  at.catalog = DatabaseCatalogOf(context, catalog::ParentIdOf(*schema));
  if (!at.catalog) {
    return at;
  }
  at.transaction = context != nullptr
                     ? at.catalog->GetCatalogTransaction(*context)
                     : at.catalog->CommittedRead();
  at.schema = at.catalog->TryGetSchemaEntry(*at.transaction,
                                            catalog::SchemaNameOf(*schema));
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

// The serenedb definition `entry` holds, when the entry is one of this kind --
// the relation namespace puts tables, views, sequences and the index-name
// wrappers in one set, so recognising a kind is a cast rather than the set it
// sits in. `perm` is filled with the owner and ACL the entry carries, and left
// alone when nothing is found.
template<typename Entry>
auto DefinitionOf(duckdb::optional_ptr<duckdb::CatalogEntry> entry,
                  catalog::Permissions* perm = nullptr)
  -> std::remove_cvref_t<decltype(std::declval<Entry&>().Definition())> {
  auto* typed = dynamic_cast<Entry*>(entry.get());
  if (typed == nullptr) {
    return nullptr;
  }
  if (perm != nullptr) {
    *perm = typed->permissions;
  }
  return typed->Definition();
}

// A function is in whichever of duckdb's two macro sets its own declaration
// puts it, so either entry class answers for one.
const duckdb::MacroCatalogEntry* FunctionOf(
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  if (const auto* scalar = EntryOf<SereneDBScalarMacroEntry>(entry)) {
    return scalar;
  }
  return EntryOf<SereneDBTableMacroEntry>(entry);
}

// Every entry of `slots` in every schema of `database`.
void ScanDatabaseSlots(duckdb::ClientContext* context, ObjectId database,
                       std::span<const duckdb::CatalogType> slots,
                       absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  auto sdb_catalog = DatabaseCatalogOf(context, database);
  if (!sdb_catalog) {
    return;
  }
  const auto transaction = context != nullptr
                             ? sdb_catalog->GetCatalogTransaction(*context)
                             : sdb_catalog->CommittedRead();
  const auto of_schema = [&](SereneDBSchemaEntry& schema) {
    for (const auto slot : slots) {
      schema.GetCatalogSet(slot).Scan(transaction, visitor);
    }
  };
  if (context != nullptr) {
    sdb_catalog->ScanSchemas(*context, [&](duckdb::SchemaCatalogEntry& schema) {
      of_schema(schema.Cast<SereneDBSchemaEntry>());
    });
    return;
  }
  // Not duckdb's context-free ScanSchemas: that one also walks the inherited
  // schema set, whose only member is the store schema -- a plain
  // DuckSchemaEntry with none of our sets at all. The static schemas are
  // skipped for the reason the transactional walk skips them: their content is
  // generated, not this database's.
  sdb_catalog->VisitSchemaEntries([&](SereneDBSchemaEntry& schema) {
    if (!schema.IsStatic()) {
      of_schema(schema);
    }
  });
}

// The same bounded by one schema, for the reads that start from a parent id.
void ScanSchemaSlots(duckdb::ClientContext* context, ObjectId schema_id,
                     std::span<const duckdb::CatalogType> slots,
                     absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  if (!at) {
    return;
  }
  for (const auto slot : slots) {
    at.Set(slot).Scan(*at.transaction, visitor);
  }
}

// The entry `parent_id`.`name` names in one of `slots`, or null.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupSchemaEntry(
  duckdb::ClientContext* context, ObjectId schema_id,
  std::span<const duckdb::CatalogType> slots, std::string_view name) {
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  if (!at) {
    return nullptr;
  }
  for (const auto slot : slots) {
    if (auto entry = at.Lookup(slot, name)) {
      return entry;
    }
  }
  return nullptr;
}

// By stable id, through the database's object index rather than through a
// cache -- so a DDL-free workload pays one entry lookup and nothing has to be
// invalidated. Every caller has the parent: a record names its own, and a
// statement resolved the object by name before it changed it.
duckdb::optional_ptr<duckdb::CatalogEntry> LookupSchemaEntryById(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id) {
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  return at ? LookupEntryById(*at.transaction, *at.catalog, id) : nullptr;
}

// A foreign server, the one kind that is a database child rather than a schema
// child, as in postgres: the set is the catalog's own, so no schema is in the
// path and no schema entry can add it -- which is why this is the one place
// besides a schema's AddEntryInternal that appends a record.
void PlaceForeignServer(duckdb::ClientContext* context,
                        std::string_view old_name,
                        const std::shared_ptr<const duckdb::CreateInfo>& info,
                        const catalog::Permissions& perm) {
  auto at =
    OpenCatalog(context, catalog::ParentIdOf(*info), /*for_write=*/true);
  if (!at) {
    return;
  }
  auto& set = at.catalog->GetForeignServerSet();
  auto entry = duckdb::make_uniq<SereneDBForeignServerEntry>(
    *at.catalog,
    std::static_pointer_cast<const catalog::CreateForeignServerInfo>(info),
    perm);
  const auto deps = EntryEdges(info, perm);
  RecordVersion(context, info, perm, !old_name.empty());
  const duckdb::Identifier name = entry->name;
  PutSetEntry(set, *at.transaction, old_name, std::move(entry), deps);
  // No schema in the location: that is what tells LookupEntryById to answer
  // out of the catalog's own set.
  const ObjectLocation location{duckdb::Identifier{}, name,
                                duckdb::CatalogType::FOREIGN_SERVER_ENTRY};
  SetObjectLocation(*at.transaction, at.catalog->GetObjectIndexSet(),
                    *at.catalog, catalog::IdOf(*info), &location);
}

// One version of one object in every slot it occupies, and the object index
// entry that says where it now is. The record is appended where the entry
// reaches the set -- so a refresh, which rebuilds an entry nothing changed,
// runs this under a RecordedScope and writes none.
//
// Returns the primary entry now in the set, or null when there was nowhere left
// to put it.
duckdb::optional_ptr<duckdb::CatalogEntry> PlaceEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  const std::shared_ptr<const duckdb::CreateInfo>& info,
  const catalog::Permissions& perm) try {
  const auto type = info->type;
  if (type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    PlaceForeignServer(context, old_name, info, perm);
    return nullptr;
  }
  auto at = OpenSchema(context, catalog::ParentIdOf(*info), /*for_write=*/true);
  if (!at) {
    // The parent went with a DROP and its sets with it: there is nothing left
    // to put this in.
    return nullptr;
  }
  // An owning name: a refused write destroys the entry it took, which is the
  // last reference to the definition a string_view name would point into.
  const duckdb::Identifier name{std::string{catalog::NameOf(*info)}};
  const auto noun = pg::ToPgObjectTypeName(type);
  // What this version replaces, under the name it is still filed by -- which a
  // rename is about to move.
  const auto superseded =
    at.Lookup(LookupSlots(type).front(),
              old_name.empty() ? name.GetIdentifierName() : old_name);
  // A replace whose superseded name that slot does not hold still says Replace:
  // it is the record's mode, and a function leaving one macro set for the other
  // would otherwise be read back as a create that leaves the old entry behind.
  const duckdb::Identifier from{old_name};
  const auto on_conflict = old_name.empty()
                             ? duckdb::OnCreateConflict::IGNORE_ON_CONFLICT
                             : duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  // The catalog-log record of this version, appended before anything reaches a
  // set -- so a mutation states its change exactly once, from the definition
  // every slot is built from, and the two cannot drift.
  RecordVersion(context, info, perm, !old_name.empty());
  duckdb::LogicalDependencyList deps;
  // The primary entry, and the slot it lands in -- which is where a by-id
  // lookup has to go looking: a table macro's own slot is the second one.
  duckdb::optional_ptr<duckdb::CatalogEntry> primary;
  auto placed_slot = duckdb::CatalogType::INVALID;
  for (const auto slot : EntrySlots(type)) {
    auto entry =
      MakeEntry(*at.catalog, *at.schema, info, perm, slot, context, superseded);
    if (!entry) {
      // A function that was a scalar macro and is now a table one is leaving
      // that set for the other.
      if (!old_name.empty()) {
        (void)at.Set(slot).DropEntry(*at.transaction, from, /*cascade=*/false);
      }
      continue;
    }
    if (!primary) {
      placed_slot = slot;
      // The wrapper carries the primary entry's edges: it is the same object.
      deps = EntryEdges(info, perm);
    }
    auto placed = at.schema->AddEntryInternal(
      *at.transaction, std::move(entry), on_conflict, deps,
      old_name.empty() ? nullptr : &from);
    if (!placed) {
      ThrowConcurrentDdlOn(noun, name.GetIdentifierName());
    }
    if (!primary) {
      primary = placed;
    }
  }
  if (!primary) {
    return nullptr;
  }
  const ObjectLocation location{at.schema->name, name, placed_slot};
  SetObjectLocation(*at.transaction, at.catalog->GetObjectIndexSet(),
                    *at.catalog, catalog::IdOf(*info), &location);
  return primary;
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

// The entry `id` names in one catalog, read through the caller's transaction
// when it has one and off what is committed otherwise.
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

void ScanForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(duckdb::CatalogEntry&)> visitor) {
  auto at = OpenCatalog(context, database, /*for_write=*/false);
  if (at) {
    at.catalog->GetForeignServerSet().Scan(*at.transaction, visitor);
  }
}

// Takes every slot one object of `type` occupies out of its schema's sets, and
// its location with them.
void DropSchemaObject(duckdb::ClientContext* context, duckdb::CatalogType type,
                      ObjectId schema_id, std::string_view name) try {
  auto at = OpenSchema(context, schema_id, /*for_write=*/true);
  if (!at) {
    return;
  }
  ObjectId id;
  for (const auto slot : LookupSlots(type)) {
    if (auto entry = at.Lookup(slot, name)) {
      id = catalog::IdOf(*entry);
      break;
    }
  }
  for (const auto slot : EntrySlots(type)) {
    (void)at.Set(slot).DropEntry(*at.transaction, duckdb::Identifier{name},
                                 /*cascade=*/false);
  }
  if (id.isSet()) {
    SetObjectLocation(*at.transaction, at.catalog->GetObjectIndexSet(),
                      *at.catalog, id, nullptr);
  }
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

// The foreign server's counterpart: its set is the catalog's own, so no schema
// is in the path.
void DropForeignServerEntry(duckdb::ClientContext* context, ObjectId database,
                            std::string_view name) try {
  auto at = OpenCatalog(context, database, /*for_write=*/true);
  if (!at) {
    return;
  }
  auto& set = at.catalog->GetForeignServerSet();
  const duckdb::Identifier key{name};
  ObjectId id;
  if (auto entry = set.GetEntry(*at.transaction, key)) {
    id = catalog::IdOf(*entry);
  }
  (void)set.DropEntry(*at.transaction, key, /*cascade=*/false);
  if (id.isSet()) {
    SetObjectLocation(*at.transaction, at.catalog->GetObjectIndexSet(),
                      *at.catalog, id, nullptr);
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
    object = LookupSchemaEntry(context, parent_id, LookupSlots(type), name);
  }
  // The relation namespace puts tables, views, sequences and the index-name
  // wrappers in one set, so a name can be held by something else -- and the
  // wrapper is nobody's object.
  if (object == nullptr || KindOf(object->type) != KindOf(type)) {
    return nullptr;
  }
  return dynamic_cast<SereneDBIndexScanEntry*>(object.get()) == nullptr
           ? object.get()
           : nullptr;
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
    RequireIndexOwner(ax, *entry.Cast<SereneDBIndexEntry>().Definition());
    return;
  }
  catalog::RequireOwner(ax.context, ax.role, entry.permissions,
                        pg::ToPgObjectTypeName(type),
                        entry.name.GetIdentifierName());
}

// The cross-tree fixups a drop implies, planned against the pre-mutation
// catalog.
catalog::DropPlan PlanEntryDrop(duckdb::ClientContext* context,
                                duckdb::CatalogType type, ObjectId id,
                                bool cascade, std::string_view name) {
  if (type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    // Nothing can depend on a foreign server today -- there are no foreign
    // tables -- so `cascade` has nothing to decide.
    return catalog::ComputeDropPlan(context, id);
  }
  if (type != duckdb::CatalogType::SEQUENCE_ENTRY) {
    return catalog::ComputeDropPlanRestrict(context, id, cascade,
                                            pg::ToPgObjectTypeName(type), name);
  }
  // A SERIAL's sequence is owned by its table, and dropping it on its own is
  // the cascade the statement has to ask for.
  auto plan = catalog::ComputeDropPlan(context, id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop sequence ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail("sequence", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too, or "
               "DROP TABLE on the owning table."));
  }
  return plan;
}

}  // namespace
namespace {

// The role set and the transaction to read or write it through. A null
// `context` is boot, compaction or a background path: they see what is
// committed and write nothing.
struct RoleSet {
  duckdb::optional_ptr<SereneDBGlobalCatalog> catalog;
  duckdb::optional_ptr<duckdb::CatalogSet> set;
  duckdb::CatalogTransaction transaction;

  explicit operator bool() const noexcept { return set != nullptr; }
};

RoleSet OpenRoleSet(duckdb::ClientContext* context, bool for_write) {
  auto global =
    context != nullptr ? TryGlobalCatalog(*context) : TryGlobalCatalog();
  if (!global) {
    return {nullptr, nullptr,
            duckdb::CatalogTransaction::GetSystemTransaction(
              duckdb::DatabaseInstance::GetDatabase(*context))};
  }
  if (context == nullptr) {
    // A bare system transaction starts at 1 and would see only what boot
    // created; a contextless caller means "whatever is committed now".
    auto transaction =
      duckdb::CatalogTransaction::GetSystemTransaction(global->GetDatabase());
    transaction.start_time = duckdb::TRANSACTION_ID_START - 1;
    return {global, &global->GetRoleSet(), transaction};
  }
  if (for_write) {
    // Roles live in their own attachment, so a role write has to be attributed
    // to it rather than to whichever database the statement runs in.
    ModifyGlobalDatabase(*context,
                         duckdb::DatabaseModificationType::ALTER_TABLE);
    connector::GetSereneDBContext(*context).wrote_roles = true;
  }
  return {global, &global->GetRoleSet(),
          global->GetCatalogTransaction(*context)};
}

// The role graph is cached per generation and filled from a committed read, so
// a bump has to follow the write becoming visible. Bumping at statement time
// lets a concurrent reader publish the pre-write graph under the new
// generation, and nothing replaces it: a revoked membership keeps granting and
// a freshly created role stays unknown by name until unrelated role DDL moves
// the generation again. So the bump rides the commit -- except for a
// contextless write, which has no transaction to hang a hook on.
void BumpRolesAfterVisible(duckdb::ClientContext* context) {
  if (context == nullptr) {
    auth::BumpRoleGeneration();
  }
}

}  // namespace

void VisitRoles(
  duckdb::ClientContext* context,
  absl::FunctionRef<void(const catalog::CreateRoleInfo&)> visitor) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return;
  }
  roles.set->Scan(roles.transaction, [&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBRoleEntry>().Role());
  });
}

std::shared_ptr<const catalog::CreateRoleInfo> FindRole(
  duckdb::ClientContext* context, std::string_view name) {
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return nullptr;
  }
  auto entry = roles.set->GetEntry(roles.transaction, duckdb::Identifier{name});
  return entry ? entry->Cast<SereneDBRoleEntry>().RoleInfo() : nullptr;
}

std::shared_ptr<const catalog::CreateRoleInfo> FindRole(
  duckdb::ClientContext* context, ObjectId id) {
  // The set is keyed by name and roles are few, so the by-id form scans it.
  // Every hot by-id read goes through the cached role graph instead.
  std::shared_ptr<const catalog::CreateRoleInfo> found;
  auto roles = OpenRoleSet(context, /*for_write=*/false);
  if (!roles) {
    return nullptr;
  }
  roles.set->Scan(roles.transaction, [&](duckdb::CatalogEntry& entry) {
    auto& role = entry.Cast<SereneDBRoleEntry>();
    if (catalog::IdOf(role) == id) {
      found = role.RoleInfo();
    }
  });
  return found;
}

bool HasUncommittedRoles(duckdb::ClientContext& context) {
  auto* state = connector::GetSereneDBContextPtr(context);
  return state != nullptr && state->wrote_roles;
}

void PinClusterGlobalReadView(duckdb::ClientContext& context) {
  auto global = TryGlobalCatalog(context);
  if (!global) {
    return;
  }
  // Taking the transaction is the pin. It fixes what this transaction sees of
  // the cluster-global sets from its first catalog write on -- the same instant
  // the overlay fixes what it sees of everything else -- and, because the
  // attachment now has an active reader, it keeps the versions that view still
  // needs from being reclaimed. Without it a role dropped and committed under
  // an open transaction leaves nothing behind at all: the tombstone is
  // collected the moment no reader can want it, and the loss becomes
  // indistinguishable from a name that never was.
  //
  // Charged to DDL and nothing else: a transaction that writes no catalog never
  // comes here, and never pays a transaction on this attachment.
  (void)global->GetCatalogTransaction(context);
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
  auto entry = roles.set->GetEntry(roles.transaction, duckdb::Identifier{name});
  if (!entry || entry->timestamp >= duckdb::TRANSACTION_ID_START) {
    // Nothing under that name, or this transaction's own uncommitted version:
    // neither can have been dropped out from under it.
    return;
  }
  if (FindRole(nullptr, name)) {
    return;
  }
  // The version this statement resolved is committed, and a committed read no
  // longer finds it: another transaction dropped it and committed while this
  // one was open. Writing the new version would resurrect it.
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent delete of \"", name,
            "\""));
}

namespace {

// The roles a role's own definition names: every grantee and grantor in its
// default-privileges entries, and itself for each entry it owns -- postgres
// refuses to drop either side while the entry stands.
std::vector<ObjectId> DefaultAclReferences(
  const catalog::CreateRoleInfo& role) {
  std::vector<ObjectId> out;
  for (const auto& entry : role.DefaultAcls()) {
    out.push_back(role.GetId());
    for (const auto& item : entry.acl) {
      out.push_back(catalog::GranteeOf(item));
      out.push_back(catalog::GrantorOf(item));
    }
  }
  return out;
}

}  // namespace

void PutRole(duckdb::ClientContext* context, std::string_view old_name,
             std::shared_ptr<const catalog::CreateRoleInfo> role) try {
  RecordPut(context, old_name, id::kInstance, duckdb::CatalogType::ROLE_ENTRY,
            role->GetId(), role, catalog::Permissions{});
  auto roles = OpenRoleSet(context, /*for_write=*/true);
  if (!roles) {
    return;
  }
  const auto references = DefaultAclReferences(*role);
  PutSetEntry(
    *roles.set, roles.transaction, old_name,
    duckdb::make_uniq<SereneDBRoleEntry>(*roles.catalog, std::move(role)),
    DependencyList(references));
  BumpRolesAfterVisible(context);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropRoleEntry(duckdb::ClientContext* context, std::string_view name) try {
  auto roles = OpenRoleSet(context, /*for_write=*/true);
  if (!roles) {
    return;
  }
  (void)roles.set->DropEntry(roles.transaction, duckdb::Identifier{name},
                             /*cascade=*/false);
  BumpRolesAfterVisible(context);
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

ObjectId DatabaseRef::Id() const noexcept {
  return info ? info->GetId() : ObjectId{};
}

std::string_view DatabaseRef::Name() const noexcept {
  return info ? info->GetName() : std::string_view{};
}

namespace {

// Same shape as the role set: the DATABASE_ENTRY set of the cluster-global
// attachment, plus the transaction to read or write it through.
struct DatabaseSet {
  duckdb::optional_ptr<SereneDBGlobalCatalog> catalog;
  duckdb::optional_ptr<duckdb::CatalogSet> set;
  // Unset before the cluster-global attachment exists and after shutdown: there
  // is no database instance to start a transaction against either.
  std::optional<duckdb::CatalogTransaction> transaction;

  explicit operator bool() const noexcept { return set != nullptr; }
};

DatabaseSet OpenDatabaseSet(duckdb::ClientContext* context, bool for_write) {
  auto global =
    context != nullptr ? TryGlobalCatalog(*context) : TryGlobalCatalog();
  if (!global) {
    return {};
  }
  if (context == nullptr) {
    // A bare system transaction starts at 1 and would see only what boot
    // created; a contextless caller means "whatever is committed now".
    auto transaction =
      duckdb::CatalogTransaction::GetSystemTransaction(global->GetDatabase());
    transaction.start_time = duckdb::TRANSACTION_ID_START - 1;
    return {global, &global->GetDatabaseSet(), transaction};
  }
  if (for_write) {
    ModifyGlobalDatabase(*context,
                         duckdb::DatabaseModificationType::ALTER_TABLE);
    connector::GetSereneDBContext(*context).wrote_databases = true;
  }
  return {global, &global->GetDatabaseSet(),
          global->GetCatalogTransaction(*context)};
}

DatabaseRef RefOf(duckdb::CatalogEntry& entry) {
  auto& database = entry.Cast<SereneDBDatabaseEntry>();
  return {database.DatabaseInfo(), database.permissions};
}

// One generation's worth of the database list, published whole and replaced
// whole. Databases are few and change only by DDL, so both lookups are exact
// hash maps rather than a scan of the set.
struct DatabaseCache {
  uint64_t generation = 0;
  containers::FlatHashMap<ObjectId, DatabaseRef> by_id;
  containers::FlatHashMap<std::string_view, ObjectId> by_name;
};

std::atomic_uint64_t gDatabaseGeneration{1};
std::shared_ptr<const DatabaseCache> gDatabaseCache =
  std::make_shared<const DatabaseCache>();

void ScanDatabases(duckdb::ClientContext* context,
                   absl::FunctionRef<void(const DatabaseRef&)> visitor) {
  auto databases = OpenDatabaseSet(context, /*for_write=*/false);
  if (!databases) {
    return;
  }
  databases.set->Scan(*databases.transaction, [&](duckdb::CatalogEntry& entry) {
    visitor(RefOf(entry));
  });
}

// A transaction that has written a database reads its own uncommitted version,
// so it neither uses nor fills the shared cache.
bool ReadsOwnDatabases(duckdb::ClientContext* context) {
  if (context == nullptr) {
    return false;
  }
  auto* state = connector::GetSereneDBContextPtr(*context);
  return state != nullptr && state->wrote_databases;
}

// Always off the committed set, never off the asking transaction's view: the
// cache is shared, and a transaction whose read view predates another's commit
// would otherwise publish that older view under the newer generation and leave
// it there. A transaction that wrote a database does not come here at all.
std::shared_ptr<const DatabaseCache> LoadDatabaseCache() {
  const auto generation = gDatabaseGeneration.load(std::memory_order_relaxed);
  auto cached = std::atomic_load(&gDatabaseCache);
  if (cached->generation == generation) {
    return cached;
  }
  auto fresh = std::make_shared<DatabaseCache>();
  fresh->generation = generation;
  ScanDatabases(nullptr, [&](const DatabaseRef& ref) {
    // The name view points into the info the map holds, so it stays valid for
    // as long as the cache does.
    fresh->by_name.emplace(ref.Name(), ref.Id());
    fresh->by_id.emplace(ref.Id(), ref);
  });
  std::shared_ptr<const DatabaseCache> published = fresh;
  std::atomic_store(&gDatabaseCache, published);
  return published;
}

}  // namespace

void BumpDatabaseGeneration() noexcept {
  gDatabaseGeneration.fetch_add(1, std::memory_order_relaxed);
  // A database carries its schemas: creating one adds a public schema and
  // dropping one takes every schema it held.
  BumpSchemaGeneration();
}

void VisitDatabases(duckdb::ClientContext* context,
                    absl::FunctionRef<void(const DatabaseRef&)> visitor) {
  if (ReadsOwnDatabases(context)) {
    ScanDatabases(context, visitor);
    return;
  }
  for (const auto& [id, ref] : LoadDatabaseCache()->by_id) {
    visitor(ref);
  }
}

DatabaseRef FindDatabase(duckdb::ClientContext* context,
                         std::string_view name) {
  if (ReadsOwnDatabases(context)) {
    auto databases = OpenDatabaseSet(context, /*for_write=*/false);
    if (!databases) {
      return {};
    }
    auto entry =
      databases.set->GetEntry(*databases.transaction, duckdb::Identifier{name});
    return entry ? RefOf(*entry) : DatabaseRef{};
  }
  auto cache = LoadDatabaseCache();
  auto it = cache->by_name.find(name);
  if (it == cache->by_name.end()) {
    return {};
  }
  auto found = cache->by_id.find(it->second);
  return found == cache->by_id.end() ? DatabaseRef{} : found->second;
}

DatabaseRef FindDatabase(duckdb::ClientContext* context, ObjectId id) {
  if (ReadsOwnDatabases(context)) {
    DatabaseRef found;
    ScanDatabases(context, [&](const DatabaseRef& ref) {
      if (ref.Id() == id) {
        found = ref;
      }
    });
    return found;
  }
  auto cache = LoadDatabaseCache();
  auto it = cache->by_id.find(id);
  return it == cache->by_id.end() ? DatabaseRef{} : it->second;
}

std::string DatabaseName(duckdb::ClientContext* context, ObjectId id) {
  auto ref = FindDatabase(context, id);
  return ref ? std::string{ref.Name()} : std::string{};
}

void PutDatabase(duckdb::ClientContext* context, std::string_view old_name,
                 std::shared_ptr<const catalog::CreateDatabaseInfo> database,
                 catalog::Permissions perm) try {
  RecordPut(context, old_name, id::kInstance,
            duckdb::CatalogType::DATABASE_ENTRY, database->GetId(), database,
            perm);
  auto databases = OpenDatabaseSet(context, /*for_write=*/true);
  if (!databases) {
    return;
  }
  auto deps = EntryDependencies(*database, perm);
  PutSetEntry(*databases.set, *databases.transaction, old_name,
              duckdb::make_uniq<SereneDBDatabaseEntry>(
                *databases.catalog, std::move(database), std::move(perm)),
              deps);
  BumpDatabaseGeneration();
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropDatabaseEntry(duckdb::ClientContext* context,
                       std::string_view name) try {
  auto databases = OpenDatabaseSet(context, /*for_write=*/true);
  if (!databases) {
    return;
  }
  const auto previous = FindDatabase(context, name);
  (void)databases.set->DropEntry(*databases.transaction,
                                 duckdb::Identifier{name},
                                 /*cascade=*/false);
  BumpDatabaseGeneration();
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

namespace {

// One generation's worth of every schema of every database, published whole
// and replaced whole -- the database cache's shape, for the same reason: both
// are small, both change only by DDL, and both back lookups that would
// otherwise scan a CatalogSet on every name resolution.
struct SchemaCache {
  uint64_t generation = 0;
  containers::FlatHashMap<ObjectId, catalog::HeldSchema> by_id;
  containers::FlatHashMap<std::pair<ObjectId, std::string_view>, ObjectId>
    by_name;
};

std::atomic_uint64_t gSchemaGeneration{1};
std::shared_ptr<const SchemaCache> gSchemaCache =
  std::make_shared<const SchemaCache>();

// A transaction that has written a schema reads its own uncommitted version,
// so it neither uses nor fills the shared cache.
bool ReadsOwnSchemas(duckdb::ClientContext* context) {
  if (context == nullptr) {
    return false;
  }
  auto* state = connector::GetSereneDBContextPtr(*context);
  return state != nullptr && state->wrote_schemas;
}

void ScanSchemasOf(duckdb::ClientContext* context, ObjectId database,
                   absl::FunctionRef<void(catalog::HeldSchema)> visitor) {
  const auto emit = [&](duckdb::SchemaCatalogEntry& entry) {
    // Held, not info plus permissions: the entry replaces the pair whole, so
    // this call has to own the version it read.
    auto held = entry.Cast<SereneDBSchemaEntry>().Held();
    if (held.first) {
      visitor(std::move(held));
    }
  };
  if (context != nullptr) {
    if (auto catalog = DatabaseCatalogOf(context, database)) {
      catalog->ScanSchemas(*context, emit);
    }
  } else if (auto catalog = DatabaseCatalogOf(nullptr, database)) {
    // Not duckdb's context-free ScanSchemas: that one also walks the inherited
    // schema set, whose only member is the store schema -- a plain
    // DuckSchemaEntry, and not a schema of ours at all.
    catalog->VisitSchemaEntries(
      [&](SereneDBSchemaEntry& entry) { emit(entry); });
  }
}

// `id` inside one database, as `context` sees it. The by-id read narrows to
// this the moment the database is known.
catalog::SchemaRef FindSchemaIn(duckdb::ClientContext* context,
                                ObjectId database, ObjectId id,
                                catalog::Permissions* perm) {
  catalog::SchemaRef found;
  ScanSchemasOf(context, database, [&](catalog::HeldSchema held) {
    if (catalog::IdOf(*held.first) == id) {
      if (perm != nullptr) {
        *perm = held.second;
      }
      found = std::move(held.first);
    }
  });
  return found;
}

// Always off the committed sets, never off the asking transaction's view: the
// cache is shared, and a transaction whose read view predates another's commit
// would otherwise publish that older view under the newer generation.
//
// A build that could not reach one of the databases -- boot has recorded it but
// its attachment is not registered yet -- is used and not published: labelled
// with the current generation it would hold that hole until the next DDL.
std::shared_ptr<const SchemaCache> LoadSchemaCache() {
  const auto generation = gSchemaGeneration.load(std::memory_order_relaxed);
  auto cached = std::atomic_load(&gSchemaCache);
  if (cached->generation == generation) {
    return cached;
  }
  auto fresh = std::make_shared<SchemaCache>();
  fresh->generation = generation;
  // The name view points into the info the map holds, so it stays valid for as
  // long as the cache does.
  const auto add = [&](catalog::HeldSchema ref) {
    fresh->by_name.emplace(std::pair{catalog::ParentIdOf(*ref.first),
                                     catalog::SchemaNameOf(*ref.first)},
                           catalog::IdOf(*ref.first));
    fresh->by_id.emplace(catalog::IdOf(*ref.first), std::move(ref));
  };
  std::vector<ObjectId> databases;
  VisitDatabases(
    nullptr, [&](const DatabaseRef& ref) { databases.push_back(ref.Id()); });
  bool complete = true;
  for (const auto database_id : databases) {
    complete &= DatabaseCatalogOf(nullptr, database_id) != nullptr;
    ScanSchemasOf(nullptr, database_id,
                  [&](catalog::HeldSchema held) { add(std::move(held)); });
  }
  std::shared_ptr<const SchemaCache> published = fresh;
  if (complete) {
    std::atomic_store(&gSchemaCache, published);
  }
  return published;
}

}  // namespace

void BumpSchemaGeneration() noexcept {
  gSchemaGeneration.fetch_add(1, std::memory_order_relaxed);
}

std::vector<catalog::HeldSchema> DatabaseSchemas(duckdb::ClientContext* context,
                                                 ObjectId database) {
  std::vector<catalog::HeldSchema> found;
  ScanSchemasOf(context, database, [&](catalog::HeldSchema held) {
    found.push_back(std::move(held));
  });
  return found;
}

void VisitSchemas(duckdb::ClientContext* context, ObjectId database,
                  absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                                         const catalog::Permissions&)>
                    visitor) {
  if (ReadsOwnSchemas(context)) {
    ScanSchemasOf(context, database, [&](catalog::HeldSchema held) {
      visitor(*held.first, held.second);
    });
    return;
  }
  for (const auto& [id, held] : LoadSchemaCache()->by_id) {
    if (catalog::ParentIdOf(*held.first) == database) {
      visitor(*held.first, held.second);
    }
  }
}

catalog::SchemaRef FindSchema(duckdb::ClientContext* context, ObjectId database,
                              std::string_view name,
                              catalog::Permissions* perm) {
  if (ReadsOwnSchemas(context)) {
    catalog::SchemaRef found;
    ScanSchemasOf(context, database, [&](catalog::HeldSchema held) {
      if (catalog::SchemaNameOf(*held.first) == name) {
        if (perm != nullptr) {
          *perm = held.second;
        }
        found = std::move(held.first);
      }
    });
    return found;
  }
  auto cache = LoadSchemaCache();
  auto it = cache->by_name.find(std::pair{database, name});
  if (it == cache->by_name.end()) {
    return nullptr;
  }
  auto found = cache->by_id.find(it->second);
  if (found == cache->by_id.end()) {
    return nullptr;
  }
  if (perm != nullptr) {
    *perm = found->second.second;
  }
  return found->second.first;
}

catalog::SchemaRef FindSchema(duckdb::ClientContext* context, ObjectId id,
                              catalog::Permissions* perm) {
  if (ReadsOwnSchemas(context)) {
    // The one hop an id does not carry is its database, and a scan per database
    // is what the cache exists to avoid. So the committed cache answers the hop
    // for every schema that was there before this transaction -- which is all
    // but the ones it created itself -- and only those fall back to the walk.
    auto cache = LoadSchemaCache();
    const auto placed = cache->by_id.find(id);
    if (placed != cache->by_id.end()) {
      return FindSchemaIn(context, catalog::ParentIdOf(*placed->second.first),
                          id, perm);
    }
    std::vector<ObjectId> databases;
    VisitDatabases(
      context, [&](const DatabaseRef& ref) { databases.push_back(ref.Id()); });
    for (const auto database_id : databases) {
      if (auto found = FindSchemaIn(context, database_id, id, perm)) {
        return found;
      }
    }
    return nullptr;
  }
  auto cache = LoadSchemaCache();
  auto it = cache->by_id.find(id);
  if (it == cache->by_id.end()) {
    return nullptr;
  }
  if (perm != nullptr) {
    *perm = it->second.second;
  }
  return it->second.first;
}

ObjectId FindSchemaId(duckdb::ClientContext* context, ObjectId database,
                      std::string_view name) {
  auto schema = FindSchema(context, database, name);
  return schema ? catalog::IdOf(*schema) : ObjectId{};
}

ObjectId SchemaDatabaseId(duckdb::ClientContext* context, ObjectId schema_id) {
  auto schema = FindSchema(context, schema_id);
  return schema ? catalog::ParentIdOf(*schema) : ObjectId{};
}

void PutSchema(duckdb::ClientContext* context, std::string_view old_name,
               std::shared_ptr<const duckdb::CreateSchemaInfo> schema,
               catalog::Permissions perm) try {
  const auto database_id = catalog::ParentIdOf(*schema);
  RecordPut(context, old_name, database_id, duckdb::CatalogType::SCHEMA_ENTRY,
            catalog::IdOf(*schema), schema, perm);
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
    connector::GetSereneDBContext(*context).wrote_schemas = true;
  }
  const auto name = catalog::SchemaNameOf(*schema);
  const auto entry_id = catalog::IdOf(*schema);
  // Stated separately, as every schema's are: the entry is mutated in place
  // rather than versioned, so no create call carries them.
  const auto deps = EntryDependencies(*schema, perm);
  auto definition = std::move(schema);
  if (!old_name.empty() && old_name != name) {
    catalog->DropSchemaEntry(transaction, old_name);
  }
  if (auto entry = catalog->TryGetSchemaEntry(transaction, name)) {
    // The entry owns the CatalogSets of its whole contents, so a new version
    // would strand them: the definition is replaced in place instead.
    entry->SetDefinition(std::move(definition), perm);
  } else if (!catalog->CreateSchemaEntry(transaction, name,
                                         {std::move(definition), perm})) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent DDL on schema \"",
              name, "\""));
  }
  SetEntryDependencies(context, *catalog, entry_id, deps);
  BumpSchemaGeneration();
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

void DropSchemaEntry(duckdb::ClientContext* context, ObjectId database,
                     std::string_view name) try {
  auto catalog = DatabaseCatalogOf(context, database);
  if (!catalog) {
    // The database went with a DROP and its catalog with it.
    return;
  }
  auto transaction = context != nullptr
                       ? catalog->GetCatalogTransaction(*context)
                       : catalog->CommittedRead();
  if (context != nullptr) {
    duckdb::MetaTransaction::Get(*context).ModifyDatabase(
      catalog->GetAttached(), duckdb::DatabaseModificationType::ALTER_TABLE);
    connector::GetSereneDBContext(*context).wrote_schemas = true;
  }
  const auto previous = FindSchema(context, database, name);
  // Taken while the entry that owns their sets is still there: the contents go
  // with it and reach no DropEntry of their own, so nothing else retires what
  // they referenced -- and their edges are this catalog's, which outlives the
  // schema entry.
  std::vector<ObjectId> contents;
  if (auto entry = catalog->TryGetSchemaEntry(transaction, name)) {
    contents = SchemaEntryContentIds(*entry);
  }
  // Nothing here re-derives a foreign key's referenced half: a key never
  // crosses a schema (CREATE refuses one that would), so every table the ones
  // going away point at is going away with them.
  catalog->DropSchemaEntry(transaction, name);
  RetireEntryEdges(context, *catalog, contents);
  BumpSchemaGeneration();
} catch (const duckdb::TransactionException&) {
  ThrowConcurrentDdl();
}

std::vector<ObjectId> SchemaEntryContentIds(SereneDBSchemaEntry& schema) {
  std::vector<ObjectId> ids;
  const auto take = [&](duckdb::CatalogType slot) {
    schema.GetCatalogSet(slot).Scan([&](duckdb::CatalogEntry& entry) {
      // The oid every serenedb entry carries is its stable id, and the same
      // object shows up once per slot it occupies -- a view and its wrapper --
      // so the same id may be seen twice. Retiring twice is a no-op.
      ids.emplace_back(entry.oid);
    });
  };
  for (const auto slot :
       {duckdb::CatalogType::TOKENIZER_ENTRY, duckdb::CatalogType::TYPE_ENTRY,
        duckdb::CatalogType::MACRO_ENTRY,
        duckdb::CatalogType::TABLE_MACRO_ENTRY,
        duckdb::CatalogType::SEQUENCE_ENTRY, duckdb::CatalogType::INDEX_ENTRY,
        duckdb::CatalogType::TABLE_ENTRY}) {
    take(slot);
  }
  return ids;
}

void RetireEntryEdges(duckdb::ClientContext* context, duckdb::Catalog& owner,
                      std::span<const ObjectId> ids) {
  for (const auto id : ids) {
    SetEntryDependencies(context, owner, id, {});
  }
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
    schema.GetCatalogSet(duckdb::CatalogType::TOKENIZER_ENTRY)
      .Scan(transaction, [&](duckdb::CatalogEntry& entry) {
        visitor(entry.Cast<SereneDBTokenizerEntry>().Definition());
      });
  });
}

catalog::TokenizerRef FindTokenizerIn(duckdb::ClientContext* context,
                                      duckdb::Catalog& catalog, ObjectId id) {
  catalog::TokenizerRef found;
  VisitTokenizersIn(context, catalog, [&](catalog::TokenizerRef tokenizer) {
    if (tokenizer->GetId() == id) {
      found = std::move(tokenizer);
    }
  });
  return found;
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
  catalog::TokenizerRef found;
  VisitSessionTokenizers(context, [&](catalog::TokenizerRef tokenizer) {
    if (tokenizer->GetId() == id) {
      found = std::move(tokenizer);
    }
  });
  return found;
}

std::vector<catalog::HeldTokenizer> DatabaseTokenizers(
  duckdb::ClientContext* context, ObjectId database) {
  std::vector<catalog::HeldTokenizer> found;
  ScanDatabaseSlots(
    context, database, LookupSlots(duckdb::CatalogType::TOKENIZER_ENTRY),
    [&](duckdb::CatalogEntry& entry) {
      if (auto ref = DefinitionOf<SereneDBTokenizerEntry>(&entry)) {
        found.push_back({std::move(ref), entry.permissions});
      }
    });
  return found;
}

catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, std::string_view name,
                                    catalog::Permissions* perm) {
  return DefinitionOf<SereneDBTokenizerEntry>(
    LookupSchemaEntry(context, schema_id,
                      LookupSlots(duckdb::CatalogType::TOKENIZER_ENTRY), name),
    perm);
}

catalog::TokenizerRef FindTokenizer(duckdb::ClientContext* context,
                                    ObjectId schema_id, ObjectId id,
                                    catalog::Permissions* perm) {
  return DefinitionOf<SereneDBTokenizerEntry>(
    LookupSchemaEntryById(context, schema_id, id), perm);
}

std::vector<const duckdb::TypeCatalogEntry*> DatabaseTypes(
  duckdb::ClientContext* context, ObjectId database) {
  std::vector<const duckdb::TypeCatalogEntry*> found;
  ScanDatabaseSlots(context, database,
                    LookupSlots(duckdb::CatalogType::TYPE_ENTRY),
                    [&](duckdb::CatalogEntry& entry) {
                      if (auto* type = EntryOf<SereneDBTypeEntry>(&entry)) {
                        found.push_back(type);
                      }
                    });
  return found;
}

duckdb::optional_ptr<SereneDBCatalog> TryDatabaseCatalog(
  duckdb::ClientContext* context, ObjectId database) {
  return DatabaseCatalogOf(context, database);
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
  ScanDatabaseSlots(context, database, LookupSlots(type), visitor);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, duckdb::CatalogType type,
  std::string_view name) {
  return LookupSchemaEntry(context, schema_id, LookupSlots(type), name);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSchema(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId id) {
  return LookupSchemaEntryById(context, schema_id, id);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInSession(
  duckdb::ClientContext& context, ObjectId id) {
  return LookupEntryById(context, id);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInCatalog(
  duckdb::ClientContext* context, duckdb::Catalog& catalog, ObjectId id) {
  return LookupEntryIn(context, catalog, id);
}

duckdb::optional_ptr<duckdb::CatalogEntry> LookupInDatabase(
  duckdb::ClientContext* context, ObjectId database, ObjectId id) {
  auto sdb_catalog = DatabaseCatalogOf(context, database);
  return sdb_catalog ? LookupEntryIn(context, *sdb_catalog, id) : nullptr;
}

void VisitFunctions(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const duckdb::MacroCatalogEntry&)> visitor) {
  ScanDatabaseSlots(context, database,
                    LookupSlots(duckdb::CatalogType::MACRO_ENTRY),
                    [&](duckdb::CatalogEntry& entry) {
                      if (const auto* found = FunctionOf(&entry)) {
                        visitor(*found);
                      }
                    });
}

const duckdb::MacroCatalogEntry* FindFunction(duckdb::ClientContext* context,
                                              ObjectId schema_id,
                                              std::string_view name) {
  return FunctionOf(LookupSchemaEntry(
    context, schema_id, LookupSlots(duckdb::CatalogType::MACRO_ENTRY), name));
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
  ScanDatabaseSlots(
    context, database, LookupSlots(duckdb::CatalogType::SEQUENCE_ENTRY),
    [&](duckdb::CatalogEntry& entry) {
      if (const auto* seq = EntryOf<SereneDBSequenceEntry>(&entry)) {
        found.push_back(seq);
      }
    });
  return found;
}

std::vector<catalog::IndexInfoRef> RelationIndexes(
  duckdb::ClientContext* context, ObjectId schema_id, ObjectId relation_id) {
  std::vector<catalog::IndexInfoRef> found;
  ScanSchemaSlots(
    context, schema_id, LookupSlots(duckdb::CatalogType::INDEX_ENTRY),
    [&](duckdb::CatalogEntry& entry) {
      auto* index = dynamic_cast<SereneDBIndexEntry*>(&entry);
      if (index != nullptr && index->GetRelationId() == relation_id) {
        found.push_back(index->Definition());
      }
    });
  return found;
}

std::vector<catalog::IndexInfoRef> RelationIndexesIn(
  duckdb::ClientContext* context, duckdb::Catalog& catalog,
  ObjectId relation_id) {
  std::vector<catalog::IndexInfoRef> found;
  if (catalog.GetCatalogType() != kSereneDBCatalogType) {
    return found;
  }
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  // Off this catalog's own schema entries: an attach reads them before the
  // attachment is in the database manager, so nothing can resolve it by id yet.
  sdb_catalog.VisitSchemaEntries([&](SereneDBSchemaEntry& schema) {
    VisitRelationIndexEntries(
      context, schema, relation_id,
      [&](SereneDBIndexEntry& index) { found.push_back(index.Definition()); });
  });
  return found;
}

void DropTableEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name) {
  const auto* entry = Find<SereneDBTableEntry>(context, schema_id, name);
  const auto previous = entry != nullptr ? entry->Definition() : nullptr;
  DropSchemaObject(context, duckdb::CatalogType::TABLE_ENTRY, schema_id, name);
  if (previous) {
    RefreshForeignKeyTargets(context, *previous);
  }
}

void DropIndexEntry(duckdb::ClientContext* context, ObjectId schema_id,
                    std::string_view name) {
  ObjectId relation_id;
  if (auto previous = Find<SereneDBIndexEntry>(context, schema_id, name)) {
    relation_id = previous->GetRelationId();
  }
  DropSchemaObject(context, duckdb::CatalogType::INDEX_ENTRY, schema_id, name);
  RefreshRelationEntry(context, schema_id, relation_id);
}

void RefreshRelationIndexEntries(duckdb::ClientContext* context,
                                 ObjectId schema_id, ObjectId relation_id) {
  // A projection, not a mutation: the index says what it already said, and the
  // relation's own write recorded everything this operation states.
  catalog::Catalog::RecordedScope recorded;
  // Collected before anything is written: rebuilding one entry reads the very
  // set this walk holds, and the lock behind it is not recursive.
  const auto indexes = RelationIndexes(context, schema_id, relation_id);
  for (const auto& index : indexes) {
    // The same definition under the same name -- what changed is the relation
    // the wrapper projects, so the entry is rebuilt rather than replaced by a
    // new version of the index.
    PlaceEntry(context, index->GetName(), index, {});
  }
}

bool TableVanished(duckdb::ClientContext* context, ObjectId schema_id,
                   std::string_view name) {
  if (context == nullptr) {
    return false;
  }
  auto at = OpenSchema(context, schema_id, /*for_write=*/false);
  if (!at) {
    return false;
  }
  auto entry = at.Lookup(duckdb::CatalogType::TABLE_ENTRY, name);
  if (!entry || entry->timestamp >= duckdb::TRANSACTION_ID_START) {
    // Nothing under that name, or this transaction's own uncommitted version:
    // neither can have been dropped out from under it.
    return false;
  }
  // The version this statement resolved is committed, and a committed read no
  // longer finds it: another transaction dropped it and committed while this
  // one was open.
  return !Find<SereneDBTableEntry>(nullptr, schema_id, name);
}

void RefreshRelationEntry(duckdb::ClientContext* context, ObjectId schema_id,
                          ObjectId relation_id) {
  // A relation's own entry advertises a virtual column per indexed column, so
  // adding or dropping an index reshapes it. Only a table does -- a view entry
  // says nothing about the indexes over it.
  //
  // A projection, not a mutation: the table says what it already said.
  catalog::Catalog::RecordedScope recorded;
  if (const auto* table =
        Find<SereneDBTableEntry>(context, schema_id, relation_id)) {
    PlaceEntry(context, table->name.GetIdentifierName(), table->Definition(),
               table->permissions);
  }
}

void VisitForeignServers(
  duckdb::ClientContext* context, ObjectId database,
  absl::FunctionRef<void(const catalog::CreateForeignServerInfo&,
                         const catalog::Permissions&)>
    visitor) {
  ScanForeignServers(context, database, [&](duckdb::CatalogEntry& entry) {
    visitor(entry.Cast<SereneDBForeignServerEntry>().ForeignServer(),
            entry.permissions);
  });
}

catalog::ForeignServerRef FindForeignServer(duckdb::ClientContext* context,
                                            ObjectId database,
                                            std::string_view name,
                                            catalog::Permissions* perm) {
  auto at = OpenCatalog(context, database, /*for_write=*/false);
  return at ? DefinitionOf<SereneDBForeignServerEntry>(
                at.catalog->GetForeignServerSet().GetEntry(
                  *at.transaction, duckdb::Identifier{name}),
                perm)
            : nullptr;
}

catalog::ForeignServerRef FindForeignServer(duckdb::ClientContext* context,
                                            ObjectId database, ObjectId id,
                                            catalog::Permissions* perm) {
  auto at = OpenCatalog(context, database, /*for_write=*/false);
  return at ? DefinitionOf<SereneDBForeignServerEntry>(
                LookupEntryById(*at.transaction, *at.catalog, id), perm)
            : nullptr;
}

catalog::ForeignServerRef FindForeignServerAnywhere(
  duckdb::ClientContext* context, std::string_view name,
  catalog::Permissions* perm) {
  std::vector<ObjectId> databases;
  VisitDatabases(
    context, [&](const DatabaseRef& ref) { databases.push_back(ref.Id()); });
  for (const auto database_id : databases) {
    if (auto found = FindForeignServer(context, database_id, name, perm)) {
      return found;
    }
  }
  return nullptr;
}

std::vector<catalog::HeldForeignServer> CatalogForeignServers(
  SereneDBCatalog& catalog) {
  std::vector<catalog::HeldForeignServer> found;
  catalog.GetForeignServerSet().Scan([&](duckdb::CatalogEntry& entry) {
    auto& server = entry.Cast<SereneDBForeignServerEntry>();
    found.push_back({server.Definition(), server.permissions});
  });
  return found;
}

std::vector<catalog::HeldForeignServer> DatabaseForeignServers(
  duckdb::ClientContext* context, ObjectId database) {
  std::vector<catalog::HeldForeignServer> found;
  ScanForeignServers(context, database, [&](duckdb::CatalogEntry& entry) {
    auto& server = entry.Cast<SereneDBForeignServerEntry>();
    found.push_back({server.Definition(), server.permissions});
  });
  return found;
}

duckdb::optional_ptr<duckdb::CatalogEntry> PutEntry(
  duckdb::ClientContext* context, std::string_view old_name,
  std::shared_ptr<const duckdb::CreateInfo> info, catalog::Permissions perm) {
  auto placed = PlaceEntry(context, old_name, info, perm);
  // The siblings the version now in the set reshapes. Nothing here touches the
  // kind's own set, so it cannot come back round.
  if (placed) {
    RefreshEntrySiblings(context, *placed);
  }
  return placed;
}

void RenameEntry(duckdb::ClientContext* context, duckdb::CatalogType type,
                 ObjectId parent_id, std::string_view name,
                 std::string_view new_name) {
  auto& entry = RequireEntryOfKind(context, type, parent_id, name);
  if (auto renamed =
        RewrittenDefinition(context, entry, new_name, std::nullopt)) {
    PutEntry(context, name, std::move(renamed), entry.permissions);
  }
}

void RequireIndexOwner(const catalog::AccessContext& ax,
                       const catalog::CreateIndexInfoBase& index) {
  catalog::Permissions perm;
  const auto relation_id = index.GetRelationId();
  const auto schema_id = index.GetParentId();
  if (const auto* table =
        Find<SereneDBTableEntry>(ax.context, schema_id, relation_id)) {
    perm = table->permissions;
  } else if (const auto* view =
               Find<SereneDBViewEntry>(ax.context, schema_id, relation_id)) {
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
  PutEntry(ax.context, name, EntryDefinition(entry), std::move(perm));
}

void ChangeEntryOwner(const catalog::AccessContext& ax,
                      duckdb::CatalogType type, ObjectId parent_id,
                      std::string_view name, ObjectId new_owner,
                      std::string_view new_owner_name) {
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  const auto& perm = entry.permissions;
  catalog::RequireOwnerTransfer(ax, parent_id, perm, new_owner, new_owner_name,
                                pg::ToPgObjectTypeName(type), name);
  PutEntry(ax.context, name, EntryDefinition(entry),
           auth::TransferredOwner(perm, new_owner));
}

void SetEntryComment(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name,
                     std::string_view comment) {
  auto& entry = RequireEntryOfKind(ax.context, type, parent_id, name);
  RequireEntryOwner(ax, type, entry);
  if (auto recommented = RewrittenDefinition(ax.context, entry, {}, comment)) {
    PutEntry(ax.context, name, std::move(recommented), entry.permissions);
  }
}

void SetViewColumnComment(const catalog::AccessContext& ax, ObjectId schema_id,
                          std::string_view name, std::string_view column,
                          std::string_view comment) {
  auto& entry = RequireEntryOfKind(ax.context, duckdb::CatalogType::VIEW_ENTRY,
                                   schema_id, name);
  RequireEntryOwner(ax, duckdb::CatalogType::VIEW_ENTRY, entry);
  auto updated =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      entry.GetInfo());
  auto& info = *updated;
  // A view's column names are its query's, so a name the statement gave may be
  // an alias -- which is what the comment map is keyed by.
  duckdb::Identifier resolved{column};
  if (!absl::c_linear_search(info.names, resolved)) {
    const auto alias = absl::c_find(info.aliases, resolved);
    if (alias == info.aliases.end()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
        ERR_MSG("column \"", column, "\" of relation \"",
                entry.name.GetIdentifierName(), "\" does not exist"));
    }
    const auto index =
      static_cast<size_t>(std::distance(info.aliases.begin(), alias));
    SDB_ASSERT(index < info.names.size());
    resolved = info.names[index];
  }
  if (comment.empty()) {
    info.column_comments_map.erase(resolved);
  } else {
    info.column_comments_map[resolved] = duckdb::Value{std::string{comment}};
  }
  PutEntry(ax.context, name,
           catalog::NextViewVersion(
             ax.context,
             std::shared_ptr<const duckdb::CreateViewInfo>{updated.release()}),
           entry.permissions);
}

bool DropEntryObject(const catalog::AccessContext& ax, duckdb::CatalogType type,
                     ObjectId database_id, ObjectId parent_id,
                     std::string_view name, bool cascade, bool missing_ok) {
  auto* object = FindEntryOfKind(ax.context, type, parent_id, name);
  if (object == nullptr) {
    // The other half of the relation namespace still answers for the name, and
    // PG reports the kind mismatch rather than a missing relation.
    if (type == duckdb::CatalogType::SEQUENCE_ENTRY && parent_id.isSet() &&
        (Find<SereneDBTableEntry>(ax.context, parent_id, name) ||
         Find<SereneDBViewEntry>(ax.context, parent_id, name))) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not a sequence"));
    }
    if (missing_ok) {
      return false;
    }
    pg::ThrowUndefinedObject(type, name);
  }
  RequireEntryOwner(ax, type, *object);
  const auto id = catalog::IdOf(*object);
  auto plan = PlanEntryDrop(ax.context, type, id, cascade, name);
  catalog::GetCatalog().DropResolved(ax.context, database_id, parent_id,
                                     KindOf(object->type), id, name, plan);
  return true;
}

std::string EntryNameOfKind(duckdb::ClientContext* context,
                            duckdb::CatalogType type, ObjectId parent_id,
                            ObjectId id) {
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY: {
      auto role = FindRole(context, id);
      return role ? std::string{role->GetName()} : std::string{};
    }
    case DATABASE_ENTRY:
      return DatabaseName(context, id);
    case SCHEMA_ENTRY: {
      auto schema = FindSchema(context, id);
      return schema ? std::string{catalog::SchemaNameOf(*schema)}
                    : std::string{};
    }
    case FOREIGN_SERVER_ENTRY: {
      auto server = FindForeignServer(context, parent_id, id);
      return server ? std::string{server->GetName()} : std::string{};
    }
    case TOKENIZER_ENTRY:
    case TYPE_ENTRY:
    case MACRO_ENTRY:
    case VIEW_ENTRY:
    case SEQUENCE_ENTRY:
    case TABLE_ENTRY:
    case INDEX_ENTRY: {
      // Every schema child answers to the one by-id read: the database's own
      // object index, which is keyed on identity alone.
      auto entry = LookupSchemaEntryById(context, parent_id, id);
      return entry ? std::string{entry->name.GetIdentifierName()}
                   : std::string{};
    }
    default:
      return {};
  }
}

void DropEntryOfKind(duckdb::ClientContext* context, duckdb::CatalogType type,
                     ObjectId parent_id, std::string_view name) {
  switch (type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
      DropRoleEntry(context, name);
      return;
    case DATABASE_ENTRY:
      DropDatabaseEntry(context, name);
      return;
    case SCHEMA_ENTRY:
      DropSchemaEntry(context, parent_id, name);
      return;
    case TABLE_ENTRY:
      DropTableEntry(context, parent_id, name);
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
    case VIEW_ENTRY:
    case SEQUENCE_ENTRY:
      DropSchemaObject(context, type, parent_id, name);
      return;
    default:
      return;
  }
}

namespace {

// Boot replay's two sequence steps. A create meets the sequence for the first
// time and its counter is seeded from what the log already holds; a replace
// keeps the counter of the version it supersedes -- a nextval against either
// advances the one both share.
void ReplaySequenceRecord(
  ObjectId schema_id, ObjectId id, std::string_view old_name, bool replace,
  std::shared_ptr<const duckdb::CreateSequenceInfo> info,
  const catalog::Permissions& perm) {
  const auto& options = catalog::SequenceOptionsOf(*info);
  auto placed = PutEntry(nullptr, old_name, std::move(info), perm);
  // A rewrite inherited its predecessor's counter when the entry was built; a
  // sequence the log is meeting for the first time gets one seeded from the
  // durable value the SetSequence records after it will fold in.
  if (const auto* seq =
        dynamic_cast<const SereneDBSequenceEntry*>(placed.get());
      seq != nullptr && !seq->Counter()) {
    seq->AdoptCounter(catalog::ReloadedCounter(id, options));
  }
}

// The definition a record carries, as the kind that wrote it. What it resolved
// to when it was written is what has to go back: a rename since has moved the
// names it was taken by.
template<typename T>
std::shared_ptr<const T> RecordInfo(const catalog::wal::PutEntry& e) {
  return std::static_pointer_cast<const T>(e.info);
}

}  // namespace

void ReplayEntryRecord(const catalog::wal::PutEntry& e) {
  const bool replace = e.mode == catalog::wal::PutMode::Replace;
  const auto old_name = replace
                          ? EntryNameOfKind(nullptr, e.type, e.parent_id, e.id)
                          : std::string{};
  switch (e.type) {
    using enum duckdb::CatalogType;
    case ROLE_ENTRY:
      PutRole(nullptr, old_name, RecordInfo<catalog::CreateRoleInfo>(e));
      return;
    case DATABASE_ENTRY: {
      auto database = RecordInfo<catalog::CreateDatabaseInfo>(e);
      const auto name = database->GetName();
      PutDatabase(nullptr, old_name, database, e.perm);
      // The attachment everything the log says about this database needs. Its
      // catalog alone: the file is opened once the whole log is in, so the
      // data WAL replays against a catalog that is already whole. A database
      // never changes name -- the only Replace it takes is a GRANT -- so the
      // attachment this reaches is always the one already under `name`.
      connector::AttachDatabaseCatalog(database->GetId(), name);
      return;
    }
    case SCHEMA_ENTRY:
      PutSchema(nullptr, old_name, RecordInfo<duckdb::CreateSchemaInfo>(e),
                e.perm);
      return;
    case SEQUENCE_ENTRY:
      ReplaySequenceRecord(e.parent_id, e.id, old_name, replace,
                           RecordInfo<duckdb::CreateSequenceInfo>(e), e.perm);
      return;
    case TOKENIZER_ENTRY:
    case TYPE_ENTRY:
    case FOREIGN_SERVER_ENTRY:
    case MACRO_ENTRY:
    case VIEW_ENTRY:
      PutEntry(nullptr, old_name, e.info, e.perm);
      return;
    case INDEX_ENTRY:
      // An index has no owner and no ACL: every privilege decision reads the
      // relation it is built on.
      PutEntry(nullptr, old_name, e.info);
      return;
    default:
      SDB_FATAL(STARTUP, "catalog: no set holds a ",
                duckdb::CatalogTypeToString(e.type));
  }
}

void ReplayTableRecord(const catalog::wal::PutTable& e) {
  const bool replace = e.mode == catalog::wal::PutMode::Replace;
  const auto old_name =
    replace ? EntryNameOfKind(nullptr, duckdb::CatalogType::TABLE_ENTRY,
                              e.schema_id, e.id)
            : std::string{};
  PutEntry(nullptr, old_name, e.info, e.perm);
  // A table's owned sequences ride its record; only a create names any, so they
  // are performed under the table's own mode.
  for (const auto& sequence : e.sequences) {
    const auto sequence_name =
      replace ? EntryNameOfKind(nullptr, duckdb::CatalogType::SEQUENCE_ENTRY,
                                e.schema_id, sequence.id)
              : std::string{};
    ReplaySequenceRecord(e.schema_id, sequence.id, sequence_name, replace,
                         sequence.info, sequence.perm);
  }
}

void RefreshForeignKeyTargets(duckdb::ClientContext* context,
                              const duckdb::CreateTableInfo& table) {
  const auto schema = FindSchema(context, catalog::ParentIdOf(table));
  const auto database_id = schema ? catalog::ParentIdOf(*schema) : ObjectId{};
  for (const auto& constraint : table.constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const ObjectId target{
      constraint->Cast<duckdb::ForeignKeyConstraint>().host_referenced_id};
    if (!target.isSet() || target == catalog::IdOf(table)) {
      continue;
    }
    if (const auto* held =
          FindIn<SereneDBTableEntry>(context, database_id, target)) {
      RefreshRelationEntry(context, ObjectId{held->ParentSchema().oid}, target);
    }
  }
}

void RefreshForeignKeyReferents(duckdb::ClientContext* context,
                                ObjectId database) {
  // A referenced table's entry carries the half of a foreign key that makes a
  // DELETE against it look for children, and that half is derived from the
  // edges the referencing table records. Boot places entries in id order, so a
  // parent is built before the child that points at it and cannot see the edge
  // yet -- every parent has to be rebuilt once all of them are in the set.
  std::vector<std::pair<ObjectId, ObjectId>> referenced;
  VisitDefinitions<SereneDBTableEntry>(
    context, database,
    [&](const catalog::TableInfoRef& table, const catalog::Permissions&) {
      for (const auto& constraint : table->constraints) {
        if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
          continue;
        }
        const ObjectId target{
          constraint->Cast<duckdb::ForeignKeyConstraint>().host_referenced_id};
        if (target.isSet() && target != catalog::IdOf(*table)) {
          referenced.emplace_back(target, ObjectId{});
        }
      }
    });
  // Resolved after the walk: opening a relation's set from inside a scan of the
  // schema's own re-enters a lock that does not nest.
  for (auto& [target, schema_id] : referenced) {
    if (const auto* held =
          FindIn<SereneDBTableEntry>(context, database, target)) {
      schema_id = ObjectId{held->ParentSchema().oid};
    }
  }
  for (const auto& [target, schema_id] : referenced) {
    if (schema_id.isSet()) {
      RefreshRelationEntry(context, schema_id, target);
    }
  }
}

void VisitRelationIndexEntries(
  duckdb::ClientContext* context, SereneDBSchemaEntry& schema,
  ObjectId relation_id, absl::FunctionRef<void(SereneDBIndexEntry&)> visitor) {
  const auto emit = [&](duckdb::CatalogEntry& entry) {
    // The ART a serenedb index is mirrored by is storage, not a catalog object:
    // it shares this set because duckdb builds it there, and a catalog walk has
    // nothing to say about it.
    auto* index = dynamic_cast<SereneDBIndexEntry*>(&entry);
    if (index != nullptr && index->GetRelationId() == relation_id) {
      visitor(*index);
    }
  };
  if (context != nullptr) {
    schema.Scan(*context, duckdb::CatalogType::INDEX_ENTRY, emit);
  } else {
    schema.Scan(duckdb::CatalogType::INDEX_ENTRY, emit);
  }
}

void VisitCatalogSetEntries(
  duckdb::ClientContext& context, ObjectId database, duckdb::CatalogType set,
  absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                         duckdb::CatalogEntry&)>
    visitor) {
  auto duck_catalog = DatabaseCatalogOf(&context, database);
  if (!duck_catalog) {
    return;
  }
  duck_catalog->ScanSchemas(context, [&](duckdb::SchemaCatalogEntry& entry) {
    auto& schema_entry = entry.Cast<SereneDBSchemaEntry>();
    auto schema = schema_entry.Definition();
    if (!schema) {
      return;
    }
    schema_entry.Scan(context, set, [&](duckdb::CatalogEntry& object_entry) {
      visitor(*schema, object_entry);
    });
  });
}

void VisitTableEntries(duckdb::ClientContext& context, ObjectId database,
                       absl::FunctionRef<void(const duckdb::CreateSchemaInfo&,
                                              const SereneDBTableEntry&)>
                         visitor) {
  VisitCatalogSetEntries(
    context, database, duckdb::CatalogType::TABLE_ENTRY,
    [&](const duckdb::CreateSchemaInfo& schema,
        duckdb::CatalogEntry& object_entry) {
      // Views and the index-name-as-table wrappers share this set; neither is
      // a SereneDBTableEntry, so the cast is the filter.
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

SereneDBTableEntry* FindTableEntryIn(duckdb::ClientContext* context,
                                     ObjectId database, ObjectId id) {
  auto catalog = DatabaseCatalogOf(context, database);
  if (!catalog) {
    return nullptr;
  }
  const auto transaction =
    context != nullptr && context->transaction.HasActiveTransaction()
      ? catalog->GetCatalogTransaction(*context)
      : catalog->CommittedRead();
  auto entry = LookupEntryById(transaction, *catalog, id);
  return entry ? dynamic_cast<SereneDBTableEntry*>(entry.get()) : nullptr;
}

}  // namespace sdb::catalog
