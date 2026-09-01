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

#pragma once

#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/database.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/log/store.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::catalog {

class SereneDBDatabaseEntry;
class SereneDBTableEntry;

// Mutation callback: fill `updated` with the changed clone (leave it null for
// a no-op). Signal errors by throwing (pg::SqlException for user-facing ones).
template<typename T>
using ChangeCallback =
  absl::AnyInvocable<void(const T&, duckdb::unique_ptr<T>&)>;

// Raised when a catalog mutation reaches an object a concurrently committed
// transaction has dropped. We do not take PostgreSQL's ACCESS EXCLUSIVE lock on
// DDL, so the two transactions cannot be ordered after the fact: 40001 is what
// PG 18 itself raises when a snapshot-isolated transaction's target was deleted
// by a concurrent commit, and it is the answer a client can retry.
[[noreturn]] void ThrowConcurrentlyDropped(duckdb::CatalogType type,
                                           std::string_view name);
// Same, for the sites that hold only the id -- a dependency node whose object
// is already gone.
[[noreturn]] void ThrowConcurrentlyDropped(ObjectId id);

struct CreateTableOperationOptions {
  // A valid id puts CreateTable in search-CTAS mode: pre-allocated by the
  // search insert operator and WITHOUT a backing store table -- the data side
  // creates it under its own transaction. An invalid (default) id creates a
  // regular table with a fresh id and its store table.
  ObjectId table_id;
  // IF NOT EXISTS: an existing relation of the same name makes CreateTable
  // return null instead of throwing "already exists".
  bool if_not_exists = false;
};

struct CreateIndexOperationOptions {
  // IF NOT EXISTS: an existing relation of the same name makes the create
  // return null instead of throwing "already exists".
  bool if_not_exists = false;
  // What the key expressions resolved to when the statement was bound --
  // collected by the binder's own lookups, the way a view's or a function's
  // references are.
  duckdb::LogicalDependencyList dependencies;
};

// Who is running a catalog operation, and the statement they are running it
// from. The statement is what store DDL executes on, so a DDL that emits store
// ops must be reached through one of the overloads that carries it -- the ones
// taking only a role are for boot, background tasks and read-side checks.
struct AccessContext {
  ObjectId role;
  duckdb::ClientContext* context = nullptr;
};

AccessContext ActingAs(duckdb::ClientContext& context);

inline AccessContext ActingAs(ObjectId role) { return {role}; }

// For the callers whose acting role is not the one the client context carries
// (a compensating drop, an owner-scoped create) but whose mutation still
// belongs to the statement's transaction.
inline AccessContext ActingAs(ObjectId role, duckdb::ClientContext& context) {
  return {role, &context};
}

inline AccessContext NoAccessCheck() { return {id::kRootUser}; }

inline AccessContext NoAccessCheck(duckdb::ClientContext& context) {
  return {id::kRootUser, &context};
}

// Restates what a bind of this table version's DEFAULT and CHECK expressions
// resolves to, replacing the piece-tagged references the version it was built
// from carried: a sub-object that left is simply not collected again. This is
// the only half of a table's references a record has to carry -- what the
// definition names by id is derived wherever a version is placed. Nothing to
// do without a context (replay, a background drop): those never bind.
void RefreshExpressionReferences(duckdb::ClientContext* context,
                                 duckdb::CreateTableInfo& table);

// ALTER TABLE ... OWNER TO. `type` is the type the statement names the table
// as; it drives the error phrasing and the ACL shape, and differs from Table
// only for the index kinds, which a statement may name a table by.
void ChangeTableOwner(const AccessContext& ax,
                      const duckdb::CreateTableInfo& table,
                      duckdb::CatalogType type, ObjectId new_owner,
                      std::string_view new_owner_name);

// Returns the created table, or null for the if_not_exists no-op. The SERIAL
// columns arrive beside the info: the catalog resolves each sequence's name,
// stamps the owning table and sets the column's nextval default, none of which
// the statement can do for itself.
const SereneDBTableEntry* CreateTable(
  const AccessContext& ax, ObjectId database_id, std::string_view schema,
  duckdb::unique_ptr<duckdb::CreateTableInfo> info,
  std::vector<SerialSequence> sequences,
  CreateTableOperationOptions operation_options);

// One ALTER TABLE action through duckdb's own alter, with the settled
// definition recorded as the table's next version. A search table takes the
// definition half alone: it has no duck rows to move. Anything the info
// carries beyond duckdb's own alter (a column's catalog oid, a constraint's
// name and ids) must be stamped by the caller first. The context form is the
// cascade victim's: authority was checked on the seed, as postgres has it.
void ApplyTableAlter(const AccessContext& ax,
                     const duckdb::CreateTableInfo& table,
                     duckdb::AlterInfo& info);
void ApplyTableAlter(duckdb::ClientContext* context,
                     const duckdb::CreateTableInfo& table,
                     duckdb::AlterInfo& info);

bool DropTable(const AccessContext& ax, std::string_view database,
               std::string_view schema, std::string_view name, bool cascade,
               bool missing_ok);

// Drops `columns` from a surviving table through duckdb's own alter: every
// covering index falls first, the store indexes that block the positional
// reshape are recreated around it, and the constraints the columns bound go
// with them (RemoveColumn's cascade). Dropping the last column keeps the
// zero-column table, as postgres does.
void DropTableColumns(duckdb::ClientContext* context,
                      const SereneDBTableEntry& table,
                      std::vector<ObjectId> dropped_columns);

// One index's whole removal: the entry, the store half and the artifact half.
// `storage` is the handle the index's entry carried, read by the caller while
// it could still see it.
void DropIndexLocked(duckdb::ClientContext* context, ObjectId database_id,
                     const CreateIndexInfo& index,
                     std::shared_ptr<search::InvertedIndexStorage> storage,
                     bool cascade);

// The artifact half of an inverted index's drop, deferred to the commit: the
// storage the entry carried -- read by the caller before the set dropped it --
// is marked dropped only once the removal is durable, so an MVCC reader that
// still sees the entry resolves postings until then. Runs inline without a
// transaction (a compensating drop).
void DropIndexArtifacts(duckdb::ClientContext* context, ObjectId database_id,
                        const CreateIndexInfo& index,
                        std::shared_ptr<search::InvertedIndexStorage> storage);

// The artifact half of a search table's drop, deferred the same way: the
// shard's directories go when the last holder of its SearchTable lets go.
void DropSearchTableArtifacts(duckdb::ClientContext* context,
                              const SereneDBTableEntry& table);

// Places one index record: the namespace checks, the record's edges and the
// entry, under the caller's mutation scope. Returns the placed entry.
duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndexImpl(
  duckdb::ClientContext* context, CreateIndexInfo& index,
  CreateIndexOperationOptions operation_options);

std::shared_ptr<const Index> CreateInvertedIndex(
  const AccessContext& ax, duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema, const duckdb::CatalogEntry& relation,
  std::string name, std::vector<CreateIndexColumn>&& columns,
  InvertedIndexOptions options, ExpressionData predicate,
  CreateIndexOperationOptions operation_options);

// ALTER INDEX ... RENAME TO, once the caller has resolved the index and its
// checks. An index occupies two slots -- its entry and the relation-namespace
// scan wrapper -- so the move is one placement of one renamed record
// (PlaceEntry with the old name), which also files the store op the replay
// re-applies.
void RenameIndex(duckdb::ClientContext* context, const CreateIndexInfo& index,
                 std::string_view new_name);

// ALTER TABLE ... DROP COLUMN.
void DropTableColumn(const AccessContext& ax, ObjectId database_id,
                     const duckdb::CreateTableInfo& table,
                     std::string_view column, bool if_exists);

// The three namespaces postgres reports an "already exists" for.
enum class NameKind : uint8_t {
  Relation,
  Type,
  Role,
};
[[noreturn]] void ThrowDuplicateName(NameKind kind, std::string_view name);

std::optional<ObjectId> TryFindSchemaId(duckdb::ClientContext* context,
                                        ObjectId database_id,
                                        std::string_view name);

// CREATE inside `parent_id`: throws 42501 unless `role` has CREATE on the
// schema. Silent when the schema is gone -- the create's own resolution is
// what reports that.
void RequireCreateOn(duckdb::ClientContext* context, ObjectId role,
                     ObjectId parent_id);
// PG's ownership test for an ALTER or a DROP: the actor must own the object,
// directly, through a role it is a member of, or as a superuser.
void RequireOwner(duckdb::ClientContext* context, ObjectId role,
                  const Permissions& perm, std::string_view noun,
                  std::string_view name);
// What ALTER ... OWNER TO requires beyond ownership of the object itself: the
// actor must be able to SET ROLE to the new owner, and the new owner must be
// able to create in the schema that will hold it. A superuser bypasses both.
void RequireOwnerTransfer(const AccessContext& ax, ObjectId schema_id,
                          const Permissions& perm, ObjectId new_owner,
                          std::string_view new_owner_name,
                          std::string_view noun, std::string_view name);

// PG: creating inside a database needs the matching privilege on it. Exported
// beside the other ownership checks because the duckdb catalog override is
// where the create happens.
void RequireDatabaseAccess(duckdb::ClientContext* context, ObjectId role,
                           const catalog::SereneDBDatabaseEntry* database,
                           AclMode need);

// The mutation mutex and boot; every statement's logic lives in free
// functions built on these.
class Catalog final {
 public:
  // All mutators throw on failure: pg::SqlException with the PG-compatible
  // errcode/message for user-facing errors, SqlException for internal
  // (store/serialization) failures.
  //
  // EXISTS and Drop* with `missing_ok` return false instead of throwing when
  // the object already exists / is absent.
  // Returns the id and owner of the public schema it wrote, which the attach
  // being built around this call writes -- there is no set for it here. Unset
  // for the if_not_exists no-op.
  std::pair<ObjectId, Permissions> CreateDatabase(
    const AccessContext& ax, duckdb::unique_ptr<CreateDatabaseInfo> database,
    ObjectId owner, bool if_not_exists);
  void CreateRole(const AccessContext& ax, duckdb::unique_ptr<Role> role);
  using AclMutator = auth::AclMutator;
  void ChangeRole(const AccessContext& ax, std::string_view name,
                  std::string_view verb, bool allow_self,
                  ChangeCallback<Role> callback);
  void ChangeDefaultAcl(const AccessContext& ax, std::string_view role_name,
                        ObjectId schema, char objtype, duckdb::CatalogType type,
                        absl::AnyInvocable<void(Acl&)> mutate);
  void ChangeMembership(const AccessContext& ax, ObjectId role,
                        std::string_view role_name, ObjectId member,
                        std::string_view member_name, const Membership& edge,
                        bool revoke, bool admin_option_only);
  void ChangeDatabaseAcl(const AccessContext& ax, ObjectId database_id,
                         AclMutator mutate);
  // The foreign-server attachments the cascade leaves behind are the caller's:
  // they are instance-global while the entries holding their names are this
  // database's, so they have to be captured before the attachment -- and the
  // set that holds them -- goes away. SereneDBCatalog::OnDetach does that and
  // runs the detaches afterwards.
  void DropDatabase(const AccessContext& ax, std::string_view name,
                    duckdb::shared_ptr<void> keep_alive);
  bool DropRole(const AccessContext& ax, std::string_view role,
                bool missing_ok);
  void FinalizeLoad();

  // Runs `fn` with catalog mutations excluded, or does nothing when a mutation
  // is already running. Compaction is the only caller: it reads the catalog and
  // rewrites the log a commit writes both halves of, and it is opportunistic,
  // so losing that race is a skip rather than a wait. Returns whether `fn` ran.
  bool TryExcludingMutations(absl::FunctionRef<void()> fn);

  // The value a sequence's counter starts from. The definition itself is
  // recorded by the commit walk, like every other version.
  void RecordSequenceSeed(duckdb::ClientContext* context, ObjectId id,
                          uint64_t seed);

  // Everything the DROP of an entry-is-the-object kind does once its target is
  // resolved and ownership checked: the entry out of the set its kind lives in,
  // where duckdb's dependency walk takes the cascade, plus what the kind adds
  // (counter row, index victims). Assumes `_mutex` is held.
  void DropResolved(duckdb::ClientContext* context, ObjectId parent_id,
                    duckdb::CatalogType type, ObjectId id,
                    std::string_view name, bool cascade);

  // The lock every mutation runs under, for the DDL that lives where duckdb
  // hands it over rather than here: a statement resolves its target, checks
  // the privilege and writes the entry under one scope.
  class [[nodiscard]] MutationScope {
   public:
    explicit MutationScope(Catalog& catalog) : _lock{&catalog._mutex} {}

   private:
    absl::MutexLock _lock;
  };

 private:
  void ChangeRoleImpl(
    duckdb::ClientContext* context, ObjectId actor_id, std::string_view name,
    absl::FunctionRef<void(duckdb::ClientContext*, const Role&)> check,
    ChangeCallback<Role> callback);

  mutable absl::Mutex _mutex;
};

// Builds the single in-process catalog, loads boot state, and attaches the
// databases. Throws on failure.
void InitCatalog();
void ShutdownCatalog();

// The id of the database named `name`, unset when there is none. Reads the
// cluster-global DATABASE_ENTRY set through the process-wide cache, so a
// DDL-free workload pays a relaxed load and a hash lookup.
ObjectId GetDatabaseId(std::string_view name);
Catalog& GetCatalog();
// Null before InitCatalog and after ShutdownCatalog, for callers that can run
// during startup-failure or shutdown teardown.
Catalog* TryGetCatalog();

}  // namespace sdb::catalog
