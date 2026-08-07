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
#include "catalog/column_expr.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object_dependency.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"

namespace sdb::connector {

struct DatabaseRef;

}  // namespace sdb::connector
namespace sdb::catalog {

// Mutation callback: fill `updated` with the changed clone (leave it null for
// a no-op). Signal errors by throwing (pg::SqlException for user-facing ones).
template<typename T>
using ChangeCallback = absl::AnyInvocable<void(const T&, std::shared_ptr<T>&)>;

// The same for a table, whose next version is a fresh info rather than an edit
// of the one handed in. Null means a sanctioned no-op.
using TableChange =
  absl::AnyInvocable<TableInfoRef(const duckdb::CreateTableInfo&)>;

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
  // A valid id puts CreateTable in CTAS mode: the entry is created with this
  // pre-allocated id and WITHOUT a backing store table (the data side creates
  // the store table itself, under its own transaction). CTAS pre-allocates the
  // id at plan time so the store table name is known to the insert operator. An
  // invalid (default) id creates a regular table with a freshly allocated id
  // and its store table.
  ObjectId table_id;
  // IF NOT EXISTS: an existing relation of the same name makes CreateTable
  // return null instead of throwing "already exists".
  bool if_not_exists = false;
};

struct CreateIndexOperationOptions {
  // IF NOT EXISTS: an existing relation of the same name makes the create
  // return null instead of throwing "already exists".
  bool if_not_exists = false;
};

// The relation an index is built on -- one of the two kinds that can carry
// one, with the owner and ACL its entry holds.
struct IndexRelation {
  TableInfoRef table;
  const duckdb::ViewCatalogEntry* view = nullptr;
  Permissions perm;

  ObjectId GetId() const noexcept {
    return table ? catalog::IdOf(*table)
                 : (view != nullptr ? ObjectId{view->oid} : ObjectId{});
  }
  ObjectId GetParentId() const noexcept {
    return table ? catalog::ParentIdOf(*table)
                 : (view != nullptr ? ObjectId{view->ParentSchema().oid}
                                    : ObjectId{});
  }
  std::string_view GetName() const noexcept {
    return table ? catalog::TableNameOf(*table)
                 : (view != nullptr ? view->name.GetIdentifierName()
                                    : std::string_view{});
  }
};

// The sequence ids each level of a drop has to name, keyed by what owns them:
// the schema for a free-standing sequence, the table for one it owns. Read off
// the entries once, before the sets holding them can go away.
struct SequenceOwners {
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> by_owner;

  std::vector<ObjectId> Of(ObjectId owner) const {
    const auto it = by_owner.find(owner);
    return it == by_owner.end() ? std::vector<ObjectId>{} : it->second;
  }
};

// Every sequence of `database_id`, filed under the object that owns it.
// `context` is the statement whose drop this is, or null at boot.
SequenceOwners CollectSequenceOwners(duckdb::ClientContext* context,
                                     ObjectId database_id);

// The indexes each level of a drop has to name, keyed by the relation they are
// built on. Read off the entries once, before the sets holding them can go
// away.
struct IndexOwners {
  containers::FlatHashMap<ObjectId, std::vector<IndexInfoRef>> by_relation;

  std::vector<IndexInfoRef> Of(ObjectId relation) const {
    const auto it = by_relation.find(relation);
    return it == by_relation.end() ? std::vector<IndexInfoRef>{} : it->second;
  }
};

// Every index of `database_id`, filed under the relation it covers.
IndexOwners CollectIndexOwners(duckdb::ClientContext* context,
                               ObjectId database_id);

// Who is running a catalog operation, and the statement they are running it
// from. The statement is what store DDL executes on, so a DDL that emits store
// ops must be reached through one of the overloads that carries it -- the ones
// taking only a role are for boot, background tasks and read-side checks.
struct AccessContext {
  ObjectId role;
  AclMode need = AclMode::NoRights;
  duckdb::ClientContext* context = nullptr;
};

AccessContext RequireAccess(duckdb::ClientContext& context, AclMode need);

inline AccessContext RequireAccess(ObjectId role, AclMode need) {
  return {role, need};
}

AccessContext ActingAs(duckdb::ClientContext& context);

inline AccessContext ActingAs(ObjectId role) { return {role}; }

// For the callers whose acting role is not the one the client context carries
// (a compensating drop, an owner-scoped create) but whose mutation still
// belongs to the statement's transaction.
inline AccessContext ActingAs(ObjectId role, duckdb::ClientContext& context) {
  return {role, AclMode::NoRights, &context};
}

inline AccessContext NoAccessCheck() { return {id::kRootUser}; }

inline AccessContext NoAccessCheck(duckdb::ClientContext& context) {
  return {id::kRootUser, AclMode::NoRights, &context};
}

using PendingDrops =
  containers::FlatHashMap<ObjectId, std::vector<std::weak_ptr<DropTask>>>;

// Access enforcement for a definition read by name: `perm` is the entry's own
// owner and ACL. Throws 42501 when `role` may not read it.
void RequireAccess(duckdb::ClientContext* context, ObjectId role,
                   duckdb::CatalogType type, std::string_view name,
                   const Permissions& perm, AclMode need);

// Every object one table's definition names, with the sub-object that names
// it: derived from the info, never stored. The ids its columns and foreign keys
// carry directly are already there, and what its DEFAULT, generated-column and
// CHECK bodies name is on the expression node stating it.
std::vector<TableReference> TableReferences(
  const duckdb::CreateTableInfo& info);

// The same as duckdb's dependency list, with the roles the table and its
// columns grant to. Nothing else produces an edge, which is why the reverse
// index can be rebuilt from the definitions alone -- at boot, and for the
// objects a transaction has rewritten but not yet committed.
duckdb::LogicalDependencyList TableDependencies(
  const duckdb::CreateTableInfo& info, const Permissions& perm);

// The next version of one view, function or index: the same definition with
// the resolution its body implies taken now, on duckdb's own
// CreateInfo::dependencies. Only the catalog the statement sees knows what the
// names in it point at, so the resolution is taken where the version is built
// and carried with the info -- boot reads the record back after a rename has
// moved those names, and re-deriving it there would drop the edges without a
// word.
//
// The siblings of NextTableVersion below, one per kind whose definition names
// other objects. A secondary index names none, and comes back unchanged.
std::shared_ptr<const duckdb::CreateViewInfo> NextViewVersion(
  duckdb::ClientContext* context,
  std::shared_ptr<const duckdb::CreateViewInfo> view);
std::shared_ptr<const duckdb::CreateMacroInfo> NextFunctionVersion(
  duckdb::ClientContext* context,
  std::shared_ptr<const duckdb::CreateMacroInfo> function);
IndexInfoRef NextIndexVersion(duckdb::ClientContext* context,
                              const IndexInfoRef& index);

// The next version of one table, with its identity stamped and the resolution
// of its bodies taken now: a record carries the resolution of the version it
// describes, and a rename after it would move the names it was taken by.
TableInfoRef NextTableVersion(duckdb::ClientContext* context, ObjectId id,
                              ObjectId schema_id, TableInfoRef info);

// The table `schema`.`name` names, with the read privilege `ax` asks for
// enforced. Null when there is no such table -- including when the name is
// held by a view or an index.
TableInfoRef GetTable(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view name,
                      Permissions* perm = nullptr);

// Cross-tree fixups for DROP `seed`; composition cleanup is async. The
// Restrict form refuses the drop when the plan turns out to be a cascade the
// statement did not ask for.
DropPlan ComputeDropPlan(duckdb::ClientContext* context, ObjectId seed);
DropPlan ComputeDropPlanRestrict(duckdb::ClientContext* context, ObjectId seed,
                                 bool cascade, std::string_view kind,
                                 std::string_view name);
// Plan for ALTER TABLE DROP COLUMN: rewrite the owning table without the
// column and cascade-drop every index covering it (PG column->index cascade).
DropPlan ComputeColumnDropPlan(duckdb::ClientContext* context,
                               const TableInfoRef& table,
                               const Permissions& perm, ObjectId col_id);
// Records the plan's cross-tree mutations: the rewritten definition of each
// surviving table and a removal for each dependent the cascade takes with it.
// Those records are also what applies the plan.
void CommitDropPlan(duckdb::ClientContext* context,
                    CatalogStore::WriteContext& ctx, DropPlan& plan);

// Publishes the entries CommitDropPlan recorded. Runs after the batch, never
// inside it: the store ops that reshape a rewritten table's storage execute
// between the two, and an entry built ahead of them binds the storage they
// replace.
void PublishDropPlan(duckdb::ClientContext* context, const DropPlan& plan);

// A monotonic count of the catalog mutations this process has performed, and
// the identity a cached plan is checked against (SereneDBCatalog::
// GetCatalogVersion, duckdb's PreparedStatementData::RequireRebind).
//
// Read without a transaction, because the extended protocol binds between
// statements and a session outside one still has to notice that its plan is
// stale. Bumped where a mutation is performed rather than where it becomes
// visible: an extra re-plan costs a re-bind, a missed one serves rows against
// a descriptor the client no longer holds.
uint64_t CatalogVersion() noexcept;

// Whether `schema_id` still holds anything a RESTRICT drop would refuse over.
bool CheckSchemaEmptyDependency(duckdb::ClientContext* context,
                                ObjectId schema_id);

// The three namespaces postgres reports an "already exists" for.
enum class NameKind : uint8_t {
  Relation,
  Type,
  Role,
};
[[noreturn]] void ThrowDuplicateName(NameKind kind, std::string_view name);

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
                           const connector::DatabaseRef& database,
                           AclMode need);

class Catalog final {
 public:
  explicit Catalog();

  // All mutators throw on failure: pg::SqlException with the PG-compatible
  // errcode/message for user-facing errors, SqlException for internal
  // (store/serialization) failures. Create* whose statement supports IF NOT
  // EXISTS and Drop* with `missing_ok` return false instead of throwing when
  // the object already exists / is absent.
  // Returns the public schema it wrote, which the attach being built around
  // this call writes -- there is no set for it here. Empty for the
  // if_not_exists no-op.
  HeldSchema CreateDatabase(const AccessContext& ax,
                            std::shared_ptr<CreateDatabaseInfo> database,
                            ObjectId owner, bool if_not_exists);
  void CreateRole(const AccessContext& ax,
                  std::shared_ptr<CreateRoleInfo> role);
  // Returns the created table, or null for the if_not_exists no-op. The
  // SERIAL columns arrive beside the info: the catalog resolves each sequence's
  // name, stamps the owning table and sets the column's nextval default,
  // none of which the statement can do for itself.
  TableInfoRef CreateTable(const AccessContext& ax, ObjectId database_id,
                           std::string_view schema,
                           std::shared_ptr<duckdb::CreateTableInfo> info,
                           std::vector<SerialSequence> sequences,
                           CreateTableOperationOptions operation_options);
  // `relation` is the already-resolved table or view the index is built on;
  // callers that only have a name resolve it themselves. Returns the created
  // index, or null for the if_not_exists no-op.
  IndexInfoRef CreateSecondaryIndex(
    const AccessContext& ax, const IndexRelation& relation, std::string name,
    std::vector<CreateIndexColumn>&& columns, bool unique,
    CreateIndexOperationOptions operation_options);
  IndexInfoRef CreateInvertedIndex(
    const AccessContext& ax, duckdb::ClientContext& context,
    ObjectId database_id, std::string_view schema,
    const IndexRelation& relation, std::string name,
    std::vector<CreateIndexColumn>&& columns, InvertedIndexOptions options,
    ExpressionData predicate, CreateIndexOperationOptions operation_options);
  // ALTER INDEX ... RENAME TO, once the caller has resolved the index and
  // checked the privilege. The physical index is filed under the catalog name,
  // so the record and the store op that moves it go into one frame. Assumes
  // `_mutex` is held.
  void RenameIndex(duckdb::ClientContext* context,
                   const CreateIndexInfoBase& index, std::string_view new_name);
  // The owner is `ax.role`: a dictionary has no ACL of its own, so its
  // permissions are the creator and nothing else.
  bool CreateTokenizer(const AccessContext& ax, ObjectId database_id,
                       std::string_view schema,
                       std::shared_ptr<CreateTokenizerInfo> tokenizer,
                       bool if_not_exists);
  // Foreign servers are database children, like PG (no schema). Every check
  // runs here, under the mutex: CREATE privilege, supported FDW, and name
  // collisions in this or ANY database (the attach alias is instance-global).
  // Returns false for the if_not_exists no-op. The live ATTACH happens
  // AFTERWARDS in the command layer, compensated by a drop on failure -- so a
  // denied or invalid CREATE never touches the network.
  bool CreateForeignServer(const AccessContext& ax, ObjectId database_id,
                           std::shared_ptr<CreateForeignServerInfo> info,
                           Permissions perm, bool if_not_exists);

  using AclMutator = auth::AclMutator;
  // One ALTER on one table: `change` produces the next version of the info from
  // the live one, or null for a sanctioned no-op (what IF [NOT] EXISTS asks
  // for). The caller resolves the table, so a name that turns out to hold
  // something else is its error to phrase.
  void ChangeTable(const AccessContext& ax,
                   const duckdb::CreateTableInfo& table, TableChange change);
  void ChangeRole(const AccessContext& ax, std::string_view name,
                  std::string_view verb, bool allow_self,
                  ChangeCallback<CreateRoleInfo> callback);
  void ChangeDefaultAcl(const AccessContext& ax, std::string_view role_name,
                        ObjectId schema, char objtype, duckdb::CatalogType type,
                        absl::AnyInvocable<void(Acl&)> mutate);
  void ChangeMembership(const AccessContext& ax, ObjectId role,
                        std::string_view role_name, ObjectId member,
                        std::string_view member_name, const Membership& edge,
                        bool revoke, bool admin_option_only);
  // `type` is the type the statement names the table as; it drives the error
  // phrasing and the ACL shape, and differs from Table only for the index
  // kinds, which a statement may name a table by.
  void ChangeTableOwner(const AccessContext& ax,
                        const duckdb::CreateTableInfo& table,
                        duckdb::CatalogType type, ObjectId new_owner,
                        std::string_view new_owner_name);
  void ChangeDatabaseAcl(const AccessContext& ax, ObjectId database_id,
                         AclMutator mutate);
  void ChangeColumnType(
    const AccessContext& ax, const duckdb::CreateTableInfo& table,
    std::string_view column, duckdb::LogicalType new_type,
    duckdb::unique_ptr<duckdb::ParsedExpression> using_expr);

  // The foreign-server attachments the cascade leaves behind are the caller's:
  // they are instance-global while the entries holding their names are this
  // database's, so they have to be captured before the attachment -- and the
  // set that holds them -- goes away. SereneDBCatalog::OnDetach does that and
  // runs the detaches afterwards.
  void DropDatabase(const AccessContext& ax, std::string_view name,
                    duckdb::shared_ptr<void> keep_alive);
  bool DropRole(const AccessContext& ax, std::string_view role,
                bool missing_ok);
  bool DropSchema(const AccessContext& ax, std::string_view database,
                  std::string_view name, bool cascade, bool missing_ok);
  bool DropTable(const AccessContext& ax, std::string_view database,
                 std::string_view schema, std::string_view name, bool cascade,
                 bool missing_ok);
  bool DropIndex(const AccessContext& ax, std::string_view database,
                 std::string_view schema, std::string_view name, bool cascade,
                 bool missing_ok);
  // Drop an index by its stable ObjectId rather than by name. Used by the
  // CREATE INDEX failure path, where a concurrent rename could otherwise make a
  // by-name lookup resolve to (and drop) the wrong index.
  void DropIndexById(duckdb::ClientContext* context, ObjectId database_id,
                     ObjectId index_id, bool cascade);
  // Undoes a CREATE INDEX whose transaction is rolling back. The definition
  // lives only on that transaction's overlay and dies with it, so nothing is
  // published and no drop record is appended -- there is no committed
  // definition to tombstone. What the create did outside the catalog is undone
  // here: the store-side index stops feeding the table's commits immediately,
  // and the artifact cleanup starts now rather than being parked on the
  // transaction, which is about to discard everything it holds.
  void DropUncommittedIndex(duckdb::ClientContext& context,
                            ObjectId database_id, ObjectId index_id);
  void DropTableColumn(const AccessContext& ax, ObjectId database_id,
                       const duckdb::CreateTableInfo& table,
                       std::string_view column, bool if_exists);

  // Applies one frame of catalog records, as boot reads them back. The records
  // are the intent and this is where they are performed: a definition record
  // registers its object, replacing the version its id already names, and a
  // removal record unregisters it. Nothing is staged anywhere else -- the
  // catalog is the only place a definition lives.
  //
  // Only legal while the catalog is still being loaded: replay is the one
  // caller, and it is what builds the first version.
  void ReplayRecords(std::span<const wal::Entry> entries);

  void FinalizeLoad();

  // The catalog half of a transaction's commit, in one step: the records it
  // produced go into the log and its effect into the committed catalog, under
  // the mutex a checkpoint has to take -- so the log never leads the catalog
  // and a rewrite is always whole.
  //
  // The append can throw, and it does so before anything is durable -- which is
  // what lets the commit be refused. Returns the log position the records
  // landed at, zero for none.
  uint64_t CommitTransaction(duckdb::ClientContext* context,
                             std::span<const std::vector<wal::Entry>> frames);

  // Runs `fn` with catalog mutations excluded, or does nothing when a mutation
  // is already running. Compaction is the only caller: it reads the catalog and
  // rewrites the log a commit writes both halves of, and it is opportunistic,
  // so losing that race is a skip rather than a wait. Returns whether `fn` ran.
  bool TryExcludingMutations(absl::FunctionRef<void()> fn);

  // One entry record, written by the put that places the same definition in
  // the set. A mutation therefore states its change exactly once, in the form
  // that is durable, and nothing can drift between the record and the entry.
  // Assumes `_mutex` is held -- the mutator takes it before it writes.
  void RecordEntry(duckdb::ClientContext* context, ObjectId parent_id,
                   duckdb::CatalogType type, ObjectId id, wal::PutMode mode,
                   std::shared_ptr<const duckdb::CreateInfo> info,
                   Permissions perm);
  // The same for a table, whose record carries the store table's shape and the
  // sequences a create hands it.
  void RecordTable(duckdb::ClientContext* context,
                   const duckdb::CreateTableInfo& table, wal::PutMode mode,
                   Permissions perm);
  // A sequence's definition and the value its counter starts from, in one
  // frame: a sequence is never durable without the value it hands out from.
  void RecordSequence(
    duckdb::ClientContext* context,
    std::shared_ptr<const duckdb::CreateSequenceInfo> sequence,
    Permissions perm, uint64_t seed);

  // The removal counterpart, for the kinds whose entry is the object:
  // everything the DROP of one does once its target has been resolved and the
  // ownership check has passed -- schedule the index cleanup, record the
  // removal beside the plan's own, publish, and take the entry out of the set
  // its kind lives in. Assumes `_mutex` is held.
  void DropResolved(duckdb::ClientContext* context, ObjectId database_id,
                    ObjectId parent_id, duckdb::CatalogType type, ObjectId id,
                    std::string_view name, DropPlan& plan);

  // The next version of one function, for DROP FUNCTION on a name that holds
  // several overloads: what survives the drop is a rewrite of the whole set.
  // A no-op when the name no longer holds a function.
  void ReplaceFunction(duckdb::ClientContext& context, ObjectId database_id,
                       std::string_view schema, std::string_view name,
                       std::shared_ptr<const duckdb::CreateMacroInfo> info);

  // The lock every mutation runs under, for the DDL that lives where duckdb
  // hands it over rather than here: a statement resolves its target, checks
  // the privilege and writes the entry under one scope, and RecordEntry
  // assumes it is held.
  class [[nodiscard]] MutationScope {
   public:
    explicit MutationScope(Catalog& catalog) : _lock{&catalog._mutex} {}

   private:
    absl::MutexLock _lock;
  };

  // States that what the entry writes inside it say is already recorded, so
  // they must not say it again. An entry write records itself -- that is what
  // makes a record a consequence of the mutation rather than a parameter of it
  // -- and three callers have already written theirs: a batched mutator, whose
  // record went into a frame beside a store op or a sibling entry; a refresh,
  // which rebuilds an entry nothing changed; and boot replay, which is reading
  // those very records back. Explicit, never inferred from `context`: a
  // background drop task has none either and must record.
  //
  // Thread-scoped and nestable -- a mutation holds `_mutex`, so its frame is
  // this thread's alone.
  class [[nodiscard]] RecordedScope {
   public:
    RecordedScope() noexcept;
    ~RecordedScope();
    RecordedScope(const RecordedScope&) = delete;
    RecordedScope& operator=(const RecordedScope&) = delete;

    // Whether this thread is inside one.
    static bool Open() noexcept;
  };

 private:
  // Records one batch and performs it: `fill` builds the batch's records and
  // its store ops, and the records are then what changes the catalog -- so a
  // mutation describes what it did exactly once, in the form that is durable.
  // `context` is the statement the batch belongs to; without one (boot,
  // background drop tasks, teardown) the effect is published immediately.
  // Assumes `_mutex` is held.
  void Apply(duckdb::ClientContext* context,
             absl::FunctionRef<void(CatalogStore::WriteContext&)> fill);

  // Binds the durable state that hangs off a definition rather than living in
  // it: an inverted index's iresearch segments, a search table's shards, and a
  // sequence's counter, which replay could only read once the whole log was in.
  // Runs over what survived the log, so nothing is opened for an object a later
  // record dropped.
  static void OpenBootStorage();

  void ChangeRoleImpl(
    duckdb::ClientContext* context, ObjectId actor_id, std::string_view name,
    absl::FunctionRef<void(duckdb::ClientContext*, const CreateRoleInfo&)>
      check,
    ChangeCallback<CreateRoleInfo> callback);

  void CreateIndexImpl(duckdb::ClientContext* context,
                       const IndexInfoRef& index,
                       CreateIndexOperationOptions operation_options);

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(
    duckdb::ClientContext* context, ObjectId db_id,
    const SequenceOwners& sequences, const IndexOwners& indexes,
    duckdb::shared_ptr<void> keep_alive);
  std::shared_ptr<SchemaDrop> CreateSchemaDrop(
    duckdb::ClientContext* context, ObjectId db_id, ObjectId schema_id,
    const SequenceOwners& sequences, const IndexOwners& indexes, bool is_root);
  std::shared_ptr<TableDropBase> CreateTableDrop(
    ObjectId db_id, ObjectId schema_id, const TableInfoRef& table,
    const SequenceOwners& sequences, const IndexOwners& indexes, bool is_root);
  std::shared_ptr<IndexDrop> CreateIndexDrop(ObjectId db_id, ObjectId schema_id,
                                             ObjectId table_id,
                                             const CreateIndexInfoBase& index,
                                             bool is_root);

  // Runs against the pre-mutation catalog, which is sound because a plan never
  // names an index inside the seed's own subtree (the cascade walk filters
  // those out; the seed's structural drop task covers them). `_mutex` must be
  // held.
  void ScheduleDropPlanIndexes(duckdb::ClientContext* context, ObjectId db_id,
                               const DropPlan& plan);

  // Hands the artifact cleanup to the statement's transaction, which starts it
  // once its removal is durable; straight to the background pool when there is
  // no transaction to wait for.
  static void ScheduleDrop(duckdb::ClientContext* context,
                           std::shared_ptr<DropTask> task);

  // Shared core of DropIndex / DropIndexById; assumes `_mutex` is held.
  void DropIndexLocked(duckdb::ClientContext* context, ObjectId database_id,
                       const IndexInfoRef& index, bool cascade);

  mutable absl::Mutex _mutex;
  // Whether boot is still reading the log. Replay writes entries outright
  // rather than through a statement's transaction, and nothing else can reach
  // the catalog while it runs.
  bool _loading = true;
  PendingDrops _pending_drops;
  CatalogStore* _engine;
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
