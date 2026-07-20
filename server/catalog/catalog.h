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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <expected>
#include <functional>
#include <memory>
#include <vector>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/column_expr.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/object_dependency.h"
#include "catalog/persistence/policy.h"
#include "catalog/resolution_table.h"
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
#include "connector/duckdb_entry_cache.h"

namespace sdb::catalog {

// Mutation callback: fill `updated` with the changed clone (leave it null for
// a no-op). Signal errors by throwing (pg::SqlException for user-facing ones).
template<typename T>
using ChangeCallback = absl::FunctionRef<void(const T&, std::shared_ptr<T>&)>;

class SecondaryIndex;
class InvertedIndex;
class Policy;
class RowSecurity;

struct CreateTableOperationOptions {
  // A valid id puts CreateTable in CTAS mode: the entry is created with this
  // pre-allocated id, tombstoned, and WITHOUT a backing store table (the data
  // side creates the store table itself, under its own transaction). CTAS
  // pre-allocates the id at plan time so the store table name is known to the
  // insert operator. An invalid (default) id creates a regular, immediately
  // visible table with a freshly allocated id and its store table.
  ObjectId table_id;
  // IF NOT EXISTS: an existing relation of the same name makes CreateTable
  // return false instead of throwing "already exists".
  bool if_not_exists = false;
};

struct CreateIndexOperationOptions {
  bool create_with_tombstone = false;
  // IF NOT EXISTS: an existing relation of the same name makes the create
  // return false instead of throwing "already exists".
  bool if_not_exists = false;
};

// Site-specific error phrasing for ChangeTable. The defaults throw
// "relation \"<name>\" does not exist" / "\"<name>\" is not a table".
struct ChangeTableOptions {
  // Missing relation: return without calling the callback instead of throwing.
  bool missing_ok = false;
  // Called (under the catalog mutex) when the name resolves to a non-table
  // relation; must throw. Empty -> the generic wrong-object-type error.
  std::function<void(const Object&)> on_type_mismatch;
};

template<typename T>
constexpr ObjectType GetObjectType() noexcept {
  if constexpr (std::is_same_v<T, PgSqlView>) {
    return ObjectType::PgSqlView;
  } else if constexpr (std::is_same_v<T, Database>) {
    return ObjectType::Database;
  } else if constexpr (std::is_same_v<T, Schema>) {
    return ObjectType::Schema;
  } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
    return ObjectType::PgSqlFunction;
  } else if constexpr (std::is_same_v<T, PgSqlType>) {
    return ObjectType::PgSqlType;
  } else if constexpr (std::is_same_v<T, Table>) {
    return ObjectType::Table;
  } else if constexpr (std::is_same_v<T, SecondaryIndex>) {
    return ObjectType::SecondaryIndex;
  } else if constexpr (std::is_same_v<T, InvertedIndex>) {
    return ObjectType::InvertedIndex;
  } else if constexpr (std::is_same_v<T, Tokenizer>) {
    return ObjectType::Tokenizer;
  } else if constexpr (std::is_same_v<T, Sequence>) {
    return ObjectType::Sequence;
  } else if constexpr (std::is_same_v<T, Role>) {
    return ObjectType::Role;
  } else if constexpr (std::is_same_v<T, Policy>) {
    return ObjectType::Policy;
  } else if constexpr (std::is_same_v<T, RowSecurity>) {
    return ObjectType::RowSecurity;
  } else {
    static_assert(false);
  }
}

struct AccessContext {
  ObjectId role;
  AclMode need = AclMode::NoRights;
  bool match_any = false;
};

AccessContext RequireAccess(duckdb::ClientContext& context, AclMode need);

inline AccessContext RequireAccess(ObjectId role, AclMode need) {
  return {role, need};
}

// Same as RequireAccess but the role only needs ANY one of the `need` bits.
AccessContext RequireAccessAny(duckdb::ClientContext& context, AclMode need);

AccessContext ActingAs(duckdb::ClientContext& context);

inline AccessContext ActingAs(ObjectId role) { return {role}; }

inline AccessContext NoAccessCheck() { return {id::kRootUser}; }

using PendingDrops =
  containers::FlatHashMap<ObjectId, std::vector<std::weak_ptr<DropTask>>>;

struct Snapshot {
  Snapshot();
  ~Snapshot();

  std::shared_ptr<Snapshot> Clone() const;

  uint64_t Version() const noexcept { return _version; }

  connector::DuckDBEntryCache& GetDuckDBEntryCache() const;

  const auth::RoleClosure& ClosureFor(ObjectId role) const;

  void RebuildRoleClosures();

  void RequireAccess(ObjectId role, const Object& object, AclMode need,
                     bool match_any = false) const;

  std::size_t RoleDependentCount(ObjectId role) const;
  // All objects that reference `role` (owner or privilege grant), sorted.
  std::vector<ObjectId> RoleReferenceIds(ObjectId role) const;

  std::vector<std::shared_ptr<Role>> GetRoles() const;
  std::vector<std::shared_ptr<Database>> GetDatabases() const;
  std::vector<std::shared_ptr<Schema>> GetSchemas(ObjectId database) const;
  std::vector<std::shared_ptr<Object>> GetRelations(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Table>> GetTables(ObjectId database,
                                                std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlView>> GetViews(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlFunction>> GetFunctions(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Index>> GetIndexes(ObjectId database,
                                                 std::string_view schema) const;
  std::vector<std::shared_ptr<Sequence>> GetSequences(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Tokenizer>> GetTokenizers(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlType>> GetTypes(
    ObjectId database, std::string_view schema) const;

  // Allocation-free iteration over schema objects. Use these when the caller
  // can process each item inline and only needs to buffer the misses.
  void VisitRelations(ObjectId database, std::string_view schema,
                      absl::FunctionRef<void(const Object&)> visitor) const;
  void VisitViews(ObjectId database, std::string_view schema,
                  absl::FunctionRef<void(const PgSqlView&)> visitor) const;
  void VisitFunctions(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const PgSqlFunction&)> visitor) const;
  void VisitIndexes(ObjectId database, std::string_view schema,
                    absl::FunctionRef<void(const Index&)> visitor) const;

  std::shared_ptr<Role> GetRole(std::string_view name) const;
  std::shared_ptr<Database> GetDatabase(std::string_view database) const;
  std::shared_ptr<Database> GetDatabase(ObjectId database) const;
  std::shared_ptr<Schema> GetSchema(ObjectId database,
                                    std::string_view schema) const;

  std::shared_ptr<Object> GetRelation(const AccessContext& ax,
                                      ObjectId database,
                                      std::string_view schema,
                                      std::string_view name) const;
  std::shared_ptr<PgSqlFunction> GetFunction(const AccessContext& ax,
                                             ObjectId database,
                                             std::string_view schema,
                                             std::string_view name) const;
  std::shared_ptr<Tokenizer> GetTokenizer(const AccessContext& ax,
                                          ObjectId database,
                                          std::string_view schema,
                                          std::string_view name) const;
  std::shared_ptr<PgSqlType> GetType(const AccessContext& ax, ObjectId database,
                                     std::string_view schema,
                                     std::string_view name) const;
  std::shared_ptr<Table> GetTable(const AccessContext& ax, ObjectId database_id,
                                  std::string_view schema,
                                  std::string_view name) const;
  std::shared_ptr<Sequence> GetSequence(const AccessContext& ax,
                                        ObjectId database, ObjectId schema_id,
                                        std::string_view name) const;

  bool HasIndexes(ObjectId relation_id) const;
  std::shared_ptr<Object> GetObject(ObjectId id) const;

  ObjectId GetDatabaseId(const Object& obj) const {
    if (obj.GetType() == ObjectType::Database) {
      return obj.GetId();
    }
    for (auto cur = obj.GetParentId(); cur.isSet();) {
      auto parent = GetObject(cur);
      if (!parent) {
        return {};
      }
      if (parent->GetType() == ObjectType::Database) {
        return cur;
      }
      cur = parent->GetParentId();
    }
    return {};
  }

  std::vector<std::shared_ptr<Index>> GetIndexesByRelation(
    ObjectId relation_id) const;

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto obj = GetObject(id);
    if (!obj) {
      return nullptr;
    }
    if constexpr (std::is_same_v<T, Object>) {
      return obj;
    } else if constexpr (std::is_same_v<T, Index>) {
      if (!IsIndex(obj->GetType())) {
        return nullptr;
      }
    } else {
      if (obj->GetType() != GetObjectType<T>()) {
        return nullptr;
      }
    }
    return basics::downCast<T>(obj);
  }

  // RLS: the policy object ids attached to a table, and the enable/force pair.
  std::vector<ObjectId> PolicyIds(ObjectId table_id) const;
  struct RowSecurityState {
    bool enabled = false;
    bool forced = false;
  };
  RowSecurityState GetRowSecurity(ObjectId table_id) const;

 private:
  friend class Catalog;

  enum class EdgeAction : uint8_t { Add, Delete };

  template<typename T>
  std::shared_ptr<T> EnforceRead(const AccessContext& ax,
                                 std::shared_ptr<T> obj) const;

  void EndLoad() noexcept;

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(
    PendingDrops& pending_drops, const std::shared_ptr<Database>& db,
    duckdb::shared_ptr<void> keep_alive);
  std::shared_ptr<SchemaDrop> CreateSchemaDrop(
    PendingDrops& pending_drops, ObjectId db_id,
    const std::shared_ptr<Schema>& schema, bool is_root);
  std::shared_ptr<TableDropBase> CreateTableDrop(
    PendingDrops& pending_drops, ObjectId db_id, ObjectId schema_id,
    const std::shared_ptr<Table>& table, bool is_root);
  std::shared_ptr<IndexDrop> CreateIndexDrop(
    PendingDrops& pending_drops, ObjectId db_id, ObjectId schema_id,
    ObjectId table_id, const std::shared_ptr<Index>& index, bool is_root);

  // Store-table name of `table_id` ("db.schema.table"), or nullopt when
  // the id is unset (self-referencing FK) or not resolvable.
  std::optional<std::string> ComposeStoreTableName(ObjectId table_id) const;

  // Cross-tree fixups for DROP seed. Composition cleanup is async.
  DropPlan ComputeDropPlan(ObjectId seed) const;
  // Plan for ALTER TABLE DROP COLUMN: rewrite the owning table without the
  // column and cascade-drop every index covering it (PG column->index cascade).
  DropPlan ComputeColumnDropPlan(ObjectId table_id, ObjectId col_id) const;
  void CommitDropPlan(CatalogStore::WriteContext& ctx,
                      const DropPlan& plan) const;
  // Apply cross-tree mutations in-memory; schedule IndexDrop tasks for
  // cascade-dropped indexes (column->index cascade).
  void ApplyDropPlan(PendingDrops& pending_drops, ObjectId db_id,
                     DropPlan& plan);

  bool CheckSchemaEmptyDependency(ObjectId schema_id) const;

  const auto& Objects() const noexcept { return _objects; }

  void AddDependencies(ObjectId parent_id, const Object& obj);
  void ModifyTableDependencies(ObjectId schema_id, const Table& table,
                               EdgeAction action);
  void ModifyViewDependencies(ObjectId schema_id, const PgSqlView& view,
                              RefKinds kinds, EdgeAction action);
  void ModifyFunctionDependencies(ObjectId schema_id, const PgSqlFunction& func,
                                  EdgeAction action);
  void ModifyInvertedIndexDependencies(const InvertedIndex& index,
                                       ObjectId index_id, EdgeAction action);
  void ModifyRoleDependencies(const Object& obj, EdgeAction action);

  // Throws the object-kind-specific "already exists" SqlException on a name
  // collision.
  template<typename T>
  void RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                      bool replace);

  template<typename T>
  void UnregisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool maybe_not_found = false) noexcept;

  template<typename DependencyType = void>
  void AddObjectDefinition(ObjectId parent_id, std::shared_ptr<Object> object);

  template<ResolveType Type>
  void AddToResolution(ObjectId parent_id, ObjectId id, std::string_view name,
                       bool replace);

  template<ResolveType Type>
  void RemoveFromResolution(ObjectId parent_id, std::string_view name,
                            bool maybe_not_found = false) noexcept;

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) const;

  // Throws the "already exists" SqlException when the new name is taken.
  template<ResolveType Type>
  void ReplaceObject(ObjectId parent_id, std::string_view old_name,
                     std::shared_ptr<Object> new_object);

  template<typename Dep, typename Member, typename Edge>
  void ModifyDependency(ObjectId target, Member Dep::* mem, const Edge& edge,
                        EdgeAction action);

  template<ResolveType Kind, ObjectType... Allowed>
  ObjectId Resolve(ObjectId db_id, ObjectId default_schema_id,
                   std::string_view catalog, std::string_view schema,
                   std::string_view name) const;

  template<typename T>
  std::shared_ptr<const T> GetDependency(ObjectId id) const;

  template<typename T>
  std::shared_ptr<T> GetDependencyForWrite(ObjectId id);

  void RemoveObjectDefinition(ObjectId parent_id, ObjectId id,
                              bool root = false,
                              bool maybe_not_found = false) noexcept;

  template<typename T>
  using ObjectSetByName =
    containers::FlatHashSet<std::shared_ptr<T>, ObjectByName, ObjectByName>;
  template<typename T>
  using ObjectSetById =
    containers::FlatHashSet<std::shared_ptr<T>, ObjectById, ObjectById>;
  template<typename K, typename V>
  using ObjectMapByName =
    containers::NodeHashMap<std::shared_ptr<K>, V, ObjectByName, ObjectByName>;
  template<typename K, typename V>
  using ObjectMapById =
    containers::FlatHashMap<std::shared_ptr<K>, V, ObjectById, ObjectById>;

  struct SchemaObjects {
    bool empty() const { return relations.empty() && functions.empty(); }

    bool VisitObjects(auto&& writer) {
      for (auto& object : relations) {
        if (!writer(object)) {
          return false;
        }
      }
      for (auto& object : functions) {
        if (!writer(object)) {
          return false;
        }
      }
      return true;
    }

    ObjectSetByName<Object> relations;
    ObjectSetByName<Object> functions;
  };

  ResolutionTable _resolution_table;
  ObjectDependencies _deps;
  ObjectSetById<Object> _objects;
  mutable connector::DuckDBEntryCache _duckdb_cache;
  auth::RoleClosureMap _role_closures;
  bool _in_load = true;
  uint64_t _version = 0;

 public:
  void StampVersion(uint64_t version) noexcept;
};

class Catalog final {
 public:
  explicit Catalog();

  // All mutators throw on failure: pg::SqlException with the PG-compatible
  // errcode/message for user-facing errors, SqlException for internal
  // (store/serialization) failures. Create* whose statement supports IF NOT
  // EXISTS and Drop* with `missing_ok` return false instead of throwing when
  // the object already exists / is absent.
  void RegisterRole(std::shared_ptr<Role> role);
  void RegisterDatabase(std::shared_ptr<Database> database);
  void RegisterSchema(ObjectId database_id, std::shared_ptr<Schema> schema);
  void RegisterView(ObjectId schema_id, std::shared_ptr<PgSqlView> view);
  void RegisterSequence(ObjectId database_id, ObjectId schema_id,
                        std::shared_ptr<Sequence> sequence);
  void RegisterFunction(ObjectId database_id, ObjectId schema_id,
                        std::shared_ptr<PgSqlFunction> function);
  void RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                         std::shared_ptr<Tokenizer> tokenizer);
  void RegisterType(ObjectId database_id, ObjectId schema_id,
                    std::shared_ptr<PgSqlType> type);
  void RegisterTable(ObjectId database_id, ObjectId schema_id,
                     std::shared_ptr<Table> table);
  void RegisterIndex(ObjectId database_id, ObjectId schema_id,
                     std::shared_ptr<Index> index);
  void RegisterPolicy(std::shared_ptr<Policy> policy);
  void RegisterRowSecurity(std::shared_ptr<RowSecurity> rs);

  bool CreateDatabase(const AccessContext& ax,
                      std::shared_ptr<Database> database, bool if_not_exists);
  void CreateRole(const AccessContext& ax, std::shared_ptr<Role> role);
  bool CreateView(const AccessContext& ax, ObjectId database_id,
                  std::string_view schema, std::shared_ptr<PgSqlView> view,
                  bool replace, bool if_not_exists);
  bool CreateSequence(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema,
                      std::shared_ptr<Sequence> sequence, bool if_not_exists);
  bool CreateSchema(const AccessContext& ax, ObjectId database_id,
                    std::shared_ptr<Schema> schema, bool if_not_exists);
  bool CreateFunction(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema,
                      std::shared_ptr<PgSqlFunction> function, bool replace,
                      bool if_not_exists);
  bool CreateTable(const AccessContext& ax, ObjectId database_id,
                   std::string_view schema, CreateTableOptions table,
                   CreateTableOperationOptions operation_options);
  bool CreateSecondaryIndex(const AccessContext& ax, ObjectId database_id,
                            std::string_view schema, std::string_view relation,
                            std::string name,
                            std::vector<CreateIndexColumn>&& columns,
                            bool unique,
                            CreateIndexOperationOptions operation_options);
  bool CreateInvertedIndex(const AccessContext& ax,
                           duckdb::ClientContext& context, ObjectId database_id,
                           std::string_view schema, std::string_view relation,
                           std::string name,
                           std::vector<CreateIndexColumn>&& columns,
                           InvertedIndexOptions options,
                           CreateIndexOperationOptions operation_options);
  bool CreateTokenizer(const AccessContext& ax, ObjectId database_id,
                       std::string_view schema, std::shared_ptr<Tokenizer> dict,
                       bool if_not_exists);
  bool CreateType(const AccessContext& ax, ObjectId database_id,
                  std::string_view schema, std::shared_ptr<PgSqlType> type,
                  bool if_not_exists);

  // Row-Level Security. All require ownership of the target table.
  void CreatePolicy(const AccessContext& ax, ObjectId database_id,
                    std::string_view schema, std::string_view relation,
                    persistence::PolicyData data);
  // Applies only the fields flagged present in `patch`: roles if has_roles,
  // using if has_using, check if has_check. `new_name` renames when non-empty.
  void AlterPolicy(const AccessContext& ax, ObjectId database_id,
                   std::string_view schema, std::string_view relation,
                   std::string_view name, std::string_view new_name,
                   bool has_roles, std::vector<ObjectId> roles, bool has_using,
                   std::string using_text, bool has_check,
                   std::string check_text);
  void DropPolicy(const AccessContext& ax, ObjectId database_id,
                  std::string_view schema, std::string_view relation,
                  std::string_view name, bool if_exists);
  void SetRowSecurity(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view relation,
                      std::optional<bool> enabled, std::optional<bool> forced);

  void RenameView(const AccessContext& ax, ObjectId database_id,
                  std::string_view schema, std::string_view name,
                  std::string_view new_name);
  void RenameRelation(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view name,
                      std::string_view new_name);
  // Returns false when the function is absent and `missing_ok` is set.
  bool RenameFunction(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view name,
                      std::string_view new_name, bool missing_ok);

  using AclMutator =
    absl::FunctionRef<void(const Snapshot&, ObjectId owner, catalog::Acl&)>;
  void ChangeTable(const AccessContext& ax, ObjectId database_id,
                   std::string_view schema, std::string_view name,
                   ChangeCallback<Table> callback,
                   const ChangeTableOptions& options = {});
  void ChangeRole(const AccessContext& ax, std::string_view name,
                  std::string_view verb, bool allow_self,
                  ChangeCallback<Role> callback);
  void ChangeDefaultAcl(const AccessContext& ax, std::string_view role_name,
                        ObjectId schema, char objtype, ObjectType type,
                        absl::FunctionRef<void(Acl&)> mutate);
  void ChangeMembership(const AccessContext& ax, ObjectId role,
                        std::string_view role_name, ObjectId member,
                        std::string_view member_name, const Membership& edge,
                        bool revoke, bool admin_option_only,
                        ObjectId granted_by);
  void ChangeOwner(const AccessContext& ax, ObjectId database_id,
                   std::string_view schema, std::string_view name,
                   ObjectType type, ObjectId new_owner,
                   std::string_view new_owner_name);
  void ReassignOwned(const AccessContext& ax,
                     std::span<const std::string> from_roles,
                     std::string_view to_role);
  // Besides dropping the owned objects, also revokes the roles' privilege
  // grants, so the roles become droppable (PostgreSQL semantics).
  void DropOwned(const AccessContext& ax,
                 std::span<const std::string> from_roles, bool cascade);
  void ChangeAcl(ObjectId database_id, std::string_view schema,
                 std::string_view name, ObjectType type, AclMutator mutate);
  void ChangeColumnAcl(ObjectId database_id, std::string_view schema,
                       std::string_view table_name, std::string_view column,
                       AclMutator mutate);
  void ChangeColumnType(const AccessContext& ax, ObjectId database_id,
                        std::string_view schema, std::string_view table,
                        std::string_view column, duckdb::LogicalType new_type,
                        std::string using_sql);

  void DropDatabase(const AccessContext& ax, std::string_view name,
                    duckdb::shared_ptr<void> keep_alive);
  bool DropRole(const AccessContext& ax, std::string_view role,
                bool missing_ok);
  bool DropSchema(const AccessContext& ax, std::string_view database,
                  std::string_view name, bool cascade, bool missing_ok);
  bool DropView(const AccessContext& ax, std::string_view database,
                std::string_view schema, std::string_view name, bool cascade,
                bool missing_ok);
  bool DropSequence(const AccessContext& ax, std::string_view database,
                    std::string_view schema, std::string_view name,
                    bool cascade, bool missing_ok);
  bool DropType(const AccessContext& ax, std::string_view database,
                std::string_view schema, std::string_view name, bool cascade,
                bool missing_ok);
  bool DropFunction(const AccessContext& ax, std::string_view database,
                    std::string_view schema, std::string_view name,
                    bool cascade, bool missing_ok);
  bool DropTokenizer(const AccessContext& ax, std::string_view database,
                     std::string_view schema, std::string_view name,
                     bool cascade, bool missing_ok);
  bool DropTable(const AccessContext& ax, std::string_view database,
                 std::string_view schema, std::string_view name, bool cascade,
                 bool missing_ok);
  bool DropIndex(const AccessContext& ax, std::string_view database,
                 std::string_view schema, std::string_view name, bool cascade,
                 bool missing_ok);
  // Drop an index by its stable ObjectId rather than by name. Used by the
  // CREATE INDEX failure path, where a concurrent rename could otherwise make a
  // by-name lookup resolve to (and drop) the wrong index.
  void DropIndexById(ObjectId database_id, ObjectId index_id, bool cascade);
  void DropTableColumn(const AccessContext& ax, ObjectId database_id,
                       std::string_view schema, std::string_view table,
                       std::string_view column, bool if_exists);

  void RemoveTombstone(ObjectId database_id, std::string_view schema,
                       std::string_view name);

  void FinalizeLoad();

  std::shared_ptr<const Snapshot> GetCatalogSnapshot() const noexcept;

 private:
  void ChangeRoleImpl(
    ObjectId actor_id, std::string_view name,
    absl::FunctionRef<void(const Snapshot&, const Role&)> check,
    ChangeCallback<Role> callback);

  void CreateIndexImpl(std::string_view schema, std::shared_ptr<Index> index,
                       CreateIndexOperationOptions operation_options);

  // Shared core of DropIndex / DropIndexById; assumes `_mutex` is held.
  void DropIndexByIdLocked(ObjectId database_id, ObjectId index_id,
                           bool cascade);

  template<typename T>
  void RenameObjectImpl(const AccessContext& ax, ObjectId database_id,
                        std::string_view schema, std::string_view name,
                        std::string_view new_name);

  template<typename T>
  void RenameObjectImpl(ObjectId schema_id, std::string_view database_name,
                        std::string_view schema_name, std::string_view name,
                        std::string_view new_name, std::shared_ptr<T> object);

  mutable absl::Mutex _mutex;
  // Accessed only via std::atomic_load/std::atomic_store (libc++ lacks
  // std::atomic<std::shared_ptr>): a leaf with no lock ordering -- mutations
  // build a clone off to the side and atomically swap it in.
  std::shared_ptr<const Snapshot> _snapshot;
  PendingDrops _pending_drops;
  CatalogStore* _engine;
};

// Builds the single in-process catalog, loads boot state, and attaches the
// databases. Throws on failure.
void InitCatalog();
void ShutdownCatalog();

std::shared_ptr<Database> GetDatabase(std::string_view name);
Catalog& GetCatalog();
// Null before InitCatalog and after ShutdownCatalog, for callers that can run
// during startup-failure or shutdown teardown.
Catalog* TryGetCatalog();

}  // namespace sdb::catalog
