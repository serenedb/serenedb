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

#include "catalog/catalog.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/common/exception/parser_exception.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/async/when_all.hpp>

#include "app/app_server.h"
#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/column_expr.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/object_dependency.h"
#include "catalog/persistence/role.h"
#include "catalog/resolution_table.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_mapping.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "network/credentials.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {

AccessContext RequireAccess(duckdb::ClientContext& context, AclMode need) {
  return RequireAccess(connector::GetSereneDBContext(context).GetRoleId(),
                       need);
}

AccessContext ActingAs(duckdb::ClientContext& context) {
  return ActingAs(connector::GetSereneDBContext(context).GetRoleId());
}

namespace {

void RequireRoleAttribute(const Snapshot& snapshot, ObjectId actor_id,
                          RoleOption attribute, std::string_view denied_action,
                          std::string_view detail = {});

void RequireAttributesGrantable(const Snapshot& snapshot, ObjectId actor_id,
                                RoleOption granting, bool creating);

template<ResolveType Type>
[[noreturn]] void ThrowDuplicateName(std::string_view name) {
  if constexpr (Type == ResolveType::Database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
                    ERR_MSG("database \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::Schema) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
                    ERR_MSG("schema \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::Relation ||
                       Type == ResolveType::Function) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::Type) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("type \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::Tokenizer) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::ForeignServer) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("server \"", name, "\" already exists"));
  } else if constexpr (Type == ResolveType::UserMapping) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("user mapping \"", name, "\" already exists"));
  } else {
    static_assert(Type == ResolveType::Role);
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("role \"", name, "\" already exists"));
  }
}

[[noreturn]] void ThrowWrongObjectType(std::string_view name,
                                       std::string_view kind,
                                       ObjectType actual) {
  const auto actual_kind = pg::ToPgObjectTypeName(actual);
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
    ERR_MSG("\"", name, "\" is not ", basics::string_utils::GetArticle(kind),
            " ", kind),
    ERR_HINT("Use DROP ", absl::AsciiStrToUpper(actual_kind), " to remove ",
             basics::string_utils::GetArticle(actual_kind), " ", actual_kind,
             "."));
}

void Apply(
  std::shared_ptr<const Snapshot>& snapshot, auto&& f,
  std::function<void(const std::shared_ptr<Snapshot>&)> rollback = {}) {
  std::shared_ptr<Snapshot> clone = snapshot->Clone();
  try {
    f(clone);
  } catch (...) {
    if (rollback) {
      rollback(clone);
    }
    throw;
  }
  clone->StampVersion(snapshot->Version() + 1);
  std::atomic_store(&snapshot,
                    std::shared_ptr<const Snapshot>(std::move(clone)));
}

// Re-render a stored expression-index `pretty_printed` (valid SQL baked with
// the column names at CREATE INDEX time) after a RENAME COLUMN: re-parse it and
// remap only ColumnRefExpression leaves whose trailing name changed. The bound
// serialized_expr (column-id keyed) is unaffected; only the display text is
// stale. Returns nullopt on parse failure (caller keeps the old text).
std::optional<std::string> RerenderPrettyAfterRename(
  std::string_view pretty,
  const containers::FlatHashMap<std::string, std::string>& renames) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
  try {
    parsed = duckdb::Parser::ParseExpressionList(std::string{pretty});
  } catch (const duckdb::ParserException&) {
    return std::nullopt;
  }
  if (parsed.size() != 1 || !parsed[0]) {
    return std::nullopt;
  }
  auto rewrite = [&](this auto& self, duckdb::ParsedExpression& e) -> void {
    if (e.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
      auto& cref = e.Cast<duckdb::ColumnRefExpression>();
      if (!cref.ColumnNames().empty()) {
        auto it = renames.find(cref.ColumnNames().back().GetIdentifierName());
        if (it != renames.end()) {
          cref.ColumnNamesMutable().back() = duckdb::Identifier{it->second};
        }
      }
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      e, [&](duckdb::ParsedExpression& child) { self(child); });
  };
  rewrite(*parsed[0]);
  return parsed[0]->ToString();
}

void UpdatePendingDrops(PendingDrops& pending_drops, ObjectId parend_id,
                        ObjectId id, const std::shared_ptr<DropTask>& task,
                        bool is_root) {
  auto it = pending_drops.find(id);
  if (it != pending_drops.end()) {
    task->SetAttached(std::move(it->second));
    pending_drops.erase(it);
  }
  if (is_root) {
    pending_drops[parend_id].emplace_back(std::move(task));
  }
}

}  // namespace

void Snapshot::StampVersion(uint64_t version) noexcept { _version = version; }

Snapshot::Snapshot() = default;
Snapshot::~Snapshot() = default;

std::shared_ptr<Snapshot> Snapshot::Clone() const {
  // TODO(gnusi): COW
  auto result = std::make_shared<Snapshot>();
  result->_resolution_table = _resolution_table;
  result->_objects = _objects;
  result->_deps = _deps;
  result->_version = _version + 1;
  result->_in_load = _in_load;
  result->_version = _version;
  // Inherit the built role closures. A mutation that changes the role graph
  // overwrites them via RebuildRoleClosures; everything else reuses the
  // parent's map unchanged.
  result->_role_closures = _role_closures;
  // New snapshot starts with empty DuckDB cache (lazily populated)
  return result;
}

void Snapshot::EndLoad() noexcept { _in_load = false; }

connector::DuckDBEntryCache& Snapshot::GetDuckDBEntryCache() const {
  return _duckdb_cache;
}

std::shared_ptr<DatabaseDrop> Snapshot::CreateDatabaseDrop(
  PendingDrops& pending_drops, const std::shared_ptr<Database>& db,
  duckdb::shared_ptr<void> keep_alive) {
  const auto db_id = db->GetId();
  auto db_deps = GetDependency<DatabaseDependency>(db_id);
  auto schemas_drop =
    db_deps->schemas | std::views::transform([&](ObjectId id) {
      auto schema = GetObject<Schema>(id);
      SDB_ASSERT(schema);
      return CreateSchemaDrop(pending_drops, db_id, schema, false);
    }) |
    std::ranges::to<std::vector>();
  auto task = std::make_shared<DatabaseDrop>(db, std::move(schemas_drop),
                                             std::move(keep_alive));
  UpdatePendingDrops(pending_drops, ObjectId{}, db_id, task, false);
  return task;
}

std::shared_ptr<SchemaDrop> Snapshot::CreateSchemaDrop(
  PendingDrops& pending_drops, ObjectId db_id,
  const std::shared_ptr<Schema>& schema, bool is_root) {
  const auto schema_id = schema->GetId();
  auto schema_deps = GetDependency<SchemaDependency>(schema_id);
  auto tables_drop =
    schema_deps->tables | std::views::transform([&](ObjectId id) {
      auto table = GetObject<Table>(id);
      SDB_ASSERT(table);
      return CreateTableDrop(pending_drops, db_id, schema_id, table, false);
    }) |
    std::ranges::to<std::vector>();

  auto task = std::make_shared<SchemaDrop>(schema, std::move(tables_drop),
                                           db_id, is_root);
  UpdatePendingDrops(pending_drops, db_id, schema_id, task, is_root);
  return task;
}

// Store-table name of `table_id` ("db.schema.table"), or nullopt when
// the id is unset (self-referencing FK) or not resolvable.
std::optional<std::string> Snapshot::ComposeStoreTableName(
  ObjectId table_id) const {
  if (!table_id.isSet()) {
    return std::nullopt;
  }
  auto table = GetObject<Table>(table_id);
  if (!table) {
    return std::nullopt;
  }
  auto schema_obj = GetObject<Schema>(table->GetParentId());
  if (!schema_obj) {
    return std::nullopt;
  }
  auto db = GetObject<Database>(schema_obj->GetParentId());
  if (!db) {
    return std::nullopt;
  }
  return StoreTableName(db->GetName(), schema_obj->GetName(), table->GetName());
}

std::shared_ptr<TableDropBase> Snapshot::CreateTableDrop(
  PendingDrops& pending_drops, ObjectId db_id, ObjectId schema_id,
  const std::shared_ptr<Table>& table, bool is_root) {
  const auto table_id = table->GetId();
  auto table_deps = GetDependency<TableDependency>(table_id);
  auto owned_sequences =
    table_deps->owned_sequences | std::ranges::to<std::vector>();

  std::shared_ptr<TableDropBase> task;
  if (table->GetEngine() == TableEngine::Search) {
    task = std::make_shared<SearchTableDrop>(
      table, db_id, std::move(owned_sequences), schema_id, is_root);
  } else {
    auto indexes = table_deps->indexes |
                   std::views::transform([&](ObjectId id) {
                     auto index = GetObject<Index>(id);
                     SDB_ASSERT(index);
                     return CreateIndexDrop(pending_drops, db_id, schema_id,
                                            table_id, index, false);
                   }) |
                   std::ranges::to<std::vector>();
    std::string store_name;
    std::vector<std::string> fk_referenced;
    if (!table->Tombstoned()) {
      auto db = GetObject<Database>(db_id);
      auto schema_obj = GetObject<Schema>(schema_id);
      SDB_ASSERT(db && schema_obj);
      store_name =
        StoreTableName(db->GetName(), schema_obj->GetName(), table->GetName());
      for (const auto& fk : table->ForeignKeys()) {
        if (auto name = ComposeStoreTableName(fk.referenced_table)) {
          fk_referenced.push_back(std::move(*name));
        }
      }
    }
    task = std::make_shared<TableDrop>(
      table, std::move(indexes), std::move(owned_sequences), schema_id,
      std::move(store_name), std::move(fk_referenced), is_root);
  }
  UpdatePendingDrops(pending_drops, schema_id, table_id, task, is_root);
  return task;
}

std::shared_ptr<IndexDrop> Snapshot::CreateIndexDrop(
  PendingDrops& pending_drops, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, const std::shared_ptr<Index>& index, bool is_root) {
  // Capture the iresearch storage handle (weak) so the async IndexDrop can wait
  // for every holder (snapshots, txns, tasks) to release before removing the
  // on-disk directory. Secondary indexes have no storage -> empty weak.
  std::weak_ptr<search::InvertedIndexStorage> data;
  if (index->GetType() == ObjectType::InvertedIndex) {
    data = basics::downCast<const InvertedIndex>(*index).GetData();
  }
  auto task = std::make_shared<IndexDrop>(index, db_id, schema_id, table_id,
                                          std::move(data), is_root);
  UpdatePendingDrops(pending_drops, table_id, index->GetId(), task, is_root);
  return task;
}

template<typename T>
void Snapshot::RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                              bool replace) {
  if constexpr (std::is_same_v<T, Database>) {
    AddToResolution<ResolveType::Database>(parent_id, object->GetId(),
                                           object->GetName(), replace);
    AddObjectDefinition<DatabaseDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Schema>) {
    AddToResolution<ResolveType::Schema>(parent_id, object->GetId(),
                                         object->GetName(), replace);
    AddObjectDefinition<SchemaDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, PgSqlView>) {
    AddToResolution<ResolveType::Relation>(parent_id, object->GetId(),
                                           object->GetName(), replace);
    AddObjectDefinition<ViewDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Sequence>) {
    // Sequences share the relation namespace (PG: pg_class.relkind='S').
    if (!_resolution_table.AddObject<ResolveType::Relation>(
          parent_id, object->GetName(), object->GetId(), replace))
      [[unlikely]] {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("relation \"", object->GetName(), "\" already exists"));
    }
    AddObjectDefinition<SequenceDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
    AddToResolution<ResolveType::Function>(parent_id, object->GetId(),
                                           object->GetName(), replace);
    AddObjectDefinition<PgSqlFunctionDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, Tokenizer>) {
    AddToResolution<ResolveType::Tokenizer>(parent_id, object->GetId(),
                                            object->GetName(), replace);
    AddObjectDefinition<TokenizerDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, ForeignServer>) {
    AddToResolution<ResolveType::ForeignServer>(parent_id, object->GetId(),
                                                object->GetName(), replace);
    AddObjectDefinition<ForeignServerDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, UserMapping>) {
    AddToResolution<ResolveType::UserMapping>(parent_id, object->GetId(),
                                              object->GetName(), replace);
    AddObjectDefinition<UserMappingDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, PgSqlType>) {
    AddToResolution<ResolveType::Type>(parent_id, object->GetId(),
                                       object->GetName(), replace);
    AddObjectDefinition<PgSqlTypeDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Table>) {
    AddToResolution<ResolveType::Relation>(parent_id, object->GetId(),
                                           object->GetName(), replace);
    AddObjectDefinition<TableDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Index>) {
    AddToResolution<ResolveType::Relation>(
      object->GetParentId(), object->GetId(), object->GetName(), replace);
    AddObjectDefinition<IndexDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Role>) {
    AddToResolution<ResolveType::Role>(parent_id, object->GetId(),
                                       object->GetName(), replace);
    AddObjectDefinition(parent_id, std::move(object));
  } else {
    static_assert(false);
  }
}

template<typename T>
void Snapshot::UnregisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                                bool maybe_not_found) noexcept {
  if constexpr (std::is_same_v<T, Database>) {
    RemoveFromResolution<ResolveType::Database>(parent_id, object->GetName(),
                                                maybe_not_found);
  } else if constexpr (std::is_same_v<T, Role>) {
    RemoveFromResolution<ResolveType::Role>(parent_id, object->GetName(),
                                            maybe_not_found);
  } else if constexpr (std::is_same_v<T, Schema>) {
    RemoveFromResolution<ResolveType::Schema>(parent_id, object->GetName(),
                                              maybe_not_found);
  } else if constexpr (std::is_same_v<T, PgSqlView>) {
    RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                maybe_not_found);
  } else if constexpr (std::is_same_v<T, Sequence>) {
    RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                maybe_not_found);
  } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
    RemoveFromResolution<ResolveType::Function>(parent_id, object->GetName(),
                                                maybe_not_found);
  } else if constexpr (std::is_same_v<T, Tokenizer>) {
    RemoveFromResolution<ResolveType::Tokenizer>(parent_id, object->GetName(),
                                                 maybe_not_found);
  } else if constexpr (std::is_same_v<T, ForeignServer>) {
    RemoveFromResolution<ResolveType::ForeignServer>(
      parent_id, object->GetName(), maybe_not_found);
  } else if constexpr (std::is_same_v<T, UserMapping>) {
    RemoveFromResolution<ResolveType::UserMapping>(parent_id, object->GetName(),
                                                   maybe_not_found);
  } else if constexpr (std::is_same_v<T, PgSqlType>) {
    RemoveFromResolution<ResolveType::Type>(parent_id, object->GetName(),
                                            maybe_not_found);
  } else if constexpr (std::is_same_v<T, Table>) {
    RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                maybe_not_found);
  } else if constexpr (std::is_same_v<T, Index>) {
    RemoveFromResolution<ResolveType::Relation>(
      object->GetParentId(), object->GetName(), maybe_not_found);
    parent_id = object->GetRelationId();
  } else {
    static_assert(false);
  }
  SDB_ASSERT(parent_id.isSet());
  RemoveObjectDefinition(parent_id, object->GetId(), true, maybe_not_found);
}

template<ResolveType Type>
void Snapshot::AddToResolution(ObjectId parent_id, ObjectId id,
                               std::string_view name, bool replace) {
  if (!_resolution_table.AddObject<Type>(parent_id, name, id, replace))
    [[unlikely]] {
    ThrowDuplicateName<Type>(name);
  }
}

template<ResolveType Type>
void Snapshot::RemoveFromResolution(ObjectId parent_id, std::string_view name,
                                    bool maybe_not_found) noexcept {
  auto res = _resolution_table.RemoveObject<Type>(parent_id, name);
  if (!maybe_not_found) {
    SDB_ASSERT(res);
  }
}

template<typename Dep, typename Member, typename Edge>
void Snapshot::ModifyDependency(ObjectId target, Member Dep::* mem,
                                const Edge& edge, EdgeAction action) {
  if (!_deps.TryGetDependency(target)) {
    // unset target: pg_catalog/information_schema ref -- no edge to track.
    // missing Dep: graph traversal reached the same target twice, fine
    // on Delete (firstly we delete auto drop dependencies and then cascade).
    SDB_ASSERT(_in_load || action == EdgeAction::Delete || !target.isSet());
    return;
  }
  auto d = GetDependencyForWrite<Dep>(target);
  switch (action) {
    case EdgeAction::Add:
      (d.get()->*mem).insert(edge);
      break;
    case EdgeAction::Delete:
      (d.get()->*mem).erase(edge);
      break;
  }
}

void Snapshot::ModifyTableDependencies(ObjectId schema_id, const Table& table,
                                       EdgeAction action) {
  auto database_id = GetDatabaseId(table);
  auto resolve_seq = [&](const QualifiedRef& ref) {
    return Resolve<ResolveType::Relation, ObjectType::Sequence>(
      database_id, schema_id, ref.catalog, ref.schema, ref.name);
  };
  for (const auto& col : table.Columns()) {
    Refs type_refs;
    CollectTypeRefs(col.type, type_refs);
    for (auto type_id : type_refs.types) {
      ModifyDependency(type_id, &PgSqlTypeDependency::column_types, col.GetId(),
                       action);
    }
    if (col.expr && col.expr->HasExpr() && !col.IsGenerated()) {
      auto refs = col.expr->GetRefs(RefKinds::Sequences | RefKinds::Functions |
                                    RefKinds::Types);
      for (const auto& ref : refs.sequences) {
        ModifyDependency(resolve_seq(ref), &SequenceDependency::column_defaults,
                         col.GetId(), action);
      }
      for (const auto& ref : refs.functions) {
        ModifyDependency(
          Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
            database_id, schema_id, ref.catalog, ref.schema, ref.name),
          &PgSqlFunctionDependency::column_defaults, col.GetId(), action);
      }
      for (const auto& ref : refs.unbound_types) {
        ModifyDependency(
          Resolve<ResolveType::Type, ObjectType::PgSqlType>(
            database_id, schema_id, ref.catalog, ref.schema, ref.name),
          &PgSqlTypeDependency::column_defaults, col.GetId(), action);
      }
    }
  }
  for (const auto& fk : table.ForeignKeys()) {
    if (fk.referenced_table.isSet()) {
      ModifyDependency(fk.referenced_table,
                       &TableDependency::fk_referencing_tables, table.GetId(),
                       action);
    }
  }
  for (const auto& c : table.CheckConstraints()) {
    if (!c.expr || !c.expr->HasExpr()) {
      continue;
    }
    auto refs = c.expr->GetRefs(RefKinds::Sequences | RefKinds::Functions |
                                RefKinds::Types);
    auto edge = c.GetId();
    for (const auto& ref : refs.functions) {
      ModifyDependency(
        Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
          database_id, schema_id, ref.catalog, ref.schema, ref.name),
        &PgSqlFunctionDependency::constraints, edge, action);
    }
    for (const auto& ref : refs.sequences) {
      ModifyDependency(resolve_seq(ref), &SequenceDependency::constraints, edge,
                       action);
    }
    for (const auto& ref : refs.unbound_types) {
      ModifyDependency(
        Resolve<ResolveType::Type, ObjectType::PgSqlType>(
          database_id, schema_id, ref.catalog, ref.schema, ref.name),
        &PgSqlTypeDependency::constraints, edge, action);
    }
  }
}

template<ResolveType Kind, ObjectType... Allowed>
ObjectId Snapshot::Resolve(ObjectId db_id, ObjectId default_schema_id,
                           std::string_view catalog, std::string_view schema,
                           std::string_view name) const {
  if (schema == StaticStrings::kPgCatalogSchema ||
      schema == StaticStrings::kInformationSchema) {
    return {};
  }
  if (!catalog.empty()) {
    auto r = _resolution_table.ResolveObject<ResolveType::Database>(
      id::kInstance, catalog);
    if (!r) {
      return {};
    }
    db_id = *r;
  }
  ObjectId schema_id = default_schema_id;
  if (!schema.empty()) {
    auto r =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!r) {
      return {};
    }
    schema_id = *r;
  }
  auto r = _resolution_table.ResolveObject<Kind>(schema_id, name);
  if (!r) {
    return {};
  }
  auto it = _objects.find(*r);
  if (it == _objects.end()) {
    return {};
  }
  auto t = (*it)->GetType();
  if (!((t == Allowed) || ...)) {
    return {};
  }
  return *r;
}

void Snapshot::ModifyViewDependencies(ObjectId schema_id, const PgSqlView& view,
                                      RefKinds kinds, EdgeAction action) {
  auto database_id = GetDatabaseId(view);
  auto view_id = view.GetId();
  auto refs = view.GetRefs(kinds);
  for (const auto& ref : refs.sequences) {
    ModifyDependency(
      Resolve<ResolveType::Relation, ObjectType::Sequence>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &SequenceDependency::views, view_id, action);
  }
  for (const auto& ref : refs.relations) {
    // Table or View -- both inherit RelationDependency.views.
    ModifyDependency(
      Resolve<ResolveType::Relation, ObjectType::Table, ObjectType::PgSqlView>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &RelationDependency::views, view_id, action);
  }
  for (const auto& ref : refs.functions) {
    ModifyDependency(
      Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &PgSqlFunctionDependency::views, view_id, action);
  }
  for (const auto& ref : refs.unbound_types) {
    ModifyDependency(
      Resolve<ResolveType::Type, ObjectType::PgSqlType>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &PgSqlTypeDependency::views, view_id, action);
  }
}

void Snapshot::ModifyFunctionDependencies(ObjectId schema_id,
                                          const PgSqlFunction& func,
                                          EdgeAction action) {
  auto database_id = GetDatabaseId(func);
  auto fn_id = func.GetId();
  auto refs = func.GetRefs(RefKinds::All);
  for (const auto& ref : refs.sequences) {
    ModifyDependency(
      Resolve<ResolveType::Relation, ObjectType::Sequence>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &SequenceDependency::functions, fn_id, action);
  }
  for (const auto& ref : refs.relations) {
    ModifyDependency(
      Resolve<ResolveType::Relation, ObjectType::Table, ObjectType::PgSqlView>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &RelationDependency::functions, fn_id, action);
  }
  for (const auto& ref : refs.functions) {
    auto target = Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
      database_id, schema_id, ref.catalog, ref.schema, ref.name);
    if (target == fn_id) {
      continue;  // skip self
    }
    ModifyDependency(target, &PgSqlFunctionDependency::functions, fn_id,
                     action);
  }
  for (const auto& ref : refs.unbound_types) {
    ModifyDependency(
      Resolve<ResolveType::Type, ObjectType::PgSqlType>(
        database_id, schema_id, ref.catalog, ref.schema, ref.name),
      &PgSqlTypeDependency::functions, fn_id, action);
  }
  for (auto type_id : refs.types) {
    ModifyDependency(type_id, &PgSqlTypeDependency::functions, fn_id, action);
  }
}

void Snapshot::ModifyInvertedIndexDependencies(const InvertedIndex& index,
                                               ObjectId index_id,
                                               EdgeAction action) {
  auto database_id = GetDatabaseId(index);
  auto schema_id = index.GetParentId();
  for (const auto& key : index.ExpressionKeys()) {
    const auto& expr = key.data;
    SDB_ASSERT(!expr.pretty_printed.empty());
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
    try {
      parsed = duckdb::Parser::ParseExpressionList(expr.pretty_printed);
    } catch (const duckdb::ParserException&) {
      continue;
    }
    for (const auto& p : parsed) {
      SDB_ASSERT(p);
      for (const auto& ref : ExtractRefs(*p, RefKinds::Functions).functions) {
        ModifyDependency(
          Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
            database_id, schema_id, ref.catalog, ref.schema, ref.name),
          &PgSqlFunctionDependency::indexes, index_id, action);
      }
    }
  }
}

void Snapshot::ModifyRoleDependencies(const Object& obj, EdgeAction action) {
  const auto self = obj.GetId();
  // PUBLIC pseudo-grantee (id 0) and the unset owner, so they are skipped --
  // PUBLIC and "no owner" are not droppable roles.
  auto touch = [&](ObjectId role) {
    if (!role.isSet()) {
      return;
    }
    if (action == EdgeAction::Add) {
      // The role's dependency node may not exist yet (a role nobody references
      // has none); create-if-missing before GetDependencyForWrite, which
      // assumes the node is present.
      _deps.AddDependency<RoleDependency>(role);
      GetDependencyForWrite<RoleDependency>(role)->referencing_objects.insert(
        self);
    } else if (_deps.TryGetDependency(role)) {
      GetDependencyForWrite<RoleDependency>(role)->referencing_objects.erase(
        self);
    }
  };
  auto touch_acl = [&](auto acl) {
    for (const auto& item : acl) {
      touch(item.grantee);
      touch(item.grantor);
    }
  };
  touch(obj.GetOwner());
  touch_acl(obj.GetAcl());
  if (obj.GetType() == ObjectType::Table) {
    for (const auto& col : basics::downCast<const Table>(obj).Columns()) {
      touch_acl(col.GetAcl());
    }
  } else if (obj.GetType() == ObjectType::UserMapping) {
    // A USER MAPPING FOR <role> references that role, so DROP ROLE is refused
    // while the mapping exists (PG-style). PUBLIC mappings have no role id.
    touch(basics::downCast<const UserMapping>(obj).GetRoleId());
  } else if (obj.GetType() == ObjectType::Role) {
    const auto& defaults = basics::downCast<const Role>(obj).DefaultAcls();
    if (!defaults.empty()) {
      touch(self);
    }
    for (const auto& entry : defaults) {
      touch_acl(entry.acl);
    }
  }
}

template<typename DependencyType>
void Snapshot::AddObjectDefinition(ObjectId parent_id,
                                   std::shared_ptr<Object> object) {
  if constexpr (!std::is_void_v<DependencyType>) {
    bool inserted = _deps.AddDependency<DependencyType>(object->GetId());
    SDB_ASSERT(inserted);
  }
  SDB_ASSERT(object->GetId().isSet());
  auto [_, inserted] = _objects.insert(object);
  SDB_ASSERT(inserted);
  AddDependencies(parent_id, *object);
}

void Snapshot::AddDependencies(ObjectId parent_id, const Object& obj) {
  auto id = obj.GetId();
  ModifyRoleDependencies(obj, EdgeAction::Add);
  switch (obj.GetType()) {
    case ObjectType::Database:
    case ObjectType::Role:
      break;
    case ObjectType::Schema:
      GetDependencyForWrite<DatabaseDependency>(parent_id)->schemas.insert(id);
      break;
    case ObjectType::Tokenizer:
      GetDependencyForWrite<SchemaDependency>(parent_id)->tokenizers.insert(id);
      break;
    case ObjectType::ForeignServer:
      GetDependencyForWrite<DatabaseDependency>(parent_id)
        ->foreign_servers.insert(id);
      break;
    case ObjectType::UserMapping:
      // parent = the owning server, so DROP SERVER cascades / RESTRICT-blocks.
      GetDependencyForWrite<ForeignServerDependency>(parent_id)
        ->user_mappings.insert(id);
      break;
    case ObjectType::Table: {
      GetDependencyForWrite<SchemaDependency>(parent_id)->tables.insert(id);
      const auto& table = basics::downCast<Table>(obj);
      for (const auto& c : table.Columns()) {
        if (!_objects.contains(c.GetId())) {
          _objects.insert(std::make_shared<Column>(c));
        }
      }
      for (const auto& c : table.CheckConstraints()) {
        if (!_objects.contains(c.GetId())) {
          _objects.insert(std::make_shared<CheckConstraint>(c));
        }
      }
      ModifyTableDependencies(parent_id, table, EdgeAction::Add);
    } break;
    case ObjectType::PgSqlView:
      GetDependencyForWrite<SchemaDependency>(parent_id)->views.insert(id);
      ModifyViewDependencies(parent_id, basics::downCast<PgSqlView>(obj),
                             RefKinds::All, EdgeAction::Add);
      break;
    case ObjectType::PgSqlFunction:
      GetDependencyForWrite<SchemaDependency>(parent_id)->functions.insert(id);
      ModifyFunctionDependencies(
        parent_id, basics::downCast<PgSqlFunction>(obj), EdgeAction::Add);
      break;
    case ObjectType::PgSqlType:
      GetDependencyForWrite<SchemaDependency>(parent_id)->types.insert(id);
      break;
    case ObjectType::Sequence: {
      // Owned sequences live under the table only
      const auto& seq = basics::downCast<Sequence>(obj);
      if (auto owner = seq.GetOwnerTableId(); owner.isSet()) {
        GetDependencyForWrite<TableDependency>(owner)->owned_sequences.insert(
          id);
        auto table = GetObject<Table>(owner);
        SDB_ASSERT(table);
        ModifyTableDependencies(parent_id, *table, EdgeAction::Add);
      } else {
        GetDependencyForWrite<SchemaDependency>(parent_id)->sequences.insert(
          id);
      }
    } break;
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex: {
      const auto& index = basics::downCast<Index>(obj);
      GetDependencyForWrite<RelationDependency>(index.GetRelationId())
        ->indexes.insert(id);
      for (auto tokenizer_id : index.GetTokenizers()) {
        GetDependencyForWrite<TokenizerDependency>(tokenizer_id)
          ->indexes.insert(id);
      }
      if (obj.GetType() == ObjectType::InvertedIndex) {
        ModifyInvertedIndexDependencies(basics::downCast<InvertedIndex>(index),
                                        id, EdgeAction::Add);
      }
    } break;
    case ObjectType::Virtual:
    case ObjectType::Column:
    case ObjectType::CheckConstraint:
      SDB_ASSERT(_in_load);
      break;
    case ObjectType::Invalid:
    case ObjectType::Tombstone:
      SDB_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<Database>> Snapshot::GetDatabases() const {
  return _resolution_table.GetDatabaseIds() |
         std::views::transform([&](ObjectId db_id) {
           auto it = _objects.find(db_id);
           SDB_ASSERT(it != _objects.end());
           return basics::downCast<Database>(*it);
         }) |
         std::ranges::to<std::vector>();
}

std::vector<std::shared_ptr<Role>> Snapshot::GetRoles() const {
  return _resolution_table.GetRoleIds() |
         std::views::transform([&](ObjectId role_id) {
           auto it = _objects.find(role_id);
           SDB_ASSERT(it != _objects.end());
           return basics::downCast<Role>(*it);
         }) |
         std::ranges::to<std::vector>();
}

std::vector<std::shared_ptr<Schema>> Snapshot::GetSchemas(
  ObjectId db_id) const {
  auto db_deps = GetDependency<DatabaseDependency>(db_id);
  return db_deps->schemas | std::views::transform([&](ObjectId schema_id) {
           auto it = _objects.find(schema_id);
           SDB_ASSERT(it != _objects.end());
           return basics::downCast<Schema>(*it);
         }) |
         std::ranges::to<std::vector>();
}

std::vector<std::shared_ptr<Object>> Snapshot::GetRelations(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      return _resolution_table.GetRelationIds(schema_id) |
             std::views::transform(
               [&](ObjectId relation_id) -> std::shared_ptr<Object> {
                 return GetObject(relation_id);
               }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<Object>>{});
}

std::vector<std::shared_ptr<Table>> Snapshot::GetTables(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
      return schema_deps->tables |
             std::views::transform([&](ObjectId table_id) {
               auto it = _objects.find(table_id);
               SDB_ASSERT(it != _objects.end());
               return basics::downCast<Table>(*it);
             }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<Table>>{});
}

std::vector<std::shared_ptr<PgSqlView>> Snapshot::GetViews(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
      return schema_deps->views | std::views::transform([&](ObjectId view_id) {
               auto it = _objects.find(view_id);
               SDB_ASSERT(it != _objects.end());
               return basics::downCast<PgSqlView>(*it);
             }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<PgSqlView>>{});
}

std::vector<std::shared_ptr<Sequence>> Snapshot::GetSequences(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
      return schema_deps->sequences |
             std::views::transform([&](ObjectId sequence_id) {
               auto it = _objects.find(sequence_id);
               SDB_ASSERT(it != _objects.end());
               return basics::downCast<Sequence>(*it);
             }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<Sequence>>{});
}

std::vector<std::shared_ptr<PgSqlFunction>> Snapshot::GetFunctions(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
      return schema_deps->functions |
             std::views::transform([&](ObjectId function_id) {
               auto it = _objects.find(function_id);
               SDB_ASSERT(it != _objects.end());
               return basics::downCast<PgSqlFunction>(*it);
             }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<PgSqlFunction>>{});
}

std::vector<std::shared_ptr<Index>> Snapshot::GetIndexes(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      std::vector<std::shared_ptr<Index>> result;
      const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
      for (const auto table_id : schema_deps->tables) {
        const auto& table_deps = GetDependency<TableDependency>(table_id);
        for (const auto index_id : table_deps->indexes) {
          auto it = _objects.find(index_id);
          SDB_ASSERT(it != _objects.end());
          result.push_back(basics::downCast<Index>(*it));
        }
      }
      for (const auto view_id : schema_deps->views) {
        const auto& view_deps = GetDependency<ViewDependency>(view_id);
        for (const auto index_id : view_deps->indexes) {
          auto it = _objects.find(index_id);
          SDB_ASSERT(it != _objects.end());
          result.push_back(basics::downCast<Index>(*it));
        }
      }
      return result;
    })
    .value_or(std::vector<std::shared_ptr<Index>>{});
}

void Snapshot::VisitRelations(
  ObjectId db_id, std::string_view schema,
  absl::FunctionRef<void(const Object&)> visitor) const {
  auto schema_id =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
  if (!schema_id) {
    return;
  }
  for (const auto relation_id : _resolution_table.GetRelationIds(*schema_id)) {
    auto it = _objects.find(relation_id);
    SDB_ASSERT(it != _objects.end());
    visitor(basics::downCast<Object>(**it));
  }
}

void Snapshot::VisitViews(
  ObjectId db_id, std::string_view schema,
  absl::FunctionRef<void(const PgSqlView&)> visitor) const {
  auto schema_id =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
  if (!schema_id) {
    return;
  }
  const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
  for (const auto view_id : schema_deps->views) {
    auto it = _objects.find(view_id);
    SDB_ASSERT(it != _objects.end());
    visitor(basics::downCast<PgSqlView>(**it));
  }
}

void Snapshot::VisitFunctions(
  ObjectId db_id, std::string_view schema,
  absl::FunctionRef<void(const PgSqlFunction&)> visitor) const {
  auto schema_id =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
  if (!schema_id) {
    return;
  }
  const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
  for (const auto function_id : schema_deps->functions) {
    auto it = _objects.find(function_id);
    SDB_ASSERT(it != _objects.end());
    visitor(basics::downCast<PgSqlFunction>(**it));
  }
}

void Snapshot::VisitIndexes(
  ObjectId db_id, std::string_view schema,
  absl::FunctionRef<void(const Index&)> visitor) const {
  auto schema_id =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
  if (!schema_id) {
    return;
  }
  const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
  for (const auto table_id : schema_deps->tables) {
    const auto& table_deps = GetDependency<TableDependency>(table_id);
    for (const auto index_id : table_deps->indexes) {
      auto it = _objects.find(index_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<Index>(**it));
    }
  }
  for (const auto view_id : schema_deps->views) {
    const auto& view_deps = GetDependency<ViewDependency>(view_id);
    for (const auto index_id : view_deps->indexes) {
      auto it = _objects.find(index_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<Index>(**it));
    }
  }
}

std::vector<std::shared_ptr<Tokenizer>> Snapshot::GetTokenizers(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      return _resolution_table.GetTokenizerIds(schema_id) |
             std::views::transform(
               [&](ObjectId tokenizer_id) -> std::shared_ptr<Tokenizer> {
                 return GetObject<Tokenizer>(tokenizer_id);
               }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<Tokenizer>>{});
}

std::vector<std::shared_ptr<ForeignServer>> Snapshot::GetForeignServers(
  ObjectId db_id) const {
  return _resolution_table.GetForeignServerIds(db_id) |
         std::views::transform(
           [&](ObjectId server_id) -> std::shared_ptr<ForeignServer> {
             return GetObject<ForeignServer>(server_id);
           }) |
         std::ranges::to<std::vector>();
}

std::vector<std::shared_ptr<UserMapping>> Snapshot::GetUserMappings(
  ObjectId server_id) const {
  return _resolution_table.GetUserMappingIds(server_id) |
         std::views::transform(
           [&](ObjectId mapping_id) -> std::shared_ptr<UserMapping> {
             return GetObject<UserMapping>(mapping_id);
           }) |
         std::ranges::to<std::vector>();
}

std::vector<std::shared_ptr<PgSqlType>> Snapshot::GetTypes(
  ObjectId db_id, std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      return _resolution_table.GetTypeIds(schema_id) |
             std::views::transform(
               [&](ObjectId type_id) -> std::shared_ptr<PgSqlType> {
                 return GetObject<PgSqlType>(type_id);
               }) |
             std::ranges::to<std::vector>();
    })
    .value_or(std::vector<std::shared_ptr<PgSqlType>>{});
}

std::shared_ptr<Role> Snapshot::GetRole(std::string_view name) const {
  auto id =
    _resolution_table.ResolveObject<ResolveType::Role>(id::kInstance, name);
  if (!id) {
    return {};
  }
  auto role = GetObject<Role>(*id);
  SDB_ASSERT(role);
  return role;
}

std::shared_ptr<Database> Snapshot::GetDatabase(
  std::string_view database) const {
  return _resolution_table
    .ResolveObject<ResolveType::Database>(id::kInstance, database)
    .transform([&](ObjectId db_id) {
      auto it = _objects.find(db_id);
      SDB_ASSERT(it != _objects.end());
      return basics::downCast<Database>(*it);
    })
    .value_or(nullptr);
}

std::shared_ptr<Schema> Snapshot::GetSchema(ObjectId db_id,
                                            std::string_view schema) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .transform([&](ObjectId schema_id) {
      auto it = _objects.find(schema_id);
      SDB_ASSERT(it != _objects.end());
      return basics::downCast<Schema>(*it);
    })
    .value_or(nullptr);
}

bool Snapshot::CheckSchemaEmptyDependency(ObjectId schema_id) const {
  return GetDependency<SchemaDependency>(schema_id)->Empty();
}

const auth::RoleClosure& Snapshot::ClosureFor(ObjectId role) const {
  if (const auto* rc = _role_closures.Find(role)) {
    return *rc;
  }
  static const auth::RoleClosure kEmpty;
  return kEmpty;
}

void Snapshot::RebuildRoleClosures() { _role_closures.Build(*this); }

std::size_t Snapshot::RoleDependentCount(ObjectId role) const {
  auto dep = _deps.TryGetDependency(role);
  if (!dep) {
    return 0;
  }
  return basics::downCast<const RoleDependency>(*dep)
    .referencing_objects.size();
}

void Snapshot::RequireAccess(ObjectId role, const Object& object,
                             AclMode need) const {
  if (need == AclMode::NoRights || ClosureFor(role).Can(object, need)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for ", pg::ToPgObjectTypeName(object.GetType()),
            " ", object.GetName()));
}

template<typename T>
std::shared_ptr<T> Snapshot::EnforceRead(const AccessContext& ax,
                                         std::shared_ptr<T> obj) const {
  if (obj) {
    RequireAccess(ax.role, *obj, ax.need);
  }
  return obj;
}

std::shared_ptr<PgSqlType> Snapshot::GetType(const AccessContext& ax,
                                             ObjectId db_id,
                                             std::string_view schema,
                                             std::string_view name) const {
  auto type =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Type>(schema_id,
                                                                  name);
      })
      .transform(
        [&](ObjectId type_id) { return GetObject<PgSqlType>(type_id); })
      .value_or(nullptr);
  return EnforceRead(ax, std::move(type));
}

std::shared_ptr<Object> Snapshot::GetRelation(const AccessContext& ax,
                                              ObjectId db_id,
                                              std::string_view schema,
                                              std::string_view relation) const {
  auto rel = _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
               .and_then([&](ObjectId schema_id) {
                 return _resolution_table.ResolveObject<ResolveType::Relation>(
                   schema_id, relation);
               })
               .transform([&](ObjectId relation_id) {
                 auto it = _objects.find(relation_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<Object>(*it);
               })
               .value_or(nullptr);
  return EnforceRead(ax, std::move(rel));
}

std::shared_ptr<PgSqlFunction> Snapshot::GetFunction(
  const AccessContext& ax, ObjectId db_id, std::string_view schema,
  std::string_view function) const {
  auto fn = _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
              .and_then([&](ObjectId schema_id) {
                return _resolution_table.ResolveObject<ResolveType::Function>(
                  schema_id, function);
              })
              .transform([&](ObjectId function_id) {
                return GetObject<PgSqlFunction>(function_id);
              })
              .value_or(nullptr);
  return EnforceRead(ax, std::move(fn));
}

std::shared_ptr<Tokenizer> Snapshot::GetTokenizer(const AccessContext& ax,
                                                  ObjectId db_id,
                                                  std::string_view schema,
                                                  std::string_view name) const {
  std::shared_ptr<Tokenizer> tok;
  if (auto schema_id =
        _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)) {
    if (auto id = _resolution_table.ResolveObject<ResolveType::Tokenizer>(
          *schema_id, name)) {
      tok = GetObject<Tokenizer>(*id);
    }
  }
  return EnforceRead(ax, std::move(tok));
}

std::shared_ptr<ForeignServer> Snapshot::GetForeignServer(
  ObjectId db_id, std::string_view name) const {
  auto id =
    _resolution_table.ResolveObject<ResolveType::ForeignServer>(db_id, name);
  if (!id) {
    return nullptr;
  }
  return GetObject<ForeignServer>(*id);
}

// `role` is the mapped role's name; "public" selects the PUBLIC mapping.
std::shared_ptr<UserMapping> Snapshot::GetUserMapping(
  ObjectId server_id, std::string_view role) const {
  auto id =
    _resolution_table.ResolveObject<ResolveType::UserMapping>(server_id, role);
  if (!id) {
    return nullptr;
  }
  return GetObject<UserMapping>(*id);
}

void Snapshot::RequireForeignServerNameGloballyUnique(
  ObjectId database_id, std::string_view name) const {
  for (const auto& db : GetDatabases()) {
    if (db->GetId() == database_id) {
      continue;
    }
    if (GetObjectId<ResolveType::ForeignServer>(db->GetId(), name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("server \"", name, "\" already exists in database \"",
                db->GetName(), "\""),
        ERR_HINT("Foreign server attachment names are instance-wide; "
                 "choose a name not used by any database."));
    }
  }
}

std::shared_ptr<Table> Snapshot::GetTable(const AccessContext& ax,
                                          ObjectId db_id,
                                          std::string_view schema,
                                          std::string_view table) const {
  auto rel = GetRelation(NoAccessCheck(), db_id, schema, table);
  if (!rel || rel->GetType() != ObjectType::Table) {
    return nullptr;
  }
  return EnforceRead(ax, basics::downCast<Table>(std::move(rel)));
}

std::shared_ptr<Sequence> Snapshot::GetSequence(const AccessContext& ax,
                                                ObjectId db_id,
                                                ObjectId schema_id,
                                                std::string_view name) const {
  std::shared_ptr<Sequence> seq;
  if (auto id = _resolution_table.ResolveObject<ResolveType::Relation>(
        schema_id, name)) {
    auto obj = GetObject(*id);
    if (obj && obj->GetType() == ObjectType::Sequence) {
      seq = basics::downCast<Sequence>(std::move(obj));
    }
  }
  return EnforceRead(ax, std::move(seq));
}

bool Snapshot::HasIndexes(ObjectId relation_id) const {
  return !GetDependency<RelationDependency>(relation_id)->indexes.empty();
}

std::shared_ptr<Object> Snapshot::GetObject(ObjectId id) const {
  auto it = _objects.find(id);
  if (it == _objects.end()) {
    return nullptr;
  }
  return *it;
}

std::vector<std::shared_ptr<Index>> Snapshot::GetIndexesByRelation(
  ObjectId relation_id) const {
  auto relation_deps = GetDependency<RelationDependency>(relation_id);
  return relation_deps->indexes | std::views::transform([&](auto index_id) {
           return GetObject<Index>(index_id);
         }) |
         std::ranges::to<std::vector>();
}

template<ResolveType Type>
std::optional<ObjectId> Snapshot::GetObjectId(ObjectId parent_id,
                                              std::string_view name) const {
  return _resolution_table.ResolveObject<Type>(parent_id, name);
}

std::shared_ptr<Database> Snapshot::GetDatabase(ObjectId database) const {
  auto obj = GetObject(database);
  if (!obj) {
    return nullptr;
  }
  return basics::downCast<Database>(obj);
}

template<ResolveType Type>
void Snapshot::ReplaceObject(ObjectId parent_id, std::string_view old_name,
                             std::shared_ptr<Object> new_object) {
  if (old_name != new_object->GetName()) {
    auto removed = _resolution_table.RemoveObject<Type>(parent_id, old_name);
    SDB_ASSERT(removed);
    if (!_resolution_table.AddObject<Type>(parent_id, new_object->GetName(),
                                           new_object->GetId(), false))
      [[unlikely]] {
      ThrowDuplicateName<Type>(new_object->GetName());
    }
  } else {
    // Name unchanged, but must refresh the string_view to point to new
    // object's _name
    bool ok = _resolution_table.AddObject<Type>(
      parent_id, new_object->GetName(), new_object->GetId(), true);
    SDB_ASSERT(ok);
  }

  auto it = _objects.find(new_object->GetId());
  SDB_ASSERT(it != _objects.end());
  SDB_ASSERT((*it)->GetId() == new_object->GetId());
  auto old_object = *it;
  ModifyRoleDependencies(*old_object, EdgeAction::Delete);
  // Refresh reverse-edges if the object's body changed. Replaces of
  // objects without body (Sequence, ...) are pure name/value
  // swaps and need no edge fixups.
  switch (new_object->GetType()) {
    case ObjectType::Table:
      ModifyTableDependencies(parent_id, basics::downCast<Table>(*old_object),
                              EdgeAction::Delete);
      break;
    case ObjectType::PgSqlView:
      ModifyViewDependencies(parent_id,
                             basics::downCast<PgSqlView>(*old_object),
                             RefKinds::All, EdgeAction::Delete);
      break;
    case ObjectType::PgSqlFunction:
      ModifyFunctionDependencies(parent_id,
                                 basics::downCast<PgSqlFunction>(*old_object),
                                 EdgeAction::Delete);
      break;
    default:
      break;
  }
  const_cast<std::shared_ptr<Object>&>(*it) = new_object;
  ModifyRoleDependencies(*new_object, EdgeAction::Add);
  switch (new_object->GetType()) {
    case ObjectType::Table: {
      const auto& table = basics::downCast<Table>(*new_object);
      for (const auto& c : table.Columns()) {
        if (!_objects.contains(c.GetId())) {
          _objects.insert(std::make_shared<Column>(c));
        }
      }
      for (const auto& c : table.CheckConstraints()) {
        if (!_objects.contains(c.GetId())) {
          _objects.insert(std::make_shared<CheckConstraint>(c));
        }
      }
      ModifyTableDependencies(parent_id, table, EdgeAction::Add);
    } break;
    case ObjectType::PgSqlView:
      ModifyViewDependencies(parent_id,
                             basics::downCast<PgSqlView>(*new_object),
                             RefKinds::All, EdgeAction::Add);
      break;
    case ObjectType::PgSqlFunction:
      ModifyFunctionDependencies(parent_id,
                                 basics::downCast<PgSqlFunction>(*new_object),
                                 EdgeAction::Add);
      break;
    default:
      break;
  }
}

template<typename T>
std::shared_ptr<const T> Snapshot::GetDependency(ObjectId id) const {
  return basics::downCast<const T>(_deps.GetDependency(id));
}

template<typename T>
std::shared_ptr<T> Snapshot::GetDependencyForWrite(ObjectId id) {
  auto dep = basics::downCast<T>(_deps.GetDependency(id)->Clone());
  _deps.SetDependency(id, dep);
  return dep;
}

// Cross-tree fixups for DROP seed. Composition cleanup is async.
DropPlan Snapshot::ComputeDropPlan(ObjectId seed) const {
  auto lookup = [this](ObjectId id) { return GetObject(id); };
  auto indexes_using_col = [this](ObjectId table_id, ObjectId col_id) {
    std::vector<ObjectId> result;
    auto td = GetDependency<TableDependency>(table_id);
    SDB_ASSERT(td);
    for (auto idx_id : td->indexes) {
      auto idx = GetObject<Index>(idx_id);
      SDB_ASSERT(idx);
      if (idx->ReferencesColumn(col_id)) {
        result.push_back(idx_id);
      }
    }
    return result;
  };
  auto dep_lookup = [this](ObjectId id) { return _deps.TryGetDependency(id); };
  return DropEmitter{seed, lookup, indexes_using_col, dep_lookup}.ComputePlan();
}

// Plan for ALTER TABLE DROP COLUMN: rewrite the owning table without the
// column and cascade-drop every index covering it (PG column->index cascade).
// The column has no dependency edges of its own, so this is built directly
// rather than seeded into DropEmitter (which walks a seed's dependents).
DropPlan Snapshot::ComputeColumnDropPlan(ObjectId table_id,
                                         ObjectId col_id) const {
  DropPlan plan;
  auto table = GetObject<Table>(table_id);
  SDB_ASSERT(table);
  auto& rw = plan.table_rewrites[table_id];
  rw.schema_id = table->GetParentId();
  rw.table = table->DropColumn(col_id);
  if (auto td = GetDependency<TableDependency>(table_id)) {
    for (auto idx_id : td->indexes) {
      auto idx = GetObject<Index>(idx_id);
      if (idx && idx->ReferencesColumn(col_id)) {
        plan.index_drops.push_back(idx_id);
      }
    }
  }
  return plan;
}

void Snapshot::CommitDropPlan(CatalogStore::WriteContext& ctx,
                              const DropPlan& plan) const {
  duckdb::MemoryStream stream;
  // Store-side index drops precede the column ALTERs below: a covering
  // index must be gone before its column can be dropped, and enforcement
  // (UNIQUE) must stop the moment the drop commits. The async task only
  // sweeps index storage and definitions.
  for (auto idx_id : plan.index_drops) {
    if (auto idx = GetObject<Index>(idx_id)) {
      ctx.WriteTombstone(idx->GetRelationId(), idx_id);
      ctx.DropStoreIndex(idx_id);
    }
  }
  for (const auto& [tid, rw] : plan.table_rewrites) {
    ctx.PutDefinition(rw.schema_id, ObjectType::Table, tid,
                      catalog::SerializeObject(*rw.table, stream));
    // Cascades can drop columns of surviving tables (e.g. a column whose
    // dependency lived in the dropped schema) -- the store table follows.
    if (rw.table->GetEngine() != TableEngine::Transactional ||
        rw.table->Tombstoned()) {
      continue;
    }
    auto old_table = GetObject<Table>(tid);
    if (!old_table) {
      continue;
    }
    auto schema_obj = GetObject<Schema>(rw.schema_id);
    SDB_ASSERT(schema_obj);
    auto database = GetObject<Database>(schema_obj->GetParentId());
    SDB_ASSERT(database);
    auto store_name = StoreTableName(database->GetName(), schema_obj->GetName(),
                                     old_table->GetName());
    for (const auto& fk : old_table->ForeignKeys()) {
      auto kept = absl::c_any_of(rw.table->ForeignKeys(), [&](const auto& f) {
        return f.referenced_table == fk.referenced_table &&
               f.columns == fk.columns;
      });
      if (kept) {
        continue;
      }
      if (auto referenced = ComposeStoreTableName(fk.referenced_table)) {
        ctx.DropStoreForeignKey(*referenced, store_name);
        ctx.DropStoreForeignKey(store_name, *referenced);
      }
    }
    auto rewritten =
      MakeStoreTableDef(database->GetName(), schema_obj->GetName(), *rw.table);
    if (rewritten.columns.empty()) {
      // Dropping the last column is refused by ALTER; recreate instead
      // (PG keeps the zero-column table). TODO(M2): rows must survive
      // this once the store tables hold the data.
      ctx.DropStoreTable(store_name);
      ctx.CreateStoreTable(std::move(rewritten));
      continue;
    }
    std::vector<std::string> dropped_columns;
    for (const auto& old_col : old_table->Columns()) {
      if (old_col.GetId() == Column::kGeneratedPKId) {
        continue;
      }
      auto it = std::ranges::find_if(rw.table->Columns(), [&](const auto& c) {
        return c.GetId() == old_col.GetId();
      });
      if (it == rw.table->Columns().end()) {
        dropped_columns.emplace_back(old_col.GetName());
      }
    }
    if (dropped_columns.empty()) {
      continue;
    }
    // Surviving store indexes block the ALTER whenever they cover a
    // column positioned after the dropped one; recreate them around the
    // drop (data lives in the rows / iresearch, so rebuilds are cheap
    // and inverted instances carry no state of their own).
    std::vector<std::shared_ptr<Index>> surviving;
    if (auto table_deps = GetDependency<TableDependency>(tid)) {
      for (auto idx_id : table_deps->indexes) {
        if (std::ranges::find(plan.index_drops, idx_id) !=
            plan.index_drops.end()) {
          continue;
        }
        if (auto idx = GetObject<Index>(idx_id)) {
          surviving.push_back(std::move(idx));
        }
      }
    }
    for (const auto& idx : surviving) {
      ctx.DropStoreIndex(idx->GetId());
    }
    for (auto& name : dropped_columns) {
      ctx.DropStoreColumn(store_name, std::move(name));
    }
    for (const auto& idx : surviving) {
      if (auto def = MakeStoreIndexDef(
            database->GetName(), schema_obj->GetName(), *rw.table, *idx)) {
        ctx.CreateStoreIndex(std::move(*def));
      }
    }
  }
  for (const auto& [schema_id, view_id] : plan.view_drops) {
    ctx.DropDefinition(schema_id, ObjectType::PgSqlView, view_id);
  }
  for (const auto& [schema_id, fn_id] : plan.function_drops) {
    ctx.DropDefinition(schema_id, ObjectType::PgSqlFunction, fn_id);
  }
}

// Apply cross-tree mutations in-memory; schedule IndexDrop tasks for
// cascade-dropped indexes (column->index cascade).
void Snapshot::ApplyDropPlan(PendingDrops& pending_drops, ObjectId db_id,
                             DropPlan& plan) {
  for (const auto& [schema_id, view_id] : plan.view_drops) {
    if (auto v = GetObject<PgSqlView>(view_id)) {
      UnregisterObject(std::move(v), schema_id);
    }
  }
  for (const auto& [schema_id, fn_id] : plan.function_drops) {
    if (auto f = GetObject<PgSqlFunction>(fn_id)) {
      UnregisterObject(std::move(f), schema_id);
    }
  }
  for (auto& [tid, rw] : plan.table_rewrites) {
    auto it = _objects.find(tid);
    if (it == _objects.end()) {
      continue;
    }
    ReplaceObject<ResolveType::Relation>(rw.schema_id, (*it)->GetName(),
                                         std::move(rw.table));
  }
  for (auto idx_id : plan.index_drops) {
    auto idx = GetObject<Index>(idx_id);
    if (!idx) {
      continue;
    }
    auto table_id = idx->GetRelationId();
    auto table = GetObject<Table>(table_id);
    if (!table) {
      continue;
    }
    auto task =
      CreateIndexDrop(pending_drops, db_id, table->GetParentId(), table_id, idx,
                      /*is_root=*/true);
    UnregisterObject(std::move(idx), table_id);
    DropTask::Schedule(std::move(task)).Detach();
  }
}

void Snapshot::RemoveObjectDefinition(ObjectId parent_id, ObjectId id,
                                      bool root,
                                      bool maybe_not_found) noexcept {
  auto node = _objects.extract(id);
  if (maybe_not_found && node.empty()) {
    return;
  }
  SDB_ASSERT(!node.empty());
  std::shared_ptr<Object> obj = node.value();
  SDB_ASSERT(obj);
  ModifyRoleDependencies(*obj, EdgeAction::Delete);
  auto drop_childs = [&](const auto& deps) {
    for (auto child_id : deps) {
      RemoveObjectDefinition(id, child_id);
    }
  };
  // Drop from parent deps
  if (root) {
    switch (obj->GetType()) {
      case ObjectType::Database:
      case ObjectType::Role:
        break;
      case ObjectType::Schema: {
        auto db_deps = GetDependencyForWrite<DatabaseDependency>(parent_id);
        SDB_ASSERT(db_deps);
        db_deps->schemas.erase(id);
      } break;
      case ObjectType::SecondaryIndex:
      case ObjectType::InvertedIndex: {
        const auto& index = basics::downCast<Index>(*obj);
        auto relation_deps =
          GetDependencyForWrite<RelationDependency>(index.GetRelationId());
        relation_deps->indexes.erase(id);
        for (auto tokenizer_id : index.GetTokenizers()) {
          auto dep = GetDependencyForWrite<TokenizerDependency>(tokenizer_id);
          SDB_ASSERT(dep);
          dep->indexes.erase(obj->GetId());
        }
        if (obj->GetType() == ObjectType::InvertedIndex) {
          ModifyInvertedIndexDependencies(
            basics::downCast<InvertedIndex>(index), id, EdgeAction::Delete);
        }
      } break;
      case ObjectType::PgSqlFunction: {
        GetDependencyForWrite<SchemaDependency>(parent_id)->functions.erase(id);
        ModifyFunctionDependencies(
          parent_id, basics::downCast<PgSqlFunction>(*obj), EdgeAction::Delete);
      } break;
      case ObjectType::Tokenizer:
        GetDependencyForWrite<SchemaDependency>(parent_id)->tokenizers.erase(
          id);
        break;
      case ObjectType::ForeignServer:
        GetDependencyForWrite<DatabaseDependency>(parent_id)
          ->foreign_servers.erase(id);
        break;
      case ObjectType::UserMapping:
        // parent = the owning server; its dependency record may already be
        // gone when the server itself is being dropped in the same batch.
        if (_deps.TryGetDependency(parent_id)) {
          GetDependencyForWrite<ForeignServerDependency>(parent_id)
            ->user_mappings.erase(id);
        }
        break;
      case ObjectType::Table: {
        GetDependencyForWrite<SchemaDependency>(parent_id)->tables.erase(id);
        const auto& t = basics::downCast<Table>(*obj);
        ModifyTableDependencies(parent_id, t, EdgeAction::Delete);
        for (const auto& col : t.Columns()) {
          _objects.erase(col.GetId());
        }
        for (const auto& c : t.CheckConstraints()) {
          _objects.erase(c.GetId());
        }
      } break;
      case ObjectType::PgSqlView: {
        GetDependencyForWrite<SchemaDependency>(parent_id)->views.erase(id);
        ModifyViewDependencies(parent_id, basics::downCast<PgSqlView>(*obj),
                               RefKinds::All, EdgeAction::Delete);
      } break;
      case ObjectType::PgSqlType: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        SDB_ASSERT(schema_deps);
        schema_deps->types.erase(id);
      } break;
      case ObjectType::Sequence: {
        // owned sequences live under the table;
        // free-standing CREATE SEQUENCE under the schema.
        const auto& seq = basics::downCast<Sequence>(*obj);
        if (auto owner = seq.GetOwnerTableId(); owner.isSet()) {
          auto table_deps = GetDependencyForWrite<TableDependency>(owner);
          SDB_ASSERT(table_deps);
          table_deps->owned_sequences.erase(id);
        } else {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->sequences.erase(id);
        }
      } break;
      case ObjectType::Invalid:
      case ObjectType::Tombstone:
      case ObjectType::Column:
      case ObjectType::CheckConstraint:
      case ObjectType::Virtual:
        SDB_UNREACHABLE();
    }
  }
  // Drop childs
  switch (obj->GetType()) {
    case ObjectType::Database: {
      auto db_deps = GetDependency<DatabaseDependency>(id);
      drop_childs(db_deps->schemas);
      drop_childs(db_deps->foreign_servers);
    } break;
    case ObjectType::Role:
      break;
    case ObjectType::Schema: {
      auto schema_deps = GetDependency<SchemaDependency>(id);
      drop_childs(schema_deps->types);
      drop_childs(schema_deps->functions);
      drop_childs(schema_deps->views);
      drop_childs(schema_deps->tables);
      drop_childs(schema_deps->sequences);
      drop_childs(schema_deps->tokenizers);
    } break;
    case ObjectType::Table:
    case ObjectType::PgSqlView: {
      auto relation_deps = GetDependency<RelationDependency>(id);
      if (obj->GetType() == ObjectType::Table) {
        const auto& table_deps =
          basics::downCast<TableDependency>(*relation_deps);
        // Owned sequences are parented under the schema (same parent_id
        // as the table), not the table -- unlike indexes.
        auto owned_sequences = table_deps.owned_sequences;
        for (auto seq_id : owned_sequences) {
          if (root) {
            auto seq = GetObject<Sequence>(seq_id);
            UnregisterObject(seq, parent_id, false);
          } else {
            RemoveObjectDefinition(parent_id, seq_id);
          }
        }
      }
      // TODO(codeworse): Avoid copy, maybe erase_if?
      auto index_ids = relation_deps->indexes;
      for (auto index_id : index_ids) {
        if (root) {
          // Indexes are not erased from the resolution table during DROP --
          // they're nested in schema scope. Erase explicitly.
          auto index = GetObject<Index>(index_id);
          UnregisterObject(index, id, false);
        } else {
          RemoveObjectDefinition(id, index_id);
        }
      }
    } break;
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
      break;
    case ObjectType::ForeignServer: {
      auto server_deps = GetDependency<ForeignServerDependency>(id);
      drop_childs(server_deps->user_mappings);
    } break;
    case ObjectType::PgSqlFunction:
    case ObjectType::PgSqlType:
    case ObjectType::Tokenizer:
    case ObjectType::UserMapping:
    case ObjectType::Sequence:
      break;
    case ObjectType::Invalid:
    case ObjectType::Tombstone:
    case ObjectType::Column:
    case ObjectType::CheckConstraint:
    case ObjectType::Virtual:
      SDB_UNREACHABLE();
  }
  _deps.RemoveDependency(id);
}

std::string DropPlan::FormatDependentsDetail(const Snapshot& snap,
                                             std::string_view seed_kind,
                                             std::string_view seed_name) const {
  std::vector<std::string> lines;
  auto add = [&](std::string_view kind, std::string_view name) {
    lines.push_back(
      absl::StrCat(kind, " ", name, " depends on ", seed_kind, " ", seed_name));
  };
  for (const auto& [tid, _] : table_rewrites) {
    if (auto t = snap.GetObject<Table>(tid)) {
      add("table", t->GetName());
    }
  }
  for (const auto& [_, view_id] : view_drops) {
    if (auto v = snap.GetObject<PgSqlView>(view_id)) {
      add("view", v->GetName());
    }
  }
  for (const auto& [_, fn_id] : function_drops) {
    if (auto f = snap.GetObject<PgSqlFunction>(fn_id)) {
      add("function", f->GetName());
    }
  }
  for (auto idx_id : index_drops) {
    if (auto i = snap.GetObject<Index>(idx_id)) {
      add("index", i->GetName());
    }
  }
  return absl::StrJoin(lines, "\n");
}

Catalog::Catalog()
  : _snapshot(std::make_shared<Snapshot>()), _engine{&GetCatalogStore()} {}

void Catalog::RegisterRole(std::shared_ptr<Role> role) {
  SDB_DEBUG(GENERAL, "Register role ", role->GetName());
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(role), id::kInstance, false);
  });
}

void Catalog::RegisterDatabase(std::shared_ptr<Database> database) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(database), id::kInstance, false);
  });
}

void Catalog::RegisterSchema(ObjectId database_id,
                             std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(schema), database_id, false);
  });
}

void Catalog::RegisterView(ObjectId schema_id,
                           std::shared_ptr<PgSqlView> view) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(view), schema_id, false);
  });
}

void Catalog::RegisterSequence(ObjectId database_id, ObjectId schema_id,
                               std::shared_ptr<Sequence> sequence) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(sequence), schema_id, false);
  });
}

void Catalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                            std::shared_ptr<Table> table) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot,
        [&](auto& clone) { clone->RegisterObject(table, schema_id, false); });
}

void Catalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                               std::shared_ptr<PgSqlFunction> function) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(function), schema_id, false);
  });
}

void Catalog::RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                                std::shared_ptr<Tokenizer> tokenizer) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(tokenizer), schema_id, false);
  });
}

void Catalog::RegisterForeignServer(
  ObjectId database_id, std::shared_ptr<ForeignServer> foreign_server) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(foreign_server), database_id, false);
  });
}

void Catalog::RegisterUserMapping(std::shared_ptr<UserMapping> user_mapping) {
  absl::MutexLock lock{&_mutex};
  const auto server_id = user_mapping->GetServerId();
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(user_mapping), server_id, false);
  });
}

void Catalog::RegisterType(ObjectId database_id, ObjectId schema_id,
                           std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(std::move(type), schema_id, false);
  });
}

bool Catalog::CreateDatabase(const AccessContext& ax,
                             std::shared_ptr<Database> database,
                             bool if_not_exists) {
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(*_snapshot, ax.role, RoleOption::CreateDb,
                       "create database");
  if (if_not_exists && _snapshot->GetObjectId<ResolveType::Database>(
                         id::kInstance, database->GetName())) {
    return false;
  }
  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(database, id::kInstance, false);
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      duckdb::MemoryStream stream;
      {
        auto bytes = catalog::SerializeObject(*database, stream);
        _engine->CreateDefinition(id::kInstance, ObjectType::Database,
                                  database_id, bytes);
      }

      auto schema = std::make_shared<Schema>(
        database->GetOwner(), database_id, ObjectId{}, StaticStrings::kPublic);
      clone->RegisterObject(schema, database_id, false);
      auto bytes = catalog::SerializeObject(*schema, stream);
      _engine->CreateDefinition(database_id, ObjectType::Schema,
                                schema->GetId(), bytes);
    },
    [&](auto clone) {
      clone->UnregisterObject(database, id::kInstance, true);
    });
  return true;
}

namespace {

// Defined below (with the other Create*/Drop* ownership helpers).
void RequireCreateOn(const Snapshot& snapshot, ObjectId role,
                     ObjectId parent_id);

}  // namespace

bool Catalog::CreateSchema(const AccessContext& ax, ObjectId database_id,
                           std::shared_ptr<Schema> schema, bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  // CREATE SCHEMA requires CREATE on the target database.
  RequireCreateOn(*_snapshot, ax.role, database_id);
  if (if_not_exists && _snapshot->GetObjectId<ResolveType::Schema>(
                         database_id, schema->GetName())) {
    return false;
  }
  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(schema, database_id, false);
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*schema, stream);
      _engine->CreateDefinition(database_id, ObjectType::Schema,
                                schema->GetId(), bytes);
    },
    [&](auto clone) { clone->UnregisterObject(schema, database_id, true); });
  return true;
}

void Catalog::CreateRole(const AccessContext& ax, std::shared_ptr<Role> role) {
  SDB_DEBUG(GENERAL, "Creating role: ", role->GetName());
  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(
    *_snapshot, ax.role, RoleOption::CreateRole, "create role",
    "Only roles with the CREATEROLE attribute may create roles.");
  RequireAttributesGrantable(*_snapshot, ax.role, role->Options(),
                             /*creating=*/true);
  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(role, id::kInstance, false);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*role, stream);
      _engine->CreateDefinition(id::kInstance, ObjectType::Role, role->GetId(),
                                bytes);
      auto creator = clone->template GetObject<Role>(ax.role);
      if (creator && !creator->IsSuperuser()) {
        auto updated = std::static_pointer_cast<Role>(creator->Clone());
        updated->AddMembership(Membership{
          .role = role->GetId(),
          .admin_option = true,
          .inherit_option = false,
          .set_option = false,
        });
        clone->template ReplaceObject<ResolveType::Role>(
          id::kInstance, updated->GetName(), updated);
        duckdb::MemoryStream cstream;
        auto cbytes = catalog::SerializeObject(*updated, cstream);
        _engine->CreateDefinition(id::kInstance, ObjectType::Role,
                                  updated->GetId(), cbytes);
      }
      clone->RebuildRoleClosures();
    },
    [&](auto& clone) { clone->UnregisterObject(role, id::kInstance, true); });
}

void Catalog::RegisterIndex(ObjectId database_id, ObjectId schema_id,
                            std::shared_ptr<Index> index) {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](auto& clone) {
    clone->RegisterObject(index, index->GetRelationId(), false);
  });
}

void Catalog::CreateIndexImpl(std::string_view relation_schema,
                              std::shared_ptr<Index> index,
                              CreateIndexOperationOptions operation_options) {
  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(index, index->GetRelationId(), false);

      if (operation_options.create_with_tombstone) {
        _engine->WriteTombstone(index->GetRelationId(), index->GetId());
        index->SetTombstoned(true);
      }
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      duckdb::MemoryStream stream;
      {  // Write index definition
        auto bytes = catalog::SerializeObject(*index, stream);
        _engine->CreateDefinition(index->GetRelationId(), index->GetType(),
                                  index->GetId(), bytes);
      }
      // The inverted index's mutable iresearch storage hangs off the metadata
      // object itself, so the CREATE INDEX build (GetGlobalSinkState) reaches
      // it via the index's GetData(). Bind it here, before the build runs.
      if (index->GetType() == ObjectType::InvertedIndex) {
        const auto& inverted = basics::downCast<const InvertedIndex>(*index);
        inverted.SetData(search::InvertedIndexStorage::Create(
          inverted.GetId(), inverted, /*is_new=*/true));
      }
      {
        auto table = clone->template GetObject<Table>(index->GetRelationId());
        auto schema_obj =
          table ? clone->template GetObject<Schema>(table->GetParentId())
                : nullptr;
        auto database =
          schema_obj
            ? clone->template GetObject<Database>(schema_obj->GetParentId())
            : nullptr;
        if (database) {
          if (auto def = MakeStoreIndexDef(
                database->GetName(), schema_obj->GetName(), *table, *index)) {
            _engine->Write(
              [&](auto& ctx) { ctx.CreateStoreIndex(std::move(*def)); });
          }
        }
      }
    },
    [&](auto& clone) {
      // Unregistering drops the index (and its just-bound storage) from the
      // clone; mirrors the prior rollback, which only dropped the registry ref.
      clone->UnregisterObject(index, index->GetRelationId(), true);
    });
}

namespace {

struct ResolvedIndexRelation {
  ObjectId relation_id;
  std::vector<Column> columns;
};

ResolvedIndexRelation ResolveIndexRelation(
  const std::shared_ptr<Object>& relation) {
  if (relation->GetType() == ObjectType::Table) {
    auto& table = basics::downCast<Table>(*relation);
    return ResolvedIndexRelation{
      .relation_id = table.GetId(),
      .columns = table.Columns(),
    };
  } else if (relation->GetType() == ObjectType::PgSqlView) {
    auto& view = basics::downCast<PgSqlView>(*relation);
    const auto& view_info = view.GetInfo();
    auto columns =
      std::views::iota(size_t{0}, view_info.names.size()) |
      std::views::transform([&](size_t i) {
        Column c{ObjectId{}, Column::Id{i},
                 view_info.names[i].GetIdentifierName(), view_info.types[i]};
        c.SetId(Column::Id{i});
        return c;
      }) |
      std::ranges::to<std::vector>();
    return ResolvedIndexRelation{
      .relation_id = view.GetId(),
      .columns = std::move(columns),
    };
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Only table or view indexes are supported"));
}

// CREATE inside `parent_id` requires CREATE on it (a schema for relations, a
// database for schemas). Throws 42501 "permission denied for <type> <name>"
// (type/name from the parent) on a non-creator; a missing parent is a no-op
// (the mutation's own resolution reports the real error).
void RequireCreateOn(const Snapshot& snapshot, ObjectId role,
                     ObjectId parent_id) {
  auto parent = snapshot.GetObject(parent_id);
  if (!parent || snapshot.ClosureFor(role).Can(*parent, AclMode::Create)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for ", pg::ToPgObjectTypeName(parent->GetType()),
            " ", parent->GetName()));
}

// ALTER/DROP requires ownership of the target object. ok() if the object is
// gone (the mutation's own resolution reports the real error).
// Throw 42501 "must be owner of <type> <name>" unless `role` owns `owner_id`
// (directly, via membership, or as superuser). The error names `reported` --
// the object the user acted on -- and its type/name come straight from it
// (ToPgObjectTypeName + GetName), so callers don't repeat them. A missing
// owner object is a no-op (the mutation's own resolution reports not-found).
void RequireObjectOwner(const Snapshot& snapshot, ObjectId role,
                        ObjectId owner_id, const Object& reported) {
  auto owner_obj = snapshot.GetObject(owner_id);
  if (!owner_obj || snapshot.ClosureFor(role).Owns(*owner_obj)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("must be owner of ", pg::ToPgObjectTypeName(reported.GetType()),
            " ", reported.GetName()));
}

// Common case: the object that confers authority IS the one acted on. Resolves
// `object_id` once; a missing object is a no-op (the mutation reports
// not-found).
void RequireObjectOwner(const Snapshot& snapshot, ObjectId role,
                        ObjectId object_id) {
  if (auto obj = snapshot.GetObject(object_id)) {
    RequireObjectOwner(snapshot, role, object_id, *obj);
  }
}

// Authority over a USER MAPPING follows its FOREIGN SERVER (PG semantics):
// the server's owner (or a superuser) manages mappings for anyone; a role may
// manage its OWN mapping if it holds USAGE on the server.
// A per-role mapping's own role may self-manage with USAGE; a
// PUBLIC mapping has an unset role, which never equals the acting role.
void RequireUserMappingAuthority(const Snapshot& snapshot, ObjectId role,
                                 ObjectId server_id, ObjectId mapping_role) {
  auto server = snapshot.GetObject(server_id);
  if (!server) {
    return;
  }
  const auto& closure = snapshot.ClosureFor(role);
  if (closure.Owns(*server)) {
    return;
  }
  if (mapping_role == role && closure.Can(*server, AclMode::Usage)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("must be owner of foreign server ", server->GetName()));
}

void RequireRoleMembership(const Snapshot& snapshot, ObjectId actor_id,
                           const Role& target) {
  const auto& rc = snapshot.ClosureFor(actor_id);
  if (rc.MemberOf(target.GetId())) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied"),
                  ERR_DETAIL("Must be a member of role \"", target.GetName(),
                             "\" to alter its default privileges."));
}

void RequireRoleAdmin(const Snapshot& snapshot, ObjectId actor_id,
                      const Role& target, std::string_view verb) {
  auto actor = snapshot.GetObject<Role>(actor_id);
  if (actor && actor->IsSuperuser()) {
    return;
  }
  if (target.IsSuperuser()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", verb, " role"),
                    ERR_DETAIL("Only roles with the SUPERUSER attribute may ",
                               verb, " roles with the SUPERUSER attribute."));
  }
  if (!actor || !actor->Has(RoleOption::CreateRole) ||
      !auth::HasAdminOption(snapshot, actor_id, target.GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role"),
      ERR_DETAIL("Only roles with the CREATEROLE attribute and the ADMIN "
                 "option on role \"",
                 target.GetName(), "\" may ", verb, " this role."));
  }
}

void RequireRoleAttribute(const Snapshot& snapshot, ObjectId actor_id,
                          RoleOption attribute, std::string_view denied_action,
                          std::string_view detail) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = snapshot.GetObject<Role>(actor_id);
  if (!actor || actor->IsSuperuser() || actor->Has(attribute)) {
    return;
  }
  if (detail.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", denied_action));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", denied_action),
                  ERR_DETAIL(detail));
}

// A role may only confer a privileged attribute it holds itself (superuser for
// SUPERUSER; the matching bit for CREATEDB/REPLICATION/BYPASSRLS). `granting`
// is the set of attributes being conferred; CREATEROLE/LOGIN/INHERIT are not
// gated (a CREATEROLE actor may set them, matching PostgreSQL).
void RequireAttributesGrantable(const Snapshot& snapshot, ObjectId actor_id,
                                RoleOption granting, bool creating) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = snapshot.GetObject<Role>(actor_id);
  const bool actor_super = actor && actor->IsSuperuser();

  const auto require = [&](RoleOption attr, bool actor_has,
                           std::string_view attr_name) {
    if ((granting & attr) == RoleOption::None || actor_has) {
      return;
    }
    const auto detail =
      creating
        ? absl::StrCat("Only roles with the ", attr_name,
                       " attribute may create roles with the ", attr_name,
                       " attribute.")
        : absl::StrCat("Only roles with the ", attr_name,
                       " attribute may change the ", attr_name, " attribute.");
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", creating ? "create" : "alter", " role"),
      ERR_DETAIL(detail));
  };

  require(RoleOption::Superuser, actor_super, "SUPERUSER");
  require(RoleOption::CreateDb,
          actor_super || (actor && actor->Has(RoleOption::CreateDb)),
          "CREATEDB");
  require(RoleOption::Replication,
          actor_super || (actor && actor->Has(RoleOption::Replication)),
          "REPLICATION");
  require(RoleOption::BypassRls,
          actor_super || (actor && actor->Has(RoleOption::BypassRls)),
          "BYPASSRLS");
}

}  // namespace

bool Catalog::CreateSecondaryIndex(
  const AccessContext& ax, ObjectId database_id, std::string_view schema,
  std::string_view relation, std::string name,
  std::vector<CreateIndexColumn>&& columns, bool unique,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Cannot create index without columns"));
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  auto rel =
    _snapshot->GetRelation(NoAccessCheck(), database_id, schema, relation);
  if (!rel) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", relation, "\" does not exist"));
  }
  // CREATE INDEX requires ownership of the target relation (an index has no
  // independent owner -- it derives from the table).
  RequireObjectOwner(*_snapshot, ax.role, rel->GetId());
  if (operation_options.if_not_exists &&
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name)) {
    return false;
  }
  auto resolved = ResolveIndexRelation(rel);
  for (auto& c : columns) {
    if (c.IsIndexedExpression()) {
      // Expression keys carry their own bound payload (dependent columns +
      // serialized expr); they have no base column to resolve by name. The
      // store-side ART builds/maintains them natively from the rendered SQL.
      continue;
    }
    auto it = absl::c_find_if(resolved.columns, [&](const Column& col) {
      return col.GetName() == c.name;
    });
    if (it == resolved.columns.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column \"", c.name, "\" does not exist"));
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateSecondaryIndex(
    database_id, *schema_id, ObjectId{0}, resolved.relation_id, std::move(name),
    std::move(columns), unique);
  CreateIndexImpl(schema, std::move(index), operation_options);
  return true;
}

bool Catalog::CreateInvertedIndex(
  const AccessContext& ax, duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema, std::string_view relation, std::string name,
  std::vector<CreateIndexColumn>&& columns, InvertedIndexOptions options,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Cannot create index without columns"));
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  auto rel =
    _snapshot->GetRelation(NoAccessCheck(), database_id, schema, relation);
  if (!rel) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", relation, "\" does not exist"));
  }
  // CREATE INDEX requires ownership of the target relation (an index has no
  // independent owner -- it derives from the table).
  RequireObjectOwner(*_snapshot, ax.role, rel->GetId());
  if (operation_options.if_not_exists &&
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name)) {
    return false;
  }
  auto resolved = ResolveIndexRelation(rel);
  for (auto& c : columns) {
    if (c.IsIndexedExpression()) {
      continue;
    }
    auto it = absl::c_find_if(resolved.columns, [&](const Column& col) {
      return col.GetName() == c.name;
    });
    if (it == resolved.columns.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column \"", c.name, "\" does not exist"));
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateInvertedIndex(
    context, database_id, schema, *schema_id, ObjectId{0}, resolved.relation_id,
    std::move(name), std::move(columns), _snapshot, std::move(options));
  CreateIndexImpl(schema, std::move(index), operation_options);
  return true;
}

bool Catalog::CreateView(const AccessContext& ax, ObjectId database_id,
                         std::string_view schema,
                         std::shared_ptr<PgSqlView> view, bool replace,
                         bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  view->SetParentId(*schema_id);

  ObjectId existed_id;
  if (replace) {
    existed_id =
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, view->GetName())
        .value_or(ObjectId{});
    if (existed_id.isSet()) {
      auto existing = _snapshot->GetObject<Object>(existed_id);
      if (existing->GetType() != ObjectType::PgSqlView) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                        ERR_MSG("\"", view->GetName(), "\" is not a view"));
      }
      // CREATE OR REPLACE preserves the original owner and grants (PG keeps the
      // existing catalog tuple's relowner and relacl).
      view->SetPermissions(existing->GetPermissions());
    }
  } else if (if_not_exists && _snapshot->GetObjectId<ResolveType::Relation>(
                                *schema_id, view->GetName())) {
    return false;
  }

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      if (existed_id.isSet()) {
        view->SetId(existed_id);
        clone->ReplaceObject<ResolveType::Relation>(*schema_id, view->GetName(),
                                                    view);
      } else {
        clone->RegisterObject(view, *schema_id, /*replace=*/false);
      }
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*view, stream);
      if (existed_id.isSet()) {
        _engine->Write([&](auto& ctx) {
          ctx.PutDefinition(*schema_id, ObjectType::PgSqlView, view->GetId(),
                            std::string_view{bytes});
        });
        return;
      }
      _engine->CreateDefinition(*schema_id, ObjectType::PgSqlView,
                                view->GetId(), bytes);
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(view, *schema_id, true);
      }
    });
  return true;
}

bool Catalog::CreateSequence(const AccessContext& ax, ObjectId database_id,
                             std::string_view schema,
                             std::shared_ptr<Sequence> sequence,
                             bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  if (auto existed = _snapshot->GetObjectId<ResolveType::Relation>(
        *schema_id, sequence->GetName())) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("relation \"", sequence->GetName(), "\" already exists"));
  }
  sequence->SetParentId(*schema_id);

  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(sequence, *schema_id, false);
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*sequence, stream);
      _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Sequence, sequence->GetId(),
                          std::string_view{bytes});
        ctx.PutSequence(sequence->GetId(), sequence->Options().Seed());
      });
    },
    [&](auto clone) { clone->UnregisterObject(sequence, *schema_id, true); });
  return true;
}

bool Catalog::CreateFunction(const AccessContext& ax, ObjectId database_id,
                             std::string_view schema,
                             std::shared_ptr<PgSqlFunction> function,
                             bool replace, bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  function->SetParentId(*schema_id);

  ObjectId existed_id;
  if (replace) {
    existed_id =
      _snapshot
        ->GetObjectId<ResolveType::Function>(*schema_id, function->GetName())
        .value_or(ObjectId{});
    // CREATE OR REPLACE preserves the original owner and grants (PG keeps the
    // existing catalog tuple's proowner and proacl).
    if (existed_id.isSet()) {
      RequireObjectOwner(*_snapshot, ax.role, existed_id);
      if (auto existing = _snapshot->GetObject<Object>(existed_id)) {
        function->SetPermissions(existing->GetPermissions());
      }
    }
  } else if (if_not_exists && _snapshot->GetObjectId<ResolveType::Function>(
                                *schema_id, function->GetName())) {
    return false;
  }

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      if (existed_id.isSet()) {
        if (auto deps =
              clone->GetDependency<PgSqlFunctionDependency>(existed_id);
            deps && !deps->indexes.empty()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ERR_MSG("cannot replace function \"", function->GetName(),
                    "\" because indexes depend on it"));
        }
        function->SetId(existed_id);
        clone->ReplaceObject<ResolveType::Function>(
          *schema_id, function->GetName(), function);
      } else {
        clone->RegisterObject(function, *schema_id, /*replace=*/false);
      }
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*function, stream);
      if (existed_id.isSet()) {
        _engine->Write([&](auto& ctx) {
          ctx.PutDefinition(*schema_id, ObjectType::PgSqlFunction,
                            function->GetId(), std::string_view{bytes});
        });
        return;
      }
      _engine->CreateDefinition(*schema_id, ObjectType::PgSqlFunction,
                                function->GetId(), bytes);
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(function, *schema_id, true);
      }
    });
  return true;
}

bool Catalog::CreateTable(const AccessContext& ax, ObjectId database_id,
                          std::string_view schema, CreateTableOptions options,
                          CreateTableOperationOptions operation_options) {
  // Uniqueness keys are enforced by the store table's DuckDB ART, which cannot
  // index nested types. Reject a nested-type key column up front with a clear
  // error instead of silently creating the table with the constraint dropped
  // (the store-side names_for clears it otherwise). Scalar key types are
  // handled natively by the ART.
  auto reject_nested_key = [&](std::span<const Column::Id> ids,
                               std::string_view what) {
    for (auto col_id : ids) {
      auto col = absl::c_find_if(
        options.columns, [&](const auto& c) { return c.GetId() == col_id; });
      SDB_ASSERT(col != options.columns.end());
      if (col->type.IsNested()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG(what, " column \"", col->GetName(),
                  "\" has unsupported nested type ", col->type.ToString()));
      }
    }
  };
  reject_nested_key(options.pk_columns, "primary key");
  for (const auto& unique : options.unique_constraints) {
    reject_nested_key(unique.columns, "unique constraint");
  }
  auto sequence_specs = std::move(options.sequences);

  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  if (operation_options.if_not_exists &&
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, options.name)) {
    return false;
  }

  for (const auto& fk : options.foreign_keys) {
    if (!fk.referenced_table.isSet()) {
      continue;
    }
    auto ref = _snapshot->GetObject<Table>(fk.referenced_table);
    if (!ref) {
      continue;
    }
    const auto& ref_ids = fk.referenced_columns;
    if (!_snapshot->ClosureFor(ax.role).CanColumns(
          *ref, AclMode::References, [&](uint64_t, const Column& c) {
            return absl::c_linear_search(ref_ids, c.GetId());
          })) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for table ", ref->GetName()));
    }
  }

  // PG mangles `<table>_<col>_seq` with a numeric suffix on collision. Done
  // under the mutex so concurrent CREATE TABLEs can't race on it.
  auto pick_unique_name = [&](std::string_view base) {
    std::string candidate{base};
    for (size_t i = 1;
         _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, candidate);
         ++i) {
      candidate = absl::StrCat(base, i);
    }
    return candidate;
  };

  auto make_nextval_default = [](std::string_view qualified) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> args;
    args.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value{std::string{qualified}}));
    return std::make_shared<ColumnExpr>(
      duckdb::make_uniq<duckdb::FunctionExpression>("nextval",
                                                    std::move(args)));
  };

  // A caller-provided id means CTAS mode: created tombstoned, no store table.
  const bool with_tombstone = operation_options.table_id.id() != 0;
  // Columns come in with whatever owner_table_id callers set (unused);
  // Table's constructor stamps the real id on each.
  auto table_id = with_tombstone ? operation_options.table_id : NextId();

  // Generated serial/PK sequences are owned by the table owner too (PG: ALTER
  // TABLE OWNER TO cascades to them, so they must start matching). The owner is
  // the acting role from the access context (root for internal/bootstrap
  // callers via NoAccessCheck).
  const ObjectId owner = ax.role;

  std::vector<std::shared_ptr<Sequence>> sequences;
  sequences.reserve(sequence_specs.size() + 1);
  for (const auto& spec : sequence_specs) {
    auto col_it = absl::c_find_if(options.columns, [&](const auto& c) {
      return c.GetId() == spec.column_id;
    });
    SDB_ASSERT(col_it != options.columns.end());
    auto resolved = pick_unique_name(
      absl::StrCat(options.name, "_", col_it->GetName(), "_seq"));
    col_it->expr = make_nextval_default(absl::StrCat(schema, ".", resolved));
    auto seq_opts = spec.options;
    seq_opts.name = resolved;
    seq_opts.owner_table_id = table_id.id();
    seq_opts.perm = Permissions{owner};
    sequences.push_back(
      std::make_shared<Sequence>(*schema_id, ObjectId{}, std::move(seq_opts)));
  }

  // Tables without an explicit PK get an auto-PK owned sequence. Table
  // holds its id directly so the insert path doesn't have to scan
  // owned_sequences for it.
  ObjectId generated_pk_seq_id;
  if (options.pk_columns.empty()) {
    auto resolved = pick_unique_name(absl::StrCat(options.name, "_pk_seq"));
    SequenceOptions opts;
    opts.name = resolved;
    opts.cache = 65536;
    opts.owner_table_id = table_id.id();
    opts.perm = Permissions{owner};
    auto pk_seq =
      std::make_shared<Sequence>(*schema_id, ObjectId{}, std::move(opts));
    generated_pk_seq_id = pk_seq->GetId();
    sequences.push_back(std::move(pk_seq));
  }

  // Constraints get real catalog OIDs like every other object: one for the
  // constraint itself and one for its backing index relation (PG allocates a
  // pg_class entry per PK/UNIQUE index; the pg_catalog views expose these).
  ObjectId pk_constraint_id;
  ObjectId pk_index_id;
  if (!options.pk_columns.empty()) {
    pk_constraint_id = NextId();
    pk_index_id = NextId();
  }
  for (auto& unique : options.unique_constraints) {
    unique.id = NextId();
    unique.index_id = NextId();
  }
  for (auto& fk : options.foreign_keys) {
    fk.id = NextId();
  }

  auto table = std::make_shared<Table>(
    Permissions{owner}, *schema_id, table_id, options.name,
    std::move(options.columns), std::move(options.pk_columns),
    std::move(options.check_constraints), generated_pk_seq_id, options.engine,
    std::move(options.unique_constraints), std::move(options.foreign_keys),
    std::move(options.pk_name), pk_constraint_id, pk_index_id);
  table->SetSearchOptions(options.search_options);
  if (with_tombstone) {
    table->SetTombstoned(true);
  }

  std::optional<StoreTableDef> store_table;
  if (table->GetEngine() == TableEngine::Transactional) {
    auto database = _snapshot->GetObject<Database>(database_id);
    SDB_ASSERT(database);
    store_table = MakeStoreTableDef(database->GetName(), schema, *table);
    for (const auto& fk : table->ForeignKeys()) {
      StoreForeignKey out;
      const Table* referenced = nullptr;
      if (!fk.referenced_table.isSet()) {
        referenced = table.get();
        out.referenced_table = store_table->name;
      } else {
        auto ref_obj = _snapshot->GetObject<Table>(fk.referenced_table);
        if (!ref_obj || ref_obj->GetEngine() != TableEngine::Transactional) {
          continue;
        }
        auto ref_schema = _snapshot->GetObject<Schema>(ref_obj->GetParentId());
        SDB_ASSERT(ref_schema);
        out.referenced_table = StoreTableName(
          database->GetName(), ref_schema->GetName(), ref_obj->GetName());
        referenced = ref_obj.get();
      }
      auto names_for = [](const Table& t, std::span<const Column::Id> ids,
                          std::vector<std::string>& out_names) {
        for (auto col_id : ids) {
          auto it = std::ranges::find_if(
            t.Columns(), [&](const auto& c) { return c.GetId() == col_id; });
          if (it == t.Columns().end() || it->type.IsNested()) {
            out_names.clear();
            return;
          }
          out_names.emplace_back(it->GetName());
        }
      };
      names_for(*table, fk.columns, out.columns);
      names_for(*referenced, fk.referenced_columns, out.referenced_columns);
      if (!out.columns.empty() && !out.referenced_columns.empty()) {
        store_table->foreign_keys.push_back(std::move(out));
      }
    }
    if (with_tombstone) {
      // Not-yet-committed (CTAS) tables live under the dropped name; commit
      // renames to the public name (RemoveTombstone), failure drops by id.
      store_table->name = DroppedStoreTableName(table->GetId());
    }
  } else if (table->GetEngine() == TableEngine::Search) {
    table->SetData(search::SearchTable::Create(database_id, *schema_id,
                                               table->GetId(), /*is_new=*/true,
                                               table->SearchOptions()));
  }

  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(table, *schema_id, false);

      for (const auto& seq : sequences) {
        clone->RegisterObject(seq, *schema_id, false);
      }
      if (with_tombstone) {
        _engine->WriteTombstone(*schema_id, table->GetId());
      }
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }
      // PutDefinition copies into the catalog store batch, so one reused stream
      // is enough: each view is consumed before the next serialize overwrites
      // it.
      duckdb::MemoryStream stream;
      _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Table, table->GetId(),
                          catalog::SerializeObject(*table, stream));
        for (const auto& seq : sequences) {
          ctx.PutDefinition(*schema_id, ObjectType::Sequence, seq->GetId(),
                            catalog::SerializeObject(*seq, stream));
          ctx.PutSequence(seq->GetId(), seq->Options().Seed());
        }
        if (store_table && !with_tombstone) {
          ctx.CreateStoreTable(std::move(*store_table));
        }
      });
    },
    [&](auto clone) { clone->UnregisterObject(table, *schema_id, true); });
  return true;
}

bool Catalog::CreateTokenizer(const AccessContext& ax, ObjectId database_id,
                              std::string_view schema,
                              std::shared_ptr<Tokenizer> dict,
                              bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  if (if_not_exists && _snapshot->GetObjectId<ResolveType::Tokenizer>(
                         *schema_id, dict->GetName())) {
    return false;
  }
  dict->SetParentId(*schema_id);

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->RegisterObject(dict, *schema_id, false);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*dict, stream);
      _engine->CreateDefinition(*schema_id, ObjectType::Tokenizer,
                                dict->GetId(), bytes);
    },
    [&](auto& clone) { clone->UnregisterObject(dict, *schema_id, true); });
  return true;
}

void Catalog::RequireCreateForeignServer(const AccessContext& ax,
                                         ObjectId database_id) {
  absl::MutexLock lock{&_mutex};
  RequireCreateOn(*_snapshot, ax.role, database_id);
}

bool Catalog::CreateForeignServer(const AccessContext& ax, ObjectId database_id,
                                  std::shared_ptr<ForeignServer> foreign_server,
                                  bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  // Servers are database children, like PG (no schema). Gated on CREATE on
  // the database, same as CREATE SCHEMA -- PG gates on FDW USAGE instead, but
  // serenedb has no foreign-data-wrapper catalog object to hang an ACL on.
  RequireCreateOn(*_snapshot, ax.role, database_id);
  if (if_not_exists && _snapshot->GetObjectId<ResolveType::ForeignServer>(
                         database_id, foreign_server->GetName())) {
    return false;
  }
  // Catalog names are per-database, but the live attachment alias is
  // instance-global: a second same-named server would race the first for the
  // alias (nondeterministic boot winner; DROP DATABASE detaching the other
  // database's attachment). Reject up front, naming the owner. The create-time
  // ATTACH cannot be relied on for this -- it only collides while the first
  // server's attachment is live, not when its remote is down.
  _snapshot->RequireForeignServerNameGloballyUnique(database_id,
                                                    foreign_server->GetName());
  foreign_server->SetParentId(database_id);

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->RegisterObject(foreign_server, database_id, false);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*foreign_server, stream);
      _engine->CreateDefinition(database_id, ObjectType::ForeignServer,
                                foreign_server->GetId(), bytes);
    },
    [&](auto& clone) {
      clone->UnregisterObject(foreign_server, database_id, true);
    });
  return true;
}

bool Catalog::CreateUserMapping(const AccessContext& ax, ObjectId database_id,
                                std::shared_ptr<UserMapping> user_mapping,
                                bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  const auto server_id = user_mapping->GetServerId();
  if (!_snapshot->GetObject<ForeignServer>(server_id)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("server \"", user_mapping->GetServerName(), "\" does not exist"));
  }
  RequireUserMappingAuthority(*_snapshot, ax.role, server_id,
                              user_mapping->GetRoleId());
  // parent = the server; the mapping's name is the mapped role.
  if (if_not_exists && _snapshot->GetObjectId<ResolveType::UserMapping>(
                         server_id, user_mapping->GetName())) {
    return false;
  }
  // Re-check the mapped role under the lock: the command layer resolved it on
  // an earlier snapshot, and the DROP-ROLE-blocking dependency edge is only
  // added by the registration below -- a concurrent DROP ROLE in that window
  // would otherwise leave an orphan mapping for a dead role. PUBLIC mappings
  // carry no role id.
  if (user_mapping->GetRoleId().isSet() &&
      !_snapshot->GetObject<Role>(user_mapping->GetRoleId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("role \"", user_mapping->GetName(), "\" does not exist"));
  }
  user_mapping->SetParentId(server_id);

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->RegisterObject(user_mapping, server_id, false);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*user_mapping, stream);
      // Store rows stay flat under the database (one range-delete sweeps them
      // on DROP DATABASE); the in-memory parent is the server.
      _engine->CreateDefinition(database_id, ObjectType::UserMapping,
                                user_mapping->GetId(), bytes);
    },
    [&](auto& clone) {
      clone->UnregisterObject(user_mapping, server_id, true);
    });
  return true;
}

bool Catalog::CreateType(const AccessContext& ax, ObjectId database_id,
                         std::string_view schema,
                         std::shared_ptr<PgSqlType> type, bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(*_snapshot, ax.role, *schema_id);
  if (if_not_exists &&
      _snapshot->GetObjectId<ResolveType::Type>(*schema_id, type->GetName())) {
    return false;
  }
  type->SetParentId(*schema_id);

  Apply(
    _snapshot,
    [&](auto& clone) {
      clone->RegisterObject(type, *schema_id, false);
      SDB_IF_FAILURE("unable_to_create") {
        THROW_SQL_ERROR(ERR_MSG("internal error"));
      }

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*type, stream);
      _engine->CreateDefinition(*schema_id, ObjectType::PgSqlType,
                                type->GetId(), bytes);
    },
    [&](auto clone) { clone->UnregisterObject(type, *schema_id, true); });
  return true;
}

template<typename T>
void Catalog::RenameObjectImpl(ObjectId schema_id,
                               std::string_view database_name,
                               std::string_view schema_name,
                               std::string_view name, std::string_view new_name,
                               std::shared_ptr<T> object) {
  constexpr auto kResolveType = std::is_same_v<T, PgSqlFunction>
                                  ? ResolveType::Function
                                  : ResolveType::Relation;

  if (object->GetName() == new_name) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", new_name, "\" already exists"));
  }

  auto cloned = object->Clone();
  if (!cloned) {
    THROW_SQL_ERROR(ERR_MSG("Failed to clone object"));
  }
  auto new_object = basics::downCast<T>(std::move(cloned));
  SDB_ASSERT(new_object);
  new_object->SetName(new_name);

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->ReplaceObject<kResolveType>(schema_id, name, new_object);

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*new_object, stream);

      ObjectId parent_id;
      if constexpr (std::is_same_v<T, Index>) {
        parent_id = object->GetRelationId();
      } else {
        parent_id = schema_id;
      }

      if constexpr (std::is_same_v<T, Table>) {
        if (new_object->GetEngine() == TableEngine::Transactional &&
            !new_object->Tombstoned()) {
          _engine->Write([&](auto& ctx) {
            ctx.PutDefinition(parent_id, new_object->GetType(),
                              new_object->GetId(), bytes);
            ctx.RenameStoreTable(
              StoreTableName(database_name, schema_name, name),
              StoreTableName(database_name, schema_name, new_name));
          });
          return;
        }
      }
      _engine->CreateDefinition(parent_id, new_object->GetType(),
                                new_object->GetId(), bytes);
    },
    [&](const std::shared_ptr<Snapshot>& clone) {
      auto current = clone->GetObject<T>(new_object->GetId());
      if (current->GetName() == new_object->GetName()) {
        clone->ReplaceObject<kResolveType>(schema_id, new_name, object);
      }
    });
}

template<typename T>
void Catalog::RenameObjectImpl(const AccessContext& ax, ObjectId database_id,
                               std::string_view schema, std::string_view name,
                               std::string_view new_name) {
  static constexpr auto kResolveType = std::is_same_v<T, PgSqlFunction>
                                         ? ResolveType::Function
                                         : ResolveType::Relation;
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }

  auto object_id = _snapshot->GetObjectId<kResolveType>(*schema_id, name);
  if (!object_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *object_id);

  auto typed = std::dynamic_pointer_cast<T>(std::move(object));
  if (!typed) {
    if constexpr (std::is_same_v<T, PgSqlView>) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not a view"));
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not a table"));
    }
  }

  auto database = _snapshot->GetObject<Database>(database_id);
  SDB_ASSERT(database);
  RenameObjectImpl<T>(*schema_id, database->GetName(), schema, name, new_name,
                      std::move(typed));
}

void Catalog::RenameView(const AccessContext& ax, ObjectId database_id,
                         std::string_view schema, std::string_view name,
                         std::string_view new_name) {
  RenameObjectImpl<PgSqlView>(ax, database_id, schema, name, new_name);
}

bool Catalog::RenameFunction(const AccessContext& ax, ObjectId database_id,
                             std::string_view schema, std::string_view name,
                             std::string_view new_name, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  std::shared_ptr<PgSqlFunction> function;
  if (schema_id) {
    if (auto object_id =
          _snapshot->GetObjectId<ResolveType::Function>(*schema_id, name)) {
      function = _snapshot->GetObject<PgSqlFunction>(*object_id);
    }
  }
  if (!function) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    ERR_MSG("could not find a function named \"", name, "\""));
  }
  RequireObjectOwner(*_snapshot, ax.role, function->GetId());

  auto database = _snapshot->GetObject<Database>(database_id);
  SDB_ASSERT(database);
  RenameObjectImpl<PgSqlFunction>(*schema_id, database->GetName(), schema, name,
                                  new_name, std::move(function));
  return true;
}

void Catalog::RenameRelation(const AccessContext& ax, ObjectId database_id,
                             std::string_view schema, std::string_view name,
                             std::string_view new_name) {
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *object_id);

  auto database = _snapshot->GetObject<Database>(database_id);
  SDB_ASSERT(database);
  switch (object->GetType()) {
    case ObjectType::Table:
      RenameObjectImpl<Table>(*schema_id, database->GetName(), schema, name,
                              new_name,
                              std::static_pointer_cast<Table>(object));
      return;
    case ObjectType::PgSqlView:
      RenameObjectImpl<PgSqlView>(*schema_id, database->GetName(), schema, name,
                                  new_name,
                                  std::static_pointer_cast<PgSqlView>(object));
      return;
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
      RenameObjectImpl<Index>(*schema_id, database->GetName(), schema, name,
                              new_name,
                              std::static_pointer_cast<Index>(object));
      return;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not a table"));
  }
}

void Catalog::ChangeRoleImpl(
  ObjectId actor_id, std::string_view name,
  absl::FunctionRef<void(const Snapshot&, const Role&)> check,
  ChangeCallback<Role> callback) {
  absl::MutexLock lock{&_mutex};
  auto role = _snapshot->GetRole(name);
  if (!role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  check(*_snapshot, *role);  // op-specific access check, on the live snapshot
  std::shared_ptr<Role> new_role_ptr;
  callback(*role, new_role_ptr);
  if (!new_role_ptr) {
    return;
  }
  // User mappings resolve by the mapped role's NAME under their server (PG
  // keys them by oid); a rename would strand every mapping for this role --
  // undroppable by either name, yet still shown by pg_user_mappings. Block
  // the rename like DROP ROLE is blocked, until mappings are re-keyed.
  if (new_role_ptr->GetName() != role->GetName()) {
    if (auto dep = _snapshot->_deps.TryGetDependency(role->GetId())) {
      // The role's referencing objects also hold owner/ACL referencers; only a
      // mapping FOR this role blocks the rename.
      for (auto id :
           basics::downCast<const RoleDependency>(*dep).referencing_objects) {
        auto obj = _snapshot->GetObject(id);
        if (!obj || obj->GetType() != ObjectType::UserMapping) {
          continue;
        }
        const auto& mapping = basics::downCast<const UserMapping>(*obj);
        if (mapping.GetRoleId() != role->GetId()) {
          continue;
        }
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
          ERR_MSG("role \"", name,
                  "\" cannot be renamed because user mappings depend on "
                  "it"),
          ERR_DETAIL("user mapping for \"", name, "\" on server \"",
                     mapping.GetServerName(), "\" depends on role \"", name,
                     "\""),
          ERR_HINT("Drop the user mapping(s) first."));
      }
    }
  }
  // A change may only add privileged attributes the actor holds itself.
  RequireAttributesGrantable(*_snapshot, actor_id,
                             new_role_ptr->Options() & ~role->Options(),
                             /*creating=*/false);
  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->ReplaceObject<ResolveType::Role>(id::kInstance, name,
                                              new_role_ptr);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*new_role_ptr, stream);
      _engine->CreateDefinition(id::kInstance, ObjectType::Role,
                                new_role_ptr->GetId(), bytes);
      clone->RebuildRoleClosures();
    },
    [&](const std::shared_ptr<Snapshot>& clone) {
      auto obj = clone->GetObject<Role>(new_role_ptr->GetId());
      if (obj->GetName() == new_role_ptr->GetName()) {
        clone->ReplaceObject<ResolveType::Role>(id::kInstance,
                                                new_role_ptr->GetName(), role);
      }
    });
}

void Catalog::ChangeRole(const AccessContext& ax, std::string_view name,
                         std::string_view verb, bool allow_self,
                         ChangeCallback<Role> callback) {
  ChangeRoleImpl(
    ax.role, name,
    [&](const Snapshot& snap, const Role& role) {
      if (allow_self && ax.role == role.GetId()) {
        return;  // a role may change its own entry (e.g. SET config)
      }
      RequireRoleAdmin(snap, ax.role, role, verb);
    },
    std::move(callback));
}

void Catalog::ChangeDefaultAcl(const AccessContext& ax,
                               std::string_view role_name, ObjectId schema,
                               char objtype, ObjectType type,
                               absl::FunctionRef<void(Acl&)> mutate) {
  ChangeRoleImpl(
    ax.role, role_name,
    [&](const Snapshot& snap, const Role& role) {
      RequireRoleMembership(snap, ax.role, role);
    },
    [&](const Role& old_role, std::shared_ptr<Role>& new_role) {
      new_role = basics::downCast<Role>(old_role.Clone());
      new_role->ChangeDefaultAcl(schema, objtype, type, mutate);
    });
}

void Catalog::ChangeMembership(const AccessContext& ax, ObjectId role,
                               std::string_view role_name, ObjectId member,
                               std::string_view member_name,
                               const Membership& edge, bool revoke,
                               bool admin_option_only) {
  absl::MutexLock lock{&_mutex};
  auto actor = _snapshot->GetObject<Role>(ax.role);
  if (!(actor && actor->IsSuperuser()) &&
      !auth::HasAdminOption(*_snapshot, ax.role, role)) {
    const auto verb = revoke ? "revoke" : "grant";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role_name, "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"", role_name,
                 "\" may ", verb, " this role."));
  }
  if (!revoke) {
    if (!_snapshot->GetObject<Role>(role)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", role_name, "\" does not exist"));
    }
    if (auth::ComputeMembershipClosure(*_snapshot, role).contains(member)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("role \"", role_name, "\" is a member of role \"",
                              member_name, "\""));
    }
  }

  auto member_role = _snapshot->GetObject<Role>(member);
  if (!member_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", member_name, "\" does not exist"));
  }
  auto new_role = std::static_pointer_cast<Role>(member_role->Clone());
  if (revoke && admin_option_only) {
    auto edges = new_role->MemberOf();
    auto it = std::ranges::find(edges, role, &Membership::role);
    if (it != edges.end()) {
      Membership kept = *it;
      kept.admin_option = false;
      new_role->AddMembership(kept);
    }
  } else if (revoke) {
    new_role->RemoveMembership(role);
  } else {
    new_role->AddMembership(edge);
  }

  Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      clone->ReplaceObject<ResolveType::Role>(id::kInstance,
                                              new_role->GetName(), new_role);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*new_role, stream);
      _engine->CreateDefinition(id::kInstance, ObjectType::Role,
                                new_role->GetId(), bytes);
      clone->RebuildRoleClosures();
    },
    [&](const std::shared_ptr<Snapshot>& clone) {
      auto obj = clone->GetObject<Role>(new_role->GetId());
      if (obj->GetName() == new_role->GetName()) {
        clone->ReplaceObject<ResolveType::Role>(
          id::kInstance, new_role->GetName(), member_role);
      }
    });
}

namespace {

// PG ownership transfer: drop the old owner's implicit self-grant and rewrite
// grantor old->new on the rows it had granted, then install the new owner.
// Operates on an already-cloned object (the COW analogue of writing a new
// catalog tuple).
void TransferOwner(Object& cloned, ObjectId new_owner) {
  auto perm = cloned.GetPermissions();
  const ObjectId old_owner = perm.owner;
  std::erase_if(perm.acl, [&](const AclItem& item) {
    return item.grantee == old_owner && item.grantor == old_owner;
  });
  for (auto& item : perm.acl) {
    if (item.grantor == old_owner) {
      item.grantor = new_owner;
    }
  }
  perm.owner = new_owner;
  cloned.SetPermissions(std::move(perm));
}

}  // namespace

void Catalog::ChangeOwner(const AccessContext& ax, ObjectId database_id,
                          std::string_view schema, std::string_view name,
                          ObjectType type, ObjectId new_owner,
                          std::string_view new_owner_name) {
  absl::MutexLock lock{&_mutex};

  auto require_owner_change = [&](const Object& obj) {
    const auto& rc = _snapshot->ClosureFor(ax.role);
    // A superuser bypasses every ALTER OWNER check (owner, SET-ROLE-to-new,
    // and CREATE on the target schema).
    if (rc.is_superuser) {
      return;
    }
    if (!rc.Owns(obj)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be owner of ", pg::ToPgObjectTypeName(type), " ", name));
    }
    const ObjectId real_owner = obj.GetOwner();
    if (!auth::ComputeSetRoleClosure(*_snapshot, ax.role).contains(new_owner)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be able to SET ROLE \"", new_owner_name, "\""));
    }
    if (type != ObjectType::Schema && new_owner != real_owner) {
      auto sch = _snapshot->GetSchema(database_id, schema);
      if (sch && !_snapshot->ClosureFor(new_owner).Can(*sch, AclMode::Create)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for schema ", schema));
      }
    }
  };

  // ALTER SCHEMA name OWNER TO: in-place body swap that rebinds the schema-name
  // resolution key onto the new body while leaving the child namespaces (keyed
  // by the stable schema_id) intact, so contained relations stay reachable.
  if (type == ObjectType::Schema) {
    auto schema_id =
      _snapshot->GetObjectId<ResolveType::Schema>(database_id, name);
    if (!schema_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", name, "\" does not exist"));
    }
    auto obj = _snapshot->GetObject(*schema_id);
    if (!obj) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", name, "\" does not exist"));
    }
    require_owner_change(*obj);
    auto cloned = obj->Clone();
    TransferOwner(*cloned, new_owner);
    Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*cloned, stream);
      clone->ReplaceObject<ResolveType::Schema>(database_id, cloned->GetName(),
                                                cloned);
      _engine->CreateDefinition(database_id, ObjectType::Schema,
                                cloned->GetId(), bytes);
    });
    return;
  }

  // Types live in their own per-schema namespace, separate from relations.
  if (type == ObjectType::PgSqlType) {
    auto schema_id =
      _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
    if (!schema_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", schema, "\" does not exist"));
    }
    auto type_id = _snapshot->GetObjectId<ResolveType::Type>(*schema_id, name);
    if (!type_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("type \"", name, "\" does not exist"));
    }
    auto obj = _snapshot->GetObject(*type_id);
    SDB_ASSERT(obj);
    require_owner_change(*obj);
    auto cloned = obj->Clone();
    TransferOwner(*cloned, new_owner);
    Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*cloned, stream);
      clone->ReplaceObject<ResolveType::Type>(*schema_id, cloned->GetName(),
                                              cloned);
      _engine->CreateDefinition(*schema_id, ObjectType::PgSqlType,
                                cloned->GetId(), bytes);
    });
    return;
  }

  // Relations (table / view / sequence) live under a schema.
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG(pg::ToPgObjectTypeName(type), " \"", name, "\" does not exist"));
  }
  auto obj = _snapshot->GetObject(*object_id);
  if (!obj) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG(pg::ToPgObjectTypeName(type), " \"", name, "\" does not exist"));
  }
  require_owner_change(*obj);

  std::vector<std::shared_ptr<Object>> cascade;
  if (obj->GetType() == ObjectType::Table) {
    cascade =
      _snapshot->GetDependency<TableDependency>(*object_id)->owned_sequences |
      std::views::transform([&](ObjectId seq_id) -> std::shared_ptr<Object> {
        auto seq = _snapshot->GetObject<Sequence>(seq_id);
        SDB_ASSERT(seq);
        return seq;
      }) |
      std::ranges::to<std::vector<std::shared_ptr<Object>>>();
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    auto restamp = [&](const std::shared_ptr<Object>& original) {
      auto cloned = original->Clone();
      TransferOwner(*cloned, new_owner);
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*cloned, stream);
      clone->ReplaceObject<ResolveType::Relation>(cloned->GetParentId(),
                                                  cloned->GetName(), cloned);
      _engine->CreateDefinition(cloned->GetParentId(), cloned->GetType(),
                                cloned->GetId(), bytes);
    };

    restamp(obj);
    for (const auto& dep : cascade) {
      restamp(dep);
    }
  });
}

void Catalog::ChangeAcl(ObjectId database_id, std::string_view schema,
                        std::string_view name, ObjectType type,
                        AclMutator mutate) {
  absl::MutexLock lock{&_mutex};

  std::shared_ptr<Object> obj;
  ObjectId parent;
  if (type == ObjectType::Database) {
    obj = _snapshot->GetObject(database_id);
  } else if (type == ObjectType::Schema) {
    // For a schema target the schema's own name arrives in `name`; `schema` is
    // empty (it has no containing schema).
    auto schema_id =
      _snapshot->GetObjectId<ResolveType::Schema>(database_id, name);
    if (!schema_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", name, "\" does not exist"));
    }
    obj = _snapshot->GetObject(*schema_id);
    parent = database_id;
  } else {
    auto schema_id =
      _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
    if (!schema_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", schema, "\" does not exist"));
    }
    // Functions and types live in their own per-schema namespaces; everything
    // else resolves in the relation namespace.
    std::optional<ObjectId> object_id;
    if (type == ObjectType::PgSqlFunction) {
      object_id =
        _snapshot->GetObjectId<ResolveType::Function>(*schema_id, name);
    } else if (type == ObjectType::PgSqlType) {
      object_id = _snapshot->GetObjectId<ResolveType::Type>(*schema_id, name);
    } else {
      object_id =
        _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
    }
    if (!object_id) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG(pg::ToPgObjectTypeName(type), " \"", name,
                              "\" does not exist"));
    }
    obj = _snapshot->GetObject(*object_id);
    parent = *schema_id;
  }
  if (!obj) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG(pg::ToPgObjectTypeName(type), " \"", name, "\" does not exist"));
  }

  // The stored ACL holds only non-owner grants; the owner's privileges are
  // derived from ownership at check time and synthesized at render time.
  const auto owner = obj->GetOwner();
  auto acl = auth::AclForStorage(obj->GetAcl(), type, owner);
  mutate(*_snapshot, owner, acl);

  auto cloned = obj->Clone();
  cloned->SetPermissions({owner, std::move(acl)});

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    duckdb::MemoryStream stream;
    auto bytes = catalog::SerializeObject(*cloned, stream);
    if (type == ObjectType::Database) {
      clone->ReplaceObject<ResolveType::Database>({}, cloned->GetName(),
                                                  cloned);
      _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(id::kInstance, ObjectType::Database, cloned->GetId(),
                          bytes);
      });
      return;
    }
    if (type == ObjectType::Schema) {
      clone->ReplaceObject<ResolveType::Schema>(parent, cloned->GetName(),
                                                cloned);
      _engine->CreateDefinition(parent, ObjectType::Schema, cloned->GetId(),
                                bytes);
      return;
    }
    if (type == ObjectType::PgSqlFunction) {
      clone->ReplaceObject<ResolveType::Function>(parent, cloned->GetName(),
                                                  cloned);
    } else if (type == ObjectType::PgSqlType) {
      clone->ReplaceObject<ResolveType::Type>(parent, cloned->GetName(),
                                              cloned);
    } else {
      clone->ReplaceObject<ResolveType::Relation>(parent, cloned->GetName(),
                                                  cloned);
    }
    _engine->CreateDefinition(parent, cloned->GetType(), cloned->GetId(),
                              bytes);
  });
}

void Catalog::ChangeColumnAcl(ObjectId database_id, std::string_view schema,
                              std::string_view table_name,
                              std::string_view column, AclMutator mutate) {
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, table_name);
  if (!object_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("relation \"", table_name, "\" does not exist"));
  }
  auto table = _snapshot->GetObject<Table>(*object_id);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("relation \"", table_name, "\" does not exist"));
  }

  const ObjectId owner = table->GetOwner();
  std::shared_ptr<Table> cloned;
  table->ChangeColumnAcl(
    cloned, column, [&](catalog::Acl& acl) { mutate(*_snapshot, owner, acl); });

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    duckdb::MemoryStream stream;
    auto bytes = catalog::SerializeObject(*cloned, stream);
    clone->ReplaceObject<ResolveType::Relation>(*schema_id, cloned->GetName(),
                                                cloned);
    _engine->CreateDefinition(*schema_id, cloned->GetType(), cloned->GetId(),
                              bytes);
  });
}

void Catalog::ChangeTable(const AccessContext& ax, ObjectId database_id,
                          std::string_view schema, std::string_view name,
                          ChangeCallback<Table> new_table,
                          const ChangeTableOptions& options) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  std::shared_ptr<Object> obj;
  if (schema_id) {
    if (auto object_id =
          _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name)) {
      obj = _snapshot->GetObject(*object_id);
    }
  }
  if (!obj) {
    if (options.missing_ok) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, obj->GetId());
  if (obj->GetType() != ObjectType::Table) {
    if (options.on_type_mismatch) {
      options.on_type_mismatch(*obj);
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", name, "\" is not a table"));
  }

  auto table = basics::downCast<Table>(std::move(obj));
  std::shared_ptr<Table> updated;
  new_table(*table, updated);
  if (!updated) {
    return;
  }

  std::string store_name;
  if (updated->GetEngine() == TableEngine::Transactional &&
      !updated->Tombstoned()) {
    auto database = _snapshot->GetObject<Database>(database_id);
    SDB_ASSERT(database);
    store_name = StoreTableName(database->GetName(), schema, name);
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    clone->ReplaceObject<ResolveType::Relation>(*schema_id, name, updated);

    {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*updated, stream);
      _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Table, updated->GetId(),
                          bytes);
        if (store_name.empty()) {
          return;
        }
        bool renamed_columns = false;
        containers::FlatHashMap<std::string, std::string> renames;
        for (const auto& old_col : table->Columns()) {
          if (old_col.GetId() == Column::kGeneratedPKId) {
            continue;
          }
          auto it = std::ranges::find_if(
            updated->Columns(),
            [&](const auto& c) { return c.GetId() == old_col.GetId(); });
          if (it != updated->Columns().end() &&
              it->GetName() != old_col.GetName()) {
            ctx.RenameStoreColumn(store_name, std::string{old_col.GetName()},
                                  std::string{it->GetName()});
            renames.emplace(std::string{old_col.GetName()},
                            std::string{it->GetName()});
            renamed_columns = true;
          }
        }
        // Newly added columns -> mirror to the store table. A constant
        // DEFAULT backfills existing rows; the store handler retries without
        // it when the expression calls a facade-only function.
        for (const auto& new_col : updated->Columns()) {
          if (new_col.GetId() == Column::kGeneratedPKId) {
            continue;
          }
          auto existed = absl::c_any_of(table->Columns(), [&](const auto& c) {
            return c.GetId() == new_col.GetId();
          });
          if (existed) {
            continue;
          }
          std::string default_sql;
          if (new_col.expr && new_col.expr->HasExpr()) {
            default_sql = new_col.expr->GetExpr().ToString();
          }
          ctx.AddStoreColumn(store_name, std::string{new_col.GetName()},
                             new_col.type.ToString(), std::move(default_sql));
        }
        if (renamed_columns) {
          // Mirrored index definitions embed column names;
          // recreate them against the renamed table (rowid
          // postings and row data are untouched).
          auto deps =
            _snapshot->GetDependency<TableDependency>(updated->GetId());
          auto schema_obj = _snapshot->GetObject<Schema>(*schema_id);
          auto database = _snapshot->GetObject<Database>(database_id);
          if (deps && schema_obj && database) {
            for (auto idx_id : deps->indexes) {
              auto idx = _snapshot->GetObject<Index>(idx_id);
              if (!idx) {
                continue;
              }
              // Expression keys bake column NAMES into pretty_printed at
              // CREATE INDEX time; re-render against the renamed columns so
              // the store recreate SQL binds, and persist the refreshed text.
              if (idx->GetType() == ObjectType::SecondaryIndex) {
                const auto& sec = basics::downCast<const SecondaryIndex>(*idx);
                if (!sec.Expressions().empty()) {
                  auto exprs = sec.Expressions();
                  bool changed = false;
                  for (auto& e : exprs) {
                    if (auto pretty = RerenderPrettyAfterRename(
                          e.pretty_printed, renames)) {
                      if (*pretty != e.pretty_printed) {
                        e.pretty_printed = std::move(*pretty);
                        changed = true;
                      }
                    }
                  }
                  if (changed) {
                    auto new_idx = std::make_shared<SecondaryIndex>(
                      sec.GetDatabaseId(), sec.GetParentId(), sec.GetId(),
                      sec.GetRelationId(), std::string{sec.GetName()},
                      sec.Columns(), std::move(exprs), sec.IsUnique());
                    clone->ReplaceObject<ResolveType::Relation>(
                      *schema_id, new_idx->GetName(), new_idx);
                    duckdb::MemoryStream idx_stream;
                    ctx.PutDefinition(
                      new_idx->GetRelationId(), ObjectType::SecondaryIndex,
                      new_idx->GetId(),
                      catalog::SerializeObject(*new_idx, idx_stream));
                    idx = new_idx;
                  }
                }
              }
              if (auto def =
                    MakeStoreIndexDef(database->GetName(),
                                      schema_obj->GetName(), *updated, *idx)) {
                ctx.DropStoreIndex(idx_id);
                ctx.CreateStoreIndex(std::move(*def));
              }
            }
          }
        }
        for (const auto& oc : table->CheckConstraints()) {
          bool survives = absl::c_any_of(
            updated->CheckConstraints(),
            [&](const auto& nc) { return nc.GetId() == oc.GetId(); });
          if (survives) {
            continue;
          }
          if (auto idx = oc.IsNotNull(table->Columns())) {
            const auto& col = table->Columns()[*idx];
            if (col.GetId() != Column::kGeneratedPKId) {
              ctx.DropStoreNotNull(store_name, std::string{col.GetName()});
            }
          } else if (oc.expr && oc.expr->HasExpr()) {
            ctx.DropStoreCheck(store_name, oc.expr->GetExpr().ToString());
          }
        }
        // Newly added PRIMARY KEY -> ADD PRIMARY KEY on the store table; it
        // recreates storage, validates existing rows (no duplicates/nulls).
        // The implied NOT NULL CHECKs flow through the loop below.
        if (table->PKColumns().empty() && !updated->PKColumns().empty()) {
          std::vector<std::string> pk_names;
          pk_names.reserve(updated->PKColumns().size());
          bool ok = true;
          for (auto id : updated->PKColumns()) {
            const auto* col = updated->ColumnById(id);
            if (!col || col->type.IsNested()) {
              ok = false;
              break;
            }
            pk_names.emplace_back(col->GetName());
          }
          if (ok) {
            ctx.AddStorePrimaryKey(store_name, std::move(pk_names));
          }
        }
        // Newly added UNIQUE constraints -> ADD UNIQUE on the store table.
        for (const auto& uc : updated->UniqueConstraints()) {
          bool existed = absl::c_any_of(
            table->UniqueConstraints(),
            [&](const auto& oc) { return oc.columns == uc.columns; });
          if (existed) {
            continue;
          }
          std::vector<std::string> names;
          names.reserve(uc.columns.size());
          bool ok = true;
          for (auto id : uc.columns) {
            const auto* col = updated->ColumnById(id);
            if (!col || col->type.IsNested()) {
              ok = false;
              break;
            }
            names.emplace_back(col->GetName());
          }
          if (ok) {
            ctx.AddStoreUnique(store_name, std::move(names));
          }
        }
        // Newly added NOT NULL constraints -> SET NOT NULL on the store
        // column (queued after the column add above, so it already exists).
        for (const auto& nc : updated->CheckConstraints()) {
          bool existed = absl::c_any_of(
            table->CheckConstraints(),
            [&](const auto& oc) { return oc.GetId() == nc.GetId(); });
          if (existed) {
            continue;
          }
          if (auto idx = nc.IsNotNull(updated->Columns())) {
            const auto& col = updated->Columns()[*idx];
            if (col.GetId() != Column::kGeneratedPKId) {
              ctx.AddStoreNotNull(store_name, col.GetName());
            }
          } else if (nc.expr && nc.expr->HasExpr()) {
            // Function calls bind against the store connection's catalog, so
            // such checks stay facade-side (as at CREATE time); plain checks
            // mirror to the store, which verifies them against existing rows.
            bool has_function = false;
            auto scan = [&](this auto& self,
                            const duckdb::ParsedExpression& e) -> void {
              if (e.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION) {
                has_function = true;
                return;
              }
              duckdb::ParsedExpressionIterator::EnumerateChildren(
                e, [&](const duckdb::ParsedExpression& child) { self(child); });
            };
            scan(nc.expr->GetExpr());
            if (!has_function) {
              ctx.AddStoreCheck(store_name, nc.expr->GetExpr().ToString());
            }
          }
        }
      });
    }
  });
}

bool Catalog::DropRole(const AccessContext& ax, std::string_view role,
                       bool missing_ok) {
  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(*_snapshot, ax.role, RoleOption::CreateRole, "drop role",
                       "Only roles with the CREATEROLE attribute and the ADMIN "
                       "option on the target roles may drop roles.");
  auto role_ptr = _snapshot->GetRole(role);
  if (!role_ptr) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", role, "\" does not exist"));
  }
  if (role_ptr->GetId() == ax.role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("current user cannot be dropped"));
  }
  if (role == StaticStrings::kDefaultUser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot drop role ", role,
                            " because it is required by the database system"));
  }
  RequireRoleAdmin(*_snapshot, ax.role, *role_ptr, "drop");
  if (auto deps = _snapshot->RoleDependentCount(role_ptr->GetId()); deps > 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("role \"", role,
              "\" cannot be dropped because some objects depend on it"),
      ERR_DETAIL(deps, " object(s) in database depend on role \"", role, "\""));
  }
  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    clone->UnregisterObject(role_ptr, id::kInstance);
    _engine->DropDefinition(id::kInstance, ObjectType::Role, role_ptr->GetId());
    clone->RebuildRoleClosures();
  });
  return true;
}

void Catalog::DropDatabase(const AccessContext& ax, std::string_view name,
                           duckdb::shared_ptr<void> keep_alive) {
  absl::MutexLock lock{&_mutex};
  auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, name);
  if (!database_id) {
    THROW_SQL_ERROR(ERR_MSG("database \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *database_id);

  auto plan = _snapshot->ComputeDropPlan(*database_id);

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto database = clone->GetObject<Database>(*database_id);
    SDB_ASSERT(database);
    auto task = clone->CreateDatabaseDrop(_pending_drops, database,
                                          std::move(keep_alive));
    _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(id::kInstance, *database_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });
    clone->UnregisterObject(std::move(database), id::kInstance);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    // Check that SereneDB won't open this database after reboot
    SDB_IF_FAILURE("crash_on_drop") { return; }
    DropTask::Schedule(std::move(task)).Detach();
  });
}

bool Catalog::DropSchema(const AccessContext& ax, std::string_view database,
                         std::string_view name, bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, name);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *schema_id);

  if (!cascade && !_snapshot->CheckSchemaEmptyDependency(*schema_id)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop schema ", name,
              " because other objects depend on it"),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  auto plan = _snapshot->ComputeDropPlan(*schema_id);

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto schema = clone->GetObject<Schema>(*schema_id);
    SDB_ASSERT(schema);
    auto task =
      clone->CreateSchemaDrop(_pending_drops, *database_id, schema, true);
    _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(*database_id, *schema_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });
    clone->UnregisterObject(std::move(schema), *database_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    // Check that SereneDB won't open this schema after reboot
    SDB_IF_FAILURE("crash_on_drop") { return; }
    DropTask::Schedule(std::move(task)).Detach();
  });
  return true;
}

bool Catalog::DropTable(const AccessContext& ax, std::string_view database,
                        std::string_view schema, std::string_view name,
                        bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!table_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *table_id);

  auto plan = _snapshot->ComputeDropPlan(*table_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop table ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot, "table", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*table_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::Table) {
      ThrowWrongObjectType(name, "table", object->GetType());
    }
    auto table = basics::downCast<Table>(std::move(object));
    auto task = clone->CreateTableDrop(_pending_drops, *database_id, *schema_id,
                                       table, true);
    _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(*schema_id, *table_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });

    clone->UnregisterObject(std::move(table), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    SDB_IF_FAILURE("crash_on_drop") { return; }
    DropTask::Schedule(std::move(task)).Detach();
  });
  return true;
}

void Catalog::DropTableColumn(const AccessContext& ax, ObjectId database_id,
                              std::string_view schema, std::string_view table,
                              std::string_view column, bool if_exists) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table, "\" does not exist"));
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, table);
  if (!table_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *table_id);
  auto object = _snapshot->GetObject(*table_id);
  if (!object || object->GetType() != ObjectType::Table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", table, "\" is not a table"));
  }
  auto table_ptr = basics::downCast<Table>(std::move(object));
  auto col_it = absl::c_find_if(table_ptr->Columns(), [&](const Column& c) {
    return c.GetName() == column;
  });
  if (col_it == table_ptr->Columns().end()) {
    if (if_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", column, "\" of relation \"", table,
                            "\" does not exist"));
  }
  const auto col_id = col_it->GetId();

  auto plan = _snapshot->ComputeColumnDropPlan(*table_id, col_id);

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    _engine->Write([&](auto& ctx) { clone->CommitDropPlan(ctx, plan); });
    clone->ApplyDropPlan(_pending_drops, database_id, plan);
  });
}

void Catalog::ChangeColumnType(const AccessContext& ax, ObjectId database_id,
                               std::string_view schema, std::string_view table,
                               std::string_view column,
                               duckdb::LogicalType new_type,
                               std::string using_sql) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table, "\" does not exist"));
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, table);
  if (!table_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", table, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *table_id);
  auto object = _snapshot->GetObject(*table_id);
  if (!object || object->GetType() != ObjectType::Table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", table, "\" is not a table"));
  }
  auto table_ptr = basics::downCast<Table>(std::move(object));
  auto col_it = absl::c_find_if(table_ptr->Columns(), [&](const Column& c) {
    return c.GetName() == column;
  });
  if (col_it == table_ptr->Columns().end()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", column, "\" of relation \"", table,
                            "\" does not exist"));
  }
  const auto col_id = col_it->GetId();

  // The index stores values of the column's old type; a type change would
  // leave them inconsistent. Reject and let the user drop the index first.
  if (auto td = _snapshot->GetDependency<TableDependency>(*table_id)) {
    for (auto idx_id : td->indexes) {
      auto idx = _snapshot->GetObject<Index>(idx_id);
      if (idx && idx->ReferencesColumn(col_id)) {
        THROW_SQL_ERROR(ERR_MSG("cannot alter type of column \"", column,
                                "\" because index \"", idx->GetName(),
                                "\" depends on it; drop the index first"));
      }
    }
  }

  std::shared_ptr<Table> updated;
  table_ptr->ChangeColumnType(updated, column, new_type);

  std::string store_name;
  if (updated->GetEngine() == TableEngine::Transactional &&
      !updated->Tombstoned()) {
    auto database = _snapshot->GetObject<Database>(database_id);
    SDB_ASSERT(database);
    store_name = StoreTableName(database->GetName(), schema, table);
  }
  std::string type_sql = new_type.ToString();

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    clone->ReplaceObject<ResolveType::Relation>(*schema_id, table, updated);
    {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*updated, stream);
      _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Table, updated->GetId(),
                          bytes);
        if (store_name.empty()) {
          return;
        }
        // The store blocks ALTER COLUMN TYPE while any index depends on the
        // table; drop the mirrored store indexes, change the type, then
        // recreate them (the data lives in the rows / iresearch, so the
        // rebuild carries no state of its own).
        std::vector<StoreIndexDef> recreate;
        auto deps = _snapshot->GetDependency<TableDependency>(*table_id);
        auto schema_obj = _snapshot->GetObject<Schema>(*schema_id);
        auto database = _snapshot->GetObject<Database>(database_id);
        if (deps && schema_obj && database) {
          for (auto idx_id : deps->indexes) {
            auto idx = _snapshot->GetObject<Index>(idx_id);
            if (!idx) {
              continue;
            }
            if (auto def = MakeStoreIndexDef(
                  database->GetName(), schema_obj->GetName(), *updated, *idx)) {
              ctx.DropStoreIndex(idx_id);
              recreate.push_back(std::move(*def));
            }
          }
        }
        ctx.ChangeStoreColumnType(store_name, std::string{column}, type_sql,
                                  using_sql);
        for (auto& def : recreate) {
          ctx.CreateStoreIndex(std::move(def));
        }
      });
    }
  });
}

void Catalog::RemoveTombstone(ObjectId database_id, std::string_view schema,
                              std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_MSG("tombstoned object \"", name, "\" not found"));
  }
  const auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    THROW_SQL_ERROR(ERR_MSG("tombstoned object \"", name, "\" not found"));
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    THROW_SQL_ERROR(ERR_MSG("tombstoned object \"", name, "\" not found"));
  }

  ObjectId tombstone_parent;
  if (IsIndex(object->GetType())) {
    auto& index = basics::downCast<Index>(*object);
    tombstone_parent = index.GetRelationId();
  } else {
    tombstone_parent = *schema_id;
  }

  // Unlike most catalog operations that clone the snapshot, here we modify the
  // object in-place because the tombstone flag is simple in-memory state.
  irs::Finally clear_tombstone = [&] noexcept {
    basics::downCast<Object>(*object).SetTombstoned(false);
  };

  _engine->Write([&](auto& ctx) {
    ctx.DropDefinition(tombstone_parent, ObjectType::Tombstone, *object_id);
    if (object->GetType() == ObjectType::Table) {
      auto& table = basics::downCast<Table>(*object);
      if (table.GetEngine() == TableEngine::Transactional) {
        auto database = _snapshot->GetObject<Database>(database_id);
        SDB_ASSERT(database);
        ctx.RenameStoreTable(DroppedStoreTableName(*object_id),
                             StoreTableName(database->GetName(), schema, name));
      }
    }
  });
}

bool Catalog::DropIndex(const AccessContext& ax, std::string_view database,
                        std::string_view schema, std::string_view name,
                        bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  const auto index_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!index_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  // An index has no independent owner (PG: its relowner is derived from the
  // table). Drop authority therefore belongs to the underlying table's owner,
  // so check that, not the index's own (unset) owner.
  if (auto index = _snapshot->GetObject<Index>(*index_id)) {
    // Check the parent table's owner, but report the index in the error.
    RequireObjectOwner(*_snapshot, ax.role, index->GetRelationId(), *index);
  }

  DropIndexByIdLocked(*database_id, *index_id, cascade);
  return true;
}

void Catalog::DropIndexById(ObjectId database_id, ObjectId index_id,
                            bool cascade) {
  absl::MutexLock lock{&_mutex};
  DropIndexByIdLocked(database_id, index_id, cascade);
}

void Catalog::DropIndexByIdLocked(ObjectId database_id, ObjectId index_id,
                                  bool cascade) {
  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto obj = clone->GetObject(index_id);
    if (!obj) {
      THROW_SQL_ERROR(
        ERR_MSG("index with id ", index_id.id(), " does not exist"));
    }
    if (!IsIndex(obj->GetType())) {
      ThrowWrongObjectType(obj->GetName(), "index", obj->GetType());
    }
    auto index = basics::downCast<Index>(std::move(obj));
    const auto schema_id = index->GetParentId();
    // Store-side index drop is synchronous: UNIQUE enforcement
    // must stop when DROP INDEX commits, not when the async sweep
    // runs.
    _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(index->GetRelationId(), index_id);
      ctx.DropStoreIndex(index_id);
    });

    // Check that SereneDB won't open this index after reboot
    SDB_IF_FAILURE("crash_on_drop") { return; }

    auto task = clone->CreateIndexDrop(_pending_drops, database_id, schema_id,
                                       index->GetRelationId(), index, true);
    clone->UnregisterObject(index, schema_id);
    DropTask::Schedule(std::move(task)).Detach();
  });
}

bool Catalog::DropView(const AccessContext& ax, std::string_view database,
                       std::string_view schema, std::string_view name,
                       bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("view \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("view \"", name, "\" does not exist"));
  }
  const auto view_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!view_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("view \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *view_id);

  auto plan = _snapshot->ComputeDropPlan(*view_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop view ", name, " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot, "view", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*view_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlView) {
      ThrowWrongObjectType(name, "view", object->GetType());
    }
    auto view = basics::downCast<PgSqlView>(std::move(object));

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlView, *view_id);
      clone->CommitDropPlan(ctx, plan);
    });
    clone->UnregisterObject(std::move(view), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropSequence(const AccessContext& ax, std::string_view database,
                           std::string_view schema, std::string_view name,
                           bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("sequence \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("sequence \"", name, "\" does not exist"));
  }
  const auto seq_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!seq_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("sequence \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *seq_id);

  auto plan = _snapshot->ComputeDropPlan(*seq_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop sequence ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot, "sequence", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too, or "
               "DROP TABLE on the owning table."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*seq_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::Sequence) {
      ThrowWrongObjectType(name, "sequence", object->GetType());
    }
    auto seq = basics::downCast<Sequence>(std::move(object));

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::Sequence, *seq_id);
      ctx.DropSequence(*seq_id);  // counter row in same atomic batch
      clone->CommitDropPlan(ctx, plan);
    });
    clone->UnregisterObject(std::move(seq), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropType(const AccessContext& ax, std::string_view database,
                       std::string_view schema, std::string_view name,
                       bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("type \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("type \"", name, "\" does not exist"));
  }
  const auto type_id =
    _snapshot->GetObjectId<ResolveType::Type>(*schema_id, name);
  if (!type_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("type \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *type_id);

  auto plan = _snapshot->ComputeDropPlan(*type_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop type ", name, " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot, "type", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*type_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlType) {
      ThrowWrongObjectType(name, "type", object->GetType());
    }
    auto type = basics::downCast<PgSqlType>(std::move(object));

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlType, *type_id);
      clone->CommitDropPlan(ctx, plan);
    });
    clone->UnregisterObject(std::move(type), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropFunction(const AccessContext& ax, std::string_view database,
                           std::string_view schema, std::string_view name,
                           bool cascade, bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("function \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("function \"", name, "\" does not exist"));
  }
  const auto function_id =
    _snapshot->GetObjectId<ResolveType::Function>(*schema_id, name);
  if (!function_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("function \"", name, "\" does not exist"));
  }
  RequireObjectOwner(*_snapshot, ax.role, *function_id);

  auto plan = _snapshot->ComputeDropPlan(*function_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop function ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot, "function", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto function = clone->GetObject<PgSqlFunction>(*function_id);
    SDB_ASSERT(function);

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlFunction, *function_id);
      clone->CommitDropPlan(ctx, plan);
    });
    clone->UnregisterObject(std::move(function), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropTokenizer(std::string_view database, std::string_view schema,
                            std::string_view name, bool cascade,
                            bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" does not exist"));
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" does not exist"));
  }
  const auto tokenizer_id =
    _snapshot->GetObjectId<ResolveType::Tokenizer>(*schema_id, name);
  if (!tokenizer_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" does not exist"));
  }

  auto plan = _snapshot->ComputeDropPlan(*tokenizer_id);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop text search dictionary ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(*_snapshot,
                                             "text search dictionary", name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto tokenizer = clone->GetObject<Tokenizer>(*tokenizer_id);
    SDB_ASSERT(tokenizer);

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::Tokenizer, *tokenizer_id);
      clone->CommitDropPlan(ctx, plan);
    });

    clone->UnregisterObject(std::move(tokenizer), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropForeignServer(const AccessContext& ax,
                                std::string_view database,
                                std::string_view name, bool cascade,
                                bool missing_ok) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  const auto foreign_server_id =
    database_id
      ? _snapshot->GetObjectId<ResolveType::ForeignServer>(*database_id, name)
      : std::nullopt;
  if (!foreign_server_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", name, "\" does not exist"));
  }
  // PG semantics: only the server's owner (or a superuser) may drop it.
  RequireObjectOwner(*_snapshot, ax.role, *foreign_server_id);

  // The server's dependent user mappings (see ForeignServerDependency).
  // PG-style RESTRICT (the default) refuses the drop while any exist; CASCADE
  // removes them together with the server, in one transaction.
  const auto server_dep =
    _snapshot->GetDependency<ForeignServerDependency>(*foreign_server_id);
  std::vector<ObjectId> mapping_ids{server_dep->user_mappings.begin(),
                                    server_dep->user_mappings.end()};
  if (!cascade && !mapping_ids.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop server \"", name,
              "\" because other objects depend on it"),
      ERR_HINT("Use DROP SERVER ... CASCADE to drop the dependent objects "
               "too."));
  }

  auto plan = _snapshot->ComputeDropPlan(*foreign_server_id);

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto foreign_server = clone->GetObject<ForeignServer>(*foreign_server_id);
    SDB_ASSERT(foreign_server);

    std::vector<std::shared_ptr<UserMapping>> mappings;
    mappings.reserve(mapping_ids.size());
    for (auto mid : mapping_ids) {
      if (auto m = clone->GetObject<UserMapping>(mid)) {
        mappings.push_back(std::move(m));
      }
    }

    _engine->Write([&](auto& ctx) {
      // Store rows for both live flat under the database.
      for (const auto& m : mappings) {
        ctx.DropDefinition(*database_id, ObjectType::UserMapping, m->GetId());
      }
      ctx.DropDefinition(*database_id, ObjectType::ForeignServer,
                         *foreign_server_id);
      clone->CommitDropPlan(ctx, plan);
    });

    for (const auto& m : mappings) {
      clone->UnregisterObject(m, m->GetParentId());
    }
    clone->UnregisterObject(std::move(foreign_server), *database_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

bool Catalog::DropUserMapping(const AccessContext& ax,
                              std::string_view database,
                              std::string_view server, std::string_view role) {
  absl::MutexLock lock{&_mutex};

  // The absent cases return false (never throw): the command layer alone can
  // phrase the PG error, which names the mapping's role and server separately.
  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return false;
  }
  const auto server_id =
    _snapshot->GetObjectId<ResolveType::ForeignServer>(*database_id, server);
  if (!server_id) {
    return false;
  }
  const auto user_mapping_id =
    _snapshot->GetObjectId<ResolveType::UserMapping>(*server_id, role);
  if (!user_mapping_id) {
    return false;
  }
  if (auto mapping = _snapshot->GetObject<UserMapping>(*user_mapping_id)) {
    RequireUserMappingAuthority(*_snapshot, ax.role, *server_id,
                                mapping->GetRoleId());
  }

  auto plan = _snapshot->ComputeDropPlan(*user_mapping_id);

  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto user_mapping = clone->GetObject<UserMapping>(*user_mapping_id);
    SDB_ASSERT(user_mapping);

    _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*database_id, ObjectType::UserMapping,
                         *user_mapping_id);
      clone->CommitDropPlan(ctx, plan);
    });

    clone->UnregisterObject(std::move(user_mapping), *server_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
  });
  return true;
}

void Catalog::FinalizeLoad() {
  absl::MutexLock lock{&_mutex};
  Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    for (const auto& obj : clone->Objects()) {
      clone->AddDependencies(obj->GetParentId(), *obj);
    }
    clone->EndLoad();
    clone->RebuildRoleClosures();
  });
}

std::shared_ptr<const Snapshot> Catalog::GetCatalogSnapshot() const noexcept {
  return std::atomic_load(&_snapshot);
}

namespace {

// In case of recovery the ColumnExpr shouldn't be parsed
TableEngine CheckTableForDrop(std::string_view bytes, ReadContext ctx) {
  auto table = catalog::DeserializeObject<Table>(bytes, ctx);
  if (!table) {
    THROW_SQL_ERROR(
      ERR_MSG("failed to deserialize table definition during drop recovery"));
  }
  return table->GetEngine();
}

std::shared_ptr<IndexDrop> CreateIndexDrop(ObjectId db_id, ObjectId schema_id,
                                           ObjectId table_id, ObjectId index_id,
                                           ObjectType index_type,
                                           bool is_root = false) {
  return std::make_shared<IndexDrop>(index_id, index_type, db_id, schema_id,
                                     table_id, is_root);
}

std::shared_ptr<TableDropBase> CreateTableDrop(
  CatalogStore& store, ObjectId db_id, ObjectId schema_id, ObjectId table_id,
  TableEngine engine, bool is_root = false) {
  std::vector<ObjectId> owned_sequences;
  store.VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto seq = catalog::DeserializeObject<Sequence>(
        bytes, {.id = key.id, .database_id = db_id, .schema_id = schema_id});
      if (seq && seq->GetOwnerTableId() == table_id) {
        owned_sequences.push_back(key.id);
      }
      return true;
    });
  if (engine == TableEngine::Search) {
    // Search tables reject CREATE INDEX, so there are no child index drops.
    return std::make_shared<SearchTableDrop>(
      table_id, db_id, std::move(owned_sequences), schema_id, is_root);
  }

  std::vector<std::shared_ptr<IndexDrop>> indexes;
  auto collect_indexes = [&](ObjectType type) {
    store.VisitBoot(
      table_id, type, [&](CatalogStore::Key key, std::string_view) {
        indexes.push_back(
          CreateIndexDrop(db_id, schema_id, table_id, key.id, type));
        return true;
      });
  };
  collect_indexes(ObjectType::SecondaryIndex);
  collect_indexes(ObjectType::InvertedIndex);
  return std::make_shared<TableDrop>(table_id, std::move(indexes),
                                     std::move(owned_sequences), schema_id,
                                     is_root);
}

std::shared_ptr<SchemaDrop> CreateSchemaDrop(CatalogStore& store,
                                             ObjectId db_id, ObjectId schema_id,
                                             bool is_root = false) {
  std::vector<std::shared_ptr<TableDropBase>> tables;
  store.VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) {
      const auto engine = CheckTableForDrop(
        bytes, {.id = key.id, .database_id = db_id, .schema_id = schema_id});
      tables.push_back(
        CreateTableDrop(store, db_id, schema_id, key.id, engine));
      return true;
    });

  return std::make_shared<SchemaDrop>(schema_id, std::move(tables), db_id,
                                      is_root);
}

std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(CatalogStore& store,
                                                 ObjectId db_id) {
  std::vector<std::shared_ptr<SchemaDrop>> schemas;
  store.VisitBoot(db_id, ObjectType::Schema,
                  [&](CatalogStore::Key key, std::string_view) {
                    schemas.push_back(CreateSchemaDrop(store, db_id, key.id));
                    return true;
                  });
  return std::make_shared<DatabaseDrop>(db_id, std::move(schemas));
}

class OpenDatabase {
 public:
  enum class DeletedScope : uint8_t {
    Root = 0,  // deleted object with parent = id::kInstance
    Database,  // parent is daatabase
    Schema,    // parent is schema
    Relation,  // parent is relation
  };

  OpenDatabase(Catalog& catalog) : _catalog{catalog} {}

  void operator()() {
    CollectDeletedDefinitions(id::kInstance, DeletedScope::Root);
    irs::Finally cleanup = [&] noexcept {
      ClearDeletedDefinitions(DeletedScope::Root);
    };
    RegisterDatabases();
  }

  void AddRoles();

 private:
  void Resolve();

  void RegisterDatabases();
  void RegisterSchemas(ObjectId database_id);
  void RegisterFunctions(ObjectId database_id, ObjectId schema_id);
  void RegisterTokenizers(ObjectId database_id, ObjectId schema_id);
  void RegisterForeignServers(ObjectId database_id);
  void RegisterUserMappings(ObjectId database_id);
  void RegisterViews(ObjectId database_id, ObjectId schema_id);
  // Sequences load in two passes: standalone before tables/views (so refs
  // to them resolve at the referrer's own registration), owned after
  // tables.
  void RegisterSequences(ObjectId database_id, ObjectId schema_id, bool owned);
  void RegisterTypes(ObjectId database_id, ObjectId schema_id);
  void RegisterTables(ObjectId database_id, ObjectId schema_id);
  void RegisterInvertedStorage(const std::shared_ptr<Index>& index);
  void RegisterSearchTable(ObjectId db_id, ObjectId schema_id,
                           const Table& table);
  void RegisterIndexes(ObjectId database_id, ObjectId schema_id,
                       ObjectId table_id);

  void AddDatabase(ObjectId database_id, std::string_view bytes);
  void AddSchema(ObjectId database_id, ObjectId schema_id,
                 std::string_view bytes);
  void AddTable(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                std::shared_ptr<Table> table);
  void AddIndex(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                ObjectId index_id, ObjectType entry_type,
                std::string_view bytes);

  bool IsDeleted(ObjectId id, DeletedScope scope) {
    return _deleted[magic_enum::enum_integer(scope)].contains(id);
  }

  void ClearDeletedDefinitions(DeletedScope scope) noexcept {
    _deleted[magic_enum::enum_integer(scope)].clear();
  }

  void CollectDeletedDefinitions(ObjectId id, DeletedScope scope) {
    auto& store = GetCatalogStore();
    auto& deleted = _deleted[magic_enum::enum_integer(scope)];
    SDB_ASSERT(deleted.empty());
    store.VisitBoot(id, ObjectType::Tombstone,
                    [&](CatalogStore::Key key, std::string_view) {
                      deleted.insert(key.id);
                      return true;
                    });
  }

  Catalog& _catalog;

  // Names of the database/schema currently being walked; the boot walk is
  // strictly nested, so plain members suffice.
  std::string _database_name;
  std::string _schema_name;

  std::array<containers::FlatHashSet<ObjectId>,
             magic_enum::enum_count<DeletedScope>()>
    _deleted;
};

void OpenDatabase::AddRoles() {
  GetCatalogStore().VisitBoot(
    id::kInstance, ObjectType::Role,
    [&](CatalogStore::Key, std::string_view bytes) {
      auto role = catalog::DeserializeObject<catalog::Role>(bytes, {});
      if (!role) {
        THROW_SQL_ERROR(ERR_MSG(
          "Failed to read roles, error: Failed to read role definition"));
      }
      _catalog.RegisterRole(std::move(role));
      return true;
    });
}

void OpenDatabase::AddDatabase(ObjectId database_id, std::string_view bytes) {
  auto db =
    catalog::DeserializeObject<catalog::Database>(bytes, {.id = database_id});
  if (!db) {
    THROW_SQL_ERROR(ERR_MSG("Failed to read database definition"));
  }
  _database_name = db->GetName();
  _catalog.RegisterDatabase(db);
  CollectDeletedDefinitions(database_id, DeletedScope::Database);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Database);
  };
  RegisterSchemas(database_id);
  RegisterForeignServers(database_id);
  RegisterUserMappings(database_id);
}

void OpenDatabase::RegisterDatabases() {
  GetCatalogStore().VisitBoot(
    id::kInstance, ObjectType::Database,
    [&](CatalogStore::Key key, std::string_view bytes) {
      if (!IsDeleted(key.id, DeletedScope::Root)) {
        AddDatabase(key.id, bytes);
        return true;
      }
      DropTask::Schedule(CreateDatabaseDrop(GetCatalogStore(), key.id))
        .Detach();
      return true;
    });
}

void OpenDatabase::RegisterSchemas(ObjectId database_id) {
  GetCatalogStore().VisitBoot(
    database_id, ObjectType::Schema,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto schema_id = key.id;
      if (!IsDeleted(key.id, DeletedScope::Database)) {
        AddSchema(database_id, schema_id, bytes);
        return true;
      }

      DropTask::Schedule(
        CreateSchemaDrop(GetCatalogStore(), database_id, key.id, true))
        .Detach();
      return true;
    });
}

void OpenDatabase::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlFunction,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto function = catalog::DeserializeObject<catalog::PgSqlFunction>(
        bytes, {
                 .id = key.id,
                 .database_id = db_id,
                 .schema_id = schema_id,
               });
      if (!function) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read function definition"));
      }
      _catalog.RegisterFunction(db_id, schema_id, std::move(function));
      return true;
    });
}

void OpenDatabase::RegisterTokenizers(ObjectId db_id, ObjectId schema_id) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Tokenizer,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto tokenizer =
        catalog::DeserializeObject<Tokenizer>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!tokenizer) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read tokenizer definition"));
      }
      _catalog.RegisterTokenizer(db_id, schema_id, std::move(tokenizer));
      return true;
    });
}

// Both live flat under the database in the store; servers are registered
// first so each mapping's server-parent namespace exists.
void OpenDatabase::RegisterForeignServers(ObjectId db_id) {
  GetCatalogStore().VisitBoot(
    db_id, ObjectType::ForeignServer,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto foreign_server =
        catalog::DeserializeObject<ForeignServer>(bytes, {
                                                           .id = key.id,
                                                           .database_id = db_id,
                                                         });
      if (!foreign_server) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read foreign server definition"));
      }
      _catalog.RegisterForeignServer(db_id, std::move(foreign_server));
      return true;
    });
}

void OpenDatabase::RegisterUserMappings(ObjectId db_id) {
  GetCatalogStore().VisitBoot(
    db_id, ObjectType::UserMapping,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto user_mapping =
        catalog::DeserializeObject<UserMapping>(bytes, {
                                                         .id = key.id,
                                                         .database_id = db_id,
                                                       });
      if (!user_mapping) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read user mapping definition"));
      }
      _catalog.RegisterUserMapping(std::move(user_mapping));
      return true;
    });
}

void OpenDatabase::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlView,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto view_id = key.id;
      auto view = catalog::DeserializeObject<PgSqlView>(
        bytes, {.id = view_id, .database_id = db_id, .schema_id = schema_id});
      if (!view) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read view definition"));
      }
      _catalog.RegisterView(schema_id, std::move(view));
      CollectDeletedDefinitions(view_id, DeletedScope::Relation);
      irs::Finally cleanup = [&] noexcept {
        ClearDeletedDefinitions(DeletedScope::Relation);
      };
      RegisterIndexes(db_id, schema_id, view_id);
      return true;
    });
}

void OpenDatabase::RegisterSequences(ObjectId db_id, ObjectId schema_id,
                                     bool owned) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto seq =
        catalog::DeserializeObject<Sequence>(bytes, {
                                                      .id = key.id,
                                                      .database_id = db_id,
                                                      .schema_id = schema_id,
                                                    });
      if (!seq) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read sequence definition"));
      }
      if (seq->GetOwnerTableId().isSet() != owned) {
        return true;
      }
      // Owner table is tombstoned -> skip; its DropTask will clean the seq.
      if (auto owner = seq->GetOwnerTableId();
          owner.isSet() && IsDeleted(owner, DeletedScope::Schema)) {
        return true;
      }
      _catalog.RegisterSequence(db_id, schema_id, std::move(seq));
      return true;
    });
}

void OpenDatabase::RegisterTypes(ObjectId db_id, ObjectId schema_id) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlType,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto type =
        catalog::DeserializeObject<PgSqlType>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!type) {
        THROW_SQL_ERROR(ERR_MSG("Failed to read type definition"));
      }
      _catalog.RegisterType(db_id, schema_id, std::move(type));
      return true;
    });
}

void OpenDatabase::RegisterIndexes(ObjectId db_id, ObjectId schema_id,
                                   ObjectId table_id) {
  auto visit = [&](ObjectType type) {
    GetCatalogStore().VisitBoot(
      table_id, type, [&](CatalogStore::Key key, std::string_view bytes) {
        auto index_id = key.id;
        if (!IsDeleted(index_id, DeletedScope::Relation)) {
          AddIndex(db_id, schema_id, table_id, index_id, type, bytes);
          return true;
        }

        auto drop =
          CreateIndexDrop(db_id, schema_id, table_id, key.id, type, true);
        DropTask::Schedule(std::move(drop)).Detach();
        return true;
      });
  };
  visit(ObjectType::SecondaryIndex);
  visit(ObjectType::InvertedIndex);
}

void OpenDatabase::RegisterInvertedStorage(
  const std::shared_ptr<Index>& index) {
  // The inverted index's durable state is its iresearch segment directory; the
  // storage is (re)opened from it here and bound onto the index. Must run
  // before InitInvertedIndexes/BindStoreTableIndexes so WAL replay resolves it
  // via index->GetData().
  const auto& inverted = basics::downCast<const InvertedIndex>(*index);
  inverted.SetData(search::InvertedIndexStorage::Create(
    inverted.GetId(), inverted, /*is_new=*/false));
}

void OpenDatabase::RegisterSearchTable(ObjectId db_id, ObjectId schema_id,
                                       const Table& table) {
  table.SetData(search::SearchTable::Create(db_id, schema_id, table.GetId(),
                                            /*is_new=*/false,
                                            table.SearchOptions()));
}

void OpenDatabase::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) {
      auto table_id = key.id;
      if (!IsDeleted(table_id, DeletedScope::Schema)) {
        auto table = catalog::DeserializeObject<Table>(
          bytes,
          {.id = table_id, .database_id = db_id, .schema_id = schema_id});
        if (!table) {
          THROW_SQL_ERROR(ERR_MSG("Failed to read table definition"));
        }
        if (table->GetEngine() == TableEngine::Transactional) {
          GetCatalogStore().ValidateStoreTable(
            MakeStoreTableDef(_database_name, _schema_name, *table));
        } else if (table->GetEngine() == TableEngine::Search) {
          RegisterSearchTable(db_id, schema_id, *table);
        }
        AddTable(db_id, schema_id, table_id, std::move(table));
        return true;
      }
      const auto engine = CheckTableForDrop(
        bytes, {.id = table_id, .database_id = db_id, .schema_id = schema_id});
      DropTask::Schedule(CreateTableDrop(GetCatalogStore(), db_id, schema_id,
                                         table_id, engine, true))
        .Detach();
      return true;
    });
}

void OpenDatabase::AddTable(ObjectId db_id, ObjectId schema_id,
                            ObjectId table_id, std::shared_ptr<Table> table) {
  _catalog.RegisterTable(db_id, schema_id, std::move(table));
  CollectDeletedDefinitions(table_id, DeletedScope::Relation);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Relation);
  };
  RegisterIndexes(db_id, schema_id, table_id);
}

void OpenDatabase::AddIndex(ObjectId database_id, ObjectId schema_id,
                            ObjectId table_id, ObjectId index_id,
                            ObjectType entry_type, std::string_view bytes) {
  ReadContext ctx{.id = index_id,
                  .database_id = database_id,
                  .schema_id = schema_id,
                  .relation_id = table_id};
  std::shared_ptr<Index> index;
  if (entry_type == ObjectType::SecondaryIndex) {
    index = catalog::DeserializeObject<SecondaryIndex>(bytes, ctx);
  } else {
    index = catalog::DeserializeObject<InvertedIndex>(bytes, ctx);
  }
  if (!index) {
    THROW_SQL_ERROR(ERR_MSG("Failed to read index definition"));
  }
  _catalog.RegisterIndex(database_id, schema_id, index);

#ifdef SDB_DEV
  // Check there are no tombstones in index scope
  size_t counter = 0;
  GetCatalogStore().VisitBoot(index_id, ObjectType::Tombstone,
                              [&](CatalogStore::Key, std::string_view) {
                                counter++;
                                return true;
                              });
  SDB_ASSERT(counter == 0);
#endif

  if (entry_type == ObjectType::InvertedIndex) {
    RegisterInvertedStorage(index);
  }
}

void OpenDatabase::AddSchema(ObjectId db_id, ObjectId schema_id,
                             std::string_view bytes) {
  auto schema =
    catalog::DeserializeObject<catalog::Schema>(bytes, {.database_id = db_id});
  if (!schema) {
    THROW_SQL_ERROR(ERR_MSG("Failed to read schema definition"));
  }
  _schema_name = schema->GetName();

  _catalog.RegisterSchema(db_id, std::move(schema));

  CollectDeletedDefinitions(schema_id, DeletedScope::Schema);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Schema);
  };

  RegisterTokenizers(db_id, schema_id);
  RegisterTypes(db_id, schema_id);
  RegisterSequences(db_id, schema_id, false);
  RegisterFunctions(db_id, schema_id);
  RegisterTables(db_id, schema_id);
  RegisterViews(db_id, schema_id);
  RegisterSequences(db_id, schema_id, true);
}

}  // namespace
namespace {

std::shared_ptr<Catalog> g_catalog;

}  // namespace

void InitCatalog() {
  g_catalog = std::make_shared<Catalog>();

  GetCatalogStore().LoadBootState();
  irs::Finally release_boot = [] noexcept {
    GetCatalogStore().ReleaseBootState();
  };

  OpenDatabase open_db{GetCatalog()};
  open_db.AddRoles();

  if (GetCatalog().GetCatalogSnapshot()->GetRoles().empty()) {
    std::string initial_verifier;
    if (const char* pw = std::getenv("POSTGRES_PASSWORD");
        pw != nullptr && *pw != '\0') {
      auto verifier = network::BuildScramVerifierString(pw);
      if (!verifier) {
        SDB_FATAL(GENERAL,
                  "could not derive a password verifier from "
                  "POSTGRES_PASSWORD");
      }
      initial_verifier = std::move(*verifier);
      SDB_INFO(GENERAL, "bootstrap: initial password set for role '",
               StaticStrings::kDefaultUser, "' from POSTGRES_PASSWORD");
    }
    auto root = std::make_shared<Role>(persistence::RoleData{
      .id = id::kRootUser,
      .name = std::string{StaticStrings::kDefaultUser},
      .options = static_cast<uint32_t>(RoleOption::All),
      .conn_limit = Role::kNoConnLimit,
      .valid_until = Role::kNoValidUntil,
      .password_verifier = std::move(initial_verifier),
    });
    GetCatalog().CreateRole(NoAccessCheck(), std::move(root));
  }

  try {
    open_db();
  } catch (const SqlException& e) {
    SDB_FATAL(GENERAL, "Failed to open database, ", e.message());
  }

  GetCatalog().FinalizeLoad();

  if (!catalog::GetDatabase(StaticStrings::kDefaultDatabase)) {
    SDB_FATAL(GENERAL, "No ", StaticStrings::kDefaultDatabase,
              " database found in database directory");
  }

  // Attach all existing databases into DuckDB
  {
    auto snapshot = GetCatalog().GetCatalogSnapshot();
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    for (auto& db : snapshot->GetDatabases()) {
      auto query = absl::StrCat("ATTACH '", db->GetId().id(), "' AS \"",
                                db->GetName(), "\" (TYPE serenedb)");
      auto result = conn->Query(query);
      if (result->HasError()) {
        SDB_FATAL(GENERAL, "Failed to attach database ", db->GetName(), ": ",
                  result->GetError());
      }
    }
  }

  // Re-attach persisted foreign servers (external DBs: clickhouse/postgres) so
  // they survive restart, the same way the databases above do. Unlike a local
  // database, a remote being unreachable must NOT abort startup -- warn and
  // continue; the server stays defined and a later access will surface it.
  {
    auto snapshot = GetCatalog().GetCatalogSnapshot();
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    for (auto& db : snapshot->GetDatabases()) {
      for (auto& server : snapshot->GetForeignServers(db->GetId())) {
        auto pub = snapshot->GetUserMapping(server->GetId(), "public");
        auto err = RunForeignServerAttach(*conn, *server, pub.get());
        if (err && !err->empty()) {
          SDB_WARN(GENERAL, "Failed to re-attach foreign server ",
                   server->GetName(), ": ", *err);
        }
      }
    }
  }
}

void ShutdownCatalog() { g_catalog.reset(); }

std::shared_ptr<Database> GetDatabase(std::string_view name) {
  return GetCatalog().GetCatalogSnapshot()->GetDatabase(name);
}

Catalog& GetCatalog() {
  SDB_ASSERT(g_catalog, "Catalog is not initialized");
  return *g_catalog;
}

Catalog* TryGetCatalog() { return g_catalog.get(); }

}  // namespace sdb::catalog
