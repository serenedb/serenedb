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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb/common/exception/parser_exception.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <iostream>
#include <iterator>
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
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/buffer.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/error_code.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/result.h"
#include "basics/result_or.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/column_expr.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/object_dependency.h"
#include "catalog/resolution_table.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_entry_cache.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {
namespace {

Result Apply(
  std::shared_ptr<const Snapshot>& snapshot, auto&& f,
  std::function<void(const std::shared_ptr<Snapshot>&)> rollback = {}) {
  std::shared_ptr<Snapshot> clone = snapshot->Clone();
  if (auto r = f(clone); !r.ok()) {
    if (rollback) {
      rollback(clone);
    }
    return r;
  }
  std::atomic_store(&snapshot,
                    std::shared_ptr<const Snapshot>(std::move(clone)));
  return {};
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
      if (!cref.column_names.empty()) {
        auto it = renames.find(cref.column_names.back());
        if (it != renames.end()) {
          cref.column_names.back() = it->second;
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

Snapshot::Snapshot() = default;
Snapshot::~Snapshot() = default;

std::shared_ptr<Snapshot> Snapshot::Clone() const {
  // TODO(gnusi): COW
  auto result = std::make_shared<Snapshot>();
  result->_resolution_table = _resolution_table;
  result->_objects = _objects;
  result->_deps = _deps;
  result->_in_load = _in_load;
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

std::shared_ptr<TableDrop> Snapshot::CreateTableDrop(
  PendingDrops& pending_drops, ObjectId db_id, ObjectId schema_id,
  const std::shared_ptr<Table>& table, bool is_root) {
  const auto table_id = table->GetId();
  auto table_deps = GetDependency<TableDependency>(table_id);
  auto indexes = table_deps->indexes | std::views::transform([&](ObjectId id) {
                   auto index = GetObject<Index>(id);
                   SDB_ASSERT(index);
                   return CreateIndexDrop(pending_drops, db_id, schema_id,
                                          table_id, index, false);
                 }) |
                 std::ranges::to<std::vector>();
  auto owned_sequences =
    table_deps->owned_sequences | std::ranges::to<std::vector>();

  std::string store_name;
  std::vector<std::string> fk_referenced;
  if (table->GetEngine() == TableEngine::Transactional &&
      !table->Tombstoned()) {
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
  auto task = std::make_shared<TableDrop>(
    table, db_id, std::move(indexes), std::move(owned_sequences), schema_id,
    std::move(store_name), std::move(fk_referenced), is_root);
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
Result Snapshot::RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                                bool replace) {
  if constexpr (std::is_same_v<T, Database>) {
    auto r = AddToResolution<ResolveType::Database>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<DatabaseDependency>(parent_id,
                                                   std::move(object));
  } else if constexpr (std::is_same_v<T, Schema>) {
    auto r = AddToResolution<ResolveType::Schema>(parent_id, object->GetId(),
                                                  object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<SchemaDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, PgSqlView>) {
    auto r = AddToResolution<ResolveType::Relation>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<ViewDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Sequence>) {
    // Sequences share the relation namespace (PG: pg_class.relkind='S').
    auto r = AddToResolution<ResolveType::Relation>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<SequenceDependency>(parent_id,
                                                   std::move(object));
  } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
    auto r = AddToResolution<ResolveType::Function>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<PgSqlFunctionDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, Tokenizer>) {
    auto r = AddToResolution<ResolveType::Tokenizer>(
      parent_id, object->GetId(), object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<TokenizerDependency>(parent_id, object);
  } else if constexpr (std::is_same_v<T, PgSqlType>) {
    auto r = AddToResolution<ResolveType::Type>(parent_id, object->GetId(),
                                                object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<PgSqlTypeDependency>(parent_id,
                                                    std::move(object));
  } else if constexpr (std::is_same_v<T, Table>) {
    auto r = AddToResolution<ResolveType::Relation>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<TableDependency>(parent_id, std::move(object));
  } else if constexpr (std::is_same_v<T, Index>) {
    auto r = AddToResolution<ResolveType::Relation>(
      object->GetParentId(), object->GetId(), object->GetName(), replace);
    if (!r.ok()) {
      return r;
    }
    return AddObjectDefinition<IndexDependency>(parent_id, std::move(object));
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
Result Snapshot::AddToResolution(ObjectId parent_id, ObjectId id,
                                 std::string_view name, bool replace) {
  return _resolution_table.AddObject<Type>(parent_id, name, id, replace);
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

template<typename DependencyType>
Result Snapshot::AddObjectDefinition(ObjectId parent_id,
                                     std::shared_ptr<Object> object) {
  if constexpr (!std::is_void_v<DependencyType>) {
    bool inserted = _deps.AddDependency<DependencyType>(object->GetId());
    SDB_ASSERT(inserted);
  }
  SDB_ASSERT(object->GetId().isSet());
  auto [_, inserted] = _objects.insert(object);
  SDB_ASSERT(inserted);
  AddDependencies(parent_id, *object);
  return {};
}

void Snapshot::AddDependencies(ObjectId parent_id, const Object& obj) {
  auto id = obj.GetId();
  switch (obj.GetType()) {
    case ObjectType::Database:
      break;
    case ObjectType::Schema:
      GetDependencyForWrite<DatabaseDependency>(parent_id)->schemas.insert(id);
      break;
    case ObjectType::Tokenizer:
      GetDependencyForWrite<SchemaDependency>(parent_id)->tokenizers.insert(id);
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

std::shared_ptr<PgSqlType> Snapshot::GetType(ObjectId db_id,
                                             std::string_view schema,
                                             std::string_view name) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .and_then([&](ObjectId schema_id) {
      return _resolution_table.ResolveObject<ResolveType::Type>(schema_id,
                                                                name);
    })
    .transform([&](ObjectId type_id) { return GetObject<PgSqlType>(type_id); })
    .value_or(nullptr);
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

std::shared_ptr<Object> Snapshot::GetRelation(ObjectId db_id,
                                              std::string_view schema,
                                              std::string_view relation) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .and_then([&](ObjectId schema_id) {
      return _resolution_table.ResolveObject<ResolveType::Relation>(schema_id,
                                                                    relation);
    })
    .transform([&](ObjectId relation_id) {
      auto it = _objects.find(relation_id);
      SDB_ASSERT(it != _objects.end());
      return basics::downCast<Object>(*it);
    })
    .value_or(nullptr);
}

std::shared_ptr<PgSqlFunction> Snapshot::GetFunction(
  ObjectId db_id, std::string_view schema, std::string_view function) const {
  return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
    .and_then([&](ObjectId schema_id) {
      return _resolution_table.ResolveObject<ResolveType::Function>(schema_id,
                                                                    function);
    })
    .transform([&](ObjectId function_id) {
      return GetObject<PgSqlFunction>(function_id);
    })
    .value_or(nullptr);
}

std::shared_ptr<Tokenizer> Snapshot::GetTokenizer(ObjectId db_id,
                                                  std::string_view schema,
                                                  std::string_view name) const {
  auto schema_id =
    _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
  if (!schema_id) {
    return nullptr;
  }
  auto id =
    _resolution_table.ResolveObject<ResolveType::Tokenizer>(*schema_id, name);
  if (!id) {
    return nullptr;
  }
  return GetObject<Tokenizer>(*id);
}

std::shared_ptr<Table> Snapshot::GetTable(ObjectId db_id,
                                          std::string_view schema,
                                          std::string_view table) const {
  auto rel = GetRelation(db_id, schema, table);
  if (!rel || rel->GetType() != ObjectType::Table) {
    return nullptr;
  }
  return basics::downCast<Table>(rel);
}

std::shared_ptr<Sequence> Snapshot::GetSequence(ObjectId db_id,
                                                ObjectId schema_id,
                                                std::string_view name) const {
  auto id =
    _resolution_table.ResolveObject<ResolveType::Relation>(schema_id, name);
  if (!id) {
    return nullptr;
  }
  auto obj = GetObject(*id);
  if (!obj || obj->GetType() != ObjectType::Sequence) {
    return nullptr;
  }
  return basics::downCast<Sequence>(std::move(obj));
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
Result Snapshot::ReplaceObject(ObjectId parent_id, std::string_view old_name,
                               std::shared_ptr<Object> new_object) {
  if (old_name != new_object->GetName()) {
    auto removed = _resolution_table.RemoveObject<Type>(parent_id, old_name);
    SDB_ASSERT(removed);
    auto r = _resolution_table.AddObject<Type>(parent_id, new_object->GetName(),
                                               new_object->GetId(), false);
    if (!r.ok()) {
      return r;
    }
  } else {
    // Name unchanged, but must refresh the string_view to point to new
    // object's _name
    auto r = _resolution_table.AddObject<Type>(parent_id, new_object->GetName(),
                                               new_object->GetId(), true);
    SDB_ASSERT(r.ok());
  }

  auto it = _objects.find(new_object->GetId());
  SDB_ASSERT(it != _objects.end());
  SDB_ASSERT((*it)->GetId() == new_object->GetId());
  auto old_object = *it;
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
  return {};
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
      if (idx->HasColumn(col_id)) {
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
      if (idx && idx->HasColumn(col_id)) {
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
    std::ignore = ReplaceObject<ResolveType::Relation>(
      rw.schema_id, (*it)->GetName(), std::move(rw.table));
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
  auto drop_childs = [&](const auto& deps) {
    for (auto child_id : deps) {
      RemoveObjectDefinition(id, child_id);
    }
  };
  // Drop from parent deps
  if (root) {
    switch (obj->GetType()) {
      case ObjectType::Database:
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
    } break;
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
    case ObjectType::PgSqlFunction:
    case ObjectType::PgSqlType:
    case ObjectType::Tokenizer:
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

Result Catalog::RegisterDatabase(std::shared_ptr<Database> database) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(database), id::kInstance, false);
  });
}

Result Catalog::RegisterSchema(ObjectId database_id,
                               std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(schema), database_id, false);
  });
}

Result Catalog::RegisterView(ObjectId schema_id,
                             std::shared_ptr<PgSqlView> view) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(view), schema_id, false);
  });
}

Result Catalog::RegisterSequence(ObjectId database_id, ObjectId schema_id,
                                 std::shared_ptr<Sequence> sequence) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(sequence), schema_id, false);
  });
}

Result Catalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                              std::shared_ptr<Table> table) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(table, schema_id, false);
  });
}

Result Catalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                                 std::shared_ptr<PgSqlFunction> function) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(function), schema_id, false);
  });
}

Result Catalog::RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                                  std::shared_ptr<Tokenizer> tokenizer) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(tokenizer), schema_id, false);
  });
}

Result Catalog::RegisterType(ObjectId database_id, ObjectId schema_id,
                             std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(type), schema_id, false);
  });
}

Result Catalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(database, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      {
        auto bytes = catalog::SerializeObject(*database, stream);
        auto r = _engine->CreateDefinition(id::kInstance, ObjectType::Database,
                                           database_id, bytes);

        if (!r.ok()) {
          return r;
        }
      }

      auto schema = std::make_shared<Schema>(
        database_id, SchemaOptions{
                       .name = std::string{StaticStrings::kPublic},
                     });
      r = clone->RegisterObject(schema, database_id, false);
      SDB_ASSERT(r.ok());
      auto bytes = catalog::SerializeObject(*schema, stream);
      return _engine->CreateDefinition(database_id, ObjectType::Schema,
                                       schema->GetId(), bytes);
    },
    [&](auto clone) {
      clone->UnregisterObject(database, id::kInstance, true);
    });
}

Result Catalog::CreateSchema(ObjectId database_id,
                             std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](auto& clone) {
      if (auto r = clone->RegisterObject(schema, database_id, false); !r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*schema, stream);
      return _engine->CreateDefinition(database_id, ObjectType::Schema,
                                       schema->GetId(), bytes);
    },
    [&](auto clone) { clone->UnregisterObject(schema, database_id, true); });
}

Result Catalog::RegisterIndex(ObjectId database_id, ObjectId schema_id,
                              std::shared_ptr<Index> index) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(index, index->GetRelationId(), false);
  });
}

Result Catalog::CreateIndexImpl(std::string_view relation_schema,
                                std::shared_ptr<Index> index,
                                CreateIndexOperationOptions operation_options) {
  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(index, index->GetRelationId(), false);
      if (!r.ok()) {
        return r;
      }

      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(index->GetRelationId(), index->GetId());
        if (!r.ok()) {
          return r;
        }
        index->SetTombstoned(true);
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      {  // Write index definition
        auto bytes = catalog::SerializeObject(*index, stream);
        r = _engine->CreateDefinition(index->GetRelationId(), index->GetType(),
                                      index->GetId(), bytes);
        if (!r.ok()) {
          return r;
        }
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
            r = _engine->Write(
              [&](auto& ctx) { ctx.CreateStoreIndex(std::move(*def)); });
            if (!r.ok()) {
              return r;
            }
          }
        }
      }
      return Result{};
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

ResultOr<ResolvedIndexRelation> ResolveIndexRelation(
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
    auto columns = std::views::iota(size_t{0}, view_info.names.size()) |
                   std::views::transform([&](size_t i) {
                     Column c{ObjectId{}, Column::Id{i}, view_info.names[i],
                              view_info.types[i]};
                     c.SetId(Column::Id{i});
                     return c;
                   }) |
                   std::ranges::to<std::vector>();
    return ResolvedIndexRelation{
      .relation_id = view.GetId(),
      .columns = std::move(columns),
    };
  }
  return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED,
                                 "Only table or view indexes are supported"};
}

}  // namespace

Result Catalog::CreateSecondaryIndex(
  ObjectId database_id, std::string_view schema, std::string_view relation,
  std::string name, std::vector<CreateIndexColumn>&& columns, bool unique,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return {ERROR_SERVER_ILLEGAL_NAME, "Cannot resolve schema \"", schema,
            "\""};
  }
  auto rel = _snapshot->GetRelation(database_id, schema, relation);
  if (!rel) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation,
            "\" does not exist"};
  }
  auto resolved = ResolveIndexRelation(rel);
  if (!resolved) {
    return std::move(resolved).error();
  }
  for (auto& c : columns) {
    if (c.IsIndexedExpression()) {
      // Expression keys carry their own bound payload (dependent columns +
      // serialized expr); they have no base column to resolve by name. The
      // store-side ART builds/maintains them natively from the rendered SQL.
      continue;
    }
    auto it = absl::c_find_if(resolved->columns, [&](const Column& col) {
      return col.GetName() == c.name;
    });
    if (it == resolved->columns.end()) {
      return Result{ERROR_BAD_PARAMETER, "column \"", c.name,
                    "\" does not exist"};
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateSecondaryIndex(
    database_id, *schema_id, ObjectId{0}, resolved->relation_id,
    std::move(name), std::move(columns), unique);
  if (!index) {
    return std::move(index).error();
  }
  return CreateIndexImpl(schema, *index, operation_options);
}

Result Catalog::CreateInvertedIndex(
  duckdb::ClientContext& context, ObjectId database_id, std::string_view schema,
  std::string_view relation, std::string name,
  std::vector<CreateIndexColumn>&& columns, InvertedIndexOptions options,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return {ERROR_SERVER_ILLEGAL_NAME, "Cannot resolve schema \"", schema,
            "\""};
  }
  auto rel = _snapshot->GetRelation(database_id, schema, relation);
  if (!rel) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation,
            "\" does not exist"};
  }
  auto resolved = ResolveIndexRelation(rel);
  if (!resolved) {
    return std::move(resolved).error();
  }
  for (auto& c : columns) {
    if (c.IsIndexedExpression()) {
      continue;
    }
    auto it = absl::c_find_if(resolved->columns, [&](const Column& col) {
      return col.GetName() == c.name;
    });
    if (it == resolved->columns.end()) {
      return Result{ERROR_BAD_PARAMETER, "column \"", c.name,
                    "\" does not exist"};
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateInvertedIndex(
    context, database_id, schema, *schema_id, ObjectId{0},
    resolved->relation_id, std::move(name), std::move(columns), _snapshot,
    std::move(options));
  if (!index) {
    return std::move(index).error();
  }
  return CreateIndexImpl(schema, *index, operation_options);
}

Result Catalog::CreateView(ObjectId database_id, std::string_view schema,
                           std::shared_ptr<PgSqlView> view, bool replace) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result(ERROR_SERVER_ILLEGAL_NAME);
  }
  view->SetParentId(*schema_id);

  ObjectId existed_id;
  if (replace) {
    existed_id =
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, view->GetName())
        .value_or(ObjectId{});
    if (existed_id.isSet() &&
        _snapshot->GetObject<Object>(existed_id)->GetType() !=
          ObjectType::PgSqlView) {
      return Result{ERROR_SERVER_ILLEGAL_NAME, "\"", view->GetName(),
                    "\" is not a view"};
    }
  }

  return Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) -> Result {
      if (existed_id.isSet()) {
        view->SetId(existed_id);
        if (auto r = clone->ReplaceObject<ResolveType::Relation>(
              *schema_id, view->GetName(), view);
            !r.ok()) {
          return r;
        }
      } else if (auto r = clone->RegisterObject(view, *schema_id,
                                                /*replace=*/false);
                 !r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*view, stream);
      if (existed_id.isSet()) {
        return _engine->Write([&](auto& ctx) {
          ctx.PutDefinition(*schema_id, ObjectType::PgSqlView, view->GetId(),
                            std::string_view{bytes});
        });
      }
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlView,
                                       view->GetId(), bytes);
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(view, *schema_id, true);
      }
    });
}

Result Catalog::CreateSequence(ObjectId database_id, std::string_view schema,
                               std::shared_ptr<Sequence> sequence,
                               bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  if (auto existed = _snapshot->GetObjectId<ResolveType::Relation>(
        *schema_id, sequence->GetName())) {
    if (if_not_exists) {
      return {};
    }
    return {ERROR_SERVER_DUPLICATE_NAME};
  }
  sequence->SetParentId(*schema_id);

  return Apply(
    _snapshot,
    [&](auto& clone) -> Result {
      auto r = clone->RegisterObject(sequence, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*sequence, stream);
      return _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Sequence, sequence->GetId(),
                          std::string_view{bytes});
        ctx.PutSequence(sequence->GetId(), sequence->Options().Seed());
      });
    },
    [&](auto clone) { clone->UnregisterObject(sequence, *schema_id, true); });
}

Result Catalog::CreateFunction(ObjectId database_id, std::string_view schema,
                               std::shared_ptr<PgSqlFunction> function,
                               bool replace) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  function->SetParentId(*schema_id);

  ObjectId existed_id;
  if (replace) {
    existed_id =
      _snapshot
        ->GetObjectId<ResolveType::Function>(*schema_id, function->GetName())
        .value_or(ObjectId{});
  }

  return Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) -> Result {
      if (existed_id.isSet()) {
        if (auto deps =
              clone->GetDependency<PgSqlFunctionDependency>(existed_id);
            deps && !deps->indexes.empty()) {
          return Result{ERROR_BAD_PARAMETER, "cannot replace function \"",
                        function->GetName(), "\" because indexes depend on it"};
        }
        function->SetId(existed_id);
        if (auto r = clone->ReplaceObject<ResolveType::Function>(
              *schema_id, function->GetName(), function);
            !r.ok()) {
          return r;
        }
      } else if (auto r = clone->RegisterObject(function, *schema_id,
                                                /*replace=*/false);
                 !r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*function, stream);
      if (existed_id.isSet()) {
        return _engine->Write([&](auto& ctx) {
          ctx.PutDefinition(*schema_id, ObjectType::PgSqlFunction,
                            function->GetId(), std::string_view{bytes});
        });
      }
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlFunction,
                                       function->GetId(), bytes);
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(function, *schema_id, true);
      }
    });
}

Result Catalog::CreateTable(ObjectId database_id, std::string_view schema,
                            CreateTableOptions options,
                            CreateTableOperationOptions operation_options) {
  // Uniqueness keys are enforced by the store table's DuckDB ART, which cannot
  // index nested types. Reject a nested-type key column up front with a clear
  // error instead of silently creating the table with the constraint dropped
  // (the store-side names_for clears it otherwise). Scalar key types are
  // handled natively by the ART.
  auto reject_nested_key = [&](std::span<const Column::Id> ids,
                               std::string_view what) -> Result {
    for (auto col_id : ids) {
      auto col = absl::c_find_if(
        options.columns, [&](const auto& c) { return c.GetId() == col_id; });
      SDB_ASSERT(col != options.columns.end());
      if (col->type.IsNested()) {
        return Result{ERROR_BAD_PARAMETER,
                      what,
                      " column \"",
                      col->GetName(),
                      "\" has unsupported nested type ",
                      col->type.ToString()};
      }
    }
    return {};
  };
  if (auto r = reject_nested_key(options.pk_columns, "primary key"); !r.ok()) {
    return r;
  }
  for (const auto& unique : options.unique_constraints) {
    if (auto r = reject_nested_key(unique, "unique constraint"); !r.ok()) {
      return r;
    }
  }
  auto sequence_specs = std::move(options.sequences);

  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
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
    auto pk_seq =
      std::make_shared<Sequence>(*schema_id, ObjectId{}, std::move(opts));
    generated_pk_seq_id = pk_seq->GetId();
    sequences.push_back(std::move(pk_seq));
  }

  auto table = std::make_shared<Table>(
    *schema_id, table_id, options.name, std::move(options.columns),
    std::move(options.pk_columns), std::move(options.check_constraints),
    generated_pk_seq_id, options.engine, std::move(options.unique_constraints),
    std::move(options.foreign_keys));
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
    table->SetData(search::SearchTable::Create(database_id, table->GetId(),
                                               /*is_new=*/true));
  }

  return Apply(
    _snapshot,
    [&](auto& clone) -> Result {
      auto r = clone->RegisterObject(table, *schema_id, false);
      if (!r.ok()) {
        return r;
      }

      for (const auto& seq : sequences) {
        r = clone->RegisterObject(seq, *schema_id, false);
        if (!r.ok()) {
          return r;
        }
      }
      if (with_tombstone) {
        r = _engine->WriteTombstone(*schema_id, table->GetId());
        if (!r.ok()) {
          return r;
        }
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      // PutDefinition copies into the catalog store batch, so one reused stream
      // is enough: each view is consumed before the next serialize overwrites
      // it.
      duckdb::MemoryStream stream;
      return _engine->Write([&](auto& ctx) {
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
}

Result Catalog::CreateTokenizer(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<Tokenizer> dict) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  dict->SetParentId(*schema_id);

  return Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) {
      auto r = clone->RegisterObject(dict, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*dict, stream);
      return _engine->CreateDefinition(*schema_id, ObjectType::Tokenizer,
                                       dict->GetId(), bytes);
    },
    [&](auto& clone) {
      return clone->UnregisterObject(dict, *schema_id, true);
    });
}

Result Catalog::CreateType(ObjectId database_id, std::string_view schema,
                           std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  type->SetParentId(*schema_id);

  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(type, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*type, stream);
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlType,
                                       type->GetId(), bytes);
    },
    [&](auto clone) { clone->UnregisterObject(type, *schema_id, true); });
}

template<typename T>
Result Catalog::RenameObjectImpl(ObjectId schema_id,
                                 std::string_view database_name,
                                 std::string_view schema_name,
                                 std::string_view name,
                                 std::string_view new_name,
                                 std::shared_ptr<T> object) {
  constexpr auto kResolveType = std::is_same_v<T, PgSqlFunction>
                                  ? ResolveType::Function
                                  : ResolveType::Relation;

  if (object->GetName() == new_name) {
    return Result{ERROR_SERVER_DUPLICATE_NAME};
  }

  auto cloned = object->Clone();
  if (!cloned) {
    return Result{ERROR_INTERNAL, "Failed to clone object"};
  }
  auto new_object = basics::downCast<T>(std::move(cloned));
  SDB_ASSERT(new_object);
  new_object->SetName(new_name);

  return Apply(
    _snapshot,
    [&](std::shared_ptr<Snapshot>& clone) -> Result {
      auto r = clone->ReplaceObject<kResolveType>(schema_id, name, new_object);
      if (!r.ok()) {
        return r;
      }

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
          return _engine->Write([&](auto& ctx) {
            ctx.PutDefinition(parent_id, new_object->GetType(),
                              new_object->GetId(), bytes);
            ctx.RenameStoreTable(
              StoreTableName(database_name, schema_name, name),
              StoreTableName(database_name, schema_name, new_name));
          });
        }
      }
      return _engine->CreateDefinition(parent_id, new_object->GetType(),
                                       new_object->GetId(), bytes);
    },
    [&](const std::shared_ptr<Snapshot>& clone) {
      auto current = clone->GetObject<T>(new_object->GetId());
      if (current->GetName() == new_object->GetName()) {
        auto r =
          clone->ReplaceObject<kResolveType>(schema_id, new_name, object);
        SDB_ASSERT(r.ok());
      }
    });
}

template<typename T>
Result Catalog::RenameObjectImpl(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  static constexpr auto kResolveType = std::is_same_v<T, PgSqlFunction>
                                         ? ResolveType::Function
                                         : ResolveType::Relation;
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id = _snapshot->GetObjectId<kResolveType>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto type = object->GetType();
  auto typed = std::dynamic_pointer_cast<T>(std::move(object));
  if (!typed) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  pg::ToPgObjectTypeName(type)};
  }

  auto database = _snapshot->GetObject<Database>(database_id);
  SDB_ASSERT(database);
  return RenameObjectImpl<T>(*schema_id, database->GetName(), schema, name,
                             new_name, std::move(typed));
}

Result Catalog::RenameView(ObjectId database_id, std::string_view schema,
                           std::string_view name, std::string_view new_name) {
  return RenameObjectImpl<PgSqlView>(database_id, schema, name, new_name);
}

Result Catalog::RenameTable(ObjectId database_id, std::string_view schema,
                            std::string_view name, std::string_view new_name) {
  return RenameObjectImpl<Table>(database_id, schema, name, new_name);
}

Result Catalog::RenameIndex(ObjectId database_id, std::string_view schema,
                            std::string_view name, std::string_view new_name) {
  return RenameObjectImpl<Index>(database_id, schema, name, new_name);
}

Result Catalog::RenameRelation(ObjectId database_id, std::string_view schema,
                               std::string_view name,
                               std::string_view new_name) {
  absl::MutexLock lock{&_mutex};

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto database = _snapshot->GetObject<Database>(database_id);
  SDB_ASSERT(database);
  switch (object->GetType()) {
    case ObjectType::Table:
      return RenameObjectImpl<Table>(*schema_id, database->GetName(), schema,
                                     name, new_name,
                                     std::static_pointer_cast<Table>(object));
    case ObjectType::PgSqlView:
      return RenameObjectImpl<PgSqlView>(
        *schema_id, database->GetName(), schema, name, new_name,
        std::static_pointer_cast<PgSqlView>(object));
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
      return RenameObjectImpl<Index>(*schema_id, database->GetName(), schema,
                                     name, new_name,
                                     std::static_pointer_cast<Index>(object));
    default:
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
  }
}

Result Catalog::RenameFunction(ObjectId database_id, std::string_view schema,
                               std::string_view name,
                               std::string_view new_name) {
  return RenameObjectImpl<PgSqlFunction>(database_id, schema, name, new_name);
}

Result Catalog::ChangeView(ObjectId database_id, std::string_view schema,
                           std::string_view name,
                           ChangeCallback<PgSqlView> new_view) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto view = basics::downCast<PgSqlView>(_snapshot->GetObject(*object_id));
  if (!view) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  std::shared_ptr<PgSqlView> updated;
  auto r = new_view(*view, updated);
  if (!r.ok()) {
    return r;
  }
  if (!updated) {
    return {};
  }
  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) -> Result {
    auto r =
      clone->ReplaceObject<ResolveType::Relation>(*schema_id, name, updated);
    if (!r.ok()) {
      return r;
    }

    duckdb::MemoryStream stream;
    auto bytes = catalog::SerializeObject(*updated, stream);
    return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlView,
                                     updated->GetId(), bytes);
  });
}

Result Catalog::ChangeTable(ObjectId database_id, std::string_view schema,
                            std::string_view name,
                            ChangeCallback<Table> new_table) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto obj = _snapshot->GetObject(*object_id);
  if (!obj) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }
  if (obj->GetType() != ObjectType::Table) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  pg::ToPgObjectTypeName(obj->GetType())};
  }

  auto table = basics::downCast<Table>(std::move(obj));
  std::shared_ptr<Table> updated;
  auto r = new_table(*table, updated);
  if (!r.ok()) {
    return r;
  }
  if (!updated) {
    return {};
  }

  std::string store_name;
  if (updated->GetEngine() == TableEngine::Transactional &&
      !updated->Tombstoned()) {
    auto database = _snapshot->GetObject<Database>(database_id);
    SDB_ASSERT(database);
    store_name = StoreTableName(database->GetName(), schema, name);
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) -> Result {
    auto r =
      clone->ReplaceObject<ResolveType::Relation>(*schema_id, name, updated);
    if (!r.ok()) {
      return r;
    }

    return basics::SafeCall([&] {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*updated, stream);
      return _engine->Write([&](auto& ctx) {
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
                    if (auto r = clone->ReplaceObject<ResolveType::Relation>(
                          *schema_id, new_idx->GetName(), new_idx);
                        !r.ok()) {
                      return;
                    }
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
          bool existed =
            absl::c_any_of(table->UniqueConstraints(),
                           [&](const auto& oc) { return oc == uc; });
          if (existed) {
            continue;
          }
          std::vector<std::string> names;
          names.reserve(uc.size());
          bool ok = true;
          for (auto id : uc) {
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
    });
  });
}

Result Catalog::DropDatabase(std::string_view name,
                             duckdb::shared_ptr<void> keep_alive) {
  absl::MutexLock lock{&_mutex};
  auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, name);
  if (!database_id) {
    return Result{ERROR_SERVER_DATABASE_NOT_FOUND, "database \"", name,
                  "\" does not exist"};
  }

  auto plan = _snapshot->ComputeDropPlan(*database_id);

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto database = clone->GetObject<Database>(*database_id);
    SDB_ASSERT(database);
    auto task = clone->CreateDatabaseDrop(_pending_drops, database,
                                          std::move(keep_alive));
    auto wr = _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(id::kInstance, *database_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(database), id::kInstance);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    // Check that SereneDB won't open this database after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result Catalog::DropSchema(std::string_view database, std::string_view name,
                           bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, name);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  if (!cascade && !_snapshot->CheckSchemaEmptyDependency(*schema_id)) {
    return Result{ERROR_BAD_PARAMETER};
  }

  auto plan = _snapshot->ComputeDropPlan(*schema_id);

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto schema = clone->GetObject<Schema>(*schema_id);
    SDB_ASSERT(schema);
    auto task =
      clone->CreateSchemaDrop(_pending_drops, *database_id, schema, true);
    auto wr = _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(*database_id, *schema_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(schema), *database_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    // Check that SereneDB won't open this schema after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result Catalog::DropTable(std::string_view database, std::string_view schema,
                          std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!table_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*table_id);
  if (!cascade && plan.IsCascade()) {
    return Result{ERROR_BAD_PARAMETER,
                  plan.FormatDependentsDetail(*_snapshot, "table", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*table_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::Table) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto table = basics::downCast<Table>(std::move(object));
    auto task = clone->CreateTableDrop(_pending_drops, *database_id, *schema_id,
                                       table, true);
    auto wr = _engine->Write([&](auto& ctx) {
      ctx.WriteTombstone(*schema_id, *table_id);
      clone->CommitDropPlan(ctx, plan);
      task->EmitStoreFkCleanups(ctx);
      task->EmitStoreDrops(ctx);
    });
    if (!wr.ok()) {
      return wr;
    }

    clone->UnregisterObject(std::move(table), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result Catalog::DropTableColumn(ObjectId database_id, std::string_view schema,
                                std::string_view table, std::string_view column,
                                bool if_exists) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, table);
  if (!table_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }
  auto object = _snapshot->GetObject(*table_id);
  if (!object || object->GetType() != ObjectType::Table) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  object ? pg::ToPgObjectTypeName(object->GetType()) : ""};
  }
  auto table_ptr = basics::downCast<Table>(std::move(object));
  auto col_it = absl::c_find_if(table_ptr->Columns(), [&](const Column& c) {
    return c.GetName() == column;
  });
  if (col_it == table_ptr->Columns().end()) {
    if (if_exists) {
      return {};
    }
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto col_id = col_it->GetId();

  auto plan = _snapshot->ComputeColumnDropPlan(*table_id, col_id);

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    auto wr =
      _engine->Write([&](auto& ctx) { clone->CommitDropPlan(ctx, plan); });
    if (!wr.ok()) {
      return wr;
    }
    clone->ApplyDropPlan(_pending_drops, database_id, plan);
    return Result{};
  });
}

Result Catalog::ChangeColumnType(ObjectId database_id, std::string_view schema,
                                 std::string_view table,
                                 std::string_view column,
                                 duckdb::LogicalType new_type,
                                 std::string using_sql) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, table);
  if (!table_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }
  auto object = _snapshot->GetObject(*table_id);
  if (!object || object->GetType() != ObjectType::Table) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  object ? pg::ToPgObjectTypeName(object->GetType()) : ""};
  }
  auto table_ptr = basics::downCast<Table>(std::move(object));
  auto col_it = absl::c_find_if(table_ptr->Columns(), [&](const Column& c) {
    return c.GetName() == column;
  });
  if (col_it == table_ptr->Columns().end()) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto col_id = col_it->GetId();

  // The index stores values of the column's old type; a type change would
  // leave them inconsistent. Reject and let the user drop the index first.
  if (auto td = _snapshot->GetDependency<TableDependency>(*table_id)) {
    for (auto idx_id : td->indexes) {
      auto idx = _snapshot->GetObject<Index>(idx_id);
      if (idx && idx->HasColumn(col_id)) {
        return Result{ERROR_BAD_PARAMETER,
                      "cannot alter type of column \"",
                      column,
                      "\" because index \"",
                      idx->GetName(),
                      "\" depends on it; drop the index first"};
      }
    }
  }

  std::shared_ptr<Table> updated;
  if (auto r = table_ptr->ChangeColumnType(updated, column, new_type);
      !r.ok()) {
    return r;
  }

  std::string store_name;
  if (updated->GetEngine() == TableEngine::Transactional &&
      !updated->Tombstoned()) {
    auto database = _snapshot->GetObject<Database>(database_id);
    SDB_ASSERT(database);
    store_name = StoreTableName(database->GetName(), schema, table);
  }
  std::string type_sql = new_type.ToString();

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) -> Result {
    auto rr =
      clone->ReplaceObject<ResolveType::Relation>(*schema_id, table, updated);
    if (!rr.ok()) {
      return rr;
    }
    return basics::SafeCall([&] {
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*updated, stream);
      return _engine->Write([&](auto& ctx) {
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
    });
  });
}

Result Catalog::RemoveTombstone(ObjectId database_id, std::string_view schema,
                                std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  ObjectId tombstone_parent;
  if (IsIndex(object->GetType())) {
    auto& index = basics::downCast<Index>(*object);
    tombstone_parent = index.GetRelationId();
  } else {
    tombstone_parent = *schema_id;
  }

  auto r = _engine->Write([&](auto& ctx) {
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

  // Unlike most catalog operations that clone the snapshot, here we modify the
  // object in-place because the tombstone flag is simple in-memory state.
  auto& schema_obj = basics::downCast<Object>(*object);
  schema_obj.SetTombstoned(false);

  return r;
}

Result Catalog::DropIndex(std::string_view database, std::string_view schema,
                          std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto index_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!index_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return DropIndexByIdLocked(*database_id, *index_id, cascade);
}

Result Catalog::DropIndexById(ObjectId database_id, ObjectId index_id,
                              bool cascade) {
  absl::MutexLock lock{&_mutex};
  return DropIndexByIdLocked(database_id, index_id, cascade);
}

Result Catalog::DropIndexByIdLocked(ObjectId database_id, ObjectId index_id,
                                    bool cascade) {
  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto obj = clone->GetObject(index_id);
    if (!obj) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    if (!IsIndex(obj->GetType())) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(obj->GetType())};
    }
    auto index = basics::downCast<Index>(std::move(obj));
    const auto schema_id = index->GetParentId();
    // Store-side index drop is synchronous: UNIQUE enforcement
    // must stop when DROP INDEX commits, not when the async sweep
    // runs.
    if (auto r = _engine->Write([&](auto& ctx) {
          ctx.WriteTombstone(index->GetRelationId(), index_id);
          ctx.DropStoreIndex(index_id);
        });
        !r.ok()) {
      return r;
    }

    // Check that SereneDB won't open this index after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }

    auto task = clone->CreateIndexDrop(_pending_drops, database_id, schema_id,
                                       index->GetRelationId(), index, true);
    clone->UnregisterObject(index, schema_id);
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result Catalog::DropView(std::string_view database, std::string_view schema,
                         std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto view_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!view_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*view_id);
  if (!cascade && plan.IsCascade()) {
    return Result{ERROR_BAD_PARAMETER,
                  plan.FormatDependentsDetail(*_snapshot, "view", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*view_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlView) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto view = basics::downCast<PgSqlView>(std::move(object));

    auto wr = _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlView, *view_id);
      clone->CommitDropPlan(ctx, plan);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(view), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    return Result{};
  });
}

Result Catalog::DropSequence(std::string_view database, std::string_view schema,
                             std::string_view name, bool if_exists,
                             bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return if_exists ? Result{} : Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return if_exists ? Result{} : Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto seq_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!seq_id) {
    return if_exists ? Result{} : Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*seq_id);
  if (!cascade && plan.IsCascade()) {
    return Result{ERROR_BAD_PARAMETER,
                  plan.FormatDependentsDetail(*_snapshot, "sequence", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*seq_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::Sequence) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto seq = basics::downCast<Sequence>(std::move(object));

    auto wr = _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::Sequence, *seq_id);
      ctx.DropSequence(*seq_id);  // counter row in same atomic batch
      clone->CommitDropPlan(ctx, plan);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(seq), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    return Result{};
  });
}

Result Catalog::DropType(std::string_view database, std::string_view schema,
                         std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto type_id =
    _snapshot->GetObjectId<ResolveType::Type>(*schema_id, name);
  if (!type_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*type_id);
  if (!cascade && plan.IsCascade()) {
    return Result{ERROR_BAD_PARAMETER,
                  plan.FormatDependentsDetail(*_snapshot, "type", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*type_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlType) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto type = basics::downCast<PgSqlType>(std::move(object));

    auto wr = _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlType, *type_id);
      clone->CommitDropPlan(ctx, plan);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(type), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    return Result{};
  });
}

Result Catalog::DropFunction(std::string_view database, std::string_view schema,
                             std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto function_id =
    _snapshot->GetObjectId<ResolveType::Function>(*schema_id, name);
  if (!function_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*function_id);
  if (!cascade && plan.IsCascade()) {
    return Result{ERROR_BAD_PARAMETER,
                  plan.FormatDependentsDetail(*_snapshot, "function", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto function = clone->GetObject<PgSqlFunction>(*function_id);
    SDB_ASSERT(function);

    auto wr = _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::PgSqlFunction, *function_id);
      clone->CommitDropPlan(ctx, plan);
    });
    if (!wr.ok()) {
      return wr;
    }
    clone->UnregisterObject(std::move(function), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    return Result{};
  });
}

Result Catalog::DropTokenizer(std::string_view database,
                              std::string_view schema, std::string_view name,
                              bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto tokenizer_id =
    _snapshot->GetObjectId<ResolveType::Tokenizer>(*schema_id, name);
  if (!tokenizer_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto plan = _snapshot->ComputeDropPlan(*tokenizer_id);
  if (!cascade && plan.IsCascade()) {
    return Result{
      ERROR_BAD_PARAMETER,
      plan.FormatDependentsDetail(*_snapshot, "text search dictionary", name)};
  }

  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) {
    SDB_ASSERT(clone);
    auto tokenizer = clone->GetObject<Tokenizer>(*tokenizer_id);
    SDB_ASSERT(tokenizer);

    auto wr = _engine->Write([&](auto& ctx) {
      ctx.DropDefinition(*schema_id, ObjectType::Tokenizer, *tokenizer_id);
      clone->CommitDropPlan(ctx, plan);
    });
    if (!wr.ok()) {
      return wr;
    }

    clone->UnregisterObject(std::move(tokenizer), *schema_id);
    clone->ApplyDropPlan(_pending_drops, *database_id, plan);
    return Result{};
  });
}

Result Catalog::FinalizeLoad() {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<Snapshot>& clone) -> Result {
    for (const auto& obj : clone->Objects()) {
      clone->AddDependencies(obj->GetParentId(), *obj);
    }
    clone->EndLoad();
    return {};
  });
}

std::shared_ptr<const Snapshot> Catalog::GetCatalogSnapshot() const noexcept {
  return std::atomic_load(&_snapshot);
}

namespace {

// In case of recovery the ColumnExpr shouldn't be parsed
Result CheckTableForDrop(std::string_view bytes, ReadContext ctx) {
  auto table = catalog::DeserializeObject<Table>(bytes, ctx);
  if (!table) {
    return {ERROR_SERVER_ILLEGAL_STATE,
            "failed to deserialize table definition during drop recovery"};
  }
  return {};
}

std::shared_ptr<IndexDrop> CreateIndexDrop(ObjectId db_id, ObjectId schema_id,
                                           ObjectId table_id, ObjectId index_id,
                                           ObjectType index_type,
                                           bool is_root = false) {
  return std::make_shared<IndexDrop>(index_id, index_type, db_id, schema_id,
                                     table_id, is_root);
}

ResultOr<std::shared_ptr<TableDrop>> CreateTableDrop(CatalogStore& store,
                                                     ObjectId db_id,
                                                     ObjectId schema_id,
                                                     ObjectId table_id,
                                                     bool is_root = false) {
  std::vector<std::shared_ptr<IndexDrop>> indexes;
  auto collect_indexes = [&](ObjectType type) {
    return store.VisitBoot(
      table_id, type, [&](CatalogStore::Key key, std::string_view) {
        indexes.push_back(
          CreateIndexDrop(db_id, schema_id, table_id, key.id, type));
        return Result{};
      });
  };
  auto r = collect_indexes(ObjectType::SecondaryIndex);
  if (r.ok()) {
    r = collect_indexes(ObjectType::InvertedIndex);
  }
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }

  std::vector<ObjectId> owned_sequences;
  r = store.VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto seq = catalog::DeserializeObject<Sequence>(
        bytes, {.id = key.id, .database_id = db_id, .schema_id = schema_id});
      if (seq && seq->GetOwnerTableId() == table_id) {
        owned_sequences.push_back(key.id);
      }
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  return std::make_shared<TableDrop>(table_id, std::move(indexes),
                                     std::move(owned_sequences), schema_id,
                                     is_root);
}

ResultOr<std::shared_ptr<SchemaDrop>> CreateSchemaDrop(CatalogStore& store,
                                                       ObjectId db_id,
                                                       ObjectId schema_id,
                                                       bool is_root = false) {
  std::vector<std::shared_ptr<TableDrop>> tables;
  auto r = store.VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      if (auto check = CheckTableForDrop(
            bytes,
            {.id = key.id, .database_id = db_id, .schema_id = schema_id});
          !check.ok()) {
        return check;
      }
      auto table_drop = CreateTableDrop(store, db_id, schema_id, key.id);
      if (!table_drop) {
        return std::move(table_drop.error());
      }
      tables.push_back(std::move(*table_drop));
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }

  return std::make_shared<SchemaDrop>(schema_id, std::move(tables), db_id,
                                      is_root);
}

ResultOr<std::shared_ptr<DatabaseDrop>> CreateDatabaseDrop(CatalogStore& store,
                                                           ObjectId db_id) {
  std::vector<std::shared_ptr<SchemaDrop>> schemas;
  auto r = store.VisitBoot(
    db_id, ObjectType::Schema, [&](CatalogStore::Key key, std::string_view) {
      auto schema_drop = CreateSchemaDrop(store, db_id, key.id);
      if (!schema_drop) {
        return std::move(schema_drop.error());
      }
      schemas.push_back(std::move(*schema_drop));
      return Result{};
    });
  if (!r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
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

  Result operator()() {
    CollectDeletedDefinitions(id::kInstance, DeletedScope::Root);
    auto r = RegisterDatabases();
    ClearDeletedDefinitions(DeletedScope::Root);
    return r;
  }

 private:
  void Resolve();

  Result RegisterDatabases();
  Result RegisterSchemas(ObjectId database_id);
  Result RegisterFunctions(ObjectId database_id, ObjectId schema_id);
  Result RegisterTokenizers(ObjectId database_id, ObjectId schema_id);
  Result RegisterViews(ObjectId database_id, ObjectId schema_id);
  // Sequences load in two passes: standalone before tables/views (so refs
  // to them resolve at the referrer's own registration), owned after
  // tables.
  Result RegisterSequences(ObjectId database_id, ObjectId schema_id,
                           bool owned);
  Result RegisterTypes(ObjectId database_id, ObjectId schema_id);
  Result RegisterTables(ObjectId database_id, ObjectId schema_id);
  Result RegisterInvertedStorage(const std::shared_ptr<Index>& index);
  Result RegisterSearchTable(ObjectId db_id, const Table& table);
  Result RegisterIndexes(ObjectId database_id, ObjectId schema_id,
                         ObjectId table_id);

  Result AddDatabase(ObjectId database_id, std::string_view bytes);
  Result AddSchema(ObjectId database_id, ObjectId schema_id,
                   std::string_view bytes);
  Result AddTable(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
                  std::shared_ptr<Table> table);
  Result AddIndex(ObjectId database_id, ObjectId schema_id, ObjectId table_id,
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
    auto r = store.VisitBoot(id, ObjectType::Tombstone,
                             [&](CatalogStore::Key key, std::string_view) {
                               deleted.insert(key.id);
                               return Result{};
                             });
    SDB_ASSERT(r.ok());
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

Result OpenDatabase::AddDatabase(ObjectId database_id, std::string_view bytes) {
  auto db =
    catalog::DeserializeObject<catalog::Database>(bytes, {.id = database_id});
  if (!db) {
    return Result{ERROR_INTERNAL, "Failed to read database definition"};
  }
  _database_name = db->GetName();
  if (auto r = _catalog.RegisterDatabase(db); !r.ok()) {
    return r;
  }
  CollectDeletedDefinitions(database_id, DeletedScope::Database);
  auto r = RegisterSchemas(database_id);
  ClearDeletedDefinitions(DeletedScope::Database);
  return r;
}

Result OpenDatabase::RegisterDatabases() {
  return GetCatalogStore().VisitBoot(
    id::kInstance, ObjectType::Database,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      if (!IsDeleted(key.id, DeletedScope::Root)) {
        return AddDatabase(key.id, bytes);
      }
      auto drop = CreateDatabaseDrop(GetCatalogStore(), key.id);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterSchemas(ObjectId database_id) {
  return GetCatalogStore().VisitBoot(
    database_id, ObjectType::Schema,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto schema_id = key.id;
      if (!IsDeleted(key.id, DeletedScope::Database)) {
        return AddSchema(database_id, schema_id, bytes);
      }

      auto drop =
        CreateSchemaDrop(GetCatalogStore(), database_id, key.id, true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::RegisterFunctions(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlFunction,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto function = catalog::DeserializeObject<catalog::PgSqlFunction>(
        bytes, {
                 .id = key.id,
                 .database_id = db_id,
                 .schema_id = schema_id,
               });
      if (!function) {
        return Result{ERROR_INTERNAL, "Failed to read function definition"};
      }
      return _catalog.RegisterFunction(db_id, schema_id, std::move(function));
    });
}

Result OpenDatabase::RegisterTokenizers(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Tokenizer,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto tokenizer =
        catalog::DeserializeObject<Tokenizer>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!tokenizer) {
        return Result{ERROR_INTERNAL, "Failed to read tokenizer definition"};
      }
      return _catalog.RegisterTokenizer(db_id, schema_id, std::move(tokenizer));
    });
}

Result OpenDatabase::RegisterViews(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlView,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto view_id = key.id;
      auto view = catalog::DeserializeObject<PgSqlView>(
        bytes, {.id = view_id, .database_id = db_id, .schema_id = schema_id});
      if (!view) {
        return Result{ERROR_INTERNAL, "Failed to read view definition"};
      }
      if (auto r = _catalog.RegisterView(schema_id, std::move(view)); !r.ok()) {
        return r;
      }
      CollectDeletedDefinitions(view_id, DeletedScope::Relation);
      irs::Finally cleanup = [&] noexcept {
        ClearDeletedDefinitions(DeletedScope::Relation);
      };
      return RegisterIndexes(db_id, schema_id, view_id);
    });
}

Result OpenDatabase::RegisterSequences(ObjectId db_id, ObjectId schema_id,
                                       bool owned) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto seq =
        catalog::DeserializeObject<Sequence>(bytes, {
                                                      .id = key.id,
                                                      .database_id = db_id,
                                                      .schema_id = schema_id,
                                                    });
      if (!seq) {
        return Result{ERROR_INTERNAL, "Failed to read sequence definition"};
      }
      if (seq->GetOwnerTableId().isSet() != owned) {
        return Result{};
      }
      // Owner table is tombstoned -> skip; its DropTask will clean the seq.
      if (auto owner = seq->GetOwnerTableId();
          owner.isSet() && IsDeleted(owner, DeletedScope::Schema)) {
        return {};
      }
      return _catalog.RegisterSequence(db_id, schema_id, std::move(seq));
    });
}

Result OpenDatabase::RegisterTypes(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::PgSqlType,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto type =
        catalog::DeserializeObject<PgSqlType>(bytes, {
                                                       .id = key.id,
                                                       .database_id = db_id,
                                                       .schema_id = schema_id,
                                                     });
      if (!type) {
        return Result{ERROR_INTERNAL, "Failed to read type definition"};
      }
      return _catalog.RegisterType(db_id, schema_id, std::move(type));
    });
}

Result OpenDatabase::RegisterIndexes(ObjectId db_id, ObjectId schema_id,
                                     ObjectId table_id) {
  auto visit = [&](ObjectType type) {
    return GetCatalogStore().VisitBoot(
      table_id, type,
      [&](CatalogStore::Key key, std::string_view bytes) -> Result {
        auto index_id = key.id;
        if (!IsDeleted(index_id, DeletedScope::Relation)) {
          return AddIndex(db_id, schema_id, table_id, index_id, type, bytes);
        }

        auto drop =
          CreateIndexDrop(db_id, schema_id, table_id, key.id, type, true);
        DropTask::Schedule(std::move(drop)).Detach();
        return {};
      });
  };
  if (auto r = visit(ObjectType::SecondaryIndex); !r.ok()) {
    return r;
  }
  if (auto r = visit(ObjectType::InvertedIndex); !r.ok()) {
    return r;
  }
  return {};
}

Result OpenDatabase::RegisterInvertedStorage(
  const std::shared_ptr<Index>& index) {
  // The inverted index's durable state is its iresearch segment directory; the
  // storage is (re)opened from it here and bound onto the index. Must run
  // before InitInvertedIndexes/BindStoreTableIndexes so WAL replay resolves it
  // via index->GetData().
  return basics::SafeCall([&] {
    const auto& inverted = basics::downCast<const InvertedIndex>(*index);
    inverted.SetData(search::InvertedIndexStorage::Create(
      inverted.GetId(), inverted, /*is_new=*/false));
    return Result{};
  });
}

Result OpenDatabase::RegisterSearchTable(ObjectId db_id, const Table& table) {
  return basics::SafeCall([&] {
    table.SetData(search::SearchTable::Create(db_id, table.GetId(),
                                              /*is_new=*/false));
    return Result{};
  });
}

Result OpenDatabase::RegisterTables(ObjectId db_id, ObjectId schema_id) {
  return GetCatalogStore().VisitBoot(
    schema_id, ObjectType::Table,
    [&](CatalogStore::Key key, std::string_view bytes) -> Result {
      auto table_id = key.id;
      if (!IsDeleted(table_id, DeletedScope::Schema)) {
        auto table = catalog::DeserializeObject<Table>(
          bytes,
          {.id = table_id, .database_id = db_id, .schema_id = schema_id});
        if (!table) {
          return Result{ERROR_INTERNAL, "Failed to read table definition"};
        }
        if (table->GetEngine() == TableEngine::Transactional) {
          GetCatalogStore().ValidateStoreTable(
            MakeStoreTableDef(_database_name, _schema_name, *table));
        } else if (table->GetEngine() == TableEngine::Search) {
          if (auto r = RegisterSearchTable(db_id, *table); !r.ok()) {
            return r;
          }
        }
        return AddTable(db_id, schema_id, table_id, std::move(table));
      }
      if (auto check = CheckTableForDrop(
            bytes,
            {.id = table_id, .database_id = db_id, .schema_id = schema_id});
          !check.ok()) {
        return check;
      }
      auto drop =
        CreateTableDrop(GetCatalogStore(), db_id, schema_id, table_id, true);
      if (!drop) {
        return std::move(drop.error());
      }
      DropTask::Schedule(std::move(*drop)).Detach();
      return {};
    });
}

Result OpenDatabase::AddTable(ObjectId db_id, ObjectId schema_id,
                              ObjectId table_id, std::shared_ptr<Table> table) {
  auto r = _catalog.RegisterTable(db_id, schema_id, std::move(table));
  if (!r.ok()) {
    return r;
  }
  CollectDeletedDefinitions(table_id, DeletedScope::Relation);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Relation);
  };
  return RegisterIndexes(db_id, schema_id, table_id);
}

Result OpenDatabase::AddIndex(ObjectId database_id, ObjectId schema_id,
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
    return Result{ERROR_INTERNAL, "Failed to read index definition"};
  }
  if (auto r = _catalog.RegisterIndex(database_id, schema_id, index); !r.ok()) {
    return r;
  }
  Result r;

#ifdef SDB_DEV
  // Check there are no tombstones in index scope
  size_t counter = 0;
  r = GetCatalogStore().VisitBoot(index_id, ObjectType::Tombstone,
                                  [&](CatalogStore::Key, std::string_view) {
                                    counter++;
                                    return Result{};
                                  });
  if (!r.ok()) {
    return r;
  }
  SDB_ASSERT(counter == 0);
#endif

  if (entry_type == ObjectType::InvertedIndex) {
    r = RegisterInvertedStorage(index);
  }
  return r;
}

Result OpenDatabase::AddSchema(ObjectId db_id, ObjectId schema_id,
                               std::string_view bytes) {
  auto schema =
    catalog::DeserializeObject<catalog::Schema>(bytes, {.database_id = db_id});
  if (!schema) {
    return Result{ERROR_INTERNAL, "Failed to read schema definition"};
  }
  _schema_name = schema->GetName();

  if (auto r = _catalog.RegisterSchema(db_id, std::move(schema)); !r.ok()) {
    return r;
  }

  CollectDeletedDefinitions(schema_id, DeletedScope::Schema);
  irs::Finally cleanup = [&] noexcept {
    ClearDeletedDefinitions(DeletedScope::Schema);
  };

  if (auto r = RegisterTokenizers(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterTypes(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterSequences(db_id, schema_id, false); !r.ok()) {
    return r;
  }
  if (auto r = RegisterFunctions(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterTables(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterViews(db_id, schema_id); !r.ok()) {
    return r;
  }
  if (auto r = RegisterSequences(db_id, schema_id, true); !r.ok()) {
    return r;
  }
  return {};
}

}  // namespace

template<typename T>
ResultOr<std::shared_ptr<Database>> GetDatabaseImpl(T key) {
  auto database = GetCatalog().GetCatalogSnapshot()->GetDatabase(key);
  if (!database) [[unlikely]] {
    return std::unexpected<Result>(std::in_place,
                                   ERROR_SERVER_DATABASE_NOT_FOUND,
                                   "Cannot find database ", key);
  }
  return database;
}

namespace {

std::shared_ptr<Catalog> g_catalog;

}  // namespace

void InitCatalog() {
  g_catalog = std::make_shared<Catalog>();

  if (auto r = GetCatalogStore().LoadBootState(); !r.ok()) {
    SDB_THROW(std::move(r));
  }
  irs::Finally release_boot = [] noexcept {
    GetCatalogStore().ReleaseBootState();
  };

  OpenDatabase open_db{GetCatalog()};
  if (auto r = open_db(); !r.ok()) {
    SDB_FATAL(GENERAL, "Failed to open database, ", r.errorMessage());
  }

  if (auto fr = GetCatalog().FinalizeLoad(); !fr.ok()) {
    SDB_FATAL(GENERAL, "FinalizeLoad failed: ", fr.errorMessage());
  }

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
}

void ShutdownCatalog() { g_catalog.reset(); }

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id) {
  return GetDatabaseImpl(database_id);
}

ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name) {
  return GetDatabaseImpl(name);
}

Catalog& GetCatalog() {
  SDB_ASSERT(g_catalog, "Catalog is not initialized");
  return *g_catalog;
}

}  // namespace sdb::catalog
