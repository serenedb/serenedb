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

#include "catalog/local_catalog.h"

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
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parser.hpp>
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
#include "app/name_validator.h"
#include "auth/role_utils.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/buffer.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/error_code.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/result.h"
#include "basics/result_or.h"
#include "basics/static_strings.h"
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
#include "catalog/role.h"
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
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_utils.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/index_shard.h"
#include "storage_engine/search_engine.h"
#include "storage_engine/secondary_index_shard.h"
#include "storage_engine/table_shard.h"
#include "utils/exec_context.h"

namespace sdb::catalog {

class SnapshotImpl;

namespace {

Result Apply(
  auto& snapshot, std::shared_mutex& snapshot_mutex, auto&& f,
  std::function<void(const std::shared_ptr<SnapshotImpl>&)> rollback = {}) {
  std::shared_ptr<SnapshotImpl> clone = snapshot->Clone();
  if (auto r = f(clone); !r.ok()) {
    if (rollback) {
      rollback(clone);
    }
    return r;
  }
  std::shared_ptr<const SnapshotImpl> clone_const = std::move(clone);
  {
    std::unique_lock guard{snapshot_mutex};
    snapshot = std::move(clone_const);
  }
  return {};
}

}  // namespace

enum class EdgeAction : uint8_t { Add, Delete };

class SnapshotImpl : public Snapshot {
 public:
  std::shared_ptr<SnapshotImpl> Clone() const {
    // TODO(gnusi): COW
    auto result = std::make_shared<SnapshotImpl>();
    result->_resolution_table = _resolution_table;
    result->_objects = _objects;
    result->_deps = _deps;
    result->_in_load = _in_load;
    // New snapshot starts with empty DuckDB cache (lazily populated)
    return result;
  }

  void EndLoad() noexcept { _in_load = false; }

  connector::DuckDBEntryCache& GetDuckDBEntryCache() const final {
    return _duckdb_cache;
  }

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(
    const std::shared_ptr<Database>& db, duckdb::shared_ptr<void> keep_alive) {
    auto db_deps = GetDependency<DatabaseDependency>(db->GetId());
    auto schemas_drop = db_deps->schemas |
                        std::views::transform([&](ObjectId id) {
                          auto schema = GetObject<Schema>(id);
                          SDB_ASSERT(schema);
                          return CreateSchemaDrop(db->GetId(), schema, false);
                        }) |
                        std::ranges::to<std::vector>();
    auto drop_task = std::make_shared<DatabaseDrop>(db, std::move(schemas_drop),
                                                    std::move(keep_alive));
    return drop_task;
  }

  std::shared_ptr<SchemaDrop> CreateSchemaDrop(
    ObjectId db_id, const std::shared_ptr<Schema>& schema, bool is_root) {
    auto schema_deps = GetDependency<SchemaDependency>(schema->GetId());
    auto tables_drop =
      schema_deps->tables | std::views::transform([&](ObjectId id) {
        auto table = GetObject<Table>(id);
        SDB_ASSERT(table);
        return CreateTableDrop(db_id, schema->GetId(), table, false);
      }) |
      std::ranges::to<std::vector>();

    auto drop_task = std::make_shared<SchemaDrop>(
      schema, std::move(tables_drop), db_id, is_root);
    return drop_task;
  }

  std::shared_ptr<TableDrop> CreateTableDrop(
    ObjectId db_id, ObjectId schema_id, const std::shared_ptr<Table>& table,
    bool is_root) {
    auto table_deps = GetDependency<TableDependency>(table->GetId());
    auto shard = GetObject<TableShard>(table_deps->shard_id);
    auto indexes = table_deps->indexes |
                   std::views::transform([&](ObjectId id) {
                     auto index = GetObject<Index>(id);
                     SDB_ASSERT(index);
                     return CreateIndexDrop(db_id, schema_id, table->GetId(),
                                            index, is_root);
                   }) |
                   std::ranges::to<std::vector>();
    auto owned_sequences =
      table_deps->owned_sequences | std::ranges::to<std::vector>();

    return std::make_shared<TableDrop>(table, shard, std::move(indexes),
                                       std::move(owned_sequences), schema_id,
                                       is_root);
  }

  std::shared_ptr<IndexDrop> CreateIndexDrop(
    ObjectId db_id, ObjectId schema_id, ObjectId table_id,
    const std::shared_ptr<Index>& index, bool is_root) {
    auto index_deps = GetDependency<IndexDependency>(index->GetId());
    auto shard = GetObject<IndexShard>(index_deps->shard_id);
    return std::make_shared<IndexDrop>(index, std::move(shard), db_id,
                                       schema_id, table_id, is_root);
  }

  template<typename T>
  Result RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool replace) {
    if constexpr (std::is_same_v<T, Database>) {
      auto r = AddToResolution<ResolveType::Database>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<DatabaseDependency>(parent_id,
                                                     std::move(object));
    } else if constexpr (std::is_same_v<T, Role>) {
      auto r = AddToResolution<ResolveType::Role>(parent_id, object->GetId(),
                                                  object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, Schema>) {
      auto r = AddToResolution<ResolveType::Schema>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<SchemaDependency>(parent_id,
                                                   std::move(object));
    } else if constexpr (std::is_same_v<T, PgSqlView>) {
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<ViewDependency>(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, Sequence>) {
      // Sequences share the relation namespace (PG: pg_class.relkind='S').
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<SequenceDependency>(parent_id,
                                                     std::move(object));
    } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
      auto r = AddToResolution<ResolveType::Function>(
        parent_id, object->GetId(), object->GetName(), replace);
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
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
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
    } else if constexpr (std::is_same_v<T, TableShard>) {
      return AddObjectDefinition(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, IndexShard>) {
      return AddObjectDefinition(parent_id, std::move(object));
    } else {
      static_assert(false);
    }
  }

  template<typename T>
  void UnregisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool maybe_not_found = false) noexcept {
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
    } else if constexpr (std::is_same_v<T, TableShard>) {
    } else if constexpr (std::is_same_v<T, IndexShard>) {
    } else {
      static_assert(false);
    }
    SDB_ASSERT(parent_id.isSet());
    RemoveObjectDefinition(parent_id, object->GetId(), true, maybe_not_found);
  }

  template<ResolveType Type>
  Result AddToResolution(ObjectId parent_id, ObjectId id, std::string_view name,
                         bool replace) {
    return _resolution_table.AddObject<Type>(parent_id, name, id, replace);
  }

  template<ResolveType Type>
  void RemoveFromResolution(ObjectId parent_id, std::string_view name,
                            bool maybe_not_found = false) noexcept {
    auto res = _resolution_table.RemoveObject<Type>(parent_id, name);
    if (!maybe_not_found) {
      SDB_ASSERT(res);
    }
  }

  template<typename Dep, typename Member, typename Edge>
  void ModifyDependency(ObjectId target, Member Dep::* mem, const Edge& edge,
                        EdgeAction action) {
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

  void ModifyTableDependencies(ObjectId schema_id, const Table& table,
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
        ModifyDependency(type_id, &PgSqlTypeDependency::column_types,
                         col.GetId(), action);
      }
      if (col.expr && col.expr->HasExpr() && !col.IsGenerated()) {
        auto refs = col.expr->GetRefs(RefKinds::Sequences |
                                      RefKinds::Functions | RefKinds::Types);
        for (const auto& ref : refs.sequences) {
          ModifyDependency(resolve_seq(ref),
                           &SequenceDependency::column_defaults, col.GetId(),
                           action);
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
    for (const auto& c : table.CheckConstraints()) {
      if (!c.expr || !c.expr->HasExpr()) {
        continue;
      }
      auto refs = c.expr->GetRefs(RefKinds::Sequences | RefKinds::Functions |
                                  RefKinds::Types);
      auto edge = std::pair{table.GetId(), c.GetId()};
      for (const auto& ref : refs.functions) {
        ModifyDependency(
          Resolve<ResolveType::Function, ObjectType::PgSqlFunction>(
            database_id, schema_id, ref.catalog, ref.schema, ref.name),
          &PgSqlFunctionDependency::constraints, edge, action);
      }
      for (const auto& ref : refs.sequences) {
        ModifyDependency(resolve_seq(ref), &SequenceDependency::constraints,
                         edge, action);
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
  ObjectId Resolve(ObjectId db_id, ObjectId default_schema_id,
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

  void ModifyViewDependencies(ObjectId schema_id, const PgSqlView& view,
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
        Resolve<ResolveType::Relation, ObjectType::Table,
                ObjectType::PgSqlView>(database_id, schema_id, ref.catalog,
                                       ref.schema, ref.name),
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

  void ModifyFunctionDependencies(ObjectId schema_id, const PgSqlFunction& func,
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
        Resolve<ResolveType::Relation, ObjectType::Table,
                ObjectType::PgSqlView>(database_id, schema_id, ref.catalog,
                                       ref.schema, ref.name),
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

  void ModifyInvertedIndexDependencies(const InvertedIndex& index,
                                       ObjectId index_id, EdgeAction action) {
    auto database_id = GetDatabaseId(index);
    auto schema_id = index.GetParentId();
    for (const auto& entry : index.GetEntries()) {
      const auto* expr = entry.second.GetExpressionData();
      if (!expr) {
        continue;
      }
      SDB_ASSERT(!expr->pretty_printed.empty());
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
      try {
        parsed = duckdb::Parser::ParseExpressionList(expr->pretty_printed);
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

  const auto& Objects() const noexcept { return _objects; }

  template<typename DependencyType = void>
  Result AddObjectDefinition(ObjectId parent_id,
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

  void AddDependencies(ObjectId parent_id, const Object& obj) {
    auto id = obj.GetId();
    switch (obj.GetType()) {
      case ObjectType::Database:
      case ObjectType::Role:
        break;
      case ObjectType::Schema:
        GetDependencyForWrite<DatabaseDependency>(parent_id)->schemas.insert(
          id);
        break;
      case ObjectType::Tokenizer:
        GetDependencyForWrite<SchemaDependency>(parent_id)->tokenizers.insert(
          id);
        break;
      case ObjectType::Table: {
        GetDependencyForWrite<SchemaDependency>(parent_id)->tables.insert(id);
        const auto& table = basics::downCast<Table>(obj);
        for (const auto& col : table.Columns()) {
          if (_objects.find(col.GetId()) == _objects.end()) {
            _objects.insert(std::make_shared<Column>(col));
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
        GetDependencyForWrite<SchemaDependency>(parent_id)->functions.insert(
          id);
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
          ModifyInvertedIndexDependencies(
            basics::downCast<InvertedIndex>(index), id, EdgeAction::Add);
        }
      } break;
      case ObjectType::TableShard:
        GetDependencyForWrite<TableDependency>(parent_id)->shard_id = id;
        break;
      case ObjectType::SecondaryIndexShard:
      case ObjectType::InvertedIndexShard:
        GetDependencyForWrite<IndexDependency>(parent_id)->shard_id = id;
        break;
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

  std::vector<std::shared_ptr<Role>> GetRoles() const final {
    return _resolution_table.GetRoleIds() |
           std::views::transform([&](ObjectId role_id) {
             auto it = _objects.find(role_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Role>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Database>> GetDatabases() const final {
    return _resolution_table.GetDatabaseIds() |
           std::views::transform([&](ObjectId db_id) {
             auto it = _objects.find(db_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Database>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Schema>> GetSchemas(ObjectId db_id) const final {
    auto db_deps = GetDependency<DatabaseDependency>(db_id);
    return db_deps->schemas | std::views::transform([&](ObjectId schema_id) {
             auto it = _objects.find(schema_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Schema>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Object>> GetRelations(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        return _resolution_table.GetRelationIds(schema_id) |
               std::views::transform(
                 [&](ObjectId relation_id) -> std::shared_ptr<Object> {
                   return GetObject<Object>(relation_id);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<Object>>{});
  }

  std::vector<std::shared_ptr<Table>> GetTables(
    ObjectId db_id, std::string_view schema) const final {
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

  std::vector<std::shared_ptr<PgSqlView>> GetViews(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
        return schema_deps->views |
               std::views::transform([&](ObjectId view_id) {
                 auto it = _objects.find(view_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<PgSqlView>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<PgSqlView>>{});
  }

  std::vector<std::shared_ptr<Sequence>> GetSequences(
    ObjectId db_id, std::string_view schema) const final {
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

  std::vector<std::shared_ptr<PgSqlFunction>> GetFunctions(
    ObjectId db_id, std::string_view schema) const final {
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

  std::vector<std::shared_ptr<Index>> GetIndexes(
    ObjectId db_id, std::string_view schema) const final {
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

  void VisitRelations(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const Object&)> visitor) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return;
    }
    for (const auto relation_id :
         _resolution_table.GetRelationIds(*schema_id)) {
      auto it = _objects.find(relation_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<Object>(**it));
    }
  }

  void VisitViews(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const PgSqlView&)> visitor) const final {
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

  void VisitFunctions(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const PgSqlFunction&)> visitor) const final {
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

  void VisitIndexes(ObjectId db_id, std::string_view schema,
                    absl::FunctionRef<void(const Index&)> visitor) const final {
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

  std::vector<std::shared_ptr<Tokenizer>> GetTokenizers(
    ObjectId db_id, std::string_view schema) const final {
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

  std::vector<std::shared_ptr<PgSqlType>> GetTypes(
    ObjectId db_id, std::string_view schema) const final {
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

  std::shared_ptr<PgSqlType> GetType(ObjectId db_id, std::string_view schema,
                                     std::string_view name) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Type>(schema_id,
                                                                  name);
      })
      .transform(
        [&](ObjectId type_id) { return GetObject<PgSqlType>(type_id); })
      .value_or(nullptr);
  }

  std::shared_ptr<Database> GetDatabase(std::string_view database) const final {
    return _resolution_table
      .ResolveObject<ResolveType::Database>(id::kInstance, database)
      .transform([&](ObjectId db_id) {
        auto it = _objects.find(db_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<Database>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Schema> GetSchema(ObjectId db_id,
                                    std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        auto it = _objects.find(schema_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<Schema>(*it);
      })
      .value_or(nullptr);
  }

  bool CheckSchemaEmptyDependency(ObjectId schema_id) const {
    return GetDependency<SchemaDependency>(schema_id)->Empty();
  }

  std::shared_ptr<Object> GetRelation(ObjectId db_id, std::string_view schema,
                                      std::string_view relation) const final {
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

  std::shared_ptr<PgSqlFunction> GetFunction(
    ObjectId db_id, std::string_view schema,
    std::string_view function) const final {
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

  std::shared_ptr<Tokenizer> GetTokenizer(ObjectId db_id,
                                          std::string_view schema,
                                          std::string_view name) const final {
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

  std::shared_ptr<Table> GetTable(ObjectId db_id, std::string_view schema,
                                  std::string_view table) const final {
    auto rel = GetRelation(db_id, schema, table);
    if (!rel || rel->GetType() != ObjectType::Table) {
      return nullptr;
    }
    return basics::downCast<Table>(rel);
  }

  std::shared_ptr<Sequence> GetSequence(ObjectId db_id, ObjectId schema_id,
                                        std::string_view name) const final {
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

  bool HasIndexes(ObjectId relation_id) const final {
    return !GetDependency<RelationDependency>(relation_id)->indexes.empty();
  }

  std::shared_ptr<Object> GetObject(ObjectId id) const final {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return nullptr;
    }
    return *it;
  }

  std::shared_ptr<TableShard> GetTableShard(ObjectId table_id) const final {
    auto table_deps = GetDependency<TableDependency>(table_id);
    if (!table_deps->shard_id.isSet()) {
      return nullptr;
    }
    return GetObject<TableShard>(table_deps->shard_id);
  }

  std::shared_ptr<IndexShard> GetIndexShard(ObjectId index_id) const final {
    auto index_deps = GetDependency<IndexDependency>(index_id);
    if (!index_deps->shard_id.isSet()) {
      return nullptr;
    }
    return GetObject<IndexShard>(index_deps->shard_id);
  }

  std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByRelation(
    ObjectId relation_id) const final {
    auto relation_deps = GetDependency<RelationDependency>(relation_id);
    return relation_deps->indexes | std::views::transform([&](auto index_id) {
             return GetIndexShard(index_id);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Index>> GetIndexesByRelation(
    ObjectId relation_id) const final {
    auto relation_deps = GetDependency<RelationDependency>(relation_id);
    return relation_deps->indexes | std::views::transform([&](auto index_id) {
             return GetObject<Index>(index_id);
           }) |
           std::ranges::to<std::vector>();
  }

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) const {
    return _resolution_table.ResolveObject<Type>(parent_id, name);
  }

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return {};
    }
    return std::dynamic_pointer_cast<T>(*it);
  }

  std::shared_ptr<Role> GetRole(std::string_view name) const final {
    auto id =
      _resolution_table.ResolveObject<ResolveType::Role>(id::kInstance, name);
    if (!id) {
      return {};
    }
    auto role = GetObject<Role>(*id);
    SDB_ASSERT(role);
    return role;
  }

  std::shared_ptr<Database> GetDatabase(ObjectId database) const final {
    auto obj = GetObject(database);
    if (!obj) {
      return nullptr;
    }
    return basics::downCast<Database>(obj);
  }

  template<ResolveType Type>
  Result ReplaceObject(ObjectId parent_id, std::string_view old_name,
                       std::shared_ptr<Object> new_object) {
    if (old_name != new_object->GetName()) {
      auto removed = _resolution_table.RemoveObject<Type>(parent_id, old_name);
      SDB_ASSERT(removed);
      auto r = _resolution_table.AddObject<Type>(
        parent_id, new_object->GetName(), new_object->GetId(), false);
      if (!r.ok()) {
        return r;
      }
    } else {
      // Name unchanged, but must refresh the string_view to point to new
      // object's _name
      auto r = _resolution_table.AddObject<Type>(
        parent_id, new_object->GetName(), new_object->GetId(), true);
      SDB_ASSERT(r.ok());
    }

    auto it = _objects.find(new_object->GetId());
    SDB_ASSERT(it != _objects.end());
    SDB_ASSERT((*it)->GetId() == new_object->GetId());
    auto old_object = *it;
    // Refresh reverse-edges if the object's body changed. Replaces of
    // objects without body (Role, Sequence, ...) are pure name/value
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
      case ObjectType::Table:
        ModifyTableDependencies(parent_id, basics::downCast<Table>(*new_object),
                                EdgeAction::Add);
        break;
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
  std::shared_ptr<const T> GetDependency(ObjectId id) const {
    return basics::downCast<const T>(_deps.GetDependency(id));
  }

  template<typename T>
  std::shared_ptr<T> GetDependencyForWrite(ObjectId id) {
    auto dep = basics::downCast<T>(_deps.GetDependency(id)->Clone());
    _deps.SetDependency(id, dep);
    return dep;
  }

  // Cross-tree fixups for DROP seed. Composition cleanup is async.
  DropPlan ComputeDropPlan(ObjectId seed) const {
    auto lookup = [this](ObjectId id) { return GetObject(id); };
    auto indexes_using_col = [this](ObjectId table_id, ObjectId col_id) {
      std::vector<ObjectId> result;
      auto td = GetDependency<TableDependency>(table_id);
      SDB_ASSERT(td);
      for (auto idx_id : td->indexes) {
        auto idx = GetObject<Index>(idx_id);
        SDB_ASSERT(idx);
        if (absl::c_contains(idx->GetColumnIds(), col_id)) {
          result.push_back(idx_id);
        }
      }
      return result;
    };
    auto dep_lookup = [this](ObjectId id) {
      return _deps.TryGetDependency(id);
    };
    return DropEmitter{seed, lookup, indexes_using_col, dep_lookup}
      .ComputePlan();
  }

  void CommitDropPlan(RocksDBEngineCatalog::CatalogWriteContext& ctx,
                      const DropPlan& plan) const {
    duckdb::MemoryStream stream;
    for (const auto& [tid, rw] : plan.table_rewrites) {
      ctx.PutDefinition(rw.schema_id, ObjectType::Table, tid,
                        catalog::SerializeObject(*rw.table, stream));
    }
    for (const auto& [schema_id, view_id] : plan.view_drops) {
      ctx.DropDefinition(schema_id, ObjectType::PgSqlView, view_id);
    }
    for (const auto& [schema_id, fn_id] : plan.function_drops) {
      ctx.DropDefinition(schema_id, ObjectType::PgSqlFunction, fn_id);
    }
    // Recovery anchor for cascade-dropped indexes (column->index cascade).
    for (auto idx_id : plan.index_drops) {
      if (auto idx = GetObject<Index>(idx_id)) {
        ctx.WriteTombstone(idx->GetRelationId(), idx_id);
      }
    }
  }

  // Apply cross-tree mutations in-memory; schedule IndexDrop tasks for
  // cascade-dropped indexes (column->index cascade).
  void ApplyDropPlan(ObjectId db_id, DropPlan& plan) {
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
      auto task = CreateIndexDrop(db_id, table->GetParentId(), table_id, idx,
                                  /*is_root=*/true);
      UnregisterObject(std::move(idx), table_id);
      DropTask::Schedule(std::move(task)).Detach();
    }
  }

 private:
  void RemoveObjectDefinition(ObjectId parent_id, ObjectId id,
                              bool root = false,
                              bool maybe_not_found = false) noexcept {
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
          GetDependencyForWrite<SchemaDependency>(parent_id)->functions.erase(
            id);
          ModifyFunctionDependencies(parent_id,
                                     basics::downCast<PgSqlFunction>(*obj),
                                     EdgeAction::Delete);
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
            auto schema_deps =
              GetDependencyForWrite<SchemaDependency>(parent_id);
            SDB_ASSERT(schema_deps);
            schema_deps->sequences.erase(id);
          }
        } break;
        case ObjectType::TableShard:
        case ObjectType::SecondaryIndexShard:
        case ObjectType::InvertedIndexShard:
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
          if (table_deps.shard_id.isSet()) {
            RemoveObjectDefinition(id, table_deps.shard_id);
          }
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
      case ObjectType::InvertedIndex: {
        auto index_deps = GetDependency<IndexDependency>(id);
        if (index_deps->shard_id.isSet()) {
          RemoveObjectDefinition(id, index_deps->shard_id);
        }

      } break;
      case ObjectType::PgSqlFunction:
      case ObjectType::PgSqlType:
      case ObjectType::Tokenizer:
      case ObjectType::Sequence:
        break;
      case ObjectType::TableShard:
      case ObjectType::SecondaryIndexShard:
      case ObjectType::InvertedIndexShard:
        SDB_ASSERT(!root);
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

  template<typename W>
  Result ResolveRole(this auto&& self, std::string_view role, W&& writer) {
    auto role_it = self._roles.find(role);
    if (role_it == self._roles.end()) {
      return {ERROR_USER_NOT_FOUND, "Role not found: ", role};
    }

    return writer(role_it);
  }

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
  bool _in_load = true;
};

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

LocalCatalog::LocalCatalog()
  : _snapshot(std::make_shared<SnapshotImpl>()), _engine{&GetServerEngine()} {}

Result LocalCatalog::RegisterRole(std::shared_ptr<Role> role) {
  SDB_INFO(GENERAL, "Register role ", role->GetName());
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(role), id::kInstance, false);
  });
}

Result LocalCatalog::RegisterDatabase(std::shared_ptr<Database> database) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(database), id::kInstance, false);
  });
}

Result LocalCatalog::RegisterSchema(ObjectId database_id,
                                    std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(schema), database_id, false);
  });
}

Result LocalCatalog::RegisterView(ObjectId schema_id,
                                  std::shared_ptr<PgSqlView> view) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(view), schema_id, false);
  });
}

Result LocalCatalog::RegisterSequence(ObjectId database_id, ObjectId schema_id,
                                      std::shared_ptr<Sequence> sequence) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(sequence), schema_id, false);
  });
}

Result LocalCatalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                                   std::shared_ptr<Table> table) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(table, schema_id, false);
  });
}

Result LocalCatalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                                      std::shared_ptr<PgSqlFunction> function) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(function), schema_id, false);
  });
}

Result LocalCatalog::RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                                       std::shared_ptr<Tokenizer> tokenizer) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(tokenizer), schema_id, false);
  });
}

Result LocalCatalog::RegisterType(ObjectId database_id, ObjectId schema_id,
                                  std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(std::move(type), schema_id, false);
  });
}

Result LocalCatalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) {
      auto r = clone->RegisterObject(database, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      {
        duckdb::MemoryStream stream;
        auto bytes = catalog::SerializeObject(*database, stream);
        auto r = _engine->CreateDefinition(
          id::kInstance, ObjectType::Database, database_id,
          [&](bool) { return std::string_view{bytes}; });

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
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*schema, stream);
      return _engine->CreateDefinition(
        database_id, ObjectType::Schema, schema->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto clone) {
      clone->UnregisterObject(database, id::kInstance, true);
    });
}

Result LocalCatalog::CreateSchema(ObjectId database_id,
                                  std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) {
      if (auto r = clone->RegisterObject(schema, database_id, false); !r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*schema, stream);
      return _engine->CreateDefinition(
        database_id, ObjectType::Schema, schema->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto clone) { clone->UnregisterObject(schema, database_id, true); });
}

Result LocalCatalog::CreateRole(std::shared_ptr<Role> role) {
  SDB_INFO(GENERAL, "Creating role: ", role->GetName());
  absl::MutexLock lock{&_mutex};
  auto r = Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) {
      auto r = clone->RegisterObject(role, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*role, stream);
      return _engine->CreateDefinition(
        id::kInstance, ObjectType::Role, role->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto& clone) { clone->UnregisterObject(role, id::kInstance, true); });

  if (!r.ok()) {
    return r;
  }

  return {};
}

Result LocalCatalog::RegisterIndex(ObjectId database_id, ObjectId schema_id,
                                   std::shared_ptr<Index> index) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(index, index->GetRelationId(), false);
  });
}

Result LocalCatalog::RegisterIndexShard(std::shared_ptr<IndexShard> shard) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(shard, shard->GetIndexId(), false);
  });
}

Result LocalCatalog::RegisterTableShard(std::shared_ptr<TableShard> shard) {
  absl::MutexLock lock{&_mutex};

  return Apply(_snapshot, _snapshot_mutex, [&](auto& clone) {
    return clone->RegisterObject(shard, shard->GetTableId(), false);
  });
}

Result LocalCatalog::CreateIndexImpl(
  std::string_view relation_schema, std::shared_ptr<Index> index,
  CreateIndexOperationOptions operation_options) {
  return Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) {
      auto r = clone->RegisterObject(index, index->GetRelationId(), false);
      if (!r.ok()) {
        return r;
      }
      auto shard = index->CreateIndexShard(true, ObjectId{0});
      if (!shard) {
        return std::move(shard).error();
      }
      r = clone->RegisterObject(*shard, index->GetId(), false);
      SDB_ASSERT(r.ok());

      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(index->GetRelationId(), index->GetId());
        if (!r.ok()) {
          return r;
        }
        index->SetTombstoned(true);
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      auto shard_type = IndexShardType(index->GetType());
      {  // Write index definition
        duckdb::MemoryStream stream;
        auto bytes = catalog::SerializeObject(*index, stream);
        r = _engine->CreateDefinition(
          index->GetRelationId(), index->GetType(), index->GetId(),
          [&](bool) { return std::string_view{bytes}; });
        if (!r.ok()) {
          return r;
        }
      }
      {  // Write index shard definition
        duckdb::MemoryStream stream;
        auto bytes = catalog::SerializeObject(**shard, stream);

        r = _engine->CreateDefinition(
          index->GetId(), shard_type, (*shard)->GetId(),
          [&](bool) { return std::string_view{bytes}; });
        if (!r.ok()) {
          return r;
        }
      }
      return Result{};
    },
    [&](auto& clone) {
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

Result LocalCatalog::CreateSecondaryIndex(
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
    auto it = absl::c_find_if(resolved->columns, [&](const Column& col) {
      return col.GetName() == c.name;
    });
    if (it == resolved->columns.end()) {
      return Result{ERROR_BAD_PARAMETER, "column \"", c.name,
                    "\" does not exist"};
    }
    if (it->store_mode == ColumnStoreMode::kIndexOnly) {
      return Result{ERROR_BAD_PARAMETER, "cannot include column \"", c.name,
                    "\" in a secondary index: column has sdb_indexonly "
                    "storage and is only readable through an inverted index"};
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

Result LocalCatalog::CreateInvertedIndex(
  ObjectId database_id, std::string_view schema, std::string_view relation,
  std::string name, std::vector<CreateIndexColumn>&& columns,
  InvertedIndexOptions options, CreateIndexOperationOptions operation_options) {
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
    database_id, schema, *schema_id, ObjectId{0}, resolved->relation_id,
    std::move(name), std::move(columns), _snapshot, std::move(options));
  if (!index) {
    return std::move(index).error();
  }
  return CreateIndexImpl(schema, *index, operation_options);
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
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
    _snapshot, _snapshot_mutex,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
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
      return _engine->CreateDefinition(
        *schema_id, ObjectType::PgSqlView, view->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(view, *schema_id, true);
      }
    });
}

Result LocalCatalog::CreateSequence(ObjectId database_id,
                                    std::string_view schema,
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
    _snapshot, _snapshot_mutex,
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

Result LocalCatalog::CreateFunction(ObjectId database_id,
                                    std::string_view schema,
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
    _snapshot, _snapshot_mutex,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
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
      return _engine->CreateDefinition(
        *schema_id, ObjectType::PgSqlFunction, function->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto clone) {
      if (!existed_id.isSet()) {
        clone->UnregisterObject(function, *schema_id, true);
      }
    });
}

Result LocalCatalog::CreateTable(
  ObjectId database_id, std::string_view schema, CreateTableOptions options,
  CreateTableOperationOptions operation_options) {
  if (auto r = TableNameValidator::validateName(options.name); !r.ok()) {
    return r;
  }
  for (auto pk_id : options.pk_columns) {
    auto col = absl::c_find_if(
      options.columns, [&](const auto& c) { return c.GetId() == pk_id; });
    SDB_ASSERT(col != options.columns.end());
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

  // Columns come in with whatever owner_table_id callers set (unused);
  // Table's constructor stamps the real id on each.
  auto table_id = NextId();

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
    generated_pk_seq_id);
  if (operation_options.create_with_tombstone) {
    table->SetTombstoned(true);
  }
  auto shard = std::make_shared<TableShard>(table->GetId(), TableStats{});

  return Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) -> Result {
      auto r = clone->RegisterObject(table, *schema_id, false);
      if (!r.ok()) {
        return r;
      }

      r = clone->RegisterObject(shard, table->GetId(), false);
      SDB_ASSERT(r.ok());
      for (const auto& seq : sequences) {
        r = clone->RegisterObject(seq, *schema_id, false);
        if (!r.ok()) {
          return r;
        }
      }
      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(*schema_id, table->GetId());
        if (!r.ok()) {
          return r;
        }
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      // PutDefinition copies into the rocksdb batch, so one reused stream is
      // enough: each view is consumed before the next serialize overwrites it.
      duckdb::MemoryStream stream;
      return _engine->Write([&](auto& ctx) {
        ctx.PutDefinition(*schema_id, ObjectType::Table, table->GetId(),
                          catalog::SerializeObject(*table, stream));
        ctx.PutDefinition(table->GetId(), ObjectType::TableShard,
                          shard->GetId(),
                          catalog::SerializeObject(*shard, stream));
        for (const auto& seq : sequences) {
          ctx.PutDefinition(*schema_id, ObjectType::Sequence, seq->GetId(),
                            catalog::SerializeObject(*seq, stream));
          ctx.PutSequence(seq->GetId(), seq->Options().Seed());
        }
      });
    },
    [&](auto clone) { clone->UnregisterObject(table, *schema_id, true); });
}

Result LocalCatalog::CreateTokenizer(ObjectId database_id,
                                     std::string_view schema,
                                     std::shared_ptr<Tokenizer> dict) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  dict->SetParentId(*schema_id);

  return Apply(
    _snapshot, _snapshot_mutex,
    [&](std::shared_ptr<SnapshotImpl>& clone) {
      auto r = clone->RegisterObject(dict, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*dict, stream);
      return _engine->CreateDefinition(
        *schema_id, ObjectType::Tokenizer, dict->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto& clone) {
      return clone->UnregisterObject(dict, *schema_id, true);
    });
}

Result LocalCatalog::CreateType(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  type->SetParentId(*schema_id);

  return Apply(
    _snapshot, _snapshot_mutex,
    [&](auto& clone) {
      auto r = clone->RegisterObject(type, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*type, stream);
      return _engine->CreateDefinition(
        *schema_id, ObjectType::PgSqlType, type->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](auto clone) { clone->UnregisterObject(type, *schema_id, true); });
}

template<typename T>
Result LocalCatalog::RenameObjectImpl(ObjectId schema_id, std::string_view name,
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
    _snapshot, _snapshot_mutex,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
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

      return _engine->CreateDefinition(
        parent_id, new_object->GetType(), new_object->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto current = clone->GetObject<T>(new_object->GetId());
      if (current->GetName() == new_object->GetName()) {
        auto r =
          clone->ReplaceObject<kResolveType>(schema_id, new_name, object);
        SDB_ASSERT(r.ok());
      }
    });
}

template<typename T>
Result LocalCatalog::RenameObjectImpl(ObjectId database_id,
                                      std::string_view schema,
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

  return RenameObjectImpl<T>(*schema_id, name, new_name, std::move(typed));
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  return RenameObjectImpl<PgSqlView>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  return RenameObjectImpl<Table>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameIndex(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  return RenameObjectImpl<Index>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameRelation(ObjectId database_id,
                                    std::string_view schema,
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

  switch (object->GetType()) {
    case ObjectType::Table:
      return RenameObjectImpl<Table>(*schema_id, name, new_name,
                                     std::static_pointer_cast<Table>(object));
    case ObjectType::PgSqlView:
      return RenameObjectImpl<PgSqlView>(
        *schema_id, name, new_name,
        std::static_pointer_cast<PgSqlView>(object));
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
      return RenameObjectImpl<Index>(*schema_id, name, new_name,
                                     std::static_pointer_cast<Index>(object));
    default:
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
  }
}

Result LocalCatalog::RenameFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::string_view name,
                                    std::string_view new_name) {
  return RenameObjectImpl<PgSqlFunction>(database_id, schema, name, new_name);
}

Result LocalCatalog::ChangeRole(std::string_view name,
                                ChangeCallback<Role> new_role) {
  absl::MutexLock lock{&_mutex};
  auto role = _snapshot->GetRole(name);
  if (!role) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  std::shared_ptr<Role> new_role_ptr;
  auto r = new_role(*role, new_role_ptr);
  if (!r.ok()) {
    return r;
  }
  r = Apply(
    _snapshot, _snapshot_mutex,
    [&](std::shared_ptr<SnapshotImpl>& clone) {
      auto r = clone->ReplaceObject<ResolveType::Role>(id::kInstance, name,
                                                       new_role_ptr);
      if (!r.ok()) {
        return r;
      }
      duckdb::MemoryStream stream;
      auto bytes = catalog::SerializeObject(*new_role_ptr, stream);
      return _engine->CreateDefinition(
        id::kInstance, ObjectType::Role, new_role_ptr->GetId(),
        [&](bool) { return std::string_view{bytes}; });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto obj = clone->GetObject<Role>(new_role_ptr->GetId());
      if (obj->GetName() == new_role_ptr->GetName()) {
        auto r = clone->ReplaceObject<ResolveType::Relation>(
          id::kInstance, new_role_ptr->GetName(), role);
        SDB_ASSERT(r.ok());
      }
    });

  if (!r.ok()) {
    return r;
  }

  return {};
}

Result LocalCatalog::ChangeView(ObjectId database_id, std::string_view schema,
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
  return Apply(_snapshot, _snapshot_mutex,
               [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
                 auto r = clone->ReplaceObject<ResolveType::Relation>(
                   *schema_id, name, updated);
                 if (!r.ok()) {
                   return r;
                 }

                 duckdb::MemoryStream stream;
                 auto bytes = catalog::SerializeObject(*updated, stream);
                 return _engine->CreateDefinition(
                   *schema_id, ObjectType::PgSqlView, updated->GetId(),
                   [&](bool) { return std::string_view{bytes}; });
               });
}

Result LocalCatalog::ChangeTable(ObjectId database_id, std::string_view schema,
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

  return Apply(_snapshot, _snapshot_mutex,
               [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
                 auto r = clone->ReplaceObject<ResolveType::Relation>(
                   *schema_id, name, updated);
                 if (!r.ok()) {
                   return r;
                 }

                 return basics::SafeCall([&] {
                   duckdb::MemoryStream stream;
                   auto bytes = catalog::SerializeObject(*updated, stream);
                   return _engine->CreateDefinition(
                     *schema_id, ObjectType::Table, updated->GetId(),
                     [&](bool) { return std::string_view{bytes}; });
                 });
               });
}

Result LocalCatalog::DropRole(std::string_view role) {
  absl::MutexLock lock{&_mutex};
  auto role_ptr = _snapshot->GetRole(role);
  if (!role_ptr) {
    return {ERROR_SERVER_ILLEGAL_NAME};
  }
  auto r = Apply(_snapshot, _snapshot_mutex,
                 [&](std::shared_ptr<SnapshotImpl>& clone) {
                   clone->UnregisterObject(role_ptr, id::kInstance);
                   return _engine->DropDefinition(
                     id::kInstance, ObjectType::Role, role_ptr->GetId());
                 });

  if (!r.ok()) {
    return r;
  }

  return {};
}

Result LocalCatalog::DropDatabase(std::string_view name,
                                  duckdb::shared_ptr<void> keep_alive) {
  absl::MutexLock lock{&_mutex};
  auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, name);
  if (!database_id) {
    return Result{ERROR_SERVER_DATABASE_NOT_FOUND, "database \"", name,
                  "\" does not exist"};
  }

  auto plan = _snapshot->ComputeDropPlan(*database_id);

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
      SDB_ASSERT(clone);
      auto database = clone->GetObject<Database>(*database_id);
      SDB_ASSERT(database);
      auto task = clone->CreateDatabaseDrop(database, std::move(keep_alive));
      auto wr = _engine->Write([&](auto& ctx) {
        ctx.WriteTombstone(id::kInstance, *database_id);
        clone->CommitDropPlan(ctx, plan);
      });
      if (!wr.ok()) {
        return wr;
      }
      clone->UnregisterObject(std::move(database), id::kInstance);
      clone->ApplyDropPlan(*database_id, plan);
      // Check that SereneDB won't open this database after reboot
      SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
      DropTask::Schedule(std::move(task)).Detach();
      return Result{};
    });
}

Result LocalCatalog::DropSchema(std::string_view database,
                                std::string_view name, bool cascade) {
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
      SDB_ASSERT(clone);
      auto schema = clone->GetObject<Schema>(*schema_id);
      SDB_ASSERT(schema);
      auto task = clone->CreateSchemaDrop(*database_id, schema, true);
      auto wr = _engine->Write([&](auto& ctx) {
        ctx.WriteTombstone(*database_id, *schema_id);
        clone->CommitDropPlan(ctx, plan);
      });
      if (!wr.ok()) {
        return wr;
      }
      clone->UnregisterObject(std::move(schema), *database_id);
      clone->ApplyDropPlan(*database_id, plan);
      // Check that SereneDB won't open this schema after reboot
      SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
      DropTask::Schedule(std::move(task)).Detach();
      return Result{};
    });
}

Result LocalCatalog::DropTable(std::string_view database,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
      SDB_ASSERT(clone);
      auto object = clone->GetObject(*table_id);
      SDB_ASSERT(object);
      if (object->GetType() != ObjectType::Table) {
        return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                      pg::ToPgObjectTypeName(object->GetType())};
      }
      auto table = basics::downCast<Table>(std::move(object));
      auto task = clone->CreateTableDrop(*database_id, *schema_id, table, true);

      auto wr = _engine->Write([&](auto& ctx) {
        ctx.WriteTombstone(*schema_id, *table_id);
        clone->CommitDropPlan(ctx, plan);
      });
      if (!wr.ok()) {
        return wr;
      }

      clone->UnregisterObject(std::move(table), *schema_id);
      clone->ApplyDropPlan(*database_id, plan);
      SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
      DropTask::Schedule(std::move(task)).Detach();
      return Result{};
    });
}

Result LocalCatalog::RemoveTombstone(ObjectId database_id,
                                     std::string_view schema,
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

  auto r = _engine->DropDefinition(tombstone_parent, ObjectType::Tombstone,
                                   *object_id);

  // Unlike most catalog operations that clone the snapshot, here we modify the
  // object in-place because the tombstone flag is simple in-memory state.
  auto& schema_obj = basics::downCast<Object>(*object);
  schema_obj.SetTombstoned(false);

  return r;
}

Result LocalCatalog::DropIndex(std::string_view database,
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
  const auto index_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!index_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
      SDB_ASSERT(clone);
      auto obj = clone->GetObject(*index_id);
      SDB_ASSERT(obj);
      if (!IsIndex(obj->GetType())) {
        return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                      pg::ToPgObjectTypeName(obj->GetType())};
      }
      auto index = basics::downCast<Index>(std::move(obj));
      if (auto r = _engine->WriteTombstone(index->GetRelationId(), *index_id);
          !r.ok()) {
        return r;
      }

      // Check that SereneDB won't open this index after reboot
      SDB_IF_FAILURE("crash_on_drop") { return Result{}; }

      auto task = clone->CreateIndexDrop(*database_id, *schema_id,
                                         index->GetRelationId(), index, true);
      clone->UnregisterObject(index, *schema_id);
      DropTask::Schedule(std::move(task)).Detach();
      return Result{};
    });
}

Result LocalCatalog::DropView(std::string_view database,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
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
      clone->ApplyDropPlan(*database_id, plan);
      return Result{};
    });
}

Result LocalCatalog::DropSequence(std::string_view database,
                                  std::string_view schema,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
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
      clone->ApplyDropPlan(*database_id, plan);
      return Result{};
    });
}

Result LocalCatalog::DropType(std::string_view database,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
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
      clone->ApplyDropPlan(*database_id, plan);
      return Result{};
    });
}

Result LocalCatalog::DropFunction(std::string_view database,
                                  std::string_view schema,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
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
      clone->ApplyDropPlan(*database_id, plan);
      return Result{};
    });
}

Result LocalCatalog::DropTokenizer(std::string_view database,
                                   std::string_view schema,
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

  return Apply(
    _snapshot, _snapshot_mutex, [&](std::shared_ptr<SnapshotImpl>& clone) {
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
      clone->ApplyDropPlan(*database_id, plan);
      return Result{};
    });
}

Result LocalCatalog::FinalizeLoad() {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, _snapshot_mutex,
               [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
                 for (const auto& obj : clone->Objects()) {
                   clone->AddDependencies(obj->GetParentId(), *obj);
                 }
                 clone->EndLoad();
                 return {};
               });
}

std::shared_ptr<const Snapshot> LocalCatalog::GetCatalogSnapshot()
  const noexcept {
  std::shared_lock guard{_snapshot_mutex};
  return _snapshot;
}

}  // namespace sdb::catalog
