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

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/table.h"

namespace sdb::catalog {

struct Snapshot;

enum class DependType : char { Normal = 'n', Auto = 'a', Internal = 'i' };

enum class DependClass : uint8_t {
  Relation,
  Type,
  Proc,
  Namespace,
  Constraint,
  Rewrite,
  Attrdef,
};

struct PgDependEdge {
  ObjectId dependent;
  int32_t dependent_sub;
  DependClass dependent_class;
  ObjectId referenced;
  int32_t referenced_sub;
  DependClass referenced_class;
  DependType deptype;
};

// One per external table that needs a column/CHECK mutation.
struct TableRewrite {
  ObjectId schema_id;
  std::shared_ptr<Table> table;
};

// Cross-tree catalog mutations needed alongside the seed's tombstone.
struct DropPlan {
  containers::FlatHashMap<ObjectId, TableRewrite> table_rewrites;
  std::vector<std::pair<ObjectId, ObjectId>> view_drops;      // (schema, view)
  std::vector<std::pair<ObjectId, ObjectId>> function_drops;  // (schema, fn)
  std::vector<ObjectId> index_drops;

  // RESTRICT would have blocked.
  bool IsCascade() const noexcept {
    return !table_rewrites.empty() || !view_drops.empty() ||
           !function_drops.empty() || !index_drops.empty();
  }

  // PG-style RESTRICT DETAIL text.
  std::string FormatDependentsDetail(const Snapshot& snap,
                                     std::string_view seed_kind,
                                     std::string_view seed_name) const;
};

struct ObjectDependencyBase;

enum class CascadeVerb : uint8_t {
  AutoDrop,
  CascadeView,
  CascadeFunction,
  CascadeIndex,
  ForeignKey,
  CheckConstraint,
  ColumnDefault,
  ColumnDrop,
};

class DropEmitter {
 public:
  using ObjectLookup = absl::FunctionRef<std::shared_ptr<Object>(ObjectId)>;
  // Indexes on `table_id` that include `col_id` -- driver for column->index
  // cascade when a column is dropped.
  using IndexesUsingColumn = absl::FunctionRef<std::vector<ObjectId>(
    ObjectId table_id, ObjectId col_id)>;
  using DepLookup =
    absl::FunctionRef<std::shared_ptr<const ObjectDependencyBase>(ObjectId)>;

  DropEmitter(ObjectId seed, ObjectLookup lookup,
              IndexesUsingColumn indexes_using_col, DepLookup dep_lookup)
    : _lookup{lookup},
      _indexes_using_col{indexes_using_col},
      _dep_lookup{dep_lookup},
      _auto_drops{seed},
      _stack{seed} {}

  DropPlan ComputePlan() &&;

  // AUTO/INTERNAL dep. Tag closure, queue for walk (dedup via _visited).
  void EmitAutoDrop(ObjectId id) {
    _auto_drops.insert(id);
    if (!_visited.insert(id).second) {
      return;
    }
    _stack.push_back(id);
  }

  void EmitCascadeViewDrop(ObjectId view_id) {
    if (_auto_drops.contains(view_id) || !_visited.insert(view_id).second) {
      return;
    }
    auto obj = _lookup(view_id);
    SDB_ASSERT(obj && obj->GetType() == ObjectType::PgSqlView);
    _plan.view_drops.emplace_back(obj->GetParentId(), view_id);
    _stack.push_back(view_id);
  }

  void EmitCascadeFunctionDrop(ObjectId fn_id) {
    if (_auto_drops.contains(fn_id) || !_visited.insert(fn_id).second) {
      return;
    }
    auto obj = _lookup(fn_id);
    SDB_ASSERT(obj && obj->GetType() == ObjectType::PgSqlFunction);
    _plan.function_drops.emplace_back(obj->GetParentId(), fn_id);
    _stack.push_back(fn_id);
  }

  void EmitCascadeColumnDefaultValueDrop(ObjectId col_id) {
    RewriteOwningTable(col_id,
                       [&](auto& t) { return t->DropColumnDefault(col_id); });
  }

  // doesn't work well now, it leaves the column data.
  // TODO: column drop rewrites the store table; emit a real cascade drop
  void EmitCascadeColumnDrop(ObjectId col_id) {
    RewriteOwningTable(col_id, [&](auto& t) { return t->DropColumn(col_id); });
    // PG's column->index cascade: any index that covers col_id goes too.
    auto col = _lookup(col_id);
    SDB_ASSERT(col);
    auto table_id = col->GetParentId();
    SDB_ASSERT(table_id.isSet());
    if (_auto_drops.contains(table_id)) {
      return;
    }
    for (auto idx_id : _indexes_using_col(table_id, col_id)) {
      EmitCascadeIndexDrop(idx_id);
    }
  }

  // Tokenizer->index, column->index etc. -- cross-tree index drop with
  // a recovery anchor written in CommitDropPlan.
  void EmitCascadeIndexDrop(ObjectId idx_id) {
    if (_auto_drops.contains(idx_id) || !_visited.insert(idx_id).second) {
      return;
    }
    _plan.index_drops.push_back(idx_id);
  }

  // Dropping a referenced table strips the FOREIGN KEYs pointing at it
  // from every surviving referencing table (PG DROP ... CASCADE semantics).
  void EmitCascadeForeignKeyDrop(ObjectId referencing_table_id,
                                 ObjectId referenced_table_id) {
    if (_auto_drops.contains(referencing_table_id)) {
      return;
    }
    auto& slot = _plan.table_rewrites[referencing_table_id];
    CloneIntoSlot(referencing_table_id, slot);
    slot.table = slot.table->DropForeignKeysReferencing(referenced_table_id);
  }

  void EmitCascadeCheckConstraintDrop(ObjectId constraint_id) {
    auto c = _lookup(constraint_id);
    SDB_ASSERT(c);
    auto table_id = c->GetParentId();
    SDB_ASSERT(table_id.isSet());
    if (_auto_drops.contains(table_id)) {
      return;
    }
    auto& slot = _plan.table_rewrites[table_id];
    CloneIntoSlot(table_id, slot);
    slot.table = slot.table->DropCheckConstraint(constraint_id);
  }

  void Apply(CascadeVerb verb, ObjectId edge, ObjectId self) {
    switch (verb) {
      case CascadeVerb::AutoDrop:
        EmitAutoDrop(edge);
        return;
      case CascadeVerb::CascadeView:
        EmitCascadeViewDrop(edge);
        return;
      case CascadeVerb::CascadeFunction:
        EmitCascadeFunctionDrop(edge);
        return;
      case CascadeVerb::CascadeIndex:
        EmitCascadeIndexDrop(edge);
        return;
      case CascadeVerb::ForeignKey:
        EmitCascadeForeignKeyDrop(edge, self);
        return;
      case CascadeVerb::CheckConstraint:
        EmitCascadeCheckConstraintDrop(edge);
        return;
      case CascadeVerb::ColumnDefault:
        EmitCascadeColumnDefaultValueDrop(edge);
        return;
      case CascadeVerb::ColumnDrop:
        EmitCascadeColumnDrop(edge);
        return;
    }
  }

 private:
  template<typename Mutate>
  void RewriteOwningTable(ObjectId col_id, Mutate&& mutate) {
    auto col = _lookup(col_id);
    SDB_ASSERT(col);
    auto table_id = col->GetParentId();
    SDB_ASSERT(table_id.isSet());
    if (_auto_drops.contains(table_id)) {
      return;
    }
    auto& slot = _plan.table_rewrites[table_id];
    CloneIntoSlot(table_id, slot);
    slot.table = std::forward<Mutate>(mutate)(slot.table);
  }

  void CloneIntoSlot(ObjectId table_id, TableRewrite& slot) {
    if (slot.table) {
      return;
    }
    auto obj = _lookup(table_id);
    SDB_ASSERT(obj && obj->GetType() == ObjectType::Table);
    slot.schema_id = obj->GetParentId();
    slot.table = basics::downCast<Table>(obj->Clone());
  }

  const ObjectLookup _lookup;
  const IndexesUsingColumn _indexes_using_col;
  const DepLookup _dep_lookup;
  DropPlan _plan;
  containers::FlatHashSet<ObjectId> _auto_drops;
  std::vector<ObjectId> _stack;
  containers::FlatHashSet<ObjectId> _visited;  // push-dedup
};

template<typename Self>
struct Edge {
  containers::FlatHashSet<ObjectId> Self::* bucket;
  CascadeVerb verb;
};

// Per-object reverse-edges. Emit routes each edge to the right callback.
struct ObjectDependencyBase {
  virtual ~ObjectDependencyBase() = default;
  virtual std::shared_ptr<ObjectDependencyBase> Clone() const = 0;
  virtual void Emit(DropEmitter&, ObjectId self) const = 0;
};

template<typename Self, typename Base = ObjectDependencyBase>
struct DependencyMixin : Base {
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<Self>(static_cast<const Self&>(*this));
  }
  void Emit(DropEmitter& emitter, ObjectId self) const final {
    for (const auto& row : Self::kEdges) {
      for (ObjectId id : static_cast<const Self&>(*this).*(row.bucket)) {
        emitter.Apply(row.verb, id, self);
      }
    }
  }
};

struct RelationDependency : ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> indexes;
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> functions;
};

struct TableDependency : DependencyMixin<TableDependency, RelationDependency> {
  containers::FlatHashSet<ObjectId> owned_sequences;
  containers::FlatHashSet<ObjectId> fk_referencing_tables;
  static constexpr std::array kEdges = {
    Edge<TableDependency>{&TableDependency::owned_sequences,
                          CascadeVerb::AutoDrop},
    Edge<TableDependency>{&TableDependency::fk_referencing_tables,
                          CascadeVerb::ForeignKey},
    Edge<TableDependency>{&TableDependency::indexes, CascadeVerb::AutoDrop},
    Edge<TableDependency>{&TableDependency::views, CascadeVerb::CascadeView},
    Edge<TableDependency>{&TableDependency::functions,
                          CascadeVerb::CascadeFunction},
  };
};

struct ViewDependency : DependencyMixin<ViewDependency, RelationDependency> {
  static constexpr std::array kEdges = {
    Edge<ViewDependency>{&ViewDependency::views, CascadeVerb::CascadeView},
    Edge<ViewDependency>{&ViewDependency::functions,
                         CascadeVerb::CascadeFunction},
  };
};

struct SequenceDependency : DependencyMixin<SequenceDependency> {
  containers::FlatHashSet<ObjectId> column_defaults;
  containers::FlatHashSet<ObjectId> constraints;
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> functions;
  static constexpr std::array kEdges = {
    Edge<SequenceDependency>{&SequenceDependency::column_defaults,
                             CascadeVerb::ColumnDefault},
    Edge<SequenceDependency>{&SequenceDependency::constraints,
                             CascadeVerb::CheckConstraint},
    Edge<SequenceDependency>{&SequenceDependency::views,
                             CascadeVerb::CascadeView},
    Edge<SequenceDependency>{&SequenceDependency::functions,
                             CascadeVerb::CascadeFunction},
  };
};

struct PgSqlTypeDependency : DependencyMixin<PgSqlTypeDependency> {
  containers::FlatHashSet<ObjectId> column_types;
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> functions;
  containers::FlatHashSet<ObjectId> constraints;
  containers::FlatHashSet<ObjectId> column_defaults;
  static constexpr std::array kEdges = {
    Edge<PgSqlTypeDependency>{&PgSqlTypeDependency::column_types,
                              CascadeVerb::ColumnDrop},
    Edge<PgSqlTypeDependency>{&PgSqlTypeDependency::views,
                              CascadeVerb::CascadeView},
    Edge<PgSqlTypeDependency>{&PgSqlTypeDependency::functions,
                              CascadeVerb::CascadeFunction},
    Edge<PgSqlTypeDependency>{&PgSqlTypeDependency::constraints,
                              CascadeVerb::CheckConstraint},
    Edge<PgSqlTypeDependency>{&PgSqlTypeDependency::column_defaults,
                              CascadeVerb::ColumnDefault},
  };
};

struct PgSqlFunctionDependency : DependencyMixin<PgSqlFunctionDependency> {
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> functions;
  containers::FlatHashSet<ObjectId> indexes;
  containers::FlatHashSet<ObjectId> constraints;
  containers::FlatHashSet<ObjectId> column_defaults;
  static constexpr std::array kEdges = {
    Edge<PgSqlFunctionDependency>{&PgSqlFunctionDependency::views,
                                  CascadeVerb::CascadeView},
    Edge<PgSqlFunctionDependency>{&PgSqlFunctionDependency::functions,
                                  CascadeVerb::CascadeFunction},
    Edge<PgSqlFunctionDependency>{&PgSqlFunctionDependency::indexes,
                                  CascadeVerb::CascadeIndex},
    Edge<PgSqlFunctionDependency>{&PgSqlFunctionDependency::constraints,
                                  CascadeVerb::CheckConstraint},
    Edge<PgSqlFunctionDependency>{&PgSqlFunctionDependency::column_defaults,
                                  CascadeVerb::ColumnDefault},
  };
};

struct IndexDependency : DependencyMixin<IndexDependency> {
  static constexpr std::array<Edge<IndexDependency>, 0> kEdges{};
};

struct SchemaDependency : DependencyMixin<SchemaDependency> {
  containers::FlatHashSet<ObjectId> tables;
  containers::FlatHashSet<ObjectId> functions;
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> tokenizers;
  containers::FlatHashSet<ObjectId> types;
  containers::FlatHashSet<ObjectId> sequences;
  bool Empty() const {
    return tables.empty() && functions.empty() && views.empty() &&
           tokenizers.empty() && types.empty() && sequences.empty();
  }
  static constexpr std::array kEdges = {
    Edge<SchemaDependency>{&SchemaDependency::tables, CascadeVerb::AutoDrop},
    Edge<SchemaDependency>{&SchemaDependency::views, CascadeVerb::AutoDrop},
    Edge<SchemaDependency>{&SchemaDependency::functions, CascadeVerb::AutoDrop},
    Edge<SchemaDependency>{&SchemaDependency::types, CascadeVerb::AutoDrop},
    Edge<SchemaDependency>{&SchemaDependency::sequences, CascadeVerb::AutoDrop},
    Edge<SchemaDependency>{&SchemaDependency::tokenizers,
                           CascadeVerb::AutoDrop},
  };
};

struct DatabaseDependency : DependencyMixin<DatabaseDependency> {
  containers::FlatHashSet<ObjectId> schemas;
  static constexpr std::array kEdges = {
    Edge<DatabaseDependency>{&DatabaseDependency::schemas,
                             CascadeVerb::AutoDrop},
  };
};

struct TokenizerDependency : DependencyMixin<TokenizerDependency> {
  containers::FlatHashSet<ObjectId> indexes;
  static constexpr std::array kEdges = {
    Edge<TokenizerDependency>{&TokenizerDependency::indexes,
                              CascadeVerb::CascadeIndex},
  };
};

struct RoleDependency : DependencyMixin<RoleDependency> {
  containers::FlatHashSet<ObjectId> referencing_objects;
  static constexpr std::array<Edge<RoleDependency>, 0> kEdges{};
};

inline DropPlan DropEmitter::ComputePlan() && {
  while (!_stack.empty()) {
    auto cur = _stack.back();
    _stack.pop_back();
    if (auto dep = _dep_lookup(cur)) {
      dep->Emit(*this, cur);
    }
  }

  // Auto drops run via the DropTask
  std::erase_if(_plan.view_drops,
                [&](const auto& p) { return _auto_drops.contains(p.second); });
  std::erase_if(_plan.function_drops,
                [&](const auto& p) { return _auto_drops.contains(p.second); });
  std::erase_if(_plan.index_drops,
                [&](auto id) { return _auto_drops.contains(id); });
  absl::erase_if(_plan.table_rewrites, [&](const auto& kv) {
    return _auto_drops.contains(kv.first);
  });
  return std::move(_plan);
}

class ObjectDependencies {
 public:
  std::shared_ptr<const ObjectDependencyBase> GetDependency(ObjectId id) const {
    auto it = _deps.find(id);
    SDB_ASSERT(it != _deps.end());
    return it->second;
  }

  std::shared_ptr<const ObjectDependencyBase> TryGetDependency(
    ObjectId id) const {
    auto it = _deps.find(id);
    return it == _deps.end() ? nullptr : it->second;
  }

  template<std::derived_from<ObjectDependencyBase> T>
  bool AddDependency(ObjectId id,
                     std::shared_ptr<T> dep = std::make_shared<T>()) {
    auto [_, inserted] = _deps.try_emplace(id, std::move(dep));
    return inserted;
  }

  void SetDependency(ObjectId id,
                     std::shared_ptr<const ObjectDependencyBase> dep) {
    _deps.insert_or_assign(id, std::move(dep));
  }

  void RemoveDependency(ObjectId id) { _deps.erase(id); }

 private:
  containers::FlatHashMap<ObjectId, std::shared_ptr<const ObjectDependencyBase>>
    _deps;
};

}  // namespace sdb::catalog
