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

#include <concepts>
#include <memory>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

struct ObjectDependencyBase {
  virtual ~ObjectDependencyBase() = default;
  virtual std::shared_ptr<ObjectDependencyBase> Clone() const = 0;
};

struct RelationDependency : ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> indexes;
};

struct TableDependency : RelationDependency {
  ObjectId shard_id;
  containers::FlatHashSet<ObjectId> owned_sequences;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<TableDependency>(*this);
  }
};

struct ViewDependency : RelationDependency {
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<ViewDependency>(*this);
  }
};

struct IndexDependency : ObjectDependencyBase {
  ObjectId shard_id;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<IndexDependency>(*this);
  }
};

struct SchemaDependency : ObjectDependencyBase {
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
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<SchemaDependency>(*this);
  }
};

struct DatabaseDependency : ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> schemas;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<DatabaseDependency>(*this);
  }
};

struct TokenizerDependency : ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> indexes;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<TokenizerDependency>(*this);
  }
};

class ObjectDependencies {
 public:
  std::shared_ptr<const ObjectDependencyBase> GetDependency(ObjectId id) const {
    auto it = _object_dependencies.find(id);
    SDB_ASSERT(it != _object_dependencies.end());
    return it->second;
  }

  template<std::derived_from<ObjectDependencyBase> T>
  bool AddDependency(ObjectId id,
                     std::shared_ptr<T> dep = std::make_shared<T>()) {
    auto [_, inserted] = _object_dependencies.insert_or_assign(id, dep);
    return inserted;
  }

  void RemoveDependency(ObjectId id) { _object_dependencies.erase(id); }

 private:
  containers::FlatHashMap<ObjectId, std::shared_ptr<const ObjectDependencyBase>>
    _object_dependencies;
};

}  // namespace sdb::catalog
