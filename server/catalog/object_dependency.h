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

#include <memory>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

struct ObjectDependencyBase {
  virtual ~ObjectDependencyBase() = default;
  virtual std::shared_ptr<ObjectDependencyBase> Clone() const = 0;
};

struct TableDependency : public ObjectDependencyBase {
  ObjectId shard_id;
  containers::FlatHashSet<ObjectId> indexes;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<TableDependency>(*this);
  }
};

struct IndexDependency : public ObjectDependencyBase {
  ObjectId shard_id;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<IndexDependency>(*this);
  }
};

struct SchemaDependency : public ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> tables;
  containers::FlatHashSet<ObjectId> functions;
  containers::FlatHashSet<ObjectId> views;
  containers::FlatHashSet<ObjectId> ts_dicts;
  bool Empty() const {
    return tables.empty() && functions.empty() && views.empty() &&
           ts_dicts.empty();
  }
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<SchemaDependency>(*this);
  }
};

struct DatabaseDependency : public ObjectDependencyBase {
  containers::FlatHashSet<ObjectId> schemas;
  std::shared_ptr<ObjectDependencyBase> Clone() const final {
    return std::make_shared<DatabaseDependency>(*this);
  }
};

}  // namespace sdb::catalog
