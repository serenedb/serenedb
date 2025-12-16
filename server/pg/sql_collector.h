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

#include <string_view>
#include <type_traits>

#include "basics/containers/flat_hash_map.h"
#include "basics/memory.hpp"
#include "catalog/object.h"
#include "pg/pg_types.h"

struct RawStmt;
struct List;
struct Node;

namespace sdb::pg {

class Objects : public irs::memory::Managed {
 public:
  enum class AccessType : uint64_t {
    None = 0,
    Read = uint64_t{1} << 0,
    Insert = uint64_t{1} << 1,
    Delete = uint64_t{1} << 2,
    Update = uint64_t{1} << 3,
    Merge = uint64_t{1} << 4,
  };

  struct ObjectName {
    std::string_view schema;  // null => current search path
    std::string_view relation;

    std::string FullName() const {
      if (schema.empty()) {
        return std::string{relation};
      }
      return absl::StrCat(schema, ".", relation);
    }

    bool operator==(const ObjectName& object) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const ObjectName& object) {
      return H::combine(std::move(h), object.schema, object.relation);
    }
  };

  struct ObjectData {
    AccessType type = AccessType::None;
    std::shared_ptr<catalog::SchemaObject> object;
  };

  using Map = containers::FlatHashMap<ObjectName, ObjectData>;

  template<typename S>
  ObjectData& ensureData(S s, std::string_view relation) {
    auto schema = ensureNotNull(s);
    return _objects[ObjectName{schema, relation}];
  }

  template<typename S>
  const ObjectData* getData(S s, std::string_view relation) const noexcept {
    auto schema = ensureNotNull(s);
    auto it = _objects.find(ObjectName{schema, relation});
    return it != _objects.end() ? &it->second : nullptr;
  }

  auto& getObjects(this auto& self) noexcept { return self._objects; }

 private:
  template<typename T>
  static std::string_view ensureNotNull(T t) noexcept {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return t;
    } else {
      static_assert(std::is_same_v<T, char*>);
      return absl::NullSafeStringView(t);
    }
  }

  containers::FlatHashMap<ObjectName, ObjectData> _objects;
};

// collect objects to objects
void Collect(std::string_view database, const RawStmt& node, Objects& objects);

// collect objects to objects
void CollectExpr(std::string_view database, const Node& expr, Objects& objects);

// collect objects to objects and track max binding param index
void Collect(std::string_view database, const RawStmt& node, Objects& objects,
             pg::ParamIndex& max_bind_param_idx);

Objects::ObjectName ParseObjectName(const List* names,
                                    std::string_view database,
                                    std::string_view default_schema = {});

}  // namespace sdb::pg
