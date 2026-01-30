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

#include <string>

#include "catalog/object.h"
#include "catalog/types.h"

namespace sdb::catalog {

struct IndexBaseOptions {
  ObjectId id;
  ObjectId relation_id;
  std::string name;
  IndexType type = IndexType::Secondary;
};

template<typename Impl>
struct IndexOptions {
  IndexBaseOptions base;
  Impl impl;
};

class Index : public SchemaObject {
 public:
  auto GetIndexType() const noexcept { return _type; }
  auto GetRelationId() const noexcept { return _relation_id; }

 protected:
  Index(IndexBaseOptions options, ObjectId database_id);

 private:
  ObjectId _relation_id;
  IndexType _type;
};

ResultOr<std::shared_ptr<Index>> CreateIndex(
  ObjectId database_id, IndexOptions<vpack::Slice> options);

ResultOr<std::shared_ptr<Index>> CreateIndex(const SchemaObject& relation,
                                             IndexBaseOptions options);

}  // namespace sdb::catalog

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<sdb::IndexType>(
  sdb::IndexType type) noexcept {
  switch (type) {
    case sdb::IndexType::Unknown:
      return "unknown";
    case sdb::IndexType::Primary:
      return "primary";
    case sdb::IndexType::Secondary:
      return "secondary";
    case sdb::IndexType::Edge:
      return "edge";
    case sdb::IndexType::NoAccess:
      return "noaccess";
    case sdb::IndexType::Inverted:
      return "inveted";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
