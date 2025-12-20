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

namespace sdb::catalog {

enum class IndexType : uint8_t {
  Secondary = 0,
  Inverted,
};

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
constexpr customize::customize_t customize::enum_name<sdb::catalog::IndexType>(
  sdb::catalog::IndexType type) noexcept {
  switch (type) {
    case sdb::catalog::IndexType::Secondary:
      return "secondary";
    case sdb::catalog::IndexType::Inverted:
      return "gin";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
