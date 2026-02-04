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

namespace sdb {

class IndexShard;

namespace catalog {

struct IndexBaseOptions {
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId id;
  ObjectId relation_id;
  std::string name;
  IndexType type = IndexType::Unknown;
  std::vector<uint16_t> column_ids;
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
  std::span<const uint16_t> GetColumnIds() const noexcept {
    return _column_ids;
  }
  void WriteInternal(vpack::Builder& builder) const override;

  virtual ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, vpack::Slice args) const = 0;

  virtual ~Index() = default;

 protected:
  Index(IndexBaseOptions options);

  ObjectId _relation_id;
  IndexType _type;
  std::vector<uint16_t> _column_ids;
};

ResultOr<std::shared_ptr<Index>> MakeIndex(IndexBaseOptions options);

}  // namespace catalog

}  // namespace sdb

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<sdb::IndexType>(
  sdb::IndexType type) noexcept {
  switch (type) {
    case sdb::IndexType::Unknown:
      return "unknown";
    case sdb::IndexType::Secondary:
      return "secondary";
    case sdb::IndexType::Inverted:
      return "inverted";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
