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
#include "catalog/table_options.h"
#include "catalog/types.h"

namespace sdb {

class IndexShard;
struct IndexShardOptions;

namespace catalog {

inline constexpr std::string_view kIndexBaseOptions = "base";
inline constexpr std::string_view kIndexImplOptions = "impl";

// Aggregated info about column for index creation.
// Filled on different levels during creaton to gather all
// necessary info for building and validating new index.
struct CreateIndexColumn {
  const catalog::Column* catalog_column{nullptr};
  std::string_view name;
  std::string opclass;
  // TODO(Dronplane): parse opclass options. Passing List* down to concrete
  // index might be an option but that will leak SQL parsing too deep. On the
  // other hand if we just parse to some generic map of strings it is unclear
  // how to implement "help".
};

struct IndexBaseOptions {
  std::string name;
  IndexType type = IndexType::Unknown;
  std::vector<Column::Id> column_ids;
};

// polymorfic wrapper for concrete index wrappers
// so we can make generic parsing/creating code.
struct IndexImplOptionsBaseWrapper {
  IndexImplOptionsBaseWrapper(IndexBaseOptions&& options) : base{options} {}
  virtual ~IndexImplOptionsBaseWrapper() = default;

  IndexBaseOptions base;
};

using ImplOptsPtr = std::unique_ptr<IndexImplOptionsBaseWrapper>;

class Index : public SchemaObject {
 public:
  auto GetIndexType() const noexcept { return _type; }
  auto GetRelationId() const noexcept { return _relation_id; }
  std::span<const Column::Id> GetColumnIds() const noexcept {
    return _column_ids;
  }

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

  // TODO(codeworse): support arguments for index shards
  virtual ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions& options) const = 0;

  virtual ~Index() = default;

 protected:
  void WriteInternalImpl(vpack::Builder& builder,
                         absl::FunctionRef<void()> impl_write) const;

  struct IndexOutput;
  IndexOutput MakeIndexOutput() const;

  Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
        ObjectId relation_id, IndexBaseOptions options);

  ObjectId _relation_id;
  IndexType _type;
  std::vector<Column::Id> _column_ids;
};

ResultOr<ImplOptsPtr> ParseImplSlice(IndexBaseOptions&& options,
                                     vpack::Slice impl_options_slice);

ResultOr<std::shared_ptr<Index>> MakeIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  IndexImplOptionsBaseWrapper&& impl_options);

ResultOr<std::shared_ptr<Index>> MakeIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, IndexBaseOptions options,
  std::vector<catalog::CreateIndexColumn> columns);

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
