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

#include "catalog/index.h"

#include "basics/errors.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/types.h"
#include "vpack/serializer.h"

namespace sdb::catalog {
namespace {

ResultOr<std::shared_ptr<catalog::Index>> CreateInvertedIndex(
  catalog::IndexBaseOptions options) {
  catalog::IndexOptions<InvertedIndexOptions> inverted_options;

  // Only generate a new ID if one isn't already set (i.e., when creating a new
  // index, not when loading an existing one from storage)
  if (!options.id.isSet()) {
    options.id = catalog::NextId();
  }
  ObjectId database_id = options.database_id;
  inverted_options.base = std::move(options);

  return std::make_shared<InvertedIndex>(inverted_options, database_id);
}

}  // namespace

ResultOr<std::shared_ptr<Index>> CreateIndex(
  catalog::IndexBaseOptions options) {
  switch (options.type) {
    case IndexType::Inverted:
      return CreateInvertedIndex(std::move(options));
    case IndexType::Primary:
    case IndexType::Secondary:
    case IndexType::NoAccess:
      return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED};
    case IndexType::Unknown:
      SDB_UNREACHABLE();
  }
}

void Index::WriteInternal(vpack::Builder& builder) const {
  IndexBaseOptions options{
    .database_id = GetDatabaseId(),
    .id = GetId(),
    .relation_id = GetRelationId(),
    .name = std::string{GetName()},
    .type = GetIndexType(),
    .column_ids = std::vector<uint16_t>{_column_ids.begin(), _column_ids.end()},
  };

  vpack::WriteTuple(builder, options);
}

Index::Index(IndexBaseOptions options, ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 options.id,
                 std::move(options.name),
                 ObjectType::Index},
    _relation_id{options.relation_id},
    _type(options.type),
    _column_ids{std::move(options.column_ids)} {}

}  // namespace sdb::catalog
