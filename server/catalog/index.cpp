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

  inverted_options.base = std::move(options);

  return std::make_shared<InvertedIndex>(inverted_options);
}

Result ValidateInvertedIndexOptions(std::span<const Column*> indexed_columns) {
  for (auto c : indexed_columns) {
    if (c->type->providesCustomComparison()) {
      return {ERROR_BAD_PARAMETER, "Column ", c->name,
              " has type with custom comparison and can not be indexed."};
    }
    if (!c->type->isPrimitiveType()) {
      return {ERROR_BAD_PARAMETER, "Column ", c->name,
              " has non primitive type and can not be indexed."};
    }
    if (c->type->kind() == velox::TypeKind::TIMESTAMP ||
        c->type->kind() == velox::TypeKind::HUGEINT) {
      return {ERROR_BAD_PARAMETER, "Column ", c->name,
              " has unsupported kind and can not be indexed."};
    }
  }
  return {};
}

}  // namespace

Result ValidateIndexOptions(const IndexBaseOptions& options,
                            std::span<const Column*> indexed_columns) {
  switch (options.type) {
    case IndexType::Inverted:
      return ValidateInvertedIndexOptions(indexed_columns);
    default:
      // TODO implement necessary validation on create for other index type
      return {};
  }
}

ResultOr<std::shared_ptr<Index>> MakeIndex(IndexBaseOptions options) {
  switch (options.type) {
    case IndexType::Inverted:
      return CreateInvertedIndex(std::move(options));
    case IndexType::Secondary:
      return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED,
                                     "Secondary index is not implemented"};
    case IndexType::Unknown:
      SDB_UNREACHABLE();
  }
}

// NOLINTBEGIN
// View wrapper for IndexBaseOptions for light-weight serialization
struct Index::IndexOutput {
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId id;
  ObjectId relation_id;
  std::string_view name;
  IndexType type;
  std::span<const Column::Id> column_ids;
};
// NOLINTEND

Index::IndexOutput Index::MakeIndexOutput() const {
  return {
    .database_id = GetDatabaseId(),
    .schema_id = GetSchemaId(),
    .id = GetId(),
    .relation_id = GetRelationId(),
    .name = GetName(),
    .type = GetIndexType(),
    .column_ids = _column_ids,
  };
}

void Index::WriteInternal(vpack::Builder& builder) const {
  vpack::WriteTuple(builder, MakeIndexOutput());
}

Index::Index(IndexBaseOptions options)
  : SchemaObject{{},         options.database_id,     options.schema_id,
                 options.id, std::move(options.name), ObjectType::Index},
    _relation_id{options.relation_id},
    _type(options.type),
    _column_ids{std::move(options.column_ids)} {
  SDB_ASSERT(options.id.isSet());
}

}  // namespace sdb::catalog
