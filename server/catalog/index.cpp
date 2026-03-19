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
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/types.h"
#include "vpack/serializer.h"

namespace sdb::catalog {
namespace {

ResultOr<std::shared_ptr<catalog::Index>> CreateInvertedIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  InvertedIndexOptions&& options) {
  return std::make_shared<InvertedIndex>(database_id, schema_id, id,
                                         relation_id, options);
}

Result ValidateInvertedIndexColumns(
  std::span<CreateIndexColumn> indexed_columns) {
  for (auto c : indexed_columns) {
    SDB_ASSERT(c.catalog_column);
    if (c.catalog_column->type->providesCustomComparison()) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has type with custom comparison and can not be indexed"};
    }
    if (!c.catalog_column->type->isPrimitiveType()) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has non primitive type and can not be indexed"};
    }
    if (c.catalog_column->type->kind() == velox::TypeKind::TIMESTAMP ||
        c.catalog_column->type->kind() == velox::TypeKind::HUGEINT) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has unsupported kind and can not be indexed"};
    }
    // TODO(Dronplane): Remove when we have default text dictionary
    if (c.catalog_column->type->kind() == velox::TypeKind::VARCHAR &&
        c.opclass.empty()) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " is VARCHAR but has no text dictionary defined"};
    } else if (c.catalog_column->type->kind() != velox::TypeKind::VARCHAR &&
               !c.opclass.empty()) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has text dictionary defined but is not VARCHAR"};
    }
  }
  return {};
}

}  // namespace

ResultOr<std::shared_ptr<Index>> MakeIndex(ObjectId database_id,
                                           ObjectId schema_id, ObjectId id,
                                           ObjectId relation_id,
                                           IndexBaseOptions options,
                                           vpack::Slice impl_options_slice) {
  switch (options.type) {
    case IndexType::Inverted: {
      InvertedIndexOptions impl_options;
      impl_options.base = std::move(options);
      if (auto r =
            vpack::ReadTupleNothrow(impl_options_slice, impl_options.impl);
          !r.ok()) {
        return std::unexpected<Result>{std::move(r)};
      }
      return CreateInvertedIndex(database_id, schema_id, id, relation_id,
                                 std::move(impl_options));
    }
    case IndexType::Secondary:
      return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED,
                                     "Secondary index is not implemented"};
    case IndexType::Unknown:
      SDB_UNREACHABLE();
  }
}

ResultOr<std::shared_ptr<Index>> MakeIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, IndexBaseOptions options,
  std::vector<catalog::CreateIndexColumn> columns) {
  switch (options.type) {
    case IndexType::Inverted: {
      auto column_validation_res = ValidateInvertedIndexColumns(columns);
      if (column_validation_res.fail()) {
        return std::unexpected<Result>(std::move(column_validation_res));
      }

      InvertedIndexOptions impl_options;
      impl_options.base = std::move(options);
      auto snapshot = catalog::GetCatalog().GetSnapshot();

      for (const auto& c : columns) {
        InvertedIndexColumnInfo index_col;
        if (!c.opclass.empty()) {
          auto object_name = pg::ParseObjectName(c.opclass, schema_name);
          if (object_name.schema != schema_name) {
            // Technically nothing prevents us from allowing so.
            // But that will make shema drop more complicated as we will need to
            // check if any dictionaries are used in the indexes from other
            // schemas and even fail schema drops on this case. For now if we
            // drop text dictionary as a child entity we can be sure that
            // indexes will also be dropped along with tables from same schema.
            return std::unexpected<Result>{
              std::in_place, ERROR_BAD_PARAMETER,
              "Accessing text dictionary from different schema is not "
              "supported"};
          }
          auto dict = snapshot->GetTokenizer(database_id, object_name.schema,
                                             object_name.relation);
          if (!dict) {
            return std::unexpected<Result>{
              std::in_place,
              ERROR_BAD_PARAMETER,
              "Text search dictionary '",
              c.opclass,
              "' does not exist. Required by column '",
              c.name,
              "'"};
          }
          index_col.text_dictionary = dict->GetId();
          index_col.features = dict->GetFeatures();
        }
        impl_options.impl.columns.emplace(c.catalog_column->id,
                                          std::move(index_col));
      }
      return CreateInvertedIndex(database_id, schema_id, id, relation_id,
                                 std::move(impl_options));
    }
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
  std::string_view name;
  IndexType type;
  std::span<const Column::Id> column_ids;
};
// NOLINTEND

Index::IndexOutput Index::MakeIndexOutput() const {
  return {
    .name = GetName(),
    .type = GetIndexType(),
    .column_ids = _column_ids,
  };
}

void Index::WriteInternal(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());
  builder.add(kIndexBaseOptions);
  vpack::WriteTuple(builder, MakeIndexOutput());
}

Index::Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
             ObjectId relation_id, IndexBaseOptions options)
  : SchemaObject{{},
                 database_id,
                 schema_id,
                 id,
                 std::move(options.name),
                 ObjectType::Index},
    _relation_id{relation_id},
    _type(options.type),
    _column_ids{std::move(options.column_ids)} {
  SDB_ASSERT(GetId().isSet());
}

}  // namespace sdb::catalog
