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

#include <vpack/serializer.h>

#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/types.h"

namespace sdb::catalog {
namespace {

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
    if (c.catalog_column->type->kind() != velox::TypeKind::VARCHAR &&
        !c.opclass.empty()) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has text dictionary defined but is not VARCHAR"};
    }
  }
  return {};
}

}  // namespace
namespace {

std::vector<Column::Id> ExtractColumnIds(
  std::span<const CreateIndexColumn> columns) {
  std::vector<Column::Id> ids;
  ids.reserve(columns.size());
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    ids.push_back(c.catalog_column->id);
  }
  return ids;
}

}  // namespace

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique) {
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    if (c.catalog_column->type->providesCustomComparison()) {
      return std::unexpected<Result>{
        std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
        " has type with custom comparison and can not be indexed"};
    }
    if (!c.catalog_column->type->isPrimitiveType()) {
      return std::unexpected<Result>{
        std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
        " has non primitive type and can not be indexed"};
    }
  }
  return std::make_shared<SecondaryIndex>(database_id, schema_id, id,
                                          relation_id, std::move(name),
                                          ExtractColumnIds(columns), unique);
}

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot) {
  auto column_validation_res = ValidateInvertedIndexColumns(columns);
  if (column_validation_res.fail()) {
    return std::unexpected<Result>(std::move(column_validation_res));
  }

  InvertedIndex::ColumnOptions inverted_columns;
  for (const auto& c : columns) {
    InvertedIndexColumnInfo index_col;
    if (!c.opclass.empty()) {
      auto object_name = pg::ParseObjectName(c.opclass, schema_name);
      if (object_name.schema != schema_name) {
        // Technically nothing prevents us from allowing so.
        // But that will make schema drop more complicated as we will need to
        // check if any dictionaries are used in the indexes from other
        // schemas and even fail schema drops on this case. For now if we
        // drop text dictionary as a child entity we can be sure that
        // indexes will also be dropped along with tables from same schema.
        return std::unexpected<Result>{
          std::in_place, ERROR_BAD_PARAMETER,
          "Accessing text dictionary from different schema is not supported"};
      }
      auto dict = snapshot->GetTokenizer(database_id, object_name.schema,
                                         object_name.relation);
      if (!dict) {
        return std::unexpected<Result>{std::in_place,
                                       ERROR_BAD_PARAMETER,
                                       "Text search dictionary '",
                                       c.opclass,
                                       "' does not exist.",
                                       " Required by column '",
                                       c.name,
                                       "'"};
      }
      index_col.text_dictionary = dict->GetId();
      index_col.features = dict->GetFeatures();
    }
    inverted_columns.emplace(c.catalog_column->id, std::move(index_col));
  }
  return std::make_shared<InvertedIndex>(
    database_id, schema_id, id, relation_id, std::move(name),
    ExtractColumnIds(columns), std::move(inverted_columns));
}

Index::Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
             ObjectId relation_id, std::string name,
             std::vector<Column::Id> column_ids, ObjectType type)
  : SchemaObject{{}, database_id, schema_id, id, std::move(name), type},
    _relation_id{relation_id},
    _column_ids{std::move(column_ids)} {
  SDB_ASSERT(GetId().isSet());
}

}  // namespace sdb::catalog
