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

#include <duckdb/common/types/value.hpp>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "basics/assert.h"
#include "catalog/object.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"

namespace sdb {

class IndexShard;
struct IndexShardOptions;

namespace catalog {

class SecondaryIndex;
class InvertedIndex;

struct ColumnRefData {
  const catalog::Column* catalog_column{nullptr};
  std::string_view name;
};

struct IndexedExpressionData {
  std::string serialized;
  std::string pretty_printed;
  std::vector<Column::Id> dependent_columns;
};

struct CreateIndexColumn {
  std::variant<ColumnRefData, IndexedExpressionData> data;
  std::string opclass;
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;

  bool IsIndexedExpression() const noexcept {
    return std::holds_alternative<IndexedExpressionData>(data);
  }

  const IndexedExpressionData& GetIndexedExpression() const {
    SDB_ASSERT(IsIndexedExpression());
    return std::get<IndexedExpressionData>(data);
  }

  const catalog::Column* GetCatalogColumn() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return std::get<ColumnRefData>(data).catalog_column;
  }

  void SetCatalogColumn(const catalog::Column* col) noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    std::get<ColumnRefData>(data).catalog_column = col;
  }

  std::string_view ColumnName() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return std::get<ColumnRefData>(data).name;
  }
};

class Index : public SchemaObject {
 public:
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
  Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
        ObjectId relation_id, std::string name,
        std::vector<Column::Id> column_ids, ObjectType type);

  ObjectId _relation_id;
  std::vector<Column::Id> _column_ids;
};

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique);

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  std::optional<ScorerOptions> wand_scorer);

}  // namespace catalog
}  // namespace sdb
